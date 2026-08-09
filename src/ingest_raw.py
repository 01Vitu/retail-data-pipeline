"""
Ingestão bruta do dataset Online Retail / E-Commerce Data.


Responsabilidades:
1. Ler um arquivo CSV 
2. Validar se o cabeçalho esperado está presente.
3. Contar a quantidade aproximada de registros.
4. Calcular o hash SHA-256 do arquivo de origem.
5. Copiar o arquivo para a camada raw sem transformar os dados.
6. Gerar um arquivo de metadados com informações básicas da ingestão.


"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_INPUT = PROJECT_ROOT / "data" / "external" / "data.csv"

EXPECTED_COLUMNS = {
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
}

logger = logging.getLogger("ingest_raw")


def setup_logging() -> None:
    """
    Configura o formato básico dos logs.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Cria os argumentos aceitos.
    """
    parser = argparse.ArgumentParser(
        description="Ingestão bruta do dataset de vendas Online Retail."
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=(
            "Caminho do arquivo CSV baixado do Kaggle. "
            "Padrão: data/external/E-Commerce Data.csv"
        ),
    )

    parser.add_argument(
        "--output-name",
        default="online_retail.csv",
        help="Nome do arquivo bruto que será salvo em data/raw.",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Força a ingestão mesmo que o arquivo já tenha sido ingerido anteriormente.",
    )

    return parser


def normalize_header(columns: List[str]) -> List[str]:
    """
    Remove espaços nas bordas e eventuais caracteres de BOM.
    """
    return [column.strip().lstrip("\ufeff") for column in columns if column is not None]


def validate_header(input_path: Path) -> List[str]:
    """
    Valida se o CSV possui as colunas esperadas.

    Retorna o cabeçalho normalizado.
    """
    with input_path.open(
        "r",
        encoding="utf-8-sig",
        errors="replace",
        newline="",
    ) as file:
        reader = csv.reader(file)
        header = next(reader, [])

    if not header:
        raise ValueError("O arquivo CSV não possui cabeçalho.")

    normalized_header = normalize_header(header)

    missing_columns = EXPECTED_COLUMNS - set(normalized_header)

    if missing_columns:
        raise ValueError(
            "O arquivo não possui as colunas obrigatórias. "
            f"Colunas ausentes: {sorted(missing_columns)}. "
            f"Cabeçalho encontrado: {normalized_header}"
        )

    return normalized_header


def count_rows(input_path: Path) -> int:
    """
    Conta quantas linhas de dados existem após o cabeçalho.


    """
    with input_path.open(
        "r",
        encoding="utf-8-sig",
        errors="replace",
        newline="",
    ) as file:
        reader = csv.reader(file)
        next(reader, None)  # pula cabeçalho
        return sum(1 for row in reader if row)


def calculate_sha256(path: Path) -> str:
    """
    Calcula o hash SHA-256 do arquivo.

    Isso ajuda a garantir rastreabilidade e detectar mudanças no arquivo original.
    """
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def load_existing_metadata(metadata_path: Path) -> Optional[dict]:
    """
    Carrega metadados já existentes, se houver.
    """
    if not metadata_path.exists():
        return None

    try:
        return json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logger.warning("Metadados existentes estão inválidos: %s", metadata_path)
        return None


def write_json_atomically(payload: dict, destination: Path) -> None:
    """
    Escreve um arquivo JSON de forma atômica.

    Primeiro escreve em um arquivo temporário e depois substitui o destino.
    """
    temp_path = destination.with_name(destination.name + ".tmp")
    temp_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    temp_path.replace(destination)


def copy_file_atomically(source: Path, destination: Path) -> None:
    """
    Copia o arquivo de forma atômica.

    Primeiro copia para um arquivo temporário e depois substitui o destino.
    """
    temp_path = destination.with_name(destination.name + ".tmp")
    shutil.copy2(source, temp_path)
    temp_path.replace(destination)


def main() -> int:
    setup_logging()
    args = build_arg_parser().parse_args()

    input_path = args.input.expanduser().resolve()

    if not input_path.exists():
        logger.error(
            "Arquivo de entrada não encontrado: %s. "
            "Baixe o dataset do Kaggle e coloque o CSV em data/external, "
            "ou informe outro caminho com --input.",
            input_path,
        )
        return 1

    try:
        header = validate_header(input_path)
    except ValueError as exc:
        logger.error("Falha na validação do arquivo: %s", exc)
        return 1

    output_path = RAW_DIR / args.output_name
    metadata_path = output_path.with_name(output_path.stem + ".metadata.json")

    logger.info("Calculando hash SHA-256 do arquivo de origem...")
    source_sha256 = calculate_sha256(input_path)

    existing_metadata = load_existing_metadata(metadata_path)

    if (
        output_path.exists()
        and existing_metadata is not None
        and existing_metadata.get("source_sha256") == source_sha256
        and not args.force
    ):
        logger.info(
            "Nada a fazer. O arquivo %s já foi ingerido com o mesmo hash.",
            output_path,
        )
        return 0

    logger.info("Contando linhas do arquivo de origem...")
    try:
        row_count = count_rows(input_path)
    except csv.Error as exc:
        logger.error("Erro ao ler o arquivo CSV: %s", exc)
        return 1

    if row_count == 0:
        logger.error("O arquivo possui cabeçalho, mas não possui registros.")
        return 1

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Copiando arquivo bruto para: %s", output_path)
    copy_file_atomically(input_path, output_path)

    metadata = {
        "dataset": "online_retail",
        "source": "Kaggle: carrie1/ecommerce-data",
        "source_file": input_path.name,
        "source_path": str(input_path),
        "source_sha256": source_sha256,
        "raw_path": str(output_path),
        "row_count": row_count,
        "columns": header,
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "stage": "raw",
        "notes": "Cópia bruta dos dados, sem limpeza ou transformação.",
    }

    logger.info("Escrevendo metadados em: %s", metadata_path)
    write_json_atomically(metadata, metadata_path)

    logger.info("Ingestão bruta concluída com sucesso.")
    logger.info("Total aproximado de registros: %s", row_count)

    return 0


if __name__ == "__main__":
    sys.exit(main())