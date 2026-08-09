"""
Etapa 2 - Construção da camada trusted em Parquet.


Responsabilidades:
1. Ler o arquivo bruto da camada raw.
2. Aplicar transformações mínimas e controladas.
3. Validar campos obrigatórios.
4. Separar registros válidos e rejeitados.
5. Salvar a camada trusted em Parquet.
6. Salvar metadados de execução.

"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
TRUSTED_DIR = PROJECT_ROOT / "data" / "trusted"
DEFAULT_RAW_CSV = RAW_DIR / "online_retail.csv"

EXPECTED_RAW_COLUMNS = {
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
}

COLUMN_MAPPING = {
    "InvoiceNo": "invoice_no",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "UnitPrice": "unit_price",
    "CustomerID": "customer_id",
    "Country": "country",
}

STRING_COLUMNS = [
    "invoice_no",
    "stock_code",
    "description",
    "country",
]

logger = logging.getLogger("build_trusted")


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
    Cria os argumentos aceitos pelo script.
    """
    parser = argparse.ArgumentParser(
        description="Constrói a camada trusted em Parquet a partir da camada raw."
    )

    parser.add_argument(
        "--raw",
        type=Path,
        default=DEFAULT_RAW_CSV,
        help="Caminho do arquivo CSV da camada raw.",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=TRUSTED_DIR,
        help="Diretório onde os arquivos trusted serão salvos.",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Força a execução mesmo que os arquivos trusted já existam para o mesmo raw.",
    )

    return parser


def calculate_sha256(path: Path) -> str:
    """
    Calcula o hash SHA-256 do arquivo.
    """
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def load_raw_metadata(raw_path: Path):
    """
    Tenta carregar o metadado da camada raw gerado na Etapa 1.
    """
    candidate = raw_path.with_name(raw_path.stem + ".metadata.json")

    if not candidate.exists():
        return None, None

    try:
        payload = json.loads(candidate.read_text(encoding="utf-8"))
        return payload, candidate
    except json.JSONDecodeError:
        logger.warning("Metadado raw inválido em: %s", candidate)
        return None, candidate


def read_raw_csv(raw_path: Path) -> pd.DataFrame:
    """
    Lê o CSV bruto preservando colunas como string.

    Primeiro tenta UTF-8. Se falhar, tenta latin-1.
    """
    try:
        return pd.read_csv(
            raw_path,
            dtype=str,
            encoding="utf-8",
            low_memory=False,
        )
    except UnicodeDecodeError:
        logger.info("Falha ao ler como UTF-8. Tentando latin-1.")
        return pd.read_csv(
            raw_path,
            dtype=str,
            encoding="latin-1",
            low_memory=False,
        )


def validate_columns(df: pd.DataFrame) -> None:
    """
    Valida se o arquivo raw possui as colunas esperadas.
    """
    missing = EXPECTED_RAW_COLUMNS - set(df.columns)

    if missing:
        raise ValueError(
            "O arquivo raw não possui as colunas esperadas. "
            f"Colunas ausentes: {sorted(missing)}. "
            f"Colunas encontradas: {list(df.columns)}"
        )


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformações mínimas e controladas.


    """
    df = df.rename(columns=COLUMN_MAPPING)

    # Mantém apenas as colunas esperadas no dataset base.
    df = df[list(COLUMN_MAPPING.values())].copy()

    # Tratamento de campos textuais.
    for column in STRING_COLUMNS:
        df[column] = df[column].astype("string")
        df[column] = df[column].str.strip()

        # Converte strings vazias em NA.
        empty_mask = df[column].str.len().fillna(0).eq(0)
        df[column] = df[column].mask(empty_mask, pd.NA)

    # Normaliza campos de código.
    df["invoice_no"] = df["invoice_no"].str.upper()
    df["stock_code"] = df["stock_code"].str.upper()

    # Conversão de tipos numéricos.
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").astype("Float64")
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")

    # Conversão de data.
    # O dataset costuma usar o formato dd/mm/yyyy hh:mm.
    # Usamos format="mixed" para ser mais tolerante a pequenas variações.
    df["invoice_date"] = pd.to_datetime(
        df["invoice_date"],
        format="mixed",
        dayfirst=True,
        errors="coerce",
    )

    # Colunas derivadas simples.
    df["line_total"] = (
        df["quantity"].astype("Float64") * df["unit_price"]
    ).round(2)

    df["is_cancellation"] = (
        df["invoice_no"]
        .fillna("")
        .str.upper()
        .str.startswith("C")
    )

    return df


def add_invalid_reasons(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona a coluna _invalid_reasons com os problemas encontrados.

    Nesta etapa, consideramos obrigatórios:
    - invoice_no
    - stock_code
    - quantity
    - unit_price
    - invoice_date
    - country

    CustomerID e Description não são obrigatórios agora.
    """
    df = df.copy()

    reasons = pd.Series("", index=df.index, dtype="object")

    checks: Dict[str, pd.Series] = {
        "missing_or_invalid_invoice_no": df["invoice_no"].isna(),
        "missing_or_invalid_stock_code": df["stock_code"].isna(),
        "missing_or_invalid_quantity": df["quantity"].isna(),
        "missing_or_invalid_unit_price": df["unit_price"].isna(),
        "missing_or_invalid_invoice_date": df["invoice_date"].isna(),
        "missing_or_invalid_country": df["country"].isna(),
    }

    for reason, mask in checks.items():
        reasons = reasons + np.where(mask.to_numpy(dtype=bool), reason + ";", "")

    df["_invalid_reasons"] = reasons.str.rstrip(";").fillna("")

    return df


def split_valid_and_rejected(df: pd.DataFrame):
    """
    Separa registros válidos e rejeitados.
    """
    invalid_mask = df["_invalid_reasons"] != ""

    valid_df = df.loc[~invalid_mask].drop(columns=["_invalid_reasons"]).copy()
    rejected_df = df.loc[invalid_mask].copy()

    return valid_df, rejected_df


def write_parquet_atomically(df: pd.DataFrame, destination: Path) -> None:
    """
    Escreve Parquet de forma atômica.
    """
    temp_path = destination.with_name(destination.name + ".tmp")
    df.to_parquet(
        temp_path,
        index=False,
        compression="snappy",
    )
    temp_path.replace(destination)


def write_json_atomically(payload: Dict[str, Any], destination: Path) -> None:
    """
    Escreve JSON de forma atômica.
    """
    temp_path = destination.with_name(destination.name + ".tmp")
    temp_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    temp_path.replace(destination)


def build_metadata(
    raw_path: Path,
    raw_sha256: str,
    raw_metadata: Optional[Dict[str, Any]],
    raw_metadata_path: Optional[Path],
    output_parquet: Path,
    rejected_parquet: Path,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Constrói o metadado da camada trusted.
    """
    reason_counts: Dict[str, int] = {}

    if not rejected_df.empty:
        exploded_reasons = (
            rejected_df["_invalid_reasons"]
            .str.split(";")
            .explode()
            .replace("", pd.NA)
            .dropna()
        )

        reason_counts = {
            str(key): int(value)
            for key, value in exploded_reasons.value_counts().items()
        }

    return {
        "dataset": "online_retail",
        "stage": "trusted",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "raw_path": str(raw_path),
        "raw_sha256": raw_sha256,
        "raw_metadata_path": str(raw_metadata_path) if raw_metadata_path else None,
        "raw_metadata_source_sha256": raw_metadata.get("source_sha256") if raw_metadata else None,
        "raw_metadata_row_count": raw_metadata.get("row_count") if raw_metadata else None,
        "output_parquet": str(output_parquet),
        "rejected_parquet": str(rejected_parquet),
        "row_count_read": int(len(valid_df) + len(rejected_df)),
        "row_count_valid": int(len(valid_df)),
        "row_count_rejected": int(len(rejected_df)),
        "invalid_reason_counts": reason_counts,
        "valid_columns": list(valid_df.columns),
        "transformations": [
            "Renomeação de colunas para snake_case.",
            "Trim em campos textuais.",
            "Conversão de strings vazias em NA.",
            "Normalização de invoice_no e stock_code para maiúsculas.",
            "Conversão de quantity para Int64.",
            "Conversão de unit_price para Float64.",
            "Conversão de customer_id para Int64.",
            "Conversão de invoice_date para timestamp.",
            "Criação de line_total = quantity * unit_price.",
            "Criação de is_cancellation baseada em invoice_no iniciando com C.",
            "Separação de registros inválidos para auditoria.",
        ],
    }


def main() -> int:
    setup_logging()
    args = build_arg_parser().parse_args()

    raw_path = args.raw.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()

    if not raw_path.exists():
        logger.error(
            "Arquivo raw não encontrado: %s. "
            "Execute a Etapa 1 antes de rodar a Etapa 2.",
            raw_path,
        )
        return 1

    output_parquet = output_dir / "online_retail.parquet"
    rejected_parquet = output_dir / "online_retail_rejected.parquet"
    output_metadata = output_dir / "online_retail.trusted.metadata.json"

    logger.info("Calculando hash SHA-256 do arquivo raw...")
    raw_sha256 = calculate_sha256(raw_path)

    existing_metadata = None
    if output_metadata.exists():
        try:
            existing_metadata = json.loads(output_metadata.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("Metadado trusted existente inválido: %s", output_metadata)

    if (
        output_parquet.exists()
        and rejected_parquet.exists()
        and existing_metadata is not None
        and existing_metadata.get("raw_sha256") == raw_sha256
        and not args.force
    ):
        logger.info(
            "Nada a fazer. Os arquivos trusted já foram gerados para este raw."
        )
        return 0

    try:
        logger.info("Lendo arquivo raw: %s", raw_path)
        raw_df = read_raw_csv(raw_path)

        logger.info("Validando colunas do arquivo raw...")
        validate_columns(raw_df)

        logger.info("Aplicando transformações mínimas...")
        cleaned_df = clean_dataframe(raw_df)

        logger.info("Adicionando motivos de invalidade...")
        cleaned_df = add_invalid_reasons(cleaned_df)

        logger.info("Separando registros válidos e rejeitados...")
        valid_df, rejected_df = split_valid_and_rejected(cleaned_df)

    except Exception:
        logger.exception("Erro ao processar o arquivo raw.")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Escrevendo registros válidos em: %s", output_parquet)
    write_parquet_atomically(valid_df, output_parquet)

    logger.info("Escrevendo registros rejeitados em: %s", rejected_parquet)
    write_parquet_atomically(rejected_df, rejected_parquet)

    raw_metadata, raw_metadata_path = load_raw_metadata(raw_path)

    metadata = build_metadata(
        raw_path=raw_path,
        raw_sha256=raw_sha256,
        raw_metadata=raw_metadata,
        raw_metadata_path=raw_metadata_path,
        output_parquet=output_parquet,
        rejected_parquet=rejected_parquet,
        valid_df=valid_df,
        rejected_df=rejected_df,
    )

    logger.info("Escrevendo metadados em: %s", output_metadata)
    write_json_atomically(metadata, output_metadata)

    logger.info("Etapa 2 concluída com sucesso.")
    logger.info("Registros lidos: %s", metadata["row_count_read"])
    logger.info("Registros válidos: %s", metadata["row_count_valid"])
    logger.info("Registros rejeitados: %s", metadata["row_count_rejected"])

    if metadata["invalid_reason_counts"]:
        logger.info("Motivos de rejeição: %s", metadata["invalid_reason_counts"])

    return 0


if __name__ == "__main__":
    sys.exit(main())