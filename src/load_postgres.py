"""
Etapa 3 - Carga da camada trusted no PostgreSQL.

Responsabilidades:
1. Ler o arquivo Parquet da camada trusted.
2. Conectar ao PostgreSQL usando variáveis de ambiente.
3. Criar o schema staging e a tabela staging.online_retail, se necessário.
4. Opcionalmente truncar a tabela para recarga idempotente.
5. Inserir os dados no PostgreSQL.
6. Registrar informações de carga, como batch_id.

"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PARQUET = PROJECT_ROOT / "data" / "trusted" / "online_retail.parquet"
ENV_FILE = PROJECT_ROOT / ".env"

EXPECTED_COLUMNS: List[str] = [
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_date",
    "unit_price",
    "customer_id",
    "country",
    "line_total",
    "is_cancellation",
]

TARGET_COLUMNS: List[str] = [
    "batch_id",
    "loaded_at_utc",
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_date",
    "unit_price",
    "customer_id",
    "country",
    "line_total",
    "is_cancellation",
]

logger = logging.getLogger("load_postgres")


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
        description="Carrega a camada trusted em Parquet para o PostgreSQL."
    )

    parser.add_argument(
        "--parquet",
        type=Path,
        default=DEFAULT_PARQUET,
        help="Caminho do arquivo Parquet da camada trusted.",
    )

    parser.add_argument(
        "--no-truncate",
        dest="truncate",
        action="store_false",
        help="Não trunca a tabela antes da carga. Padrão: truncar.",
    )
    parser.set_defaults(truncate=True)

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limita a quantidade de registros carregados. Útil para testes.",
    )

    return parser


def get_env(name: str, default: Optional[str] = None, required: bool = True) -> str:
    """
    Lê uma variável de ambiente.
    """
    value = os.getenv(name, default)

    if required and (value is None or value == ""):
        raise RuntimeError(f"Variável de ambiente obrigatória ausente: {name}")

    return value


def create_sqlalchemy_engine():
    """
    Cria a conexão SQLAlchemy com o PostgreSQL.
    """
    host = get_env("POSTGRES_HOST")
    port_str = get_env("POSTGRES_PORT", default="5432")
    database = get_env("POSTGRES_DB")
    user = get_env("POSTGRES_USER")
    password = get_env("POSTGRES_PASSWORD")

    try:
        port = int(port_str)
    except ValueError as exc:
        raise RuntimeError(f"POSTGRES_PORT deve ser um número inteiro. Valor atual: {port_str}") from exc

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=port,
        database=database,
    )

    return create_engine(url, pool_pre_ping=True)


def ensure_database_objects(engine) -> None:
    """
    Cria o schema staging e a tabela staging.online_retail, se não existirem.
    """
    create_schema_sql = text("CREATE SCHEMA IF NOT EXISTS staging;")

    create_table_sql = text(
        """
        CREATE TABLE IF NOT EXISTS staging.online_retail (
            online_retail_stg_id BIGSERIAL PRIMARY KEY,
            batch_id TEXT NOT NULL,
            loaded_at_utc TIMESTAMPTZ NOT NULL,
            invoice_no TEXT,
            stock_code TEXT,
            description TEXT,
            quantity BIGINT,
            invoice_date TIMESTAMP,
            unit_price NUMERIC(18, 2),
            customer_id BIGINT,
            country TEXT,
            line_total NUMERIC(18, 2),
            is_cancellation BOOLEAN
        );
        """
    )

    with engine.begin() as conn:
        logger.info("Criando schema staging, se necessário...")
        conn.execute(create_schema_sql)

        logger.info("Criando tabela staging.online_retail, se necessário...")
        conn.execute(create_table_sql)


def read_and_prepare_parquet(parquet_path: Path, limit: Optional[int] = None):
    """
    Lê o Parquet e prepara o DataFrame para carga.
    """
    logger.info("Lendo arquivo Parquet: %s", parquet_path)

    df = pd.read_parquet(parquet_path)

    missing_columns = set(EXPECTED_COLUMNS) - set(df.columns)

    if missing_columns:
        raise ValueError(
            "O arquivo Parquet não possui as colunas esperadas. "
            f"Colunas ausentes: {sorted(missing_columns)}. "
            f"Colunas encontradas: {list(df.columns)}"
        )

    df = df[EXPECTED_COLUMNS].copy()

    if limit is not None:
        if limit < 0:
            raise ValueError("O argumento --limit não pode ser negativo.")

        df = df.head(limit)
        logger.info("Limitando carga para %s registros.", len(df))

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")

    df["batch_id"] = batch_id
    df["loaded_at_utc"] = pd.Timestamp.now(tz="UTC")

    # Se invoice_date estiver com timezone, converte para UTC e remove timezone,
    # pois a tabela está como TIMESTAMP.
    if pd.api.types.is_datetime64_any_dtype(df["invoice_date"]):
        if getattr(df["invoice_date"].dt, "tz", None) is not None:
            df["invoice_date"] = (
                df["invoice_date"]
                .dt.tz_convert("UTC")
                .dt.tz_localize(None)
            )

    df = df[TARGET_COLUMNS].copy()

    return df, batch_id


def truncate_table(engine) -> None:
    """
    Trunca a tabela staging.online_retail.
    """
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE staging.online_retail RESTART IDENTITY;"))


def insert_dataframe(df: pd.DataFrame, engine) -> None:
    """
    Insere o DataFrame na tabela staging.online_retail usando bulk insert nativo do PostgreSQL.
    Inclui conversão de tipos Numpy/Pandas para tipos nativos do Python para evitar erros de adaptação do psycopg2.
    """
    import numpy as np
    from psycopg2.extras import execute_values

    # Pega a conexão "crua" do psycopg2 por baixo do SQLAlchemy
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        
        # Monta a query de INSERT dinamicamente baseada nas colunas alvo
        cols = ", ".join(TARGET_COLUMNS)
        query = f"INSERT INTO staging.online_retail ({cols}) VALUES %s"
        
        # Função auxiliar para converter tipos Numpy/Pandas para nativos do Python
        def to_native(val):
            if pd.isna(val):
                return None
            if isinstance(val, (np.integer,)):
                return int(val)
            if isinstance(val, (np.floating,)):
                return float(val)
            if isinstance(val, (np.bool_,)):
                return bool(val)
            if isinstance(val, pd.Timestamp):
                return val.to_pydatetime()
            return val

        # Gera lista de tuplas com tipos convertidos
        records = [
            tuple(to_native(v) for v in row)
            for row in df.itertuples(index=False, name=None)
        ]
        
        logger.info("Iniciando bulk insert de %s registros...", len(records))
        
        # Executa o bulk insert
        execute_values(cursor, query, records, page_size=5000)
        
        raw_conn.commit()
        logger.info("Bulk insert concluído com sucesso via psycopg2.")
    except Exception as e:
        raw_conn.rollback()
        logger.error("Erro durante o bulk insert: %s", e)
        raise
    finally:
        raw_conn.close()

def get_counts(engine, batch_id: str):
    """
    Retorna a contagem total da tabela e a contagem do batch carregado.
    """
    total_sql = text("SELECT COUNT(*) FROM staging.online_retail;")

    batch_sql = text(
        """
        SELECT COUNT(*)
        FROM staging.online_retail
        WHERE batch_id = :batch_id;
        """
    )

    with engine.connect() as conn:
        total = conn.execute(total_sql).scalar()
        batch_count = conn.execute(batch_sql, {"batch_id": batch_id}).scalar()

    return int(total or 0), int(batch_count or 0)


def main() -> int:
    setup_logging()
    args = build_arg_parser().parse_args()

    load_dotenv(ENV_FILE)

    parquet_path = args.parquet.expanduser().resolve()

    if not parquet_path.exists():
        logger.error(
            "Arquivo Parquet não encontrado: %s. "
            "Execute a Etapa 2 antes de carregar no PostgreSQL.",
            parquet_path,
        )
        return 1

    try:
        engine = create_sqlalchemy_engine()
    except Exception:
        logger.exception("Erro ao criar conexão com o PostgreSQL.")
        return 1

    try:
        ensure_database_objects(engine)
    except Exception:
        logger.exception("Erro ao criar schema/tabela no PostgreSQL.")
        return 1

    try:
        df, batch_id = read_and_prepare_parquet(parquet_path, args.limit)
    except Exception:
        logger.exception("Erro ao ler/preparar o arquivo Parquet.")
        return 1

    logger.info("Batch de carga: %s", batch_id)
    logger.info("Registros preparados para carga: %s", len(df))

    if args.truncate:
        try:
            logger.info("Truncando staging.online_retail antes da carga...")
            truncate_table(engine)
        except Exception:
            logger.exception("Erro ao truncar staging.online_retail.")
            return 1
    else:
        logger.info("Modo append: a tabela não será truncada.")

    try:
        logger.info("Iniciando carga no PostgreSQL...")
        insert_dataframe(df, engine)
    except Exception:
        logger.exception("Erro ao inserir dados no PostgreSQL.")
        return 1

    try:
        total_count, batch_count = get_counts(engine, batch_id)
    except Exception:
        logger.exception("Erro ao consultar contagens após carga.")
        return 1

    logger.info("Carga concluída com sucesso.")
    logger.info("Total de registros na tabela: %s", total_count)
    logger.info("Registros carregados neste batch: %s", batch_count)

    return 0


if __name__ == "__main__":
    sys.exit(main())