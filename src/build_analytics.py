"""
Etapa 4 - Construção da camada analytics no PostgreSQL.

Responsabilidades:
1. Conectar ao PostgreSQL usando variáveis de ambiente.
2. Criar o schema analytics.
3. Criar tabelas analíticas.
4. Truncar as tabelas analíticas para reconstrução idempotente.
5. Popular analytics.fact_sales a partir de staging.online_retail.
6. Popular analytics.product_sales_summary.
7. Popular analytics.customer_sales_summary.
8. Criar índices úteis.
9. Atualizar estatísticas do PostgreSQL.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"

logger = logging.getLogger("build_analytics")


CREATE_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS analytics
"""

CREATE_FACT_SALES_SQL = """
CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    fact_sales_id BIGSERIAL PRIMARY KEY,
    invoice_no TEXT NOT NULL,
    stock_code TEXT,
    customer_id BIGINT,
    country TEXT,
    invoice_date TIMESTAMP,
    invoice_date_date DATE,
    invoice_year INTEGER,
    invoice_month INTEGER,
    invoice_day INTEGER,
    quantity BIGINT NOT NULL,
    unit_price NUMERIC(18, 2) NOT NULL,
    line_total NUMERIC(18, 2) NOT NULL,
    is_cancellation BOOLEAN NOT NULL,
    batch_id TEXT,
    loaded_at_utc TIMESTAMPTZ
)
"""

CREATE_PRODUCT_SALES_SUMMARY_SQL = """
CREATE TABLE IF NOT EXISTS analytics.product_sales_summary (
    product_summary_id BIGSERIAL PRIMARY KEY,
    stock_code TEXT NOT NULL,
    latest_description TEXT,
    first_invoice_date TIMESTAMP,
    last_invoice_date TIMESTAMP,
    total_invoices BIGINT,
    total_order_lines BIGINT,
    gross_quantity BIGINT,
    net_quantity BIGINT,
    gross_revenue NUMERIC(18, 2),
    net_revenue NUMERIC(18, 2),
    updated_at_utc TIMESTAMPTZ,
    CONSTRAINT uq_product_sales_summary_stock_code UNIQUE (stock_code)
)
"""

CREATE_CUSTOMER_SALES_SUMMARY_SQL = """
CREATE TABLE IF NOT EXISTS analytics.customer_sales_summary (
    customer_summary_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    first_order_date TIMESTAMP,
    last_order_date TIMESTAMP,
    total_orders BIGINT,
    total_order_lines BIGINT,
    net_quantity BIGINT,
    net_revenue NUMERIC(18, 2),
    updated_at_utc TIMESTAMPTZ,
    CONSTRAINT uq_customer_sales_summary_customer_id UNIQUE (customer_id)
)
"""

TRUNCATE_ANALYTICS_SQL = """
TRUNCATE TABLE
    analytics.fact_sales,
    analytics.product_sales_summary,
    analytics.customer_sales_summary
RESTART IDENTITY
"""

POPULATE_FACT_SALES_SQL = """
INSERT INTO analytics.fact_sales (
    invoice_no,
    stock_code,
    customer_id,
    country,
    invoice_date,
    invoice_date_date,
    invoice_year,
    invoice_month,
    invoice_day,
    quantity,
    unit_price,
    line_total,
    is_cancellation,
    batch_id,
    loaded_at_utc
)
SELECT
    invoice_no,
    stock_code,
    customer_id,
    country,
    invoice_date,
    invoice_date::date AS invoice_date_date,
    EXTRACT(YEAR FROM invoice_date)::INTEGER AS invoice_year,
    EXTRACT(MONTH FROM invoice_date)::INTEGER AS invoice_month,
    EXTRACT(DAY FROM invoice_date)::INTEGER AS invoice_day,
    quantity,
    unit_price,
    line_total,
    is_cancellation,
    batch_id,
    loaded_at_utc
FROM staging.online_retail
WHERE invoice_no IS NOT NULL
  AND invoice_date IS NOT NULL
  AND quantity IS NOT NULL
  AND unit_price IS NOT NULL
  AND line_total IS NOT NULL
"""

POPULATE_PRODUCT_SALES_SUMMARY_SQL = """
WITH latest_description AS (
    SELECT DISTINCT ON (stock_code)
        stock_code,
        description AS latest_description
    FROM staging.online_retail
    WHERE stock_code IS NOT NULL
    ORDER BY
        stock_code,
        invoice_date DESC NULLS LAST,
        online_retail_stg_id DESC
)
INSERT INTO analytics.product_sales_summary (
    stock_code,
    latest_description,
    first_invoice_date,
    last_invoice_date,
    total_invoices,
    total_order_lines,
    gross_quantity,
    net_quantity,
    gross_revenue,
    net_revenue,
    updated_at_utc
)
SELECT
    f.stock_code,
    ld.latest_description,
    MIN(f.invoice_date) AS first_invoice_date,
    MAX(f.invoice_date) AS last_invoice_date,
    COUNT(DISTINCT f.invoice_no) AS total_invoices,
    COUNT(*) AS total_order_lines,
    SUM(f.quantity) AS gross_quantity,
    SUM(
        CASE
            WHEN NOT f.is_cancellation THEN f.quantity
            ELSE 0
        END
    ) AS net_quantity,
    SUM(f.line_total) AS gross_revenue,
    SUM(
        CASE
            WHEN NOT f.is_cancellation THEN f.line_total
            ELSE 0
        END
    ) AS net_revenue,
    NOW() AS updated_at_utc
FROM analytics.fact_sales f
LEFT JOIN latest_description ld
    ON f.stock_code = ld.stock_code
WHERE f.stock_code IS NOT NULL
GROUP BY
    f.stock_code,
    ld.latest_description
"""

POPULATE_CUSTOMER_SALES_SUMMARY_SQL = """
INSERT INTO analytics.customer_sales_summary (
    customer_id,
    first_order_date,
    last_order_date,
    total_orders,
    total_order_lines,
    net_quantity,
    net_revenue,
    updated_at_utc
)
SELECT
    customer_id,
    MIN(invoice_date) AS first_order_date,
    MAX(invoice_date) AS last_order_date,
    COUNT(DISTINCT invoice_no) AS total_orders,
    COUNT(*) AS total_order_lines,
    SUM(
        CASE
            WHEN NOT is_cancellation THEN quantity
            ELSE 0
        END
    ) AS net_quantity,
    SUM(
        CASE
            WHEN NOT is_cancellation THEN line_total
            ELSE 0
        END
    ) AS net_revenue,
    NOW() AS updated_at_utc
FROM analytics.fact_sales
WHERE customer_id IS NOT NULL
GROUP BY customer_id
"""

CREATE_INDEX_STATEMENTS: List[str] = [
    """
    CREATE INDEX IF NOT EXISTS idx_fact_sales_invoice_no
    ON analytics.fact_sales (invoice_no)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fact_sales_stock_code
    ON analytics.fact_sales (stock_code)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_id
    ON analytics.fact_sales (customer_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fact_sales_country
    ON analytics.fact_sales (country)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fact_sales_invoice_date_date
    ON analytics.fact_sales (invoice_date_date)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fact_sales_year_month
    ON analytics.fact_sales (invoice_year, invoice_month)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_product_sales_summary_net_revenue
    ON analytics.product_sales_summary (net_revenue DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_customer_sales_summary_net_revenue
    ON analytics.customer_sales_summary (net_revenue DESC)
    """,
]

ANALYZE_STATEMENTS: List[str] = [
    "ANALYZE analytics.fact_sales",
    "ANALYZE analytics.product_sales_summary",
    "ANALYZE analytics.customer_sales_summary",
]

DDL_STATEMENTS: List[str] = [
    CREATE_SCHEMA_SQL,
    CREATE_FACT_SALES_SQL,
    CREATE_PRODUCT_SALES_SUMMARY_SQL,
    CREATE_CUSTOMER_SALES_SUMMARY_SQL,
]


def setup_logging() -> None:
    """
    Configura o formato básico dos logs.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


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
        raise RuntimeError(
            f"POSTGRES_PORT deve ser um número inteiro. Valor atual: {port_str}"
        ) from exc

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=port,
        database=database,
    )

    return create_engine(url, pool_pre_ping=True)


def run_build(engine) -> None:
    """
    Executa a construção da camada analytics.
    """
    with engine.begin() as conn:
        logger.info("Criando schema analytics e tabelas analíticas...")
        for statement in DDL_STATEMENTS:
            conn.execute(text(statement))

        logger.info("Truncando tabelas analytics para reconstrução...")
        conn.execute(text(TRUNCATE_ANALYTICS_SQL))

        logger.info("Populando analytics.fact_sales...")
        conn.execute(text(POPULATE_FACT_SALES_SQL))

        logger.info("Populando analytics.product_sales_summary...")
        conn.execute(text(POPULATE_PRODUCT_SALES_SUMMARY_SQL))

        logger.info("Populando analytics.customer_sales_summary...")
        conn.execute(text(POPULATE_CUSTOMER_SALES_SUMMARY_SQL))

        logger.info("Criando índices...")
        for statement in CREATE_INDEX_STATEMENTS:
            conn.execute(text(statement))

    with engine.begin() as conn:
        logger.info("Atualizando estatísticas das tabelas...")
        for statement in ANALYZE_STATEMENTS:
            conn.execute(text(statement))


def get_counts(engine) -> Dict[str, int]:
    """
    Retorna contagens das tabelas analytics.
    """
    queries = {
        "fact_sales": "SELECT COUNT(*) FROM analytics.fact_sales",
        "product_sales_summary": "SELECT COUNT(*) FROM analytics.product_sales_summary",
        "customer_sales_summary": "SELECT COUNT(*) FROM analytics.customer_sales_summary",
    }

    counts: Dict[str, int] = {}

    with engine.connect() as conn:
        for table_name, query in queries.items():
            result = conn.execute(text(query)).scalar()
            counts[table_name] = int(result or 0)

    return counts


def main() -> int:
    setup_logging()

    load_dotenv(ENV_FILE)

    try:
        engine = create_sqlalchemy_engine()
    except Exception:
        logger.exception("Erro ao criar conexão com o PostgreSQL.")
        return 1

    try:
        run_build(engine)
    except Exception:
        logger.exception(
            "Erro ao construir a camada analytics. "
            "Verifique se a Etapa 3 foi executada e se a tabela staging.online_retail existe."
        )
        return 1

    try:
        counts = get_counts(engine)
    except Exception:
        logger.exception("Erro ao consultar contagens finais.")
        return 1

    logger.info("Etapa 4 concluída com sucesso.")
    logger.info("analytics.fact_sales: %s registros", counts["fact_sales"])
    logger.info(
        "analytics.product_sales_summary: %s registros",
        counts["product_sales_summary"],
    )
    logger.info(
        "analytics.customer_sales_summary: %s registros",
        counts["customer_sales_summary"],
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())