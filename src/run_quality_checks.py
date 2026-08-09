"""
Etapa 5 - Testes de qualidade de dados no PostgreSQL.

Responsabilidades:
1. Conectar ao PostgreSQL usando variáveis de ambiente.
2. Executar verificações de qualidade em staging e analytics.
3. Gerar um relatório JSON em data/quality_checks.
4. Retornar exit code 1 se houver falhas críticas.

"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"
QUALITY_DIR = PROJECT_ROOT / "data" / "quality_checks"
REPORT_PATH = QUALITY_DIR / "quality_checks_report.json"

logger = logging.getLogger("run_quality_checks")


CHECKS: List[Dict[str, Any]] = [
    {
        "name": "staging_has_rows",
        "severity": "critical",
        "description": "A tabela staging.online_retail deve possuir registros.",
        "sql": """
            SELECT COUNT(*)
            FROM staging.online_retail
        """,
        "rule": {
            "operator": ">",
            "value": 0,
        },
    },
    {
        "name": "analytics_fact_sales_has_rows",
        "severity": "critical",
        "description": "A tabela analytics.fact_sales deve possuir registros.",
        "sql": """
            SELECT COUNT(*)
            FROM analytics.fact_sales
        """,
        "rule": {
            "operator": ">",
            "value": 0,
        },
    },
    {
        "name": "staging_critical_fields_not_null",
        "severity": "critical",
        "description": (
            "Campos críticos de staging.online_retail não devem ser nulos: "
            "invoice_no, invoice_date, quantity, unit_price, line_total, "
            "country e is_cancellation."
        ),
        "sql": """
            SELECT COUNT(*)
            FROM staging.online_retail
            WHERE invoice_no IS NULL
               OR invoice_date IS NULL
               OR quantity IS NULL
               OR unit_price IS NULL
               OR line_total IS NULL
               OR country IS NULL
               OR is_cancellation IS NULL
        """,
        "rule": {
            "operator": "==",
            "value": 0,
        },
    },
    {
        "name": "analytics_fact_sales_critical_fields_not_null",
        "severity": "critical",
        "description": (
            "Campos críticos de analytics.fact_sales não devem ser nulos: "
            "invoice_no, invoice_date, invoice_date_date, quantity, unit_price, "
            "line_total e is_cancellation."
        ),
        "sql": """
            SELECT COUNT(*)
            FROM analytics.fact_sales
            WHERE invoice_no IS NULL
               OR invoice_date IS NULL
               OR invoice_date_date IS NULL
               OR quantity IS NULL
               OR unit_price IS NULL
               OR line_total IS NULL
               OR is_cancellation IS NULL
        """,
        "rule": {
            "operator": "==",
            "value": 0,
        },
    },
    {
        "name": "staging_line_total_consistency",
        "severity": "critical",
        "description": (
            "O campo line_total deve ser aproximadamente igual a quantity * unit_price "
            "em staging.online_retail."
        ),
        "sql": """
            SELECT COUNT(*)
            FROM staging.online_retail
            WHERE quantity IS NULL
               OR unit_price IS NULL
               OR line_total IS NULL
               OR ABS(
                       line_total - ROUND((quantity * unit_price)::NUMERIC, 2)
                   ) > 0.01
        """,
        "rule": {
            "operator": "==",
            "value": 0,
        },
    },
    {
        "name": "analytics_fact_sales_line_total_consistency",
        "severity": "critical",
        "description": (
            "O campo line_total deve ser aproximadamente igual a quantity * unit_price "
            "em analytics.fact_sales."
        ),
        "sql": """
            SELECT COUNT(*)
            FROM analytics.fact_sales
            WHERE quantity IS NULL
               OR unit_price IS NULL
               OR line_total IS NULL
               OR ABS(
                       line_total - ROUND((quantity * unit_price)::NUMERIC, 2)
                   ) > 0.01
        """,
        "rule": {
            "operator": "==",
            "value": 0,
        },
    },
    {
        "name": "staging_fact_sales_row_count_difference",
        "severity": "critical",
        "description": (
            "A quantidade de registros em staging.online_retail deve ser igual "
            "à quantidade de registros em analytics.fact_sales."
        ),
        "sql": """
            SELECT (
                SELECT COUNT(*)
                FROM staging.online_retail
            ) - (
                SELECT COUNT(*)
                FROM analytics.fact_sales
            )
        """,
        "rule": {
            "operator": "==",
            "value": 0,
        },
    },
    {
        "name": "product_summary_has_rows",
        "severity": "critical",
        "description": "A tabela analytics.product_sales_summary deve possuir registros.",
        "sql": """
            SELECT COUNT(*)
            FROM analytics.product_sales_summary
        """,
        "rule": {
            "operator": ">",
            "value": 0,
        },
    },
    {
        "name": "product_summary_unique_stock_code",
        "severity": "critical",
        "description": "Não devem existir stock_code duplicados em product_sales_summary.",
        "sql": """
            SELECT COUNT(*)
            FROM (
                SELECT stock_code
                FROM analytics.product_sales_summary
                GROUP BY stock_code
                HAVING COUNT(*) > 1
            ) duplicated_products
        """,
        "rule": {
            "operator": "==",
            "value": 0,
        },
    },
    {
        "name": "customer_summary_unique_customer_id",
        "severity": "critical",
        "description": "Não devem existir customer_id duplicados em customer_sales_summary.",
        "sql": """
            SELECT COUNT(*)
            FROM (
                SELECT customer_id
                FROM analytics.customer_sales_summary
                GROUP BY customer_id
                HAVING COUNT(*) > 1
            ) duplicated_customers
        """,
        "rule": {
            "operator": "==",
            "value": 0,
        },
    },
    {
        "name": "product_summary_covers_fact_sales_stock_codes",
        "severity": "critical",
        "description": (
            "Todos os stock_code existentes em analytics.fact_sales devem estar "
            "representados em analytics.product_sales_summary."
        ),
        "sql": """
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT stock_code
                FROM analytics.fact_sales
                WHERE stock_code IS NOT NULL
            ) fact_stock_codes
            LEFT JOIN analytics.product_sales_summary product_summary
                ON fact_stock_codes.stock_code = product_summary.stock_code
            WHERE product_summary.stock_code IS NULL
        """,
        "rule": {
            "operator": "==",
            "value": 0,
        },
    },
    {
        "name": "customer_summary_covers_fact_sales_customers",
        "severity": "critical",
        "description": (
            "Todos os customer_id existentes em analytics.fact_sales devem estar "
            "representados em analytics.customer_sales_summary."
        ),
        "sql": """
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT customer_id
                FROM analytics.fact_sales
                WHERE customer_id IS NOT NULL
            ) fact_customers
            LEFT JOIN analytics.customer_sales_summary customer_summary
                ON fact_customers.customer_id = customer_summary.customer_id
            WHERE customer_summary.customer_id IS NULL
        """,
        "rule": {
            "operator": "==",
            "value": 0,
        },
    },
    {
        "name": "staging_invoice_date_out_of_reasonable_range",
        "severity": "warning",
        "description": (
            "Datas de invoice muito antigas ou futuras são suspeitas. "
            "Intervalo esperado: a partir de 2005-01-01 até a data atual mais 1 dia."
        ),
        "sql": """
            SELECT COUNT(*)
            FROM staging.online_retail
            WHERE invoice_date < TIMESTAMP '2005-01-01 00:00:00'
               OR invoice_date > NOW() + INTERVAL '1 day'
        """,
        "rule": {
            "operator": "==",
            "value": 0,
        },
    },
    {
        "name": "staging_negative_unit_price",
        "severity": "warning",
        "description": (
            "Preços negativos são suspeitos e devem ser investigados, "
            "embora possam existir em alguns cenários de ajuste ou retorno."
        ),
        "sql": """
            SELECT COUNT(*)
            FROM staging.online_retail
            WHERE unit_price < 0
        """,
        "rule": {
            "operator": "==",
            "value": 0,
        },
    },
    {
        "name": "staging_negative_quantity_not_cancellation",
        "severity": "warning",
        "description": (
            "Quantidades negativas em registros não marcados como cancelamento "
            "podem indicar devolução não identificada ou problema de qualidade."
        ),
        "sql": """
            SELECT COUNT(*)
            FROM staging.online_retail
            WHERE quantity < 0
              AND NOT is_cancellation
        """,
        "rule": {
            "operator": "==",
            "value": 0,
        },
    },
    {
        "name": "customer_summary_has_rows",
        "severity": "warning",
        "description": (
            "A tabela analytics.customer_sales_summary deve possuir registros "
            "quando houver clientes válidos na fato."
        ),
        "sql": """
            SELECT COUNT(*)
            FROM analytics.customer_sales_summary
        """,
        "rule": {
            "operator": ">",
            "value": 0,
        },
    },
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


def normalize_observed(value: Any) -> Any:
    """
    Normaliza o valor observado para tipos simples no relatório.
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return int(value)

    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return str(value)

    if numeric_value.is_integer():
        return int(numeric_value)

    return numeric_value


def evaluate_rule(observed_value: Any, rule: Dict[str, Any]) -> bool:
    """
    Avalia se o valor observado passa na regra.
    """
    if observed_value is None:
        return False

    operator = rule.get("operator")
    expected_value = rule.get("value")

    try:
        if operator == ">":
            return observed_value > expected_value
        if operator == ">=":
            return observed_value >= expected_value
        if operator == "<":
            return observed_value < expected_value
        if operator == "<=":
            return observed_value <= expected_value
        if operator == "==":
            return observed_value == expected_value
        if operator == "!=":
            return observed_value != expected_value
    except TypeError:
        return False

    return False


def run_check(engine, check: Dict[str, Any]) -> Dict[str, Any]:
    """
    Executa uma verificação de qualidade.
    """
    try:
        with engine.connect() as conn:
            raw_value = conn.execute(text(check["sql"])).scalar()

        observed_value = normalize_observed(raw_value)
        passed = evaluate_rule(observed_value, check["rule"])
        error = None

    except Exception as exc:
        observed_value = None
        passed = False
        error = str(exc)

    return {
        "name": check["name"],
        "severity": check["severity"],
        "description": check["description"],
        "status": "pass" if passed else "fail",
        "observed_value": observed_value,
        "expected_rule": check["rule"],
        "error": error,
    }


def build_report(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Constrói o relatório final de qualidade.
    """
    total_checks = len(results)
    passed_checks = sum(1 for result in results if result["status"] == "pass")
    failed_checks = total_checks - passed_checks

    critical_failed = sum(
        1
        for result in results
        if result["severity"] == "critical" and result["status"] == "fail"
    )

    warning_failed = sum(
        1
        for result in results
        if result["severity"] == "warning" and result["status"] == "fail"
    )

    if critical_failed > 0:
        status = "FAIL"
    elif warning_failed > 0:
        status = "WARN"
    else:
        status = "PASS"

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "summary": {
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "critical_failed": critical_failed,
            "warning_failed": warning_failed,
        },
        "checks": results,
    }


def write_report(report: Dict[str, Any]) -> None:
    """
    Escreve o relatório JSON de forma atômica.
    """
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)

    temp_path = REPORT_PATH.with_name(REPORT_PATH.name + ".tmp")
    temp_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    temp_path.replace(REPORT_PATH)


def main() -> int:
    setup_logging()

    load_dotenv(ENV_FILE)

    try:
        engine = create_sqlalchemy_engine()
    except Exception:
        logger.exception("Erro ao criar conexão com o PostgreSQL.")
        return 1

    logger.info("Iniciando verificações de qualidade.")

    results: List[Dict[str, Any]] = []

    for check in CHECKS:
        logger.info("Executando verificação: %s", check["name"])
        result = run_check(engine, check)
        results.append(result)

        if result["status"] == "pass":
            logger.info("OK: %s", check["name"])
        else:
            log_level = logging.ERROR if result["severity"] == "critical" else logging.WARNING
            logger.log(
                log_level,
                "FAIL: %s | observed=%s | error=%s",
                check["name"],
                result["observed_value"],
                result["error"],
            )

    report = build_report(results)

    try:
        write_report(report)
    except Exception:
        logger.exception("Erro ao escrever o relatório de qualidade.")
        return 1

    logger.info("Relatório salvo em: %s", REPORT_PATH)
    logger.info("Status final: %s", report["status"])
    logger.info(
        "Resumo: total=%s, passed=%s, failed=%s, critical_failed=%s, warning_failed=%s",
        report["summary"]["total_checks"],
        report["summary"]["passed_checks"],
        report["summary"]["failed_checks"],
        report["summary"]["critical_failed"],
        report["summary"]["warning_failed"],
    )

    if report["status"] == "FAIL":
        logger.error("Existem verificações críticas falhando.")
        return 1

    if report["status"] == "WARN":
        logger.warning("Não há falhas críticas, mas existem avisos de qualidade.")

    return 0


if __name__ == "__main__":
    sys.exit(main())