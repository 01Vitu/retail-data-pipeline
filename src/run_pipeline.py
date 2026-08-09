"""
Etapa 6 - Orquestração local do Retail Data Pipeline.

Etapas disponíveis:
- ingest: ingestão bruta dos dados.
- trusted: construção da camada trusted em Parquet.
- load: carga da camada trusted no PostgreSQL.
- analytics: construção da camada analytics no PostgreSQL.
- quality: execução dos testes de qualidade de dados.

"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


PROJECT_ROOT = Path(__file__).resolve().parents[1]

logger = logging.getLogger("run_pipeline")


@dataclass
class Step:
    name: str
    script: str
    supports_force: bool = False


STEPS: List[Step] = [
    Step(
        name="ingest",
        script="src/ingest_raw.py",
        supports_force=True,
    ),
    Step(
        name="trusted",
        script="src/build_trusted.py",
        supports_force=True,
    ),
    Step(
        name="load",
        script="src/load_postgres.py",
        supports_force=False,
    ),
    Step(
        name="analytics",
        script="src/build_analytics.py",
        supports_force=False,
    ),
    Step(
        name="quality",
        script="src/run_quality_checks.py",
        supports_force=False,
    ),
]

STEP_NAMES: List[str] = [step.name for step in STEPS]


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
    Cria os argumentos aceitos pelo orquestrador.
    """
    parser = argparse.ArgumentParser(
        description="Executa o Retail Data Pipeline localmente."
    )

    parser.add_argument(
        "--start-at",
        choices=STEP_NAMES,
        default="ingest",
        help="Etapa inicial do pipeline. Padrão: ingest.",
    )

    parser.add_argument(
        "--stop-after",
        choices=STEP_NAMES,
        default="quality",
        help="Etapa final do pipeline. Padrão: quality.",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Força reexecução das etapas que suportam --force. "
            "Atualmente: ingest e trusted."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra as etapas e comandos, mas não executa nada.",
    )

    return parser


def get_selected_steps(start_at: str, stop_after: str) -> List[Step]:
    """
    Retorna as etapas selecionadas com base em --start-at e --stop-after.
    """
    start_index = STEP_NAMES.index(start_at)
    stop_index = STEP_NAMES.index(stop_after)

    if start_index > stop_index:
        raise ValueError(
            "O argumento --start-at não pode ser posterior ao --stop-after."
        )

    return STEPS[start_index : stop_index + 1]


def check_prerequisites(selected_steps: List[Step]) -> bool:
    """
    Verifica pré-condições simples antes de executar o pipeline.
    """
    selected_names = {step.name for step in selected_steps}

    raw_csv = PROJECT_ROOT / "data" / "raw" / "online_retail.csv"
    trusted_parquet = PROJECT_ROOT / "data" / "trusted" / "online_retail.parquet"

    if "trusted" in selected_names and not raw_csv.exists():
        logger.error(
            "Arquivo raw não encontrado: %s. "
            "Execute a etapa ingest antes ou use --start-at load/analytics/quality "
            "se já possuir camadas anteriores.",
            raw_csv,
        )
        return False

    if "load" in selected_names and not trusted_parquet.exists():
        logger.error(
            "Arquivo trusted não encontrado: %s. "
            "Execute a etapa trusted antes ou use --start-at analytics/quality "
            "se o PostgreSQL já estiver preparado.",
            trusted_parquet,
        )
        return False

    return True


def build_command(step: Step, args: argparse.Namespace) -> List[str]:
    """
    Monta o comando que será executado para uma etapa.
    """
    script_path = PROJECT_ROOT / step.script

    if not script_path.exists():
        raise FileNotFoundError(f"Script não encontrado: {script_path}")

    command = [
        sys.executable,
        str(script_path),
    ]

    if args.force and step.supports_force:
        command.append("--force")

    return command


def run_step(step: Step, args: argparse.Namespace) -> Dict[str, object]:
    """
    Executa uma etapa do pipeline.
    """
    command = build_command(step, args)

    logger.info("Etapa: %s", step.name)
    logger.info("Comando: %s", " ".join(command))

    start_time = time.time()

    if args.dry_run:
        return {
            "name": step.name,
            "status": "dry-run",
            "duration_seconds": 0.0,
        }

    try:
        completed_process = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
        )

        duration_seconds = time.time() - start_time

        if completed_process.returncode == 0:
            status = "success"
        else:
            status = "failed"

        return {
            "name": step.name,
            "status": status,
            "duration_seconds": duration_seconds,
        }

    except FileNotFoundError:
        duration_seconds = time.time() - start_time
        logger.error("Script não encontrado para a etapa: %s", step.name)
        return {
            "name": step.name,
            "status": "failed",
            "duration_seconds": duration_seconds,
        }

    except Exception:
        duration_seconds = time.time() - start_time
        logger.exception("Erro inesperado ao executar a etapa: %s", step.name)
        return {
            "name": step.name,
            "status": "failed",
            "duration_seconds": duration_seconds,
        }


def log_summary(results: List[Dict[str, object]]) -> None:
    """
    Mostra um resumo final da execução.
    """
    logger.info("Resumo do pipeline:")

    for result in results:
        logger.info(
            "%s: %s (%.2fs)",
            result["name"],
            result["status"],
            float(result["duration_seconds"]),
        )


def main() -> int:
    setup_logging()

    parser = build_arg_parser()
    args = parser.parse_args()

    try:
        selected_steps = get_selected_steps(args.start_at, args.stop_after)
    except ValueError as exc:
        logger.error("%s", exc)
        return 1

    selected_names = ", ".join(step.name for step in selected_steps)
    logger.info("Pipeline iniciado.")
    logger.info("Etapas selecionadas: %s", selected_names)

    if args.dry_run:
        logger.info("Modo dry-run ativado. Nenhuma etapa será realmente executada.")

    if not check_prerequisites(selected_steps):
        return 1

    results: List[Dict[str, object]] = []
    pipeline_failed = False

    try:
        for step in selected_steps:
            result = run_step(step, args)
            results.append(result)

            if result["status"] == "failed":
                logger.error(
                    "A etapa '%s' falhou. O pipeline foi interrompido.",
                    step.name,
                )
                pipeline_failed = True
                break

    except KeyboardInterrupt:
        logger.error("Pipeline interrompido pelo usuário.")
        log_summary(results)
        return 130

    log_summary(results)

    if pipeline_failed:
        logger.error("Pipeline finalizado com falha.")
        return 1

    if args.dry_run:
        logger.info("Dry-run concluído com sucesso.")
    else:
        logger.info("Pipeline concluído com sucesso.")

    return 0


if __name__ == "__main__":
    sys.exit(main())