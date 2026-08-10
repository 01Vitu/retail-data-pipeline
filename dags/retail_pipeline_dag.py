from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG(
    'retail_data_pipeline',
    default_args=default_args,
    description='Pipeline de dados de varejo usando Airflow',
    schedule_interval=None, # None significa execução manual (trigger)
    catchup=False,
    tags=['retail', 'portfolio', 'etl'],
) as dag:

    # O WORKDIR da imagem do Airflow é /opt/airflow

    ingest = BashOperator(
        task_id='ingest_raw',
        bash_command='python /opt/airflow/src/ingest_raw.py',
    )

    trusted = BashOperator(
        task_id='build_trusted',
        bash_command='python /opt/airflow/src/build_trusted.py',
    )

    load = BashOperator(
        task_id='load_postgres',
        bash_command='python /opt/airflow/src/load_postgres.py',
    )

    analytics = BashOperator(
        task_id='build_analytics',
        bash_command='python /opt/airflow/src/build_analytics.py',
    )

    quality = BashOperator(
        task_id='run_quality_checks',
        bash_command='python /opt/airflow/src/run_quality_checks.py',
    )

    # Definindo a ordem de execução (dependências)
    ingest >> trusted >> load >> analytics >> quality
