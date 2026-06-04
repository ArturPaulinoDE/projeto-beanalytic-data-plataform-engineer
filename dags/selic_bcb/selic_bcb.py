import os
import sys
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor

# Adiciona o diretório /opt/airflow ao path para garantir que os módulos sejam encontrados
sys.path.append("/opt/airflow")

from tasks.selic_bcb.bronze.ingest import ingest_data
from tasks.selic_bcb.silver.transform import transform_data
from tasks.selic_bcb.gold.aggregate import aggregate_data
from tasks.selic_bcb.gold.validate import validate_gold_data

# Caminho para o JSON de configuração
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(DAG_DIR, "selic_bcb.json")

with open(config_path, "r", encoding="utf-8") as f:
    config = json.load(f)

default_args = {
    "owner": "Artur Paulino Varela de Araújo",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": config["retries"],
    "retry_delay": timedelta(seconds=config["retry_delay"]),
}

with DAG(
    dag_id=config["dag_id"],
    default_args=default_args,
    schedule_interval=config["schedule_interval"],
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task 1: Sensor que valida a disponibilidade da API antes de iniciar (usando primeiro dia útil de 2020 para evitar 404 de feriado)
    task_verificar_api = HttpSensor(
        task_id="verificar_api_sensor",
        http_conn_id="bcb_api",
        endpoint="dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=01/01/2020&dataFinal=31/12/2024",
        method="GET",
        response_check=lambda response: response.status_code == 200,
        poke_interval=30,
        timeout=300,
        mode="poke",
    )

    # Task 2: Ingestão de dados brutos na Bronze com DQ integrado e particionamento por ano
    task_ingestao_bronze = PythonOperator(
        task_id="ingestao_bronze",
        python_callable=ingest_data,
        op_kwargs={"base_dir": config["base_dir"]},
    )

    # Task 3: Limpeza e estruturação na Silver com rejeição e logs estruturados de registros inválidos
    task_conformacao_silver = PythonOperator(
        task_id="conformacao_silver",
        python_callable=transform_data,
        op_kwargs={"base_dir": config["base_dir"]},
    )

    # Task 4: Agregações (média, variação mensal e acumulado anual) na Gold com DQ integrado
    task_agregacao_gold = PythonOperator(
        task_id="agregacao_gold",
        python_callable=aggregate_data,
        op_kwargs={"base_dir": config["base_dir"]},
    )

    # Task 5: Validação final da integridade das tabelas Gold
    task_validacao_final = PythonOperator(
        task_id="validacao_final",
        python_callable=validate_gold_data,
        op_kwargs={"base_dir": config["base_dir"]},
    )

    # Dependências explícitas do Pipeline
    task_verificar_api >> task_ingestao_bronze >> task_conformacao_silver >> task_agregacao_gold >> task_validacao_final
