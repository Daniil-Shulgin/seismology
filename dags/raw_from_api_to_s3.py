import logging

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

# Конфигурация DAG
OWNER = "d.shulgin"
DAG_ID = "raw_from_api_to_s3"
LAYER = "raw"
SOURCE = "seismology"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

LONG_DESCRIPTION = """
# DAG загружает данные о землетрясениях из API
и сохраняет их в S3 (Minio) в формате Parquet.

Для выполнения операций используется ClickHouse и его S3-engine.
Загрузка инициируется через `ClickHouseOperator`.
"""

SHORT_DESCRIPTION = "Загрузка данных о землетрясениях (API -> S3) через ClickHouse"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025,11, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

start_date = '{{ ds }}'
end_date = '{{ tomorrow_ds }}'


SQL_API_TO_S3_QUERY = f'''
INSERT INTO FUNCTION s3(
    'http://minio:9000/prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.parquet',
    '{ACCESS_KEY}',
    '{SECRET_KEY}',
    'Parquet' 
)
SELECT *
FROM url('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}', 'CSV');
'''

s3_settings = {
    's3_truncate_on_insert': 1,
    'format_parquet_compression_codec': 'lz4', 
}

def log_start():
    logging.info(f"✅ Start load for dates: {start_date}/{end_date}")

def log_finish():
    logging.info(f"✅ Download for date success: {start_date}")

# Определение DAG
with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 6 * * *",
    default_args=args,
    tags=["s3", "raw", "clickhouse"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    get_and_transfer_api_data_to_s3 = ClickHouseOperator(
        task_id="transfer_api_data_to_s3",
        clickhouse_conn_id="clickhouse_default",

        sql=SQL_API_TO_S3_QUERY,

        settings=s3_settings,

        on_execute_callback=[log_start],
        on_success_callback=[log_finish],
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_and_transfer_api_data_to_s3 >> end