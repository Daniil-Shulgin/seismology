import logging

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

# Конфигурация DAG
OWNER = "d.shulgin"
DAG_ID = "raw_from_s3_to_pg"

# Параметры DWH
LAYER = "raw"
SOURCE = "seismology"
SCHEMA = "ods"
TABLE_NAME = "fct_seismology"
PASSWORD = Variable.get("pg_password")

PG_CONNECT = "postgres_dwh"

# Параметры MinIO
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

LONG_DESCRIPTION = """
DAG загружает данные о землетрясениях в формате Parquet 
из S3/MinIO и загружает их в таблицу PostgreSQL DWH.
Конвертирует типы и стандартизирует названия столбцов.
"""

SHORT_DESCRIPTION = "S3 -> PostgreSQL DWH"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025,11, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

start_date = '{{ ds }}'
end_date = '{{ tomorrow_ds }}'

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE_NAME} (
    "time" DATE,
    latitude varchar,
    longitude varchar,
    depth varchar,
    mag NUMERIC,
    mag_type varchar,
    nst varchar,
    gap varchar,
    dmin varchar,
    rms varchar,
    net varchar,
    id varchar,
    updated varchar,
    place varchar,
    type varchar,
    horizontal_error varchar,
    depth_error varchar,
    mag_error varchar,
    mag_nst varchar,
    status varchar,
    location_source varchar,
    mag_source varchar
)
"""

SQL_S3_TO_PG_QUERY=f"""
    INSERT INTO FUNCTION postgresql(
        'postgres_dwh:5432', 
        'postgres',
        '{TABLE_NAME}',
        'postgres', 
        '{PASSWORD}',
        '{SCHEMA}'
    ) 
    (
        time,
        latitude,
        longitude,
        depth,
        mag,
        mag_type,
        nst,
        gap,
        dmin,
        rms,
        net,
        id,
        updated,
        place,
        type,
        horizontal_error,
        depth_error,
        mag_error,
        mag_nst,
        status,
        location_source,
        mag_source
    )
    SELECT
        toDate(parseDateTimeBestEffortOrNull(time)) AS time,
        latitude,
        longitude,
        depth,
        mag,
        magType AS mag_type,
        nst,
        gap,
        dmin,
        rms,
        net,
        id,
        updated,
        place,
        type,
        horizontalError AS horizontal_error,
        depthError AS depth_error,
        magError AS mag_error,
        magNst AS mag_nst,
        status,
        locationSource AS location_source,
        magSource AS mag_source
    FROM s3(
        'http://minio:9000/prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.parquet',
        '{ACCESS_KEY}', 
        '{SECRET_KEY}', 
        'Parquet'
    )"""


def log_start():
    logging.info(f"✅ Start load for dates: {start_date}/{end_date}")

def log_finish():
    logging.info(f"✅ Download for date success: {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 6 * * *",
    default_args=args,
    tags=["s3", "ods", "pg"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,
        poke_interval=60,
    )

    create_table_in_dwh = SQLExecuteQueryOperator(
        task_id="create_table_in_dwh",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA};
        {CREATE_TABLE_SQL}
        """,
    )

    get_and_transfer_raw_data_to_ods_pg = ClickHouseOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        clickhouse_conn_id="clickhouse_default",

        sql=SQL_S3_TO_PG_QUERY,

        on_execute_callback=[log_start],
        on_success_callback=[log_finish],
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_on_raw_layer >> create_table_in_dwh >> get_and_transfer_raw_data_to_ods_pg >> end