import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

# --- КОНФИГУРАЦИЯ DAG ---
OWNER = "d.shulgin"
DAG_ID = "daily_avg_depth"

# Определения схемы и таблицы
SCHEMA = "dm"
TARGET_TABLE = "daily_avg_depth"

# Имя подключения PostgreSQL (должно совпадать с Airflow Connection ID)
PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
DAG выбирает столбец дата и магнитуда, и записывает 
их в DM.
"""

SHORT_DESCRIPTION = "Фиксация ежедневной магнитуды"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025,10, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 6 * * *",
    default_args=args,
    tags=["dm", "daily_avg_depth", "pg"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    # 1. Сенсор: ждем успешного завершения DAG, который загрузил raw-данные
    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_s3_to_pg",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,
        poke_interval=60,
    )

    # 2. Создание финальной таблицы, если не существует
    create_daily_avg_depth = SQLExecuteQueryOperator(
        task_id="create_table_if_not_exists",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA};
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TARGET_TABLE} (
            activity_date DATE PRIMARY KEY,
            avg_depth_km FLOAT8
        );
        """,
    )

    # 3. Расчет метрик и обработка уже существующих
    insert_daily_avg_depth = SQLExecuteQueryOperator(
        task_id="insert_daily_avg_depth",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE} (
            activity_date, 
            avg_depth_km
        )
        SELECT
            DATE("time") AS activity_date,
            AVG(CASE WHEN mag >= 4.0 THEN depth ELSE NULL END) AS avg_depth_km
        FROM
            ods.fct_seismology
        WHERE
            DATE("time") = '{{{{ ds }}}}'        
        GROUP BY
            1
        ON CONFLICT (activity_date) DO UPDATE SET
            avg_depth_km = EXCLUDED.avg_depth_km;
        """,
    )
    
    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_on_raw_layer >> create_daily_avg_depth >> insert_daily_avg_depth >> end