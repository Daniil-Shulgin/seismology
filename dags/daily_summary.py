import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

# --- КОНФИГУРАЦИЯ DAG ---
OWNER = "d.shulgin"
DAG_ID = "daily_summary"

# Определения схемы и таблицы
SCHEMA = "dm"
TARGET_TABLE = "daily_summary"

# Имя подключения PostgreSQL (должно совпадать с Airflow Connection ID)
PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
DAG рассчитывает общее количество событий и количество 
сильных событий >= 5.0 за 1 день и записывает 
их в DM.
"""

SHORT_DESCRIPTION = "Ежедневный подсчет кол-ва событий"

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
    tags=["dm", "daily_summary", "pg", "metrics"],
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
    create_daily_summary = SQLExecuteQueryOperator(
        task_id="create_table_if_not_exists",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA};
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TARGET_TABLE} (
            calculation_date DATE PRIMARY KEY, 
            total_events_24h INT,
            strong_events_24h_ge_5_0 INT,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
        );
        """,
    )

    # 3. Расчет метрик и обработка уже существующих
    insert_daily_summary = SQLExecuteQueryOperator(
        task_id="insert_daily_summary",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE} (
            calculation_date, 
            total_events_24h, 
            strong_events_24h_ge_5_0
        )
        SELECT
            '{{{{ ds }}}}'::DATE - INTERVAL '1 DAY' AS calculation_date,
            COUNT(id) AS total_events_24h,
            COUNT(CASE WHEN mag >= 5.0 THEN 1 END) AS strong_events_24h_ge_5_0
        FROM
            ods.fct_seismology
        WHERE
            "time" = ('{{{{ ds }}}}'::DATE - INTERVAL '1 DAY') 
        ON CONFLICT (calculation_date) DO UPDATE SET
            total_events_24h = EXCLUDED.total_events_24h,
            strong_events_24h_ge_5_0 = EXCLUDED.strong_events_24h_ge_5_0,
            created_at = NOW();
        """,
    )
    
    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_on_raw_layer >> create_daily_summary >> insert_daily_summary >> end