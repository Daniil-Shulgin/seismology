# pet_project_seismology
ETL-конвейер (Airflow, Python): API → S3/MinIO → PostgreSQL DWH

'''
python 3.13.5 -m venv venv
'''
```bash
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE_NAME} (
    time date,
    latitude float8,
    longitude float8,
    depth float8,
    mag float8,
    mag_type varchar,
    nst smallint,
    gap smallint,
    dmin float8,
    rms float8,
    net varchar,
    id varchar,
    updated date,
    place varchar,
    type varchar,
    horizontal_error float8,
    depth_error float8,
    mag_error float8,
    mag_nst SMALLINT,
    status varchar,
    location_source varchar,
    mag_source varchar
)
"""
```
| Категория | Технология | Назначение |

| ----- | ----- | ----- |

| Оркестрация | Apache Airflow (Python) | Планирование, мониторинг и управление всем конвейером ETL. |

| Сырые данные (Storage) | MinIO (S3-совместимое) | Хранилище (Data Lake) для сырых данных, полученных из API. |

| Хранилище DWH | PostgreSQL | Реляционное хранилище для слоев ODS (Оперативный Источник Данных) и AGG (Агрегированные данные). |

| Массовая аналитика | ClickHouse | (Опционально) Высокопроизводительная колоночная СУБД для аналитических запросов и исторического хранения больших объемов RAW-данных. |

| Визуализация | Power BI | Инструмент для создания дашбордов и отчетности на основе агрегированных метрик. |
