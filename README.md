🌋 Seismology. Конвейер анализа сейсмических данных

ETL-конвейер (Airflow, Python): API → S3/MinIO → PostgreSQL DWH

Обзор проекта

Данный проект представляет собой ELT-конвейер для ежедневного сбора, обработки и агрегации глобальных сейсмических данных.

Основная цель — преобразование сырых данных о землетрясениях в структурированные метрики, пригодные для анализа и визуализации. Конвейер построен с акцентом на надежность и идемпотентность.  

<span style="font-size: 2.5em; font-weight: 700;">🛠️ Стек.

Конвейер построен на базе Apache Airflow с использованием подхода Data Lake (MinIO) и реляционного DWH (PostgreSQL) для слоев ODS и DM.  
| **Категория** | **Технология** | **Версия** |
| ----- | ----- | ----- |
| Оркестрация | Apache Airflow | 2.10.5 |
| Хранилище RAW | MinIO S3 | RELEASE.2025-02-18T16-25-55Z |
| Хранилище DWH | PostgreSQL | 13 |
| Data Mover | ClickHouse | 24.3.6 |
| Визуализация | Power BI | desktop |


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
