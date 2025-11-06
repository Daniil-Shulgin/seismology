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
