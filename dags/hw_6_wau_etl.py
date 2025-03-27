from airflow import DAG
from datetime import datetime
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def set_stage(user_session_chnl_tbl_name, user_session_timestamp_tbl_name, stage_name):
    create_session_chl_tbl_sql = f"""CREATE TABLE IF NOT EXISTS {user_session_chnl_tbl_name} (
        userId int not NULL,
        sessionId varchar(100) primary key,
        channel varchar(100) default 'direct'
    );"""

    create_session_timestamp_tbl_sql = f"""CREATE TABLE IF NOT EXISTS {user_session_timestamp_tbl_name} (
        sessionId varchar(100) primary key,
        ts timestamp    
    );"""

    create_stage_sql = f"""CREATE OR REPLACE STAGE {stage_name}
        url = 's3://s3-geospatial/readonly/'
        file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = "'"
    );"""

    try:
        cursor.execute("BEGIN;")
        cursor.execute(f"ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = 'AUTO';")
        cursor.execute(create_session_chl_tbl_sql)
        cursor.execute(create_session_timestamp_tbl_sql)
        cursor.execute(create_stage_sql)

        cursor.execute(f"DELETE FROM {user_session_timestamp_tbl_name}")
        cursor.execute(f"DELETE FROM {user_session_chnl_tbl_name}")
        
        cursor.execute("COMMIT;")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(e)
        raise e

@task
def load(cursor, user_session_chnl_tbl_name, user_session_timestamp_tbl_name, stage_name):
    copy_into_session_chl_tbl_sql = f"""COPY INTO {user_session_chnl_tbl_name}
        FROM @{stage_name}/user_session_channel.csv
        FORCE = TRUE;
    ;"""

    copy_into_session_timestamp_tbl_sql = f"""COPY INTO {user_session_timestamp_tbl_name}
        FROM @{stage_name}/session_timestamp.csv
        FORCE = TRUE;
    ;"""

    try:
        cursor.execute("BEGIN;")
        cursor.execute(f"ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = 'AUTO';")
        cursor.execute(copy_into_session_chl_tbl_sql)
        cursor.execute(copy_into_session_timestamp_tbl_sql)
        
        cursor.execute("COMMIT;")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id="hw_6_wau_etl",
    start_date=datetime(2025, 3, 17),
    schedule_interval="@daily",
    catchup=False,
    tags=["homework-6", "etl"],
) as dag:
    cursor = return_snowflake_conn()
    user_session_chnl_tbl_name = "HOMEWORK_DB.RAW.user_session_channel"
    user_session_timestamp_tbl_name = "HOMEWORK_DB.RAW.session_timestamp"
    stage_name = "HOMEWORK_DB.RAW.blob_stage"

    set_stage(user_session_chnl_tbl_name, user_session_timestamp_tbl_name, stage_name) >> load(cursor, user_session_chnl_tbl_name, user_session_timestamp_tbl_name, stage_name)
