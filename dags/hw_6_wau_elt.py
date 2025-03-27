from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import logging

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def run_ctas(cur, database, schema, table, select_sql, primary_key=None):
    logging.info(table)
    logging.info(select_sql)

    try:
        sql = f"CREATE OR REPLACE TABLE {database}.{schema}.tmp_{table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        logging.info("1")

        if primary_key is not None:
            sql = f"""
                SELECT {primary_key}, COUNT(1) AS cnt
                FROM {database}.{schema}.tmp_{table}
                GROUP BY {primary_key}
                ORDER BY cnt DESC
                LIMIT 1
            """
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            if int(result[1]) > 1:
                print("!!!!!!!!!!!!!!")
                raise Exception(f"Primary Key uniqueness failed: {result}")
        
        ## Check duplicate rows.
        cur.execute(f"""SELECT * FROM {database}.{schema}.tmp_{table} LIMIT 1;""")
        column_names = [desc[0] for desc in cur.description]

        # Get the column names int the table.
        print("Column names:", column_names)
        sql = f"""
            SELECT {column_names[0]}, {column_names[1]}, {column_names[2]}, {column_names[3]}, COUNT(*)
            FROM {database}.{schema}.tmp_{table}
            GROUP BY {column_names[0]}, {column_names[1]}, {column_names[2]}, {column_names[3]}
            HAVING COUNT(*) > 1;
        """

        cur.execute(sql)
        results = cur.fetchall()

        num_records = len(results)
        print(sql)
        if num_records > 0:
            print("!!!!!!!!!!!!!!")
            raise Exception(f"Duplicate rows found: {num_records}")


        main_table_creation_if_not_exists_sql = f"""
            CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} AS
            SELECT * FROM {database}.{schema}.tmp_{table} WHERE 1=0;
        """
        cur.execute(main_table_creation_if_not_exists_sql)

        swap_sql = f"""ALTER TABLE {database}.{schema}.{table} SWAP WITH {database}.{schema}.tmp_{table}"""
        cur.execute(swap_sql)
    except Exception as e:
        raise


with DAG(
    dag_id = 'hw_6_wau_elt',
    start_date = datetime(2025, 3, 17),
    catchup = False,
    schedule_interval = '@daily',
    tags = ['homework-6', 'elt']
) as dag:
    database = "HOMEWORK_DB"
    schema = "ANALYTICS"
    table = "session_summary"
    cur = return_snowflake_conn()

    select_sql = f"""SELECT u.*, s.ts
        FROM HOMEWORK_DB.RAW.user_session_channel u
        JOIN HOMEWORK_DB.RAW.session_timestamp s
        ON u.sessionId = s.sessionId
    """
    logging.info("HELLOU")
    run_ctas(cur, database, schema, table, select_sql, primary_key="sessionId")