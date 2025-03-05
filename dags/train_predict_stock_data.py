from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def train(cursor, train_input_table, train_view, forecast_function_name):
    """
        - Create a view with training related columns.
        - Create a model with the view above.
    """
    
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE, CLOSE, SYMBOL
    FROM {train_input_table};"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cursor.execute(create_view_sql)
        cursor.execute(create_model_sql)
        cursor.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise e

@task
def predict(cursor, forecast_function_name, train_input_table, forecast_table, final_table):
    """
        - Generate predictions and store the results to a table named forecast table.
        - Union the predictions with historical data, then make a final table.
    """
    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x)); 
    END;"""

    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS 
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL as forecast, NULL as lower_bound, NULL as upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL as actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""
    
    try:
        cursor.execute(make_prediction_sql)
        cursor.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise e

with DAG(
    dag_id = 'train_predict_stock_data',
    start_date = datetime(2025, 3, 5),
    catchup = False,
    tags = ['ML', 'ELT']
) as dag:
    cursor = return_snowflake_conn()

    train_input_table = "DEV.LAB.STOCK_DATA"
    train_view = "DEV.LAB.STOCK_DATA_VIEW"
    forecast_table = "DEV.LAB.STOCK_DATA_FORECAST"
    forecast_function_name = "DEV.LAB.PREDICT_"
    final_table = "DEV.LAB.MARKET_DATA"

    train(cursor, train_input_table, train_view, forecast_function_name) >> predict(cursor, forecast_function_name, train_input_table, forecast_table, final_table)