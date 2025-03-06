from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from datetime import datetime
from airflow.models import Variable
import requests

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract(cursor, train_input_table):
    symbol = "GOOG"
    vantage_api_key = Variable.get('vantage_api_key')
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}"
    r = requests.get(url)
    data = r.json()
    return data

@task
def tl_full_refresh(cursor, data, train_input_table):
    symbol = "GOOG"
    records = 0
    # Transform and Loading with Full Refresh.
    try:
        cursor.execute("BEGIN;")
        create_stock_table = f"""CREATE TABLE IF NOT EXISTS {train_input_table} (
            stock_symbol VARCHAR(10),
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume FLOAT,
            PRIMARY KEY (stock_symbol, date)    
        );"""
        cursor.execute(create_stock_table)
        cursor.execute(f"DELETE FROM {train_input_table}")
        
        for d in data['Time Series (Daily)']:
            open = data['Time Series (Daily)'][d]['1. open']
            high = data['Time Series (Daily)'][d]['2. high']
            low = data['Time Series (Daily)'][d]['3. low']
            close = data['Time Series (Daily)'][d]['4. close']
            volume = data['Time Series (Daily)'][d]['5. volume']

            print(f"INSERT INTO {train_input_table} ({symbol}, {d}, {open}, {high}, {low}, {close}, {volume})")
            insert_statement = f"INSERT INTO {train_input_table} (stock_symbol, date, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(insert_statement, (symbol, d, open, high, low, close, volume))
            
            records += 1
            if records == 90:
                break

        cursor.execute("COMMIT;")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(e)
        raise e

@task
def train(cursor, train_input_table, train_view, forecast_function_name):
    """
        - Create a view with training related columns.
        - Create a model with the view above.
    """
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE, CLOSE, STOCK_SYMBOL AS SYMBOL
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
        - Generate predictions and store the results to a table named forecast_table.
        - Union your predictions with your historical data, then create the final table.
    """
    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        LET x:= SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:X));
    END;"""

    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT STOCK_SYMBOL as SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""
    
    try:
        cursor.execute(make_prediction_sql)
        cursor.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise e

with DAG(
    dag_id = 'etl_using_full_refresh_and_then_train_predict',
    start_date = datetime(2025, 3, 3),
    catchup = False,
    tags = ['ML', 'ELT']
) as dag:
    train_input_table = "DEV.RAW.STOCK_DATA"
    train_view = "DEV.RAW.STOCK_DATA_VIEW"
    forecast_table = "DEV.RAW.STOCK_DATA_FORECAST"
    forecast_function_name = "DEV.ANALYTICS.PREDICT_"
    final_table = "DEV.ANALYTICS.MARKET_DATA"
    cursor = return_snowflake_conn()
    extract_data = extract(cursor, train_input_table)
    transform_and_load = tl_full_refresh(cursor, extract_data, train_input_table)
    train = train(cursor, train_input_table, train_view, forecast_function_name)
    predict = predict(cursor, forecast_function_name, train_input_table, forecast_table, final_table)

    extract_data >> transform_and_load >> train >> predict
