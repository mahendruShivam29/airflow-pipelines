import yfinance as yf
from airflow import DAG
from datetime import datetime
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract_and_transform_last_180d_data_from_yfinance(cursor, symbols):
    """
        - Extract the last 180 days of Stock price data from Y-finance.
        - Perform Transformation, here itself. TimeStamp Object to Date. Round decimal to 4 places.
    """
    result = []
    for symb in symbols:
        stock = yf.Ticker(symb)
        df = stock.history(period='180d', interval='1d')
        
        for row in df.itertuples(index=True, name="Row"):
            record = {}
            # Transformation.
            record["date"] = row.Index.strftime('%Y-%m-%d')
            record["symbol"] = symb
            record["open"] = "{:.4f}".format(row.Open)
            record["high"] = "{:.4f}".format(row.High)
            record["low"] = "{:.4f}".format(row.Low)
            record["close"] = "{:.4f}".format(row.Close)
            record["volume"] = str(row.Volume)
            result.append(record)
    
    for r in result:
        print(r)
    return result

@task
def load_stock_data(cursor, stock_data, stock_data_table):
    """
        - Loads Stock Data in Snowflake using full refresh, try-except, commit and rollback.
    """
    try:
        cursor.execute("BEGIN;")
        create_stock_data_table_sql = f"""CREATE TABLE IF NOT EXISTS {stock_data_table} (
            symbol VARCHAR(10),
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume FLOAT,
            PRIMARY KEY (symbol, date)
        );"""
        cursor.execute(create_stock_data_table_sql)
        cursor.execute(f"DELETE FROM {stock_data_table}")

        for record in stock_data:
            symbol = record["symbol"]
            date = record["date"]
            open = record["open"]
            high = record["high"]
            low = record["low"]
            close = record["close"]
            volume = record["volume"]

            insert_record_statement = f"INSERT INTO {stock_data_table} (symbol, date, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(insert_record_statement, (symbol, date, open, high, low, close, volume))
            print(f"INSERT INTO {stock_data_table} ({symbol}, {date}, {open}, {high}, {low}, {close}, {volume})")
        
        cursor.execute("COMMIT;")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id = 'extract_transform_load_stock_data',
    start_date = datetime(2025, 3, 3),
    catchup = False,
    tags = ['ETL']
) as dag:
    cursor = return_snowflake_conn()
    symbols = ['AAPL', 'GOOG']
    stock_data_table = "DEV.LAB.STOCK_DATA"
    stock_data = extract_and_transform_last_180d_data_from_yfinance(cursor, symbols)
    load_stock_data(cursor, stock_data, stock_data_table)