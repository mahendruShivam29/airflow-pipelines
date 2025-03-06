# Airflow
Airflow dags for DATA-226 Data Warehouse.

- DAG `extract_transform_load_stock_data` extracts Stock Data for 2 symbols for the last 180 days using y-finance. After extracting, it performs some transformations and finally loads the data into Snowflake.
- DAG `train_predict_stock_data` reads data from the above Snowflake table, performs training and predicts the stock price for next 7 days and stores the merged results into a new Table.
- The first DAG triggers the second DAG, once the data is populated into Snowflake.
