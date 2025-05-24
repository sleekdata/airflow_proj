# Imports
from airflow import DAG, Dataset
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Define the DAG
dag = DAG(
    'data_consumer_dag',
    default_args={'start_date': days_ago(1)},
    schedule=[Dataset("s3://sleek-data/oms/xrate.json")],

#   We usually code cron-based (or time based) schedule_interval as below:
#   schedule_interval="0 23 * * *",
)

# Define the Task
load_table = SnowflakeOperator(
    task_id='data_consumer_task',
    sql='./sqls/xrate_sf.sql',
    snowflake_conn_id='snowflake_conn',
    dag=dag
)
