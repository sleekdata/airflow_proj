# Imports
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Define the DAG
dag = DAG(
    'load_profit_uk',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 21 * * *',
    catchup=False
)

# Define the Task
load_table = SnowflakeOperator(
    task_id='load_table',
#    sql='./sqls/profit_uk.sql',
    sql='select 1',
    snowflake_conn_id='snowflake_conn',
    dag=dag
)

send_email = EmailOperator(
    task_id='send_email',
    to="{{ var.value.get('support_email') }}",
    subject='UK profit table load - Successful',
    html_content='UK Sales table to Profit table Load Completed',
    dag=dag,
)


# Define the Dependencies
load_table >> send_email
