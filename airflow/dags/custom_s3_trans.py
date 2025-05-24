from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.dates import days_ago

# Define your custom transformation function using Hooks
def custom_transformation(bucketname, sourcekey, destinationkey):
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    # Read S3 File
    content = s3_hook.read_key(bucket_name=bucketname, key=sourcekey)

    # Apply Custom Transformations
    transformed_content = content.upper()

    # Load S3 File
    s3_hook.load_string(transformed_content, bucket_name=bucketname, key=destinationkey)

# Define DAG
dag = DAG(
    'cust_s3_trans',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
)

# Define your tasks
transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=custom_transformation,
    op_args=['sleek-data', 'oms/customer.csv', 'oms/customer_transformed.csv'],
    dag=dag,
)

# Define Dependencies
transform_task
