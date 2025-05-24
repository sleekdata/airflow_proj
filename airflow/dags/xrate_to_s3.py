from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.models import Variable
import subprocess

dag = DAG(
    'xrate_to_s3',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
)

def fetch_xrate_fn(ti):
    xrate_filename = f'xrate_{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.json'
    curl_command = ["curl", Variable.get('web_api_key'), "-o", f'/tmp/{xrate_filename}']
    subprocess.run(curl_command)
    ti.xcom_push(key='xrate_file', value=xrate_filename)

fetch_xrate = PythonOperator(
    task_id='fetch_xrate',
    python_callable=fetch_xrate_fn,
    provide_context=True,
    dag=dag
)

upload_to_s3 = LocalFilesystemToS3Operator(
    task_id='upload_to_s3',
    filename="/tmp/{{ ti.xcom_pull(task_ids='fetch_xrate', key='xrate_file') }}",
    dest_bucket='sleek-data',
    dest_key=f'oms/{{{{ ti.xcom_pull(task_ids="fetch_xrate", key="xrate_file") }}}}',
    aws_conn_id='aws_conn',
    dag=dag
)

# Define Dependencies
fetch_xrate >> upload_to_s3