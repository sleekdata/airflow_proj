from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def extract(ti=None, **kwargs):
    # Extract logic here
    raw_data = "Raw order data"
    ti.xcom_push(key="raw_data", value=raw_data)

def transform(ti=None, **kwargs):
    # Transformation logic here
    raw_data = ti.xcom_pull(task_ids="extract", key="raw_data")
    processed_data = f"Processed: {raw_data}"
    ti.xcom_push(key="processed_data", value=processed_data)

def validate(ti=None, **kwargs):
    # Validation logic here
    processed_data = ti.xcom_pull(task_ids="transform", key="processed_data")
    validated_data = f"Validated: {processed_data}"
    ti.xcom_push(key="validated_data", value=validated_data)

def load(ti=None, **kwargs):
    # Load logic here
    validated_data = ti.xcom_pull(task_ids="validate", key="validated_data")
    print(f"Data loaded successfully: {validated_data}")

dag = DAG(
    'traditional_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 21 * * *',
    catchup=False
)

extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform",
    python_callable=transform,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate",
    python_callable=validate,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load",
    python_callable=load,
    dag=dag,
)

# Set Dependencies
extract_task >> transform_task >> validate_task >> load_task