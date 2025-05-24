from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago

dag = DAG(
    'LatestOnly',
    default_args={'start_date': days_ago(3)},
    schedule_interval='0 2 * * *',
    catchup=True,
)

# Define tasks
task_a = PythonOperator(
    task_id='task_a',
    python_callable=lambda: print("Executing Task A"),
    dag=dag,
)

task_b = PythonOperator(
    task_id='task_b',
    python_callable=lambda: print("Executing Task B"),
    dag=dag,
)

# Send email based on success
send_email_completed = EmailOperator(
    task_id="send_email_completed",
    to="{{ var.value.get('support_email') }}",
    subject="UK Sales Data Load - Successful",
    html_content="UK Sales Data Load Completed Successfully.",
    dag=dag,
)

# Send email based on failure
send_email_failed = EmailOperator(
    task_id="send_email_failed",
    to="{{ var.value.get('support_email') }}",
    subject="UK Sales Data Load  - Failed",
    html_content="UK Sales Data Load Failed. Please check the logs for more details.",
    dag=dag,
    trigger_rule="all_failed",
)

# Task for LatestOnlyOperator branch
latest_only = LatestOnlyOperator(
    task_id="latest_only", 
    dag=dag)

# Connect tasks
task_a >> task_b >> send_email_failed
task_b >> latest_only >> send_email_completed

