# Import
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago

# Define the DAG
dag = DAG(
    'live_exchange_rates',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
)

# Define the Tasks
fetch_exchange_rates = BashOperator(
    task_id='fetch_exchange_rates',
    bash_command="curl '{{ var.value.get('web_api_key') }}' -o xrate.json",
    cwd='/tmp',
    dag=dag,
)

send_email_task = EmailOperator(
    task_id='send_email',
    to="{{ var.value.get('support_email') }}",
    subject='Live Exchange Rate Download - Successful.',
    html_content='Live Exchange Rate data has been successfully downloaded.',
    dag=dag,
)



# Define the Dependencies
fetch_exchange_rates >> send_email_task
