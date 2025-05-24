from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


dag = DAG(
    'depends_on_past',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 21 * * *',
    catchup=False,
)

# Define tasks
task_a = PythonOperator(
    task_id="task_a",
    python_callable=lambda: print("Executing Task A"),
    dag=dag,
)

task_b = PythonOperator(
    task_id="task_b",
    python_callable=lambda: print("Executing Task B"),
    depends_on_past=True,
    dag=dag,
)


# Connect tasks
task_a >> task_b
