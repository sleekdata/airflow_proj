from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


dag = DAG(
    'setup_teardown',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 21 * * *',
    catchup=False
)

# Define tasks
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=lambda: print("Creating Cluster"),
    dag=dag,
)

run_query1 = PythonOperator(
    task_id='run_query1',
    python_callable=lambda: print("Running Query 1"),
    dag=dag,
)

# run_query2 = PythonOperator(
#     task_id='run_query2',
#     python_callable=lambda: print("Running Query 2"),
#     dag=dag,
# )

run_query2 = PythonOperator(
    task_id='run_query2',
    python_callable=lambda: raise_exception("Failure in Query 2"),
    dag=dag,
)


run_query3 = PythonOperator(
    task_id='run_query3',
    python_callable=lambda: print("Running Query 3"),
    dag=dag,
)

delete_cluster = PythonOperator(
    task_id='delete_cluster',
    python_callable=lambda: print("Deleting Cluster"),
    dag=dag,
)

# Define task dependencies
create_cluster >> [run_query1, run_query2]
[run_query1, run_query2] >> run_query3
run_query3 >> delete_cluster.as_teardown(setups=create_cluster)
