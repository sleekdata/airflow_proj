from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.github.operators.github import GithubOperator
from airflow.operators.dummy import DummyOperator
import logging

# Define the DAG
dag = DAG(
    'git_repo_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 21 * * *',
    catchup=False
)

# Start Dummy Operator
start = DummyOperator(task_id='start', dag=dag)

# List GitRepository Tags
list_repo_tags = GithubOperator(
    task_id="list_repo_tags",
    github_method="get_repo",
    github_method_args={"full_name_or_id": "sunapana2003/airflow-demo"},
    result_processor=lambda repo: logging.info(list(repo.get_tags())),
    dag=dag,
)

# End Dummy Operator
end = DummyOperator(task_id='end', dag=dag)

# Define task dependencies
start >> list_repo_tags >> end