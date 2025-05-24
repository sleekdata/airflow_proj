from airflow import DAG, Dataset
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from airflow.utils.dates import days_ago
from airflow.models import Variable

dag = DAG(
    "data_producer_dag",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 23 * * *",
)

http_to_s3_task = HttpToS3Operator(
    task_id="data_producer_task",
    endpoint=Variable.get("web_api_key"),
    s3_bucket="sleek-data",
    s3_key="oms/xrate.json",
    aws_conn_id="aws_conn",
    http_conn_id=None,
    replace=True,
    dag=dag,
    outlets=[Dataset("s3://sleek-data/oms/xrate.json")]
)
