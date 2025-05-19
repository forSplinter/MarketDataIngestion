import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'alpaca',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
with DAG(
    dag_id='test_airflow',
    default_args=default_args,
    description='A simple test DAG for Airflow',
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='@daily'
) as dag:
    hello_task = BashOperator(
        task_id='hello_task',
        bash_command='echo "Hello, Airflow!"'
    )
    hello_task
 