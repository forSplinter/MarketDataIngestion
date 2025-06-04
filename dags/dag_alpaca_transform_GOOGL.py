from airflow import DAG
import pendulum
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

BUCKET_NAME = 'raw'
SYMBOL = 'GOOGL'
ALPACA_KEY = f'ALPACA_API/{SYMBOL}/event_date={{ ds }}/'
FINNHUB_KEY = f'FINNHUB_API/RECO/{SYMBOL}/event_date={{ ds }}/'

local_tz = 'Europe/Prague'  # Adjust to your local timezone if needed
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='alpaca_transform_GOOGL',
    default_args=default_args,
    description='DAG to transform and aggregate GOOGL data from Alpaca and Finnhub',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['GOOGL', 'Alpaca', 'Finnhub'],
) as dag:

    wait_for_alpaca = S3KeySensor(
        task_id='alpaca_sensor',
        bucket_key=ALPACA_KEY,
        bucket_name=BUCKET_NAME,
        aws_conn_id='minio_s3_conn', # Ensure this connection is set up in Airflow
        poke_interval=60,
        timeout=60*60,
        mode='poke'
    )

    wait_for_finnhub = S3KeySensor(
        task_id='sensor_finnhub',
        bucket_key=FINNHUB_KEY,
        bucket_name=BUCKET_NAME,
        aws_conn_id='minio_s3_conn',
        poke_interval=60,
        timeout=60*60,
        mode='poke'
    )

    transform_alpaca = SparkSubmitOperator(
        task_id='transform_alpaca_data',
        application='/path/to/your/spark_transform_alpaca.py',
        name='transform_alpaca_data',
        conn_id='spark_conn',
        application_args=[f's3://{BUCKET_NAME}/{ALPACA_KEY}'],
        dag=dag
    )

    transform_finnhub = SparkSubmitOperator(
        task_id='transform_finnhub_data',
        application='/path/to/your/spark_transform_finnhub.py',
        name='transform_finnhub_data',
        conn_id='spark_conn',
        application_args=[f's3://{BUCKET_NAME}/{FINNHUB_KEY}'],
        dag=dag
    )

    aggregate_data = SparkSubmitOperator(
        task_id='aggregate_data',
        application='/path/to/your/spark_aggregate.py',
        name='aggregate_data',
        conn_id='spark_conn',
        application_args=[],
        dag=dag
    )

    [wait_for_alpaca, wait_for_finnhub] >> [transform_alpaca, transform_finnhub] >> aggregate_data