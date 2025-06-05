from sys import prefix
from airflow import DAG
import pendulum
from airflow.providers.amazon.aws.sensors.s3 import S3KeysUnchangedSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

BUCKET_NAME = "raw"
SYMBOL = "GOOGL"
ALPACA_KEY = "ALPACA_API/GOOGL/event_date={{macros.ds_add(ds, -2) }}/"
FINNHUB_KEY = "FINNHUB_API/RECO/GOOGL/event_date={{macros.ds_add(ds, -2) }}/"

local_tz = "Europe/Prague"  # Adjust to your local timezone if needed
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="alpaca_transform_GOOGL",
    default_args=default_args,
    description="DAG to transform and aggregate GOOGL data from Alpaca and Finnhub",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["GOOGL", "Alpaca", "Finnhub"],
) as dag:
    wait_for_alpaca = S3KeysUnchangedSensor(
        task_id="alpaca_sensor",
        bucket_name=BUCKET_NAME,
        prefix=ALPACA_KEY,
        aws_conn_id="minio_conn",  # Ensure this connection is set up in Airflow
        inactivity_period=60,
        min_objects=1,
        poke_interval=60,
        timeout=60 * 60,
    )

    wait_for_finnhub = S3KeysUnchangedSensor(
        task_id="sensor_finnhub",
        bucket_name=BUCKET_NAME,
        prefix=FINNHUB_KEY,
        aws_conn_id="minio_conn",
        inactivity_period=60,
        min_objects=1,
        poke_interval=60,
        timeout=60 * 60,
    )

    transform_alpaca = SparkSubmitOperator(
        task_id="transform_alpaca_data",
        application="/opt/bitnami/spark/jobs/job_alpaca_transform.py",
        # conf={"spark.master": "spark://spark:7077"},
        name="transform_alpaca_data",
        conn_id="spark_conn",
        application_args=[f"s3://{BUCKET_NAME}/{ALPACA_KEY}"],
        dag=dag,
    )

    transform_finnhub = SparkSubmitOperator(
        task_id="transform_finnhub_data",
        application="/opt/bitnami/spark/jobs/job_finnhub_transform.py",
        # conf={"spark.master": "spark://spark:7077"},
        name="transform_finnhub_data",
        conn_id="spark_conn",
        application_args=[f"s3://{BUCKET_NAME}/{FINNHUB_KEY}"],
        dag=dag,
    )

    aggregate_data = SparkSubmitOperator(
        task_id="aggregate_data",
        application="/opt/bitnami/spark/jobs/job_alpaca_aggregates.py",
        name="aggregate_data",
        conn_id="spark_conn",
        application_args=[],
        dag=dag,
    )

    (wait_for_alpaca >> transform_alpaca) >> aggregate_data
    (wait_for_finnhub >> transform_finnhub) >> aggregate_data
