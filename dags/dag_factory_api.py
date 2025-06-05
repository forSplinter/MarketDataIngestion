from airflow import DAG
import pendulum
from airflow.providers.amazon.aws.sensors.s3 import S3KeysUnchangedSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
import logging

log = logging.getLogger(__name__)

BUCKET_NAME = "raw"
local_tz = "Europe/Paris"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def create_alpaca_finnhub_dag(dag_id, symbol):
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"DAG to transform and aggregate data for {symbol}",
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False,
        tags=[symbol, "Alpaca", "Finnhub", "MarketEnrich"],
    ) as dag:
        wait_for_alpaca = S3KeysUnchangedSensor(
            task_id="alpaca_sensor",
            bucket_name=BUCKET_NAME,
            prefix="ALPACA_API/{{ params.symbol }}/event_date={{ macros.ds_add(ds, -1) }}/",
            aws_conn_id="minio_conn",
            inactivity_period=60,
            min_objects=1,
            poke_interval=60,
            timeout=60 * 60,
            params={"symbol": symbol},
        )

        wait_for_finnhub = S3KeysUnchangedSensor(
            task_id="finnhub_sensor",
            bucket_name=BUCKET_NAME,
            prefix="FINNHUB_API/RECO/{{ params.symbol }}/event_date={{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}/",
            aws_conn_id="minio_conn",
            inactivity_period=60,
            min_objects=1,
            poke_interval=60,
            timeout=60 * 60,
            params={"symbol": symbol},
        )

        transform_alpaca = SparkSubmitOperator(
            task_id="transform_alpaca_data",
            application="/opt/bitnami/spark/jobs/job_alpaca_transform.py",
            name="transform_alpaca_data",
            conn_id="spark_conn",
            verbose=True,
            packages="org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901",
            application_args=[
                "--symbol",
                symbol,
                "--event_date",
                "{{ macros.ds_add(ds, -1) }}",
                "--input_path",
                "s3a://{}/ALPACA_API/{{{{ params.symbol }}}}/event_date={{{{ macros.ds_add(ds, -1) }}}}/".format(
                    BUCKET_NAME
                ),
                "--output_path",
                "s3a://trusted/ALPACA_DATA",
            ],
            params={"symbol": symbol},
        )

        wait_for_trusted_alpaca = S3KeysUnchangedSensor(
            task_id="wait_for_trusted_alpaca",
            bucket_name="trusted",
            prefix="ALPACA_DATA/symbol={{ params.symbol }}/event_date={{ macros.ds_add(ds, -1) }}/",
            aws_conn_id="minio_conn",
            inactivity_period=60,
            min_objects=1,
            poke_interval=60,
            timeout=60 * 60,
            params={"symbol": symbol},
        )

        aggregate_alpaca = SparkSubmitOperator(
            task_id="aggregate_alpaca",
            application="/opt/bitnami/spark/jobs/job_alpaca_aggregates.py",
            name="aggregate_alpaca",
            conn_id="spark_conn",
            verbose=True,
            packages="org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901",
            application_args=[
                "--symbol",
                symbol,
                "--event_date",
                "{{ macros.ds_add(ds, -1) }}",
                "--input_path",
                "s3a://trusted/ALPACA_DATA",
                "--output_path",
                "s3a://trusted/ALPACA_AGG",
                "--window_size",
                "1 minute",
            ],
            params={"symbol": symbol},
        )

        wait_for_alpaca_agg = S3KeysUnchangedSensor(
            task_id="wait_for_alpaca_agg",
            bucket_name="trusted",
            prefix="ALPACA_AGG/symbol={{ params.symbol }}/event_date={{ macros.ds_add(ds, -1) }}/",
            aws_conn_id="minio_conn",
            inactivity_period=60,
            min_objects=1,
            poke_interval=60,
            timeout=60 * 60,
            params={"symbol": symbol},
        )

        transform_finnhub = SparkSubmitOperator(
            task_id="transform_finnhub_data",
            application="/opt/bitnami/spark/jobs/job_finnhub_transform.py",
            name="transform_finnhub_data",
            conn_id="spark_conn",
            verbose=True,
            packages="org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901",
            application_args=[
                "--symbol",
                symbol,
                "--event_date",
                "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}",
                "--input_path",
                "s3a://{}/FINNHUB_API/RECO/{{{{ params.symbol }}}}/event_date={{{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}}}/".format(
                    BUCKET_NAME
                ),
                "--output_path",
                "s3a://trusted/FINNHUB_DATA",
            ],
            params={"symbol": symbol},
        )

        wait_for_trusted_finnhub = S3KeysUnchangedSensor(
            task_id="wait_for_trusted_finnhub",
            bucket_name="trusted",
            prefix="FINNHUB_DATA/symbol={{ params.symbol }}/event_date={{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}/",
            aws_conn_id="minio_conn",
            inactivity_period=60,
            min_objects=1,
            poke_interval=60,
            timeout=60 * 60,
            params={"symbol": symbol},
        )

        aggregate_finnhub = SparkSubmitOperator(
            task_id="aggregate_finnhub",
            application="/opt/bitnami/spark/jobs/job_finnhub_aggregates.py",
            name="aggregate_finnhub",
            conn_id="spark_conn",
            verbose=True,
            packages="org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901",
            application_args=[
                "--symbol",
                symbol,
                "--event_date",
                "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}",
                "--input_path",
                "s3a://trusted/FINNHUB_DATA",
                "--output_path",
                "s3a://trusted/FINNHUB_AGG",
            ],
            params={"symbol": symbol},
        )

        wait_for_finnhub_agg = S3KeysUnchangedSensor(
            task_id="wait_for_finnhub_agg",
            bucket_name="trusted",
            prefix="FINNHUB_AGG/symbol={{ params.symbol }}/event_date={{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}/",
            aws_conn_id="minio_conn",
            inactivity_period=60,
            min_objects=1,
            poke_interval=60,
            timeout=60 * 60,
            params={"symbol": symbol},
        )

        aggregate_market = SparkSubmitOperator(
            task_id="aggregate_market",
            application="/opt/bitnami/spark/jobs/job_join_api.py",
            name="aggregate_market",
            conn_id="spark_conn",
            verbose=True,
            packages="org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901",
            application_args=[
                "--symbol",
                symbol,
                "--event_date",
                "{{ macros.ds_add(ds, -1) }}",
                "--alpaca_path",
                "s3a://trusted/ALPACA_AGG",
                "--finnhub_path",
                "s3a://trusted/FINNHUB_AGG",
                "--output_path",
                "s3a://enriched/MARKET_ENRICHED",
            ],
            params={"symbol": symbol},
        )

        (
            (wait_for_alpaca >> transform_alpaca >> wait_for_trusted_alpaca)
            >> aggregate_alpaca
            >> wait_for_alpaca_agg
            >> aggregate_market
        )
        (
            (wait_for_finnhub >> transform_finnhub >> wait_for_trusted_finnhub)
            >> aggregate_finnhub
            >> wait_for_finnhub_agg
            >> aggregate_market
        )

        return dag


SYMBOLS = Variable.get("SYMBOLS_TO_RUN", deserialize_json=True)

log.info(f"Generating DAGs for symbols: {SYMBOLS}")

for symbol in SYMBOLS:
    dag_id = f"alpaca_transform_{symbol}"
    globals()[dag_id] = create_alpaca_finnhub_dag(dag_id, symbol)
    log.info(f"Created DAG: {dag_id}")
