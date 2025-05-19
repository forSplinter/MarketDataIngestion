from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

BUCKET_NAME = 'raw'
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA", "AMZN"]

def list_files_and_check(**context):
    # event_date = context['ds']  
    event_date = '2025-05-02' 
    s3 = S3Hook(aws_conn_id='minio_s3_conn')
    
    all_present = True
    for symbol in SYMBOLS:
        prefix = f"{symbol}/event_date={event_date}/"
        files = s3.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)
        if not files:
            logging.warning(f"No files found for {symbol} at {prefix}")
            all_present = False
        else:
            logging.info(f"Found {len(files)} files for {symbol} at {prefix}")
    
    if not all_present:
        raise ValueError("Some symbols are missing data!")

# DAG configuration
default_args = {
    'owner': 'alpaca',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dag_alpaca_check_symbols',
    default_args=default_args,
    description="Sensor-like check for all symbol data in MinIO raw bucket",
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['minio', 'raw', 'alpaca'],
) as dag:

    check_all_symbols = PythonOperator(
        task_id='check_all_symbols_for_event_date',
        python_callable=list_files_and_check,
        provide_context=True,
    )

