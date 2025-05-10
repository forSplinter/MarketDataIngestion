from collections import defaultdict
import os
import json
from kafka import KafkaConsumer
from datetime import datetime, timedelta, timezone 
import boto3
from io import BytesIO

import sys
sys.path.append('/app')

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "alpaca_raw_stream")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = "raw"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="alpaca-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name="eu-west-1",
)

def upload_to_minio(symbol,event_date , messages):
    filename = f"{symbol}/event_date={event_date}/{datetime.now(timezone.utc).isoformat()}.json"
    json_data = json.dumps(messages, indent=2).encode("utf-8")
    s3.put_object(Bucket=BUCKET_NAME, Key=filename, Body=json_data)
    print(f"Saved {len(messages)} records to s3://{BUCKET_NAME}/{filename}")

def consume_messages():
    print("Starting Kafka consumer...")
    buffer = defaultdict(list)
    mssg = {}
    for mssg in consumer:
        symbol = mssg.value["symbol"]
        event_date = mssg.value["event_date"]
        buffer[(symbol, event_date)].append(mssg.value)
        
        # Check if we have 1000 messages for a symbol or if the market is closed
        if len(buffer[(symbol, event_date)]) >= 1000:
            upload_to_minio(symbol, event_date,buffer[(symbol, event_date)])
            buffer[(symbol, event_date)].clear() 

if __name__ == "__main__":
    # Create bucket if it doesn't exist
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
    except Exception as e:
        print(f"Bucket {BUCKET_NAME} already exists or could not be created: {e}")
    
    consume_messages()
    print("Kafka consumer stopped.")