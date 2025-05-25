from collections import defaultdict
import os
import json
from kafka import KafkaConsumer
from datetime import datetime, timezone
import boto3
from io import BytesIO

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "alpaca_raw_stream")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")

AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
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


def upload_to_minio(symbol, event_date, messages):
    path = f"{KAFKA_TOPIC}/symbol={symbol}/event_date={event_date}/"
    filename = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + ".json"
    key = path + filename
    json_data = json.dumps(messages, indent=2).encode("utf-8")

    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=json_data)
        print(f" Uploaded {len(messages)} records to s3://{BUCKET_NAME}/{key}")
    except Exception as e:
        print(f" Error uploading to MinIO: {e}")


def consume_messages():
    print(f"Starting Kafka consumer on topic '{KAFKA_TOPIC}'")
    buffer = defaultdict(list)

    for msg in consumer:
        try:
            data = msg.value
            symbol = data["symbol"]
            event_date = data["event_date"]
            buffer[(symbol, event_date)].append(data)

            if len(buffer[(symbol, event_date)]) >= 1000:
                upload_to_minio(symbol, event_date, buffer[(symbol, event_date)])
                buffer[(symbol, event_date)].clear()

        except Exception as e:
            print(f"Failed to process message: {e}")


if __name__ == "__main__":
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
    except s3.exceptions.ClientError:
        try:
            s3.create_bucket(Bucket=BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' created.")
        except Exception as e:
            print(f"Could not create bucket '{BUCKET_NAME}': {e}")

    consume_messages()
