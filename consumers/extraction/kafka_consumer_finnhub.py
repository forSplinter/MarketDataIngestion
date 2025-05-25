from collections import defaultdict
import os
import json
from kafka import KafkaConsumer
from datetime import datetime, timezone
import boto3

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "broker:29092")
KAFKA_TOPICS = os.getenv(
    "KAFKA_TOPICS", "finnhub_raw_stream,finnhub_news_stream"
).split(",")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")

AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = "raw"

consumer = KafkaConsumer(
    *KAFKA_TOPICS,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="finnhub-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name="eu-west-1",
)


def upload_to_minio(data_type, symbol, event_date, messages):
    path = f"finnhub/{data_type}/symbol={symbol}/event_date={event_date}/"
    filename = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + ".json"
    key = path + filename
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(messages, indent=2).encode("utf-8"),
        )
        print(
            f"Uploaded {len(messages)} {data_type} messages to s3://{BUCKET_NAME}/{key}"
        )
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")


def consume_messages():
    print(f"Listening to topics: {KAFKA_TOPICS}")
    buffer = defaultdict(list)

    for msg in consumer:
        try:
            value = msg.value
            data_type = value.get("type")
            symbol = value.get("symbol")
            event_date = value.get("event_date")

            if not (data_type and symbol and event_date):
                print(f"Skipping malformed message: {value}")
                continue

            key = (data_type, symbol, event_date)
            buffer[key].append(value)

            if len(buffer[key]) >= 100:
                upload_to_minio(data_type, symbol, event_date, buffer[key])
                buffer[key].clear()

        except Exception as e:
            print(f"Error processing message: {e}")


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
