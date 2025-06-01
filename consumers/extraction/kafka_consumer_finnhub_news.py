import os
import json
from datetime import datetime, timezone
from kafka import KafkaConsumer
import boto3
from collections import defaultdict

print("[DEBUG] Script started")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
BUCKET_NAME = "raw"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "finnhub_news_stream")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="finnhub-news-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)
print("Listening to finnhub_news_stream...")

for msg in consumer:
    print(f"[RECEIVED] {msg.value}")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name="eu-west-1",
)


def upload_to_minio(symbol, event_date, messages):
    path = f"FINNHUB_API/NEWS/{symbol}/event_date={event_date}/"
    filename = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + ".json"
    key = path + filename

    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(messages, indent=2).encode("utf-8"),
        )
        print(f"Uploaded {len(messages)} news messages to s3://{BUCKET_NAME}/{key}")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")


def consume_messages():
    print(f"Listening to topic: {KAFKA_TOPIC}")
    buffer = defaultdict(list)
    message_count = 0

    for msg in consumer:
        try:
            value = msg.value
            symbol = value.get("symbol")
            event_date = value.get("event_date")

            message_count += 1
            print(f"[INFO] Total message recived {message_count}")

            if not (symbol and event_date):
                print(f"Skipping malformed news message: {value}")
                continue

            key = (symbol, event_date)
            buffer[key].append(value)

            if len(buffer[key]) >= 10:
                upload_to_minio(symbol, event_date, buffer[key])
                buffer[key].clear()

        except Exception as e:
            print(f" Error processing news message: {e}")


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
