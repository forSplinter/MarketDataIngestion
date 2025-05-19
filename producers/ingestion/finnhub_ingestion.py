import os 
import json
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
import finnhub


FINNUB_API_KEY = os.getenv("FINNHUB_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "finnhub_raw_stream")

if not FINNUB_API_KEY:
    raise ValueError("Finnhub API key not set in environment variables.")

client = finnhub.Client(api_key=FINNUB_API_KEY)

producer = KafkaProducer(

