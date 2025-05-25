import os
import json
from datetime import date, datetime, time, timedelta, timezone
from socket import PF_PACKET
from kafka import KafkaProducer
import finnhub

FINNUB_API_KEY = os.getenv("FINNHUB_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "broker:29092")
KAFKA_RECO_TOPIC = os.getenv("KAFKA_TOPIC", "finnhub_raw_stream")
KAFKA_NEWS_TOPIC = os.getenv("KAFKA_TOPIC", "finnhub_news_stream")

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA", "AMZN"]

if not FINNUB_API_KEY:
    raise ValueError("Finnhub API key not set in environment variables.")

client = finnhub.Client(api_key=FINNUB_API_KEY)


def get_market_day():
    # TODO: set this in a utils
    day = datetime.now(timezone.utc) - timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def serialize_reco(symbol: str, reco: dict) -> dict:
    return {
        "symbol": symbol,
        "event_date": reco["period"],
        "type": "RECO",
        "payload": reco,
    }


def serialize_news(symbol: str, news: dict) -> dict:
    event_date = datetime.utcfromtimestamp(news["datetime"]).strftime("%Y-%m-%d")
    return {"symbol": symbol, "event_date": event_date, "type": "NEWS", "payload": news}


producer = KafkaProducer(
    # TODO: set this in a utils
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
)


def run_stream():
    market_day = get_market_day()
    date_str = market_day.strftime("%Y-%m-%d")
    print(f"Fetching trades for {date_str}")

    for symbol in SYMBOLS:
        try:
            recos = client.recommendation_trends(symbol)
            for reco in recos:
                message = serialize_reco(symbol, reco)
                producer.send(KAFKA_RECO_TOPIC, message)
            print(f"sent {len(recos)} recommendation for {symbol}")

            news_item = client.company_news(symbol, _from=date_str, to=date_str)
            for news in news_item:
                message = serialize_news(symbol, news)
                producer.send(KAFKA_NEWS_TOPIC, message)
            print(f"sent {len(news_item)} news for {symbol}")

        except Exception as e:
            print(f"Error fetching trades for {symbol}: {e}")

        producer.flush()
        print("All message sent.")


if __name__ == "__main__":
    run_stream()
