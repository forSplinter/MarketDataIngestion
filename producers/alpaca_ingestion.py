import os
import json
import time
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
from alpaca.data import StockHistoricalDataClient, StockTradesRequest

# ENV vars
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "alpaca_raw_stream")

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA", "AMZN"]
INTERVAL_SECONDS = 5

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    raise ValueError("Credentials not set")

# Initialize client & producer
client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
)

def serialize_trade(symbol, trade):
    return {
        "symbol": symbol,
        "price": trade.price,
        "size": trade.size,
        "exchange": trade.exchange,
        "conditions": trade.conditions,
        "tape": trade.tape,
        "id": trade.id,
        "timestamp": trade.timestamp.isoformat(),
        "event_date": trade.timestamp.date().isoformat()
    }

def marketOpen():
    print("Checking if market is open...")
    now = datetime.now(timezone.utc)
    return 14 <= now.hour < 21 # 14h30–21h00 UTC approx


def runStream():
    print("Starting Alpaca producer stream...")
    now = datetime.now(timezone.utc) - timedelta(days=4)
    start = now.replace(hour=18, minute=0, second=0, microsecond=0)
    end = now.replace(hour=18, minute=5, second=0, microsecond=0)

    print(f"[{datetime.now()}]Fetching trades from {start} to {end}")

    for symbol in SYMBOLS:
        try:
            request = StockTradesRequest(
                symbol_or_symbols=symbol,
                start=start,
                end=end
            )
            result = client.get_stock_trades(request)
            trades = result.data.get(symbol, [])
            print(f"{len(trades)} trades for {symbol}")
            for trade in trades:
                serialized = serialize_trade(symbol, trade)
                producer.send(KAFKA_TOPIC, value=serialized)
        except Exception as e:
            print(f"Error fetching trades for {symbol}: {e}")

    producer.flush()
    print("Finished fetching trades.")


    # Uncomment the following lines to run the stream continuously

# def runStream():
#     print("Starting Alpaca producer stream...")
#     last_fetch = datetime.now(timezone.utc) - timedelta(seconds=INTERVAL_SECONDS)

#     while marketOpen():
#         now = datetime.now(timezone.utc)
#         print(f"[{now}] Fetching trades since {last_fetch}")
#         for symbol in SYMBOLS:
#             try:
#                 request = StockTradesRequest(
#                     symbol_or_symbols=symbol,
#                     start=last_fetch,
#                     end=now
#                 )
#                 result = client.get_stock_trades(request)
#                 trades = result.data.get(symbol, [])
#                 print(f"{len(trades)} trades for {symbol}")
#                 for trade in trades:
#                     serialized = serialize_trade(symbol, trade)
#                     producer.send(KAFKA_TOPIC, value=serialized)
#             except Exception as e:
#                 print(f"Error fetching trades for {symbol}: {e}")

#         last_fetch = now
#         producer.flush()
#         time.sleep(INTERVAL_SECONDS)

    print("Market closed. Stopping stream.")

if __name__ == "__main__":
    runStream()