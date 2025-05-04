import os
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from alpaca.data import StockHistoricalDataClient, StockTradesRequest

# ENV vars
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "alpaca_raw_stream")

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA", "AMZN"]
INTERVAL_SECONDS = 5

# Initialize client & producer
client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
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
    now = datetime.utcnow()
    return now.hour >= 14 and now.hour < 21  # 14h30–21h00 UTC approx

def runStream():
    print("Starting Alpaca producer stream...")
    last_fetch = datetime.utcnow() - timedelta(seconds=INTERVAL_SECONDS)

    while marketOpen():
        now = datetime.utcnow()
        print(f"[{now}] Fetching trades since {last_fetch}")
        for symbol in SYMBOLS:
            try:
                request = StockTradesRequest(
                    symbol_or_symbols=symbol,
                    start=last_fetch,
                    end=now
                )
                result = client.get_stock_trades(request)
                trades = result.data.get(symbol, [])
                print(f"{len(trades)} trades for {symbol}")
                for trade in trades:
                    serialized = serialize_trade(symbol, trade)
                    producer.send(KAFKA_TOPIC, value=serialized)
            except Exception as e:
                print(f"Error fetching trades for {symbol}: {e}")

        last_fetch = now
        producer.flush()
        time.sleep(INTERVAL_SECONDS)

    print("Market closed. Stopping stream.")

if __name__ == "__main__":
    runStream()

# request_params = StockTradesRequest(
#     symbol_or_symbols= "AAPL",
#     start=datetime(2024, 1, 30, 14, 30),
#     end=datetime(2024, 1, 30, 14, 45)
# )

# trades = data_client.get_stock_trades(request_params)

# for trade in trades.data["AAPL"]:
#     print(trade)
#     break