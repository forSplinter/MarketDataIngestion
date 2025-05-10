import os
import json
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
from alpaca.data import StockHistoricalDataClient, StockTradesRequest

# ENV vars
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "alpaca_raw_stream")

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA", "AMZN"]

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    raise ValueError("Alpaca API credentials not set in environment variables.")

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
        "event_date": trade.timestamp.date().isoformat(),
    }

def get_previous_market_day():
    """Retourne la dernière date de marché ouvré (exclut les week-ends)."""
    day = datetime.now(timezone.utc) - timedelta(days=1)
    while day.weekday() >= 5:  # 5 = samedi, 6 = dimanche
        day -= timedelta(days=1)
    return day

def run_stream():
    market_day = get_previous_market_day()
    start = market_day.replace(hour=13, minute=30, second=0, microsecond=0)  # 9h30 NY (UTC)
    end = market_day.replace(hour=20, minute=0, second=0, microsecond=0)     # 16h NY (UTC)

    print(f"Fetching trades for {market_day.date()} from {start} to {end}")

    for symbol in SYMBOLS:
        try:
            request = StockTradesRequest(symbol_or_symbols=symbol, start=start, end=end)
            result = client.get_stock_trades(request)
            trades = result.data.get(symbol, [])

            print(f"Fetched {len(trades)} trades for {symbol}")
            for trade in trades:
                serialized = serialize_trade(symbol, trade)
                producer.send(KAFKA_TOPIC, value=serialized)

        except Exception as e:
            print(f"Error fetching trades for {symbol}: {e}")

    producer.flush()
    print("All trades sent to Kafka.")

if __name__ == "__main__":
    run_stream()
