FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
COPY extraction ./extraction
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "kafka_consumer_alpaca.py" ]

