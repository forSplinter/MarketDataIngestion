FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
COPY ingestion ./ingestion

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "ingestion/alpaca_ingestion.py" ]
