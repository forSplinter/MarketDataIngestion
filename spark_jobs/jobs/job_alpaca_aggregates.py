import argparse 
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, sum as sum_, avg as avg_, count, stddev, max as max_, min as min_, first, last
)
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
BUCKET_NAME = "raw"

def init_spark():
   spark = SparkSession.builder \
        .appName("MarketDataIngestion") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

   spark.sparkContext.setLogLevel("WARN")
   print("Spark session initialized with S3A configuration.")
   return spark

def compute_market_aggregates(df, window_size="1 minute"):
    df_windowed = df.groupBy(
        window(col("timestamp_parsed"), window_size),
        col("symbol"),
        col("event_date")
    ).agg(
        first("price").alias("open_price"),
        max_("price").alias("high_price"),
        min_("price").alias("low_price"),
        last("price").alias("close_price"),
        (sum_(col("price") * col("size")) / sum_("size")).alias("vwap"),
        sum_("size").alias("volume_sum"),
        count("*").alias("trade_count"),
        stddev("price").alias("volatility_price")
    )
    return df_windowed

def main(symbol, event_date, input_path, output_path, window_size):
    spark = init_spark()

    print(f"symbol={symbol}, event_date={event_date}, window={window_size}")

    input_full_path = f"{input_path}/"
    df = spark.read.parquet(input_full_path).filter(
        (col("symbol") == symbol) & (col("event_date") == event_date)
    )

    if "timestamp_parsed" not in df.columns:
        from pyspark.sql.functions import to_timestamp
        df = df.withColumn("timestamp_parsed", to_timestamp(col("timestamp_epoch").cast("timestamp")))

    df_agg = compute_market_aggregates(df, window_size=window_size)

    df_agg.write.mode("overwrite").partitionBy("symbol", "event_date") \
        .parquet(output_path)

    print(f"Output: {output_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Market Data Aggregates Job")
    parser.add_argument("--symbol", type=str, required=True, help="Symbol (ticker) to process")
    parser.add_argument("--event_date", type=str, required=True, help="Event date (YYYY-MM-DD)")
    parser.add_argument("--input_path", type=str, default="s3a://trusted/ALPACA_DATA", help="Input trusted path")
    parser.add_argument("--output_path", type=str, default="s3a://trusted/ALPACA_AGG", help="Output agg path")
    parser.add_argument("--window_size", type=str, default="1 minute", help="Window size (ex: '1 minute', '5 minutes')")

    args = parser.parse_args()

    main(
        symbol=args.symbol,
        event_date=args.event_date,
        input_path=args.input_path,
        output_path=args.output_path,
        window_size=args.window_size
    )

