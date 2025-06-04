import argparse 
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import(
    col, to_timestamp, hour, minute, second, dayofweek,unix_timestamp,array_contains,
    avg, stddev, sum as sum_, round as round_, to_date
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

def transform_recommendation(df):
    df = df.withColumn("period_date", to_date(col("period"), "yyyy-MM-dd")) 
    
    df = df.withColumn("timestamp_period", unix_timestamp(col("period_date"))) 

    df = df.withColumn("total_recommendations", col("buy") + col("sell") + col("hold") + col("strongBuy") + col("strongSell"))

    df = df.withColumn("buy_ratio", round_((col("buy") + col("strongBuy")) / col("total_recommendations"), 4))
    df = df.withColumn("sell_ratio", round_((col("sell") + col("strongSell")) / col("total_recommendations"), 4))

    return df

def main(symbol, event_date, input_path, output_path):
    spark = init_spark()

    print(f"symbol={symbol}, event_date={event_date}")

    input_full_path = f"{input_path}/"
    df = spark.read.parquet(input_full_path).filter(
        (col("symbol") == symbol) & (col("event_date") == event_date)
    )
    
    df_final = transform_recommendation(df)
    df_final.write.mode("overwrite").partitionBy("symbol", "event_date")\
        .parquet(f"{output_path}")
    
    print(f"Trusted reco:{output_path}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform Finnub recommendations data.")
    parser.add_argument("--symbol", type=str, required=True, help="Stock symbol to filter by.")
    parser.add_argument("--event_date", type=str, required=True, help="Event date to filter by (YYYY-MM-DD).")
    parser.add_argument("--input_path", type=str, default="s3a://raw/FINNHUB_API", help="Input path for the raw data.")
    parser.add_argument("--output_path", type=str,default="s3a://trusted/FINNHUB_DATA", help="Output path for the transformed data.")

    args = parser.parse_args()
    
    main(args.symbol, args.event_date, args.input_path, args.output_path)