import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    hour,
    minute,
    second,
    dayofweek,
    unix_timestamp,
    array_contains,
    avg,
    stddev,
    sum as sum_,
)
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
BUCKET_NAME = "raw"


def init_spark():
    spark = (
        SparkSession.builder.appName("MarketDataIngestion")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("Spark session initialized with S3A configuration.")
    return spark


def transform_data(df):
    df = df.withColumn("timestamp_parsed", to_timestamp(col("timestamp")))
    df = df.withColumn("hour", hour(col("timestamp_parsed")))
    df = df.withColumn("minute", minute(col("timestamp_parsed")))
    df = df.withColumn("second", second(col("timestamp_parsed")))
    df = df.withColumn("day_of_week", dayofweek(col("timestamp_parsed")))
    df = df.withColumn("timestamp_epoch", unix_timestamp(col("timestamp_parsed")))

    exchange_indexer = StringIndexer(inputCol="exchange", outputCol="exchange_index")
    df = exchange_indexer.fit(df).transform(df)

    tape_indexer = StringIndexer(inputCol="tape", outputCol="tape_index")
    df = tape_indexer.fit(df).transform(df)

    df = df.withColumn(
        "condition_at", array_contains(col("conditions"), "@").cast("int")
    )
    df = df.withColumn(
        "condition_i", array_contains(col("conditions"), "I").cast("int")
    )
    df = df.withColumn(
        "condition_t", array_contains(col("conditions"), "T").cast("int")
    )

    window_spec = (
        Window.partitionBy("symbol", "event_date")
        .orderBy("timestamp_parsed")
        .rowsBetween(-10, 0)
    )

    df = df.withColumn("rolling_avg_price", avg(col("price")).over(window_spec))
    df = df.withColumn("rolling_std_price", stddev(col("price")).over(window_spec))
    df = df.withColumn("cumulative_volume", sum_(col("size")).over(window_spec))

    return df


def select_final_columns(df):
    return df.select(
        "symbol",
        "event_date",
        "price",
        "size",
        "exchange_index",
        "tape_index",
        "condition_at",
        "condition_i",
        "condition_t",
        "hour",
        "minute",
        "second",
        "day_of_week",
        "timestamp_epoch",
        "rolling_avg_price",
        "rolling_std_price",
        "cumulative_volume",
    )


def main(symbol, event_date, input_path, output_path):
    spark = init_spark()

    print(f"Symbol={symbol}, event_date={event_date}")

    input_full_path = os.path.join(input_path, "*.json")
    df = spark.read.option("multiline", "true").json(input_full_path)

    df_transformed = transform_data(df)

    df_final = select_final_columns(df_transformed)

    df_final.write.mode("overwrite").partitionBy("symbol", "event_date").parquet(
        output_path
    )

    print(f"Path: {output_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL job for Alpaca")
    parser.add_argument(
        "--symbol", type=str, required=True, help="Symbol (ticker) to process"
    )
    parser.add_argument(
        "--event_date", type=str, required=True, help="Event date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--input_path", type=str, default="s3a://raw//ALPACA_API", help="Input S3 path"
    )
    parser.add_argument(
        "--output_path",
        type=str,
        default="s3a://trusted/ALPACA_DATA",
        help="Output S3 path",
    )

    args = parser.parse_args()

    main(
        symbol=args.symbol,
        event_date=args.event_date,
        input_path=args.input_path,
        output_path=args.output_path,
    )
