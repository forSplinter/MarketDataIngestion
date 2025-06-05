import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, to_date, lit
from pyspark.sql.window import Window

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")


def init_spark():
    spark = (
        SparkSession.builder.appName("FinnhubAggregateJob")
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


def main(symbol: str, input_path: str, output_path: str, event_date: str):
    spark = init_spark()
    print(f"symbol={symbol}, event_date={event_date}")

    input_full_path = f"{input_path}/symbol={symbol}/*"

    df = spark.read.option("basePath", input_path).parquet(input_full_path)

    print("Schema of the DataFrame:")
    df.printSchema()

    print("Available periods in data:")
    df.select("event_date").distinct().show(10, False)

    window_spec = Window.partitionBy("symbol")

    df_with_max_date = df.withColumn(
        "max_event_date", spark_max("event_date").over(window_spec)
    )

    event_date_literal = to_date(lit(event_date), "yyyy-MM-dd")

    df_selected = df_with_max_date.filter(
        (col("event_date") <= event_date_literal)
        & (col("event_date") == col("max_event_date"))
    )

    print("Selected rows after filtering:")
    df_selected.show(10, False)

    df_final = df_selected.select(
        col("symbol"),
        col("event_date"),
        col("buy"),
        col("hold"),
        col("sell"),
        col("strongBuy"),
        col("strongSell"),
    )

    df_final.write.mode("overwrite").partitionBy("symbol", "event_date").parquet(
        output_path
    )

    print(f"FINNHUB transform completed. Output saved to: {output_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Finnhub Aggregate Job")
    parser.add_argument("--symbol", required=True, help="Symbol (ticker) to process")
    parser.add_argument("--event_date", required=True, help="Event date (YYYY-MM-DD)")
    parser.add_argument(
        "--input_path",
        type=str,
        default="s3a://trusted/FINNHUB_DATA",
        help="Input trusted path",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        default="s3a://trusted/FINNHUB_AGG",
        help="Output aggregate path",
    )

    args = parser.parse_args()

    main(
        symbol=args.symbol,
        event_date=args.event_date,
        input_path=args.input_path,
        output_path=args.output_path,
    )

