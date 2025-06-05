import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql.window import Window

# Load credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")


def init_spark():
    spark = (
        SparkSession.builder.appName("MarketFinnhubTransformJob")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark session initialized with S3A configuration.")
    return spark


def main(symbol: str, event_date: str, input_path: str, output_path: str):
    spark = init_spark()
    print(f"Transforming FINNHUB data | symbol={symbol}, event_date={event_date}")

    # Load raw data
    df = spark.read.option("multiline", "true").json(f"{input_path}/*.json")

    print("🔍 Available periods in data:")
    df.select("payload.period").distinct().show(10, False)

    df_flat = df.select(
        col("symbol"),
        col("event_date"),
        col("payload.period").alias("period"),
        col("payload.buy").alias("buy"),
        col("payload.hold").alias("hold"),
        col("payload.sell").alias("sell"),
        col("payload.strongBuy").alias("strongBuy"),
        col("payload.strongSell").alias("strongSell"),
    ).filter(col("symbol") == symbol)

    print("After flattening:")
    df_flat.show(5)

    # Add max_event_date window
    window_spec = Window.partitionBy("symbol")
    df_with_max_date = df_flat.withColumn(
        "max_event_date", spark_max("period").over(window_spec)
    )

    # Filter on period <= event_date AND take latest period (max_event_date == period)
    df_selected = df_with_max_date.filter(
        (col("period") <= event_date) & (col("max_event_date") == col("period"))
    )

    print("Selected rows after filtering:")
    df_selected.show(5)

    df_final = df_selected.select(
        col("symbol"),
        col("period").alias("event_date"),
        col("buy"),
        col("hold"),
        col("sell"),
        col("strongBuy"),
        col("strongSell"),
    )

    df_final.write.mode("overwrite").partitionBy("symbol", "event_date").parquet(
        output_path
    )

    print(f" FINNHUB transform completed. Output saved to: {output_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Finnhub Transform Job")
    parser.add_argument("--symbol", required=True, help="Stock symbol (ex: AAPL)")
    parser.add_argument("--event_date", required=True, help="Event date (YYYY-MM-DD)")
    parser.add_argument(
        "--input_path",
        type=str,
        default="s3a://raw/FINNHUB_API",
        help="Input path for FINNHUB raw data",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        default="s3a://trusted/FINNHUB_DATA",
        help="Output path for FINNHUB trusted data",
    )

    args = parser.parse_args()

    main(
        symbol=args.symbol,
        event_date=args.event_date,
        input_path=args.input_path,
        output_path=args.output_path,
    )
