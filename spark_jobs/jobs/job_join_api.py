import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")


def init_spark():
    spark = (
        SparkSession.builder.appName("MarketFullEnrichmentJob")
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


def main(
    symbol: str, event_date: str, alpaca_path: str, finnhub_path: str, output_path: str
):
    spark = init_spark()
    print(f"\n Full Enriching symbol={symbol}, event_date={event_date}")

    df_alpaca = spark.read.parquet(alpaca_path).filter(
        (col("symbol") == symbol) & (col("event_date") == event_date)
    )

    df_alpaca_features = df_alpaca.select(
        col("symbol"),
        col("event_date"),
        col("close_price"),
        col("volume_sum").alias("volume"),
        col("volatility_price").alias("volatility"),
        col("trade_count").alias("nb_transactions"),
    )

    print("\n ALPACA AGG features:")
    df_alpaca_features.show(5, truncate=False)

    df_finnhub = spark.read.parquet(finnhub_path).filter(
        (col("symbol") == symbol) & (col("event_date") <= event_date)
    )

    print("\n FINNHUB AGG before latest filtering:")
    df_finnhub.show(5, truncate=False)

    # Window to get latest reco <= event_date
    window_spec = Window.partitionBy("symbol").orderBy(col("event_date").desc())

    df_finnhub_latest = (
        df_finnhub.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    df_finnhub_features = df_finnhub_latest.select(
        col("symbol"),
        col("event_date").alias("reco_event_date"),
        col("buy").alias("buy_score"),
        col("sell").alias("sell_score"),
        col("strongBuy").alias("strong_buy_score"),
        col("strongSell").alias("strong_sell_score"),
        col("hold").alias("hold_score"),
    )

    print("\n FINNHUB LATEST RECO features:")
    df_finnhub_features.show(5, truncate=False)

    df_enriched = df_alpaca_features.join(df_finnhub_features, on="symbol", how="left")

    print("\n FINAL ENRICHED DATA:")
    df_enriched.show(5, truncate=False)

    df_enriched.write.mode("overwrite").partitionBy("symbol", "event_date").parquet(
        output_path
    )

    print(f"\n Output written to: {output_path}")
    spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Market Full Enrichment Job")
    parser.add_argument("--symbol", required=True, help="Stock symbol (ex: AAPL)")
    parser.add_argument("--event_date", required=True, help="Event date (YYYY-MM-DD)")
    parser.add_argument(
        "--alpaca_path",
        type=str,
        default="s3a://trusted/ALPACA_AGG",
        help="Input path for ALPACA AGG",
    )
    parser.add_argument(
        "--finnhub_path",
        type=str,
        default="s3a://trusted/FINNHUB_AGG",
        help="Input path for FINNHUB AGG",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        default="s3a://enriched/MARKET_ENRICHED",
        help="Output full enriched path",
    )

    args = parser.parse_args()

    main(
        symbol=args.symbol,
        event_date=args.event_date,
        alpaca_path=args.alpaca_path,
        finnhub_path=args.finnhub_path,
        output_path=args.output_path,
    )
