import argparse 
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import(
    col, to_timestamp, hour, minute, second, dayofweek,unix_timestamp,array_contains,
    avg, stddev, sum as sum_
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