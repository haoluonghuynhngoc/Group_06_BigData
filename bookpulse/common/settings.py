"""Paths, connection settings and small helpers shared by every BookPulse job."""
import json
import os
import time
from pathlib import Path

ROOT = Path(os.getenv("BOOKPULSE_HOME", Path(__file__).resolve().parents[1]))
DATA_DIR = ROOT / "data"
PARQUET_DIR = DATA_DIR / "parquet"
STREAM_EVENTS_DIR = PARQUET_DIR / "stream_events"  # valid streamed ratings, merged in by batch/clean.py
MODEL_DIR = ROOT / "models"
REPORT_DIR = ROOT / "reports"
CHECKPOINT_DIR = ROOT / "checkpoints"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "bookpulse")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "rating-events")


def get_spark(app_name, kafka=False):
    """Local Spark session; SPARK_MASTER=local[1|2|4] is how the core-scaling runs are made."""
    import pyspark
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
    )
    if kafka:
        builder = builder.config(
            "spark.jars.packages",
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}",
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_db():
    from pymongo import MongoClient

    return MongoClient(MONGO_URI)[MONGO_DB]


def save_report(name, payload):
    """Write a measurement to reports/<name>_<timestamp>.json and print it."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"{name}_{time.strftime('%Y%m%d_%H%M%S')}.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    print(f"-> {path.relative_to(ROOT)}")
    return path
