"""Session 9, section 3.1: controlled performance experiment on the project's own join query.

ONE factor is changed at a time, everything else fixed (same data, same query, same Spark session,
same machine, same number of cores):
  factor A  join strategy      broadcast hash join (default, books.csv 10k rows < 10 MB threshold)
                               vs sort-merge join (spark.sql.autoBroadcastJoinThreshold = -1)
  factor B  shuffle partitions spark.sql.shuffle.partitions = 8 (project default) vs 1 vs 64

The query is Q4 of batch/analytics_sql.py (most-rated books: ratings Parquet LEFT JOIN books.csv,
GROUP BY book, ORDER BY count, LIMIT 10). Each configuration is run REPEATS times after one warm-up
run; the median wall-clock time of .collect() is reported, plus the physical plan operators
(number of Exchange = shuffles, BroadcastExchange, join type).

    docker compose exec spark python scripts/session9/benchmark_join.py
Writes evidence/session9/B_benchmark.json and B_benchmark_plans.txt.
"""
import json
import os
import re
import statistics
import time

from pyspark.sql import functions as F

from common.settings import DATA_DIR, PARQUET_DIR, ROOT, get_spark

OUT_DIR = ROOT / "evidence" / "session9"
REPEATS = 3

Q4 = """
    SELECT r.book_id, COALESCE(b.title, 'UNKNOWN') AS title,
           COUNT(*) AS n_ratings, ROUND(AVG(r.rating), 3) AS avg_rating
    FROM ratings r LEFT JOIN books b ON r.book_id = b.book_id
    GROUP BY r.book_id, COALESCE(b.title, 'UNKNOWN')
    ORDER BY n_ratings DESC, r.book_id LIMIT 10
"""


def operators(plan_text):
    ops = re.findall(r"^\(\d+\) (\w+)", plan_text, flags=re.MULTILINE)
    return {"operators": ops, "shuffle_exchanges": ops.count("Exchange"),
            "broadcast_exchanges": ops.count("BroadcastExchange"),
            "join": next((op for op in ops if op.endswith("Join")), None)}


def run_config(spark, name, settings, plans_out):
    for key, value in settings.items():
        spark.conf.set(key, value)
    spark.catalog.clearCache()
    plan = spark.sql("EXPLAIN FORMATTED " + Q4).first()[0]
    plans_out.append(f"===== {name}  {settings}\n{plan}\n")
    spark.sql(Q4).collect()  # warm-up (JIT, file listing, page cache)
    times = []
    for _ in range(REPEATS):
        t = time.perf_counter()
        spark.sql(Q4).collect()
        times.append(round(time.perf_counter() - t, 3))
    return {"name": name, "settings": settings, "runs_seconds": times,
            "median_seconds": round(statistics.median(times), 3), **operators(plan)}


def main():
    spark = get_spark("session9-benchmark-join")
    ratings = spark.read.parquet(str(PARQUET_DIR / "ratings"))
    ratings.createOrReplaceTempView("ratings")
    books_raw = spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(
        str(DATA_DIR / "goodbooks" / "books.csv"))
    books_raw.select(F.col("book_id").cast("int").alias("book_id"), "title").dropna(
        subset=["book_id"]).createOrReplaceTempView("books")
    n_ratings = ratings.count()
    n_books = spark.table("books").count()
    parquet_files = len(list((PARQUET_DIR / "ratings").glob("*.parquet")))

    plans = []
    results = []
    # Factor A: join strategy (shuffle partitions fixed at the project default 8)
    results.append(run_config(spark, "A0 baseline: broadcast hash join, shuffle.partitions=8",
                              {"spark.sql.autoBroadcastJoinThreshold": "10485760",
                               "spark.sql.shuffle.partitions": "8"}, plans))
    results.append(run_config(spark, "A1 variant: sort-merge join (broadcast disabled), shuffle.partitions=8",
                              {"spark.sql.autoBroadcastJoinThreshold": "-1",
                               "spark.sql.shuffle.partitions": "8"}, plans))
    # Factor B: shuffle partitions (join strategy fixed at the default broadcast)
    for parts in ("1", "64"):
        results.append(run_config(spark, f"B variant: broadcast hash join, shuffle.partitions={parts}",
                                  {"spark.sql.autoBroadcastJoinThreshold": "10485760",
                                   "spark.sql.shuffle.partitions": parts}, plans))

    report = {
        "measured_on": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "measured",
        "environment": {
            "spark_master": os.getenv("SPARK_MASTER", "local[*]"),
            "cores_visible_to_spark": spark.sparkContext.defaultParallelism,
            "spark_version": spark.version,
            "driver_memory": os.getenv("SPARK_DRIVER_MEMORY", "4g"),
            "container": "bookpulse-spark (Docker Desktop, shared with mongo/kafka/app)",
        },
        "input": {"ratings_rows": n_ratings, "ratings_parquet_files": parquet_files,
                  "books_rows": n_books, "query": "Q4 of batch/analytics_sql.py"},
        "method": f"one warm-up run, then {REPEATS} timed runs per configuration, median reported; "
                  "only the listed setting changes between baseline and variant",
        "results": results,
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "B_benchmark.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    (OUT_DIR / "B_benchmark_plans.txt").write_text("\n".join(plans), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
