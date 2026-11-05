"""Check the MapReduce output against Spark SQL on the same input.

Spark reads the same ratings.csv with the same validation as batch/clean.py (integer fields, rating 1-5),
computes the same per-book statistics with Spark SQL, and the two results are compared:
  * exceptAll in both directions on the exact integer columns (count, sum, n1..n5) -> both must be empty
  * avg_rating may differ only by the 4-decimal rounding of the MapReduce output
It also reads the Hadoop job log (counters: records per stage, combiner effect, rejects) and the HDFS
block report, and writes everything to reports/mapreduce_verify_<timestamp>.json.

  docker compose exec spark python mapreduce/verify_with_spark.py [--mr-output reports/mapreduce/...tsv]
"""
import argparse
import glob
import re
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StructField, StructType

from common.settings import DATA_DIR, REPORT_DIR, get_spark, save_report

MR_DIR = REPORT_DIR / "mapreduce"
INT_COLS = ["book_id", "n_ratings", "sum_rating", "n_liked", "n1", "n2", "n3", "n4", "n5"]
MR_SCHEMA = StructType([StructField("book_id", IntegerType()), StructField("n_ratings", LongType()),
                        StructField("sum_rating", LongType()), StructField("avg_rating", DoubleType()),
                        StructField("n_liked", LongType())]
                       + [StructField(f"n{i}", LongType()) for i in range(1, 6)])
COUNTERS = ["Launched map tasks", "Launched reduce tasks", "Map input records", "Map output records",
            "Combine input records", "Combine output records", "Reduce input groups", "Reduce shuffle bytes",
            "Reduce input records", "Reduce output records", "WrongColumnCount", "NotAnInteger",
            "RatingOutOfRange"]


def latest(pattern):
    files = sorted(glob.glob(str(MR_DIR / pattern)))
    return files[-1] if files else None


def job_counters(log_path):
    """Hadoop prints counters as '<tab><tab>Name=value' at the end of the job."""
    found = {}
    for line in Path(log_path).read_text(encoding="utf-8", errors="replace").splitlines():
        m = re.match(r"^\s+([A-Za-z][A-Za-z \-]*?)=(\d+)$", line)
        if m and m.group(1) in COUNTERS:
            found[m.group(1)] = int(m.group(2))
        m = re.match(r"^elapsed_seconds=(\d+)", line)
        if m:
            found["elapsed_seconds"] = int(m.group(1))
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mr-output", default=None, help="default: newest reports/mapreduce/hadoop_output_*.tsv")
    parser.add_argument("--input", default=str(DATA_DIR / "goodbooks" / "ratings.csv"))
    args = parser.parse_args()

    mr_path = args.mr_output or latest("hadoop_output_*.tsv")
    if not mr_path:
        raise SystemExit("no MapReduce output found: run mapreduce/run_hadoop.sh first")
    stamp = re.search(r"(\d{8}_\d{6})", Path(mr_path).name)
    log_path = MR_DIR / f"job_{stamp.group(1)}.log" if stamp else None
    blocks_path = MR_DIR / f"hdfs_blocks_{stamp.group(1)}.txt" if stamp else None

    spark = get_spark("bookpulse-verify-mapreduce")
    mr = spark.read.csv(mr_path, sep="\t", schema=MR_SCHEMA)

    # Same input and the same validation rules as batch/clean.py, then the same statistics in Spark SQL
    (spark.read.csv(args.input, header=True)
     .select(*(F.col(c).cast("int").alias(c) for c in ("user_id", "book_id", "rating")))
     .dropna().filter(F.col("rating").between(1, 5))
     .createOrReplaceTempView("ratings_csv"))
    spark_stats = spark.sql("""
        SELECT book_id, COUNT(*) AS n_ratings, SUM(rating) AS sum_rating, AVG(rating) AS avg_rating,
               SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) AS n_liked,
               SUM(CASE WHEN rating = 1 THEN 1 ELSE 0 END) AS n1, SUM(CASE WHEN rating = 2 THEN 1 ELSE 0 END) AS n2,
               SUM(CASE WHEN rating = 3 THEN 1 ELSE 0 END) AS n3, SUM(CASE WHEN rating = 4 THEN 1 ELSE 0 END) AS n4,
               SUM(CASE WHEN rating = 5 THEN 1 ELSE 0 END) AS n5
        FROM ratings_csv GROUP BY book_id
    """).cache()

    mr_int = mr.select(*(F.col(c).cast("long") for c in INT_COLS))
    sp_int = spark_stats.select(*(F.col(c).cast("long") for c in INT_COLS))
    only_mr = mr_int.exceptAll(sp_int).count()
    only_spark = sp_int.exceptAll(mr_int).count()
    joined = mr.alias("m").join(spark_stats.alias("s"), "book_id")
    max_avg_diff = joined.agg(F.max(F.abs(F.col("m.avg_rating") - F.col("s.avg_rating")))).first()[0]
    totals = mr.agg(F.sum("n_ratings"), F.count("*")).first()
    top = [r.asDict() for r in mr.orderBy(F.desc("n_ratings"), "book_id").limit(5).collect()]

    report = {
        "mapreduce_output": mr_path,
        "input": args.input,
        "books_mapreduce": mr.count(),
        "books_spark": spark_stats.count(),
        "ratings_counted_mapreduce": totals[0],
        "rows_only_in_mapreduce": only_mr,
        "rows_only_in_spark": only_spark,
        "max_abs_avg_rating_diff": round(max_avg_diff, 6) if max_avg_diff is not None else None,
        "match": only_mr == 0 and only_spark == 0 and (max_avg_diff or 0) <= 0.00005,
        "top5_most_rated": top,
        "hadoop_counters": job_counters(log_path) if log_path and log_path.exists() else None,
        "hdfs_blocks": blocks_path.read_text(encoding="utf-8").strip() if blocks_path and blocks_path.exists() else None,
    }
    c = report["hadoop_counters"] or {}
    if c.get("Combine input records") and c.get("Combine output records"):
        report["combiner_reduction"] = round(1 - c["Combine output records"] / c["Combine input records"], 4)
    save_report("mapreduce_verify", report)
    spark.stop()


if __name__ == "__main__":
    main()
