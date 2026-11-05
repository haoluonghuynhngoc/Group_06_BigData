"""Batch step 1b: Spark SQL analytics on the cleaned ratings.

Registers the cleaned Parquet (and books.csv, when given) as temporary views and answers the
project's analytical questions in Spark SQL:
  Q1 dataset profile and sparsity          Q4 most-rated books (join with book metadata)
  Q2 rating distribution                   Q5 best-rated books with enough ratings (HAVING)
  Q3 ratings per user (percentiles)        Q6 curated per-book table -> Parquet (data/parquet/book_stats)

It also checks that one query gives the same answer through the DataFrame API, saves the physical
plan of the join query (Exchange = shuffle) and times the same aggregation on CSV vs Parquet.
"""
import argparse
import os
import re
import time

from pyspark.sql import functions as F

from common.settings import PARQUET_DIR, REPORT_DIR, get_spark, save_report


def timed(fn):
    started = time.time()
    result = fn()
    return result, round(time.time() - started, 2)


def rows(df):
    return [r.asDict() for r in df.collect()]


def load_books(spark, path):
    """books.csv from Goodbooks-10k (many columns) or the sample (book_id,title,authors)."""
    raw = (spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(path))
    year = (F.col("original_publication_year").cast("double").cast("int")
            if "original_publication_year" in raw.columns else F.lit(None).cast("int"))
    return raw.select(F.col("book_id").cast("int").alias("book_id"), "title", "authors",
                      year.alias("pub_year")).dropna(subset=["book_id"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--books", default=None, help="books.csv (optional, enables the join queries)")
    parser.add_argument("--raw-csv", default=None, help="raw ratings CSV, to time the same query on CSV vs Parquet")
    parser.add_argument("--min-ratings", type=int, default=1000, help="HAVING threshold for Q5")
    args = parser.parse_args()

    spark = get_spark("bookpulse-analytics-sql")
    started = time.time()
    ratings = spark.read.parquet(str(PARQUET_DIR / "ratings"))
    ratings.createOrReplaceTempView("ratings")
    has_books = args.books is not None
    if has_books:
        load_books(spark, args.books).createOrReplaceTempView("books")

    timings = {}

    # Q1 – profile and sparsity of the user x book matrix
    q1, timings["q1_profile"] = timed(lambda: spark.sql("""
        SELECT COUNT(*)                       AS ratings,
               COUNT(DISTINCT user_id)        AS users,
               COUNT(DISTINCT book_id)        AS books,
               ROUND(AVG(rating), 3)          AS avg_rating,
               ROUND(CAST(100 * COUNT(*) AS DOUBLE)
                     / (COUNT(DISTINCT user_id) * COUNT(DISTINCT book_id)), 3) AS density_pct
        FROM ratings
    """).first().asDict())

    # Q2 – how ratings are distributed over 1..5 stars
    q2, timings["q2_distribution"] = timed(lambda: rows(spark.sql("""
        SELECT rating, COUNT(*) AS n, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM ratings GROUP BY rating ORDER BY rating
    """)))

    # Q3 – activity per user: long tail decides which users ALS can serve well
    q3, timings["q3_user_activity"] = timed(lambda: spark.sql("""
        WITH per_user AS (SELECT user_id, COUNT(*) AS n FROM ratings GROUP BY user_id)
        SELECT MIN(n) AS min, percentile_approx(n, 0.25) AS p25, percentile_approx(n, 0.5) AS median,
               percentile_approx(n, 0.75) AS p75, MAX(n) AS max,
               SUM(CASE WHEN n < 10 THEN 1 ELSE 0 END) AS users_under_10_ratings
        FROM per_user
    """).first().asDict())

    report = {
        "spark_master": os.getenv("SPARK_MASTER", "local[*]"),
        "q1_profile": q1,
        "q2_rating_distribution": q2,
        "q3_ratings_per_user": q3,
    }

    if has_books:
        top_sql = """
            SELECT r.book_id, b.title, COUNT(*) AS n_ratings, ROUND(AVG(r.rating), 3) AS avg_rating
            FROM ratings r JOIN books b ON r.book_id = b.book_id
            GROUP BY r.book_id, b.title
            ORDER BY n_ratings DESC, r.book_id
            LIMIT 10
        """
        # Q4 – most-rated books, enriched with title from books.csv
        q4, timings["q4_most_rated"] = timed(lambda: rows(spark.sql(top_sql)))

        # Same question through the DataFrame API: both must give identical rows
        books_df = spark.table("books")
        df_top = (ratings.join(books_df, "book_id").groupBy("book_id", "title")
                  .agg(F.count("*").alias("n_ratings"), F.round(F.avg("rating"), 3).alias("avg_rating"))
                  .orderBy(F.desc("n_ratings"), "book_id").limit(10))
        report["sql_equals_dataframe_api"] = rows(df_top) == q4

        # Physical plan of the join + aggregation (look for Exchange / BroadcastHashJoin)
        plan = spark.sql(f"EXPLAIN FORMATTED {top_sql}").first()[0]
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        plan_path = REPORT_DIR / f"sql_plan_{time.strftime('%Y%m%d_%H%M%S')}.txt"
        plan_path.write_text(plan, encoding="utf-8")
        operators = re.findall(r"^\(\d+\) (\w+)", plan, flags=re.MULTILINE)
        report["plan"] = {
            "file": plan_path.name,
            "operators": operators,
            "shuffle_exchanges": operators.count("Exchange"),
            "broadcast_exchanges": operators.count("BroadcastExchange"),
            "join": next((op for op in operators if op.endswith("Join")), None),
        }

        # Q5 – best-rated books among those with enough ratings (HAVING filters after grouping)
        q5, timings["q5_best_rated"] = timed(lambda: rows(spark.sql(f"""
            SELECT r.book_id, b.title, COUNT(*) AS n_ratings, ROUND(AVG(r.rating), 3) AS avg_rating
            FROM ratings r JOIN books b ON r.book_id = b.book_id
            GROUP BY r.book_id, b.title
            HAVING COUNT(*) >= {args.min_ratings}
            ORDER BY avg_rating DESC, n_ratings DESC
            LIMIT 10
        """)))

        # Q6 – curated per-book table for the report and the app; small, so one file, no partitioning
        book_stats = spark.sql("""
            SELECT b.book_id, b.title, b.authors, b.pub_year,
                   COUNT(r.rating) AS n_ratings, ROUND(AVG(r.rating), 3) AS avg_rating,
                   ROUND(STDDEV(r.rating), 3) AS std_rating,
                   SUM(CASE WHEN r.rating >= 4 THEN 1 ELSE 0 END) AS n_liked
            FROM books b LEFT JOIN ratings r ON r.book_id = b.book_id
            GROUP BY b.book_id, b.title, b.authors, b.pub_year
        """)
        out = str(PARQUET_DIR / "book_stats")
        _, timings["q6_write_book_stats"] = timed(
            lambda: book_stats.coalesce(1).write.mode("overwrite").parquet(out))
        report.update({
            "q4_most_rated": q4,
            "q5_best_rated": {"min_ratings": args.min_ratings, "rows": q5},
            "q6_book_stats": {"output": out, "rows": spark.read.parquet(out).count()},
        })

    # Same aggregation on raw CSV vs cleaned Parquet: why the pipeline stores Parquet
    if args.raw_csv:
        agg = "SELECT book_id, COUNT(*) AS n, AVG(rating) AS avg FROM {} GROUP BY book_id"
        spark.read.csv(args.raw_csv, header=True, inferSchema=False).createOrReplaceTempView("raw_csv")
        _, csv_s = timed(lambda: spark.sql(agg.format("raw_csv")).count())
        _, pq_s = timed(lambda: spark.sql(agg.format("ratings")).count())
        report["csv_vs_parquet_seconds"] = {"csv": csv_s, "parquet": pq_s}

    report["timings_seconds"] = timings
    report["total_seconds"] = round(time.time() - started, 1)
    save_report("analytics_sql", report)
    spark.stop()


if __name__ == "__main__":
    main()
