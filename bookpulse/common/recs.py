"""Recommendation helpers used by both training (evaluation) and the job that writes to MongoDB."""
from pyspark.sql import Window
from pyspark.sql import functions as F


def explode_recs(raw):
    """ALS recommendForUserSubset/AllUsers output -> rows of (user_id, book_id, score)."""
    return raw.select("user_id", F.explode("recommendations").alias("r")).select(
        "user_id", F.col("r.book_id").alias("book_id"), F.col("r.rating").alias("score")
    )


def top_k_unseen(candidates, seen, k):
    """Keep the k best-scored candidates per user after removing books the user already rated."""
    w = Window.partitionBy("user_id").orderBy(F.desc("score"))
    return (
        candidates.join(seen, ["user_id", "book_id"], "left_anti")
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= k)
    )


def split_train_test(ratings, test_ratio, seed):
    """Train/test split used by train_als.py. Returns (train, test, number of streamed ratings in train).

    The Goodbooks CSV has no rating timestamps, so CSV ratings are split at random (seeded, reproducible).
    Ratings that came through the stream (from_stream = true, written by clean.py) are the newest data and
    the reason to retrain, so they always go to train: the served model must learn them, and a random split
    would hold about test_ratio of them out. Parquet from an older clean.py has no from_stream column.
    """
    if "from_stream" not in ratings.columns:
        ratings = ratings.withColumn("from_stream", F.lit(False))
    csv_ratings = ratings.filter(~F.col("from_stream")).select("user_id", "book_id", "rating")
    streamed = ratings.filter(F.col("from_stream")).select("user_id", "book_id", "rating")
    train_csv, test = csv_ratings.randomSplit([1 - test_ratio, test_ratio], seed=seed)
    return train_csv.unionByName(streamed), test, streamed.count()
