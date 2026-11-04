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
