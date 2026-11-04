"""Batch step 3: top-N unseen books per user -> MongoDB, then switch the app to the new version.

Recommendations go to a new collection recommendations_<version>. Only when every user is written
does the meta document "active" point to it, so the app never reads a half-written list.
"""
import argparse
import time
from datetime import datetime, timezone

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import functions as F

from common.recs import explode_recs, top_k_unseen
from common.settings import MODEL_DIR, MONGO_DB, MONGO_URI, PARQUET_DIR, get_db, get_spark, save_report

KEEP_VERSIONS = 2


def write_partition_factory(collection, version, created_at):
    def write_partition(rows):
        from pymongo import MongoClient, ReplaceOne

        coll = MongoClient(MONGO_URI)[MONGO_DB][collection]
        ops = []
        for row in rows:
            recs = [{"rank": r["rank"], "book_id": r["book_id"], "score": round(float(r["score"]), 4)}
                    for r in row["recs"]]
            ops.append(ReplaceOne({"_id": row["user_id"]},
                                  {"_id": row["user_id"], "recs": recs, "model_version": version,
                                   "created_at": created_at}, upsert=True))
            if len(ops) == 1000:
                coll.bulk_write(ops, ordered=False)
                ops = []
        if ops:
            coll.bulk_write(ops, ordered=False)

    return write_partition


def load_books(spark, db, path):
    books = (spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(path)
             .select(F.col("book_id").cast("int"), "title", "authors").dropna(subset=["book_id"]))
    docs = [{"_id": r["book_id"], "title": r["title"], "authors": r["authors"]} for r in books.collect()]
    db.books.delete_many({})
    if docs:
        db.books.insert_many(docs)
    return len(docs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--top-n", type=int, default=30, help="stored per user; the app shows the first 10 unread")
    parser.add_argument("--candidates", type=int, default=200)
    parser.add_argument("--books", default=None, help="books.csv with book_id,title,authors (optional)")
    args = parser.parse_args()

    spark = get_spark("bookpulse-write-recs")
    db = get_db()
    started = time.time()
    created_at = datetime.now(timezone.utc)
    version = created_at.strftime("%Y%m%d_%H%M%S")
    collection = f"recommendations_{version}"

    ratings = spark.read.parquet(str(PARQUET_DIR / "ratings"))
    model = ALSModel.load(str(MODEL_DIR / "als"))
    recs = top_k_unseen(explode_recs(model.recommendForAllUsers(args.candidates)),
                        ratings.select("user_id", "book_id"), args.top_n)
    per_user = recs.groupBy("user_id").agg(
        F.sort_array(F.collect_list(F.struct("rank", "book_id", "score"))).alias("recs"))
    per_user.foreachPartition(write_partition_factory(collection, version, created_at))
    users_written = db[collection].count_documents({})

    # Fallback list for users the model has never seen (cold start)
    popular = (ratings.groupBy("book_id")
               .agg(F.count("*").alias("n_ratings"), F.avg("rating").alias("avg_rating"))
               .orderBy(F.desc("n_ratings")).limit(100).collect())
    popular_docs = [{"book_id": r["book_id"], "n_ratings": r["n_ratings"],
                     "avg_rating": round(r["avg_rating"], 3)} for r in popular]

    # Atomic switch: the app reads whatever collection "active" points to
    db.meta.replace_one({"_id": "active"}, {"_id": "active", "recs_collection": collection,
                                             "model_version": version, "popular": popular_docs,
                                             "switched_at": datetime.now(timezone.utc)}, upsert=True)
    old = sorted(c for c in db.list_collection_names() if c.startswith("recommendations_"))[:-KEEP_VERSIONS]
    for name in old:
        db.drop_collection(name)

    books_loaded = load_books(spark, db, args.books) if args.books else None
    save_report("write_recs", {
        "model_version": version,
        "collection": collection,
        "users_written": users_written,
        "top_n": args.top_n,
        "books_loaded": books_loaded,
        "dropped_old_versions": old,
        "seconds": round(time.time() - started, 1),
    })
    spark.stop()


if __name__ == "__main__":
    main()
