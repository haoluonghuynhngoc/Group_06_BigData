"""Batch step 3: top-N unseen books per user -> MongoDB, then switch the app to the new version.

Recommendations go to a new collection recommendations_<version>. Only when every user is written
(gate: documents written == users expected, otherwise the new collection is dropped and the job fails)
does the meta document "active" point to it, so the app never reads a half-written list. Book titles
are upserted before the switch. Exactly two versions are kept: the active one and the one that was
active before ("previous_collection" in meta, for a manual rollback); leftovers of failed runs are dropped.

Each user document also carries "fallback": the most-rated books that user has not rated, which the
app uses to refill the top-10 once the user has read through the stored recommendations.
"""
import argparse
import time
from datetime import datetime, timezone

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import functions as F
from pymongo import ReplaceOne

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
                    for r in row["recs"] or []]
            fallback = [r["book_id"] for r in row["fallback"] or []]
            ops.append(ReplaceOne({"_id": row["user_id"]},
                                  {"_id": row["user_id"], "recs": recs, "fallback": fallback,
                                   "model_version": version, "created_at": created_at}, upsert=True))
            if len(ops) == 1000:
                coll.bulk_write(ops, ordered=False)
                ops = []
        if ops:
            coll.bulk_write(ops, ordered=False)

    return write_partition


def load_books(spark, db, path):
    """Upsert book titles by _id, so the collection is never empty while the app is reading it."""
    books = (spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(path)
             .select(F.col("book_id").cast("int"), "title", "authors").dropna(subset=["book_id"]))
    docs = [{"_id": r["book_id"], "title": r["title"], "authors": r["authors"]} for r in books.collect()]
    if docs:
        db.books.bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs], ordered=False)
    return len(docs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--top-n", type=int, default=30, help="stored per user; the app shows the first 10 unread")
    parser.add_argument("--candidates", type=int, default=200)
    parser.add_argument("--books", default=None, help="books.csv with book_id,title,authors (optional)")
    parser.add_argument("--fallback-n", type=int, default=10,
                        help="popular books the user has not rated, stored per user to refill the top-10")
    parser.add_argument("--fallback-pool", type=int, default=200, help="most-rated books to pick the fallback from")
    parser.add_argument("--model", default="als_implicit", choices=["als_implicit", "als"],
                        help="serving model in models/: als_implicit (chosen on Precision@10, see "
                             "docs/chon-mo-hinh.md) or als (explicit, predicts stars)")
    args = parser.parse_args()

    spark = get_spark("bookpulse-write-recs")
    db = get_db()
    started = time.time()
    created_at = datetime.now(timezone.utc)
    version = created_at.strftime("%Y%m%d_%H%M%S")
    collection = f"recommendations_{version}"

    ratings = spark.read.parquet(str(PARQUET_DIR / "ratings"))
    seen = ratings.select("user_id", "book_id")
    model = ALSModel.load(str(MODEL_DIR / args.model))
    model_type = "implicit" if args.model == "als_implicit" else "explicit"
    recs = top_k_unseen(explode_recs(model.recommendForAllUsers(args.candidates)), seen, args.top_n)
    per_user = recs.groupBy("user_id").agg(
        F.sort_array(F.collect_list(F.struct("rank", "book_id", "score"))).alias("recs"))

    # Per-user refill list: most-rated books this user has NOT rated. When the user reads through the
    # stored recs, the app tops the list up from here instead of showing fewer than 10 books
    # (a global popular list could repeat books the user already read).
    pool = (ratings.groupBy("book_id").agg(F.count("*").alias("score"))
            .orderBy(F.desc("score"), "book_id").limit(args.fallback_pool))
    fallback = (top_k_unseen(seen.select("user_id").distinct().crossJoin(F.broadcast(pool)), seen,
                             args.fallback_n)
                .groupBy("user_id")
                .agg(F.sort_array(F.collect_list(F.struct("rank", "book_id"))).alias("fallback")))
    per_user = per_user.join(fallback, "user_id", "left").cache()
    expected_users = per_user.count()
    try:
        per_user.foreachPartition(write_partition_factory(collection, version, created_at))
        users_written = db[collection].count_documents({})
        # Quality gate before the switch: every user written, none missing
        if expected_users == 0 or users_written != expected_users:
            raise RuntimeError(f"{collection}: wrote {users_written} of {expected_users} users")
    except Exception:
        db.drop_collection(collection)  # never leave a half-written version behind
        raise

    # Fallback list for users the model has never seen (cold start)
    popular = (ratings.groupBy("book_id")
               .agg(F.count("*").alias("n_ratings"), F.avg("rating").alias("avg_rating"))
               .orderBy(F.desc("n_ratings")).limit(100).collect())
    popular_docs = [{"book_id": r["book_id"], "n_ratings": r["n_ratings"],
                     "avg_rating": round(r["avg_rating"], 3)} for r in popular]

    # Book titles first (upsert), so they are there when the app switches to the new version
    books_loaded = load_books(spark, db, args.books) if args.books else None

    # Atomic switch: the app reads whatever collection "active" points to. The version that was active
    # until now is kept as "previous_collection" for a manual rollback (set recs_collection back to it).
    previous = (db.meta.find_one({"_id": "active"}) or {}).get("recs_collection")
    db.meta.replace_one({"_id": "active"}, {"_id": "active", "recs_collection": collection,
                                             "model_version": version, "model_type": model_type,
                                             "previous_collection": previous,
                                             "popular": popular_docs,
                                             "switched_at": datetime.now(timezone.utc)}, upsert=True)
    # Keep exactly the active and the previously active version (KEEP_VERSIONS = 2); anything else,
    # e.g. a collection left by a run that died before the gate, is dropped
    keep = {collection, previous}
    old = sorted(c for c in db.list_collection_names() if c.startswith("recommendations_") and c not in keep)
    for name in old:
        db.drop_collection(name)
    save_report("write_recs", {
        "model_version": version,
        "model": args.model,
        "collection": collection,
        "users_written": users_written,
        "top_n": args.top_n,
        "fallback_n": args.fallback_n,
        "users_with_full_fallback": db[collection].count_documents(
            {f"fallback.{args.fallback_n - 1}": {"$exists": True}}),
        "books_loaded": books_loaded,
        "expected_users": expected_users,
        "previous_collection": previous,
        "dropped_old_versions": old,
        "seconds": round(time.time() - started, 1),
    })
    spark.stop()


if __name__ == "__main__":
    main()
