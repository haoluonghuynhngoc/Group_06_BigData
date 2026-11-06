"""Streaming job: Kafka rating events -> validate, de-duplicate -> MongoDB.

Per micro-batch it writes three things:
  rating_stats  live average rating and count per book
  user_seen     books each user has just rated (the app hides them from the top-10)
  online_eval   online hit rate: share of liked events (4-5 stars) whose book was in the user's top-10

Spark + Kafka + checkpoint gives at-least-once delivery, so a batch can be replayed after a restart.
The last applied batch id is stored in MongoDB and a replayed batch is skipped, so counts are not doubled.

A third query archives every valid event to Parquet (data/parquet/stream_events, partitioned by
event_date): stateless, no watermark, no de-duplication, so a rating that arrives late is NOT lost from
the training data. Spark's file sink records committed files in _spark_metadata, so the archive is
exactly-once. batch/clean.py removes duplicates exactly by event_id and merges the events into the
training data, so the next retrain learns from ratings that arrived through the stream.
(Speed layer drops late events to bound its state; batch layer keeps them: Lambda architecture.)

A fourth query is a stateful event-time window: ratings per book in a 10-minute window sliding every
5 minutes ("trending"), in update mode. Events older than the watermark are dropped by Spark before
they reach any state, which the progress report shows as numRowsDroppedByWatermark.

Observability: the progress of every query (input rows, rows/s, batch duration, state size, rows
dropped by watermark or as duplicates, current watermark) is accumulated per batch and written to
reports/stream_progress_<timestamp>.json every 10 seconds and when the job stops.
"""
import argparse
import json
import time
from datetime import datetime, timezone

from pymongo import ReplaceOne, UpdateOne
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

from common.settings import (CHECKPOINT_DIR, KAFKA_BOOTSTRAP, KAFKA_TOPIC, REPORT_DIR, STREAM_EVENTS_DIR,
                             get_db, get_spark)

SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("book_id", IntegerType()),
    StructField("rating", IntegerType()),
    StructField("event_time", StringType()),
    StructField("simulated", BooleanType()),
])
TOP_K = 10


def already_applied(db, key, batch_id):
    state = db.stream_state.find_one({"_id": key})
    return state is not None and state.get("last_batch", -1) >= batch_id


def mark_applied(db, key, batch_id, **inc):
    update = {"$set": {"last_batch": batch_id, "updated": datetime.now(timezone.utc)}}
    if inc:
        update["$inc"] = inc
    db.stream_state.update_one({"_id": key}, update, upsert=True)


def update_online_eval(db, rows):
    meta = db.meta.find_one({"_id": "active"})
    if meta is None:
        return
    liked = [r for r in rows if r["rating"] >= 4]
    users = list({r["user_id"] for r in liked})
    top = {d["_id"]: {x["book_id"] for x in d["recs"]}
           for d in db[meta["recs_collection"]].find({"_id": {"$in": users}}, {"recs": {"$slice": TOP_K}})}
    known = [r for r in liked if r["user_id"] in top]
    hits = sum(1 for r in known if r["book_id"] in top[r["user_id"]])
    db.online_eval.update_one(
        {"_id": f"hit_rate@{TOP_K}"},
        {"$inc": {"liked_events": len(known), "hits": hits, "cold_start_events": len(liked) - len(known)},
         "$set": {"model_version": meta["model_version"], "updated": datetime.now(timezone.utc)}},
        upsert=True,
    )


def ensure_topic(partitions):
    """Spark stops with UnknownTopicOrPartitionException when the topic does not exist yet (Kafka only
    auto-creates it on the producer's first send), so create it before subscribing."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        if KAFKA_TOPIC not in admin.list_topics():
            admin.create_topics([NewTopic(KAFKA_TOPIC, num_partitions=partitions, replication_factor=1)])
            print(f"created topic '{KAFKA_TOPIC}' with {partitions} partitions")
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def process_valid(df, batch_id):
    db = get_db()
    if already_applied(db, "valid_events", batch_id):
        return
    rows = df.select("user_id", "book_id", "rating", "event_time").collect()
    if rows:
        per_book = {}
        for r in rows:
            s, n = per_book.get(r["book_id"], (0, 0))
            per_book[r["book_id"]] = (s + r["rating"], n + 1)
        now = datetime.now(timezone.utc)
        db.rating_stats.bulk_write([
            UpdateOne({"_id": book_id}, [
                {"$set": {"rating_sum": {"$add": [{"$ifNull": ["$rating_sum", 0]}, s]},
                          "n_ratings": {"$add": [{"$ifNull": ["$n_ratings", 0]}, n]},
                          "updated": now}},
                {"$set": {"avg_rating": {"$divide": ["$rating_sum", "$n_ratings"]}}},
            ], upsert=True)
            for book_id, (s, n) in per_book.items()
        ], ordered=False)
        db.user_seen.bulk_write([
            UpdateOne({"_id": f"{r['user_id']}_{r['book_id']}"},
                      {"$set": {"user_id": r["user_id"], "book_id": r["book_id"], "rating": r["rating"],
                                "event_time": r["event_time"]}}, upsert=True)
            for r in rows
        ], ordered=False)
        update_online_eval(db, rows)
    mark_applied(db, "valid_events", batch_id, events=len(rows))


def process_rejected(df, batch_id):
    db = get_db()
    if not already_applied(db, "rejected_events", batch_id):
        mark_applied(db, "rejected_events", batch_id, events=df.count())


def process_trending(df, batch_id):
    """Update mode: each row is the complete current aggregate of one (window, book), so replacing the
    document is idempotent - a batch replayed after a restart writes the same values again.
    A batch only holds the (window, book) pairs touched in the last trigger, so collect() stays small."""
    rows = df.select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                     "book_id", "n_ratings", "avg_rating", "n_liked").collect()
    if not rows:
        return
    now = datetime.now(timezone.utc)
    get_db().trending.bulk_write([
        ReplaceOne({"_id": f"{r['book_id']}_{r['window_start']:%Y%m%dT%H%M}"},
                   {**r.asDict(), "avg_rating": round(r["avg_rating"], 3), "updated": now}, upsert=True)
        for r in rows
    ], ordered=False)


class ProgressLog:
    """Accumulates StreamingQueryProgress per query and batch id, and writes a JSON summary."""

    def __init__(self, path, settings):
        self.path, self.settings, self.batches = path, settings, {}

    def collect(self, queries):
        for name, q in queries.items():
            for p in q.recentProgress:  # Spark keeps the last 100 updates; we poll every 10 s
                self.batches.setdefault(name, {})[p["batchId"]] = p

    def write(self, queries):
        out = {"settings": self.settings, "written": datetime.now(timezone.utc).isoformat(), "queries": {}}
        for name, q in queries.items():
            progress = [self.batches.get(name, {})[b] for b in sorted(self.batches.get(name, {}))]
            ops = [op for p in progress for op in p.get("stateOperators", [])]
            out["queries"][name] = {
                "query_id": str(q.id),
                "batches": len(progress),
                "input_rows": sum(p["numInputRows"] for p in progress),
                "max_input_rows_per_second": max((p.get("inputRowsPerSecond") or 0 for p in progress), default=0),
                "max_processed_rows_per_second": max((p.get("processedRowsPerSecond") or 0 for p in progress),
                                                     default=0),
                "max_batch_duration_ms": max((p["durationMs"].get("triggerExecution", 0) for p in progress),
                                             default=0),
                "rows_dropped_by_watermark": sum(op.get("numRowsDroppedByWatermark", 0) for op in ops),
                "duplicates_dropped": sum(op.get("customMetrics", {}).get("numDroppedDuplicateRows", 0)
                                          for op in ops),
                "state_rows_now": sum(op.get("numRowsTotal", 0) for op in (progress[-1].get("stateOperators", [])
                                                                          if progress else [])),
                "watermark_now": progress[-1].get("eventTime", {}).get("watermark") if progress else None,
                "last_progress": progress[-1] if progress else None,
            }
        self.path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--watermark", default="10 minutes", help="how late an event may arrive")
    parser.add_argument("--trigger", default="5 seconds")
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--run-seconds", type=int, default=0, help="0 = run until Ctrl+C")
    parser.add_argument("--partitions", type=int, default=3, help="partitions if the topic has to be created")
    parser.add_argument("--window", default="10 minutes", help="trending window length (event time)")
    parser.add_argument("--slide", default="5 minutes", help="trending window slide")
    args = parser.parse_args()

    ensure_topic(args.partitions)
    db = get_db()
    db.user_seen.create_index("user_id")  # the app reads user_seen by user_id (no full collection scan)
    db.trending.create_index("window_end", expireAfterSeconds=24 * 3600)  # MongoDB TTL drops old windows
    db.trending.create_index([("window_start", 1), ("n_ratings", -1)])
    spark = get_spark("bookpulse-stream", kafka=True)
    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
        .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
    )
    is_valid = F.coalesce(
        F.col("event_id").isNotNull() & F.col("user_id").isNotNull() & F.col("book_id").isNotNull()
        & F.col("event_time").isNotNull() & F.col("rating").between(1, 5),
        F.lit(False),
    )
    valid = (events.filter(is_valid)
             .withWatermark("event_time", args.watermark)
             .dropDuplicatesWithinWatermark(["event_id"]))
    rejected = events.filter(~is_valid)
    # Stateful event-time window on the de-duplicated stream, so a duplicate is never counted twice
    trending = (valid.groupBy(F.window("event_time", args.window, args.slide), "book_id")
                .agg(F.count("*").alias("n_ratings"), F.avg("rating").alias("avg_rating"),
                     F.sum(F.when(F.col("rating") >= 4, 1).otherwise(0)).alias("n_liked")))

    queries = {
        "valid": valid.writeStream.queryName("valid").foreachBatch(process_valid)
        .option("checkpointLocation", str(CHECKPOINT_DIR / "stream_valid"))
        .trigger(processingTime=args.trigger).start(),
        "rejected": rejected.writeStream.queryName("rejected").foreachBatch(process_rejected)
        .option("checkpointLocation", str(CHECKPOINT_DIR / "stream_rejected"))
        .trigger(processingTime=args.trigger).start(),
        # Archive for the next batch retrain: every valid event, late ones included; stateless and
        # exactly-once (file sink). De-duplication happens in batch (clean.py), exactly, by event_id.
        "events": events.filter(is_valid).withColumn("event_date", F.to_date("event_time"))
        .writeStream.queryName("events_to_parquet").format("parquet").outputMode("append").partitionBy("event_date")
        .option("path", str(STREAM_EVENTS_DIR))
        .option("checkpointLocation", str(CHECKPOINT_DIR / "stream_events_archive"))
        .trigger(processingTime=args.trigger).start(),
        "trending": trending.writeStream.queryName("trending").outputMode("update").foreachBatch(process_trending)
        .option("checkpointLocation", str(CHECKPOINT_DIR / "stream_trending"))
        .trigger(processingTime=args.trigger).start(),
    }
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    log = ProgressLog(REPORT_DIR / f"stream_progress_{time.strftime('%Y%m%d_%H%M%S')}.json",
                      {"watermark": args.watermark, "window": args.window, "slide": args.slide,
                       "trigger": args.trigger, "topic": KAFKA_TOPIC})
    print(f"streaming from '{KAFKA_TOPIC}' at {KAFKA_BOOTSTRAP}; Ctrl+C to stop; progress -> {log.path.name}")
    started = time.time()
    try:
        while all(q.isActive for q in queries.values()):
            time.sleep(10)
            log.collect(queries)
            log.write(queries)
            for name, q in queries.items():
                p = q.lastProgress
                if p and p["numInputRows"]:
                    dropped = sum(op.get("customMetrics", {}).get("numDroppedDuplicateRows", 0)
                                  for op in p.get("stateOperators", []))
                    print(f"[{name}] batch {p['batchId']}: {p['numInputRows']} rows, "
                          f"{p.get('processedRowsPerSecond', 0):.1f} rows/s, duplicates dropped {dropped}")
            if args.run_seconds and time.time() - started > args.run_seconds:
                break
        for q in queries.values():
            if q.exception():
                raise q.exception()
    except KeyboardInterrupt:
        pass
    finally:
        log.collect(queries)
        log.write(queries)
        for q in queries.values():
            q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
