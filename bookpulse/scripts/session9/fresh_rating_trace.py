"""Session 9, Card G mini-test G3: emit ONE new rating with a stable event_id and trace it end to end.

Runs inside the spark container while streaming/stream_stats.py is running (scripts/session9/run_g3.sh
does the whole sequence). Steps, every one timestamped:
  1. read the user's visible top-10 from MongoDB exactly as the app does (active recs minus user_seen);
  2. send one rating event for the user's #1 book with a FIXED event_id, then send the SAME message again
     (a duplicate, as a producer retry would);
  3. poll MongoDB until user_seen holds the (user, book) pair; record the latency Kafka -> Spark -> MongoDB;
  4. check rating_stats (the book must count 1 rating, not 2), stream_state, online_eval (a 5-star rating
     of a top-10 book is one hit), the recomputed visible top-10 (book hidden, next one moved up);
  5. check the Parquet archive data/parquet/stream_events: both messages are there (the archive keeps
     duplicates; batch/clean.py removes them by event_id), and meta.active is unchanged: the ALS list in
     MongoDB only changes after clean.py -> train_als.py -> write_recs.py (the retrain boundary).

    docker compose exec spark python scripts/session9/fresh_rating_trace.py --user 1 --rating 5
Writes evidence/session9/G3_fresh_rating_trace.json.
"""
import argparse
import glob
import json
import time
import uuid
from datetime import datetime, timezone

from common.events import make_producer
from common.settings import KAFKA_TOPIC, ROOT, STREAM_EVENTS_DIR, get_db

TOP_K = 10
OUT_DIR = ROOT / "evidence" / "session9"


def now():
    return datetime.now(timezone.utc)


def visible_top10(db, meta, user_id):
    """Same rule as app/home.py: stored ALS recs minus books in user_seen, first 10."""
    seen = {d["book_id"] for d in db.user_seen.find({"user_id": user_id}, {"book_id": 1})}
    doc = db[meta["recs_collection"]].find_one({"_id": user_id})
    if doc is None:
        return [], seen
    return [r["book_id"] for r in doc["recs"] if r["book_id"] not in seen][:TOP_K], seen


def archive_rows(event_id):
    import pyarrow.parquet as pq
    rows = 0
    for path in glob.glob(str(STREAM_EVENTS_DIR / "**" / "*.parquet"), recursive=True):
        t = pq.read_table(path, columns=["event_id"])
        rows += sum(1 for e in t.column("event_id").to_pylist() if e == event_id)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user", type=int, default=1)
    parser.add_argument("--rating", type=int, default=5, choices=range(1, 6))
    parser.add_argument("--timeout", type=int, default=90, help="seconds to wait for the stream job")
    args = parser.parse_args()

    db = get_db()
    meta = db.meta.find_one({"_id": "active"})
    if meta is None:
        raise SystemExit("meta.active not found: run batch/write_recs.py first")
    titles = lambda ids: {b["_id"]: b.get("title") for b in db.books.find({"_id": {"$in": list(ids)}})}

    before, seen_before = visible_top10(db, meta, args.user)
    if not before:
        raise SystemExit(f"user {args.user} has no visible recommendations")
    book_id = before[0]
    stats_before = db.rating_stats.find_one({"_id": book_id}) or {}
    state_before = db.stream_state.find_one({"_id": "valid_events"}) or {}
    online_before = db.online_eval.find_one({"_id": f"hit_rate@{TOP_K}"}) or {}

    event_id = uuid.uuid4().hex
    event = {"event_id": event_id, "user_id": args.user, "book_id": book_id, "rating": args.rating,
             "event_time": now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], "simulated": True}
    producer = make_producer()
    t_send = now()
    md1 = producer.send(KAFKA_TOPIC, key=args.user, value=event).get(timeout=30)
    md2 = producer.send(KAFKA_TOPIC, key=args.user, value=event).get(timeout=30)  # duplicate on purpose
    producer.flush()
    kafka = [{"partition": m.partition, "offset": m.offset} for m in (md1, md2)]

    t_seen = None
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        if db.user_seen.find_one({"_id": f"{args.user}_{book_id}"}):
            t_seen = now()
            break
        time.sleep(0.5)
    if t_seen is None:
        raise SystemExit("user_seen did not receive the event: is streaming/stream_stats.py running?")
    time.sleep(12)  # let the trending/archive queries commit their next micro-batch too

    after, seen_after = visible_top10(db, meta, args.user)
    stats_after = db.rating_stats.find_one({"_id": book_id}) or {}
    state_after = db.stream_state.find_one({"_id": "valid_events"}) or {}
    online_after = db.online_eval.find_one({"_id": f"hit_rate@{TOP_K}"}) or {}
    meta_after = db.meta.find_one({"_id": "active"})
    t = titles(set(before) | set(after))

    report = {
        "test": "G3 fresh rating: Kafka -> Structured Streaming -> MongoDB -> app view; retrain boundary",
        "status": "measured",
        "measured_on": t_send.isoformat(),
        "event": event,
        "sent_twice_same_event_id": True,
        "kafka_records": kafka,
        "timeline_utc": {"sent": t_send.isoformat(), "visible_in_user_seen": t_seen.isoformat(),
                         "latency_seconds_kafka_to_mongodb": round((t_seen - t_send).total_seconds(), 1),
                         "note": "stream job trigger is 5 seconds, so the latency is bounded by ~1-2 triggers"},
        "book": {"book_id": book_id, "title": t.get(book_id), "rank_before": 1},
        "mongodb_before": {"model_version": meta["model_version"], "recs_collection": meta["recs_collection"],
                           "user_seen_pairs_for_user": len(seen_before),
                           "rating_stats_for_book": {k: stats_before.get(k) for k in ("n_ratings", "avg_rating")},
                           "valid_events_counted": state_before.get("events", 0),
                           "online_eval": {k: online_before.get(k) for k in ("liked_events", "hits")}},
        "mongodb_after": {"model_version": meta_after["model_version"], "recs_collection": meta_after["recs_collection"],
                          "user_seen_pairs_for_user": len(seen_after),
                          "rating_stats_for_book": {k: stats_after.get(k) for k in ("n_ratings", "avg_rating")},
                          "valid_events_counted": state_after.get("events", 0),
                          "online_eval": {k: online_after.get(k) for k in ("liked_events", "hits")}},
        "checks": {
            "duplicate_dropped_in_speed_layer": (stats_after.get("n_ratings", 0) - stats_before.get("n_ratings", 0)) == 1
                                               and state_after.get("events", 0) - state_before.get("events", 0) == 1,
            "book_hidden_from_top10": book_id not in after,
            "next_book_moved_up": after[:1] == before[1:2],
            "online_hit_counted": (online_after.get("hits", 0) - online_before.get("hits", 0)) == (1 if args.rating >= 4 else 0),
            "als_recommendations_unchanged_until_retrain": meta_after["recs_collection"] == meta["recs_collection"],
        },
        "visible_top10_before": [{"rank": i + 1, "book_id": b, "title": t.get(b)} for i, b in enumerate(before)],
        "visible_top10_after": [{"rank": i + 1, "book_id": b, "title": t.get(b)} for i, b in enumerate(after)],
        "archive_parquet_rows_with_this_event_id": archive_rows(event_id),
        "retrain_boundary": "The ALS factors and the stored top-30 do NOT change from the stream. The event is in "
                            "data/parquet/stream_events; batch/clean.py merges it (dedup by event_id, from_stream=true), "
                            "train_als.py keeps it in train, write_recs.py writes recommendations_<new version> and "
                            "switches meta.active. Measured on 10/11/2026 (Q&A 36): after that sequence a user who "
                            "rated Harry Potter 1-5 got volumes 6 and 7 at #1-#2.",
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "G3_fresh_rating_trace.json").write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str),
                                                        encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
