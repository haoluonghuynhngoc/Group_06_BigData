"""Count what is actually in the Kafka topic, as ground truth for checking the streaming job.

Reads every message of the topic from the beginning and reports total messages, distinct event_ids,
valid vs invalid events, how many messages repeat an event_id, and how many valid events are "late"
(Kafka received them more than --late-minutes after their event_time, i.e. beyond the watermark).

It then reads the streaming job's counters back from MongoDB and checks them against Kafka:
  valid events counted   == distinct valid events - distinct valid late events   (no loss, no double count)
  sum of rating_stats    == valid events counted
  rejected events        == invalid messages
  sum over trending windows == 2 x (valid - late)   (10-minute windows sliding every 5 minutes)
and counts the Parquet archive (data/parquet/stream_events) through its _spark_metadata log:
  archive rows == valid messages (no watermark, no de-duplication),  distinct ids == distinct valid
so every streaming number in the report has a saved check. Writes reports/kafka_audit_<ts>.json.
"""
import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

from kafka import KafkaConsumer

from common.settings import KAFKA_BOOTSTRAP, KAFKA_TOPIC, STREAM_EVENTS_DIR, get_db, save_report


def parse_json(value):
    """A message that is not JSON (or not an object) counts as invalid instead of stopping the audit."""
    try:
        parsed = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--late-minutes", type=float, default=10,
                        help="an event is 'late' when the producer sent it (Kafka message timestamp) this much "
                             "after its event_time (the streaming job's watermark delay). This approximates "
                             "Spark's rule (event_time < max event_time seen - watermark); the two agree because "
                             "the demo's late events are 30 minutes old, far beyond the 10-minute watermark")
    parser.add_argument("--no-mongo", action="store_true", help="only count Kafka, skip the MongoDB check")
    parser.add_argument("--no-archive", action="store_true", help="skip the Parquet archive check")
    args = parser.parse_args()

    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP, auto_offset_reset="earliest",
                             enable_auto_commit=False, group_id=None, consumer_timeout_ms=10000,
                             value_deserializer=parse_json)
    total, seen, valid_ids, invalid, valid_messages = 0, set(), set(), 0, 0
    late_valid_ids = set()
    partitions = {}
    for msg in consumer:
        total += 1
        partitions[msg.partition] = partitions.get(msg.partition, 0) + 1
        e = msg.value
        seen.add(e.get("event_id"))
        if isinstance(e.get("rating"), int) and 1 <= e["rating"] <= 5:
            valid_ids.add(e["event_id"])
            valid_messages += 1
            # msg.timestamp = when the producer sent it (ms); event_time = when the rating "happened"
            event_time = datetime.strptime(e["event_time"], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)
            if msg.timestamp / 1000 - event_time.timestamp() > args.late_minutes * 60:
                late_valid_ids.add(e["event_id"])
        else:
            invalid += 1
    consumer.close()
    report = {
        "topic": KAFKA_TOPIC,
        "messages": total,
        "messages_per_partition": dict(sorted(partitions.items())),
        "distinct_event_ids": len(seen),
        "repeated_messages": total - len(seen),
        "distinct_valid_events": len(valid_ids),
        "distinct_valid_late_events": len(late_valid_ids),
        "late_threshold_minutes": args.late_minutes,
        "invalid_messages": invalid,
        "valid_messages": valid_messages,
    }
    if not args.no_mongo:
        db = get_db()
        valid = db.stream_state.find_one({"_id": "valid_events"}) or {}
        rejected = db.stream_state.find_one({"_id": "rejected_events"}) or {}
        stats_sum = next(iter(db.rating_stats.aggregate(
            [{"$group": {"_id": None, "n": {"$sum": "$n_ratings"}}}])), {}).get("n", 0)
        trending_sum = next(iter(db.trending.aggregate(
            [{"$group": {"_id": None, "n": {"$sum": "$n_ratings"}}}])), {}).get("n", 0)
        expected = len(valid_ids) - len(late_valid_ids)
        report["mongo_check"] = {
            "valid_events_counted": valid.get("events", 0),
            "expected_valid_minus_late": expected,
            "sum_n_ratings_in_rating_stats": stats_sum,
            "rejected_events_counted": rejected.get("events", 0),
            "user_seen_pairs": db.user_seen.count_documents({}),
            "trending_windows": db.trending.count_documents({}),
            # windows are 10 min sliding every 5 min, so every on-time event sits in exactly 2 windows
            "trending_sum_n_ratings": trending_sum,
            "expected_trending_2x": 2 * expected,
            "no_loss_no_double_count": valid.get("events", 0) == expected == stats_sum,
            "rejected_match": rejected.get("events", 0) == invalid,
            "trending_match": trending_sum == 2 * expected,
        }
    if not args.no_archive:
        report["archive_check"] = archive_check(valid_messages, len(valid_ids))
    save_report("kafka_audit", report)


def archive_check(valid_messages, distinct_valid):
    """Count the Parquet archive the way a reader must: only files committed in _spark_metadata.

    The file sink is exactly-once because a micro-batch's files only count once its entry is in
    _spark_metadata; files of a batch killed half-way stay on disk but are not listed. The archive has
    no watermark and no de-duplication, so it should hold every valid message, late and repeated ones
    included, and every distinct valid event_id.
    """
    import pyarrow.parquet as pq

    meta_dir = STREAM_EVENTS_DIR / "_spark_metadata"
    if not meta_dir.is_dir():
        return {"present": False}
    # batch log files are named 0, 1, 2, ... and N.compact; skip Hadoop's .N.crc checksum files
    logs = sorted((p for p in meta_dir.iterdir() if re.fullmatch(r"\d+(\.compact)?", p.name)),
                  key=lambda p: int(p.name.split(".")[0]))
    compacts = [p for p in logs if p.name.endswith(".compact")]
    if compacts:  # a .compact file already holds every entry up to and including its batch
        logs = [p for p in logs if int(p.name.split(".")[0]) >= int(compacts[-1].name.split(".")[0])]
    committed = set()
    for log in logs:
        for line in log.read_text(encoding="utf-8").splitlines()[1:]:  # first line is the version "v1"
            entry = json.loads(line)
            path = Path(unquote(urlparse(entry["path"]).path))
            if entry.get("action", "add") == "add":
                committed.add(path)
            else:
                committed.discard(path)
    on_disk = {p for p in STREAM_EVENTS_DIR.rglob("*.parquet")}
    rows, ids = 0, set()
    for path in committed:
        table = pq.read_table(path, columns=["event_id"])
        rows += table.num_rows
        ids.update(table.column("event_id").to_pylist())
    return {
        "present": True,
        "committed_batches": len([p for p in meta_dir.iterdir() if re.fullmatch(r"\d+(\.compact)?", p.name)]),
        "committed_files": len(committed),
        "uncommitted_files_on_disk": len(on_disk - committed),
        "rows": rows,
        "expected_rows_valid_messages": valid_messages,
        "distinct_event_ids": len(ids),
        "expected_distinct_valid_events": distinct_valid,
        "exactly_once": rows == valid_messages and len(ids) == distinct_valid,
    }


if __name__ == "__main__":
    main()
