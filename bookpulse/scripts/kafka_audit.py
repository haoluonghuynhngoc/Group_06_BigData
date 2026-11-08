"""Count what is actually in the Kafka topic, as ground truth for checking the streaming job.

Reads every message of the topic from the beginning and reports total messages, distinct event_ids,
valid vs invalid events, how many messages repeat an event_id, and how many valid events are "late"
(Kafka received them more than --late-minutes after their event_time, i.e. beyond the watermark).

It then reads the streaming job's counters back from MongoDB and checks them against Kafka:
  valid events counted   == distinct valid events - distinct valid late events   (no loss, no double count)
  sum of rating_stats    == valid events counted
  rejected events        == invalid messages
  sum over trending windows == 2 x (valid - late)   (10-minute windows sliding every 5 minutes)
so every streaming number in the report has a saved check. Writes reports/kafka_audit_<ts>.json.
"""
import argparse
import json
from datetime import datetime, timezone

from kafka import KafkaConsumer

from common.settings import KAFKA_BOOTSTRAP, KAFKA_TOPIC, get_db, save_report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--late-minutes", type=float, default=10,
                        help="an event is 'late' when Kafka received it this much after its event_time "
                             "(the streaming job's watermark delay)")
    parser.add_argument("--no-mongo", action="store_true", help="only count Kafka, skip the MongoDB check")
    args = parser.parse_args()

    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP, auto_offset_reset="earliest",
                             enable_auto_commit=False, group_id=None, consumer_timeout_ms=10000,
                             value_deserializer=lambda v: json.loads(v.decode("utf-8")))
    total, seen, valid_ids, invalid = 0, set(), set(), 0
    late_valid_ids = set()
    partitions = {}
    for msg in consumer:
        total += 1
        partitions[msg.partition] = partitions.get(msg.partition, 0) + 1
        e = msg.value
        seen.add(e.get("event_id"))
        if isinstance(e.get("rating"), int) and 1 <= e["rating"] <= 5:
            valid_ids.add(e["event_id"])
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
    save_report("kafka_audit", report)


if __name__ == "__main__":
    main()
