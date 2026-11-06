"""Count what is actually in the Kafka topic, as ground truth for checking the streaming job.

Reads every message of the topic from the beginning and reports total messages, distinct event_ids,
valid vs invalid events, how many messages repeat an event_id, and how many valid events are "late"
(Kafka received them more than --late-minutes after their event_time, i.e. beyond the watermark). Writes reports/kafka_audit_<ts>.json.
"""
import argparse
import json
from datetime import datetime, timezone

from kafka import KafkaConsumer

from common.settings import KAFKA_BOOTSTRAP, KAFKA_TOPIC, save_report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--late-minutes", type=float, default=10,
                        help="an event is 'late' when Kafka received it this much after its event_time "
                             "(the streaming job's watermark delay)")
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
    save_report("kafka_audit", {
        "topic": KAFKA_TOPIC,
        "messages": total,
        "messages_per_partition": dict(sorted(partitions.items())),
        "distinct_event_ids": len(seen),
        "repeated_messages": total - len(seen),
        "distinct_valid_events": len(valid_ids),
        "distinct_valid_late_events": len(late_valid_ids),
        "late_threshold_minutes": args.late_minutes,
        "invalid_messages": invalid,
    })


if __name__ == "__main__":
    main()
