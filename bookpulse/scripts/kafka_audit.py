"""Count what is actually in the Kafka topic, as ground truth for checking the streaming job.

Reads every message of the topic from the beginning and reports total messages, distinct event_ids,
valid vs invalid events and how many messages repeat an event_id. Writes reports/kafka_audit_<ts>.json.
"""
import json

from kafka import KafkaConsumer

from common.settings import KAFKA_BOOTSTRAP, KAFKA_TOPIC, save_report


def main():
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP, auto_offset_reset="earliest",
                             enable_auto_commit=False, group_id=None, consumer_timeout_ms=10000,
                             value_deserializer=lambda v: json.loads(v.decode("utf-8")))
    total, seen, valid_ids, invalid = 0, set(), set(), 0
    partitions = {}
    for msg in consumer:
        total += 1
        partitions[msg.partition] = partitions.get(msg.partition, 0) + 1
        e = msg.value
        seen.add(e.get("event_id"))
        if isinstance(e.get("rating"), int) and 1 <= e["rating"] <= 5:
            valid_ids.add(e["event_id"])
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
        "invalid_messages": invalid,
    })


if __name__ == "__main__":
    main()
