"""Rating events in the format the stream job reads (same schema as streaming/producer.py)."""
import json
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

from common.settings import KAFKA_BOOTSTRAP


def make_producer():
    return KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP,
                         key_serializer=lambda k: str(k).encode("utf-8"),
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"), acks="all")


def rating_event(user_id, book_id, rating, minutes_ago=0.0):
    """A user rating a book now (or minutes_ago earlier: a late event for the watermark test).
    simulated=True: made by the demo, not taken from the dataset."""
    now = (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return {"event_id": uuid.uuid4().hex, "user_id": int(user_id), "book_id": int(book_id), "rating": int(rating),
            "event_time": now, "simulated": True}
