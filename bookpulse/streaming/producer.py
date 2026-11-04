"""Event generator: replays real ratings from the dataset as simulated rating events into Kafka.

Every event gets a fresh event_id and event_time (the dataset has no rating timestamps), so all
events are labelled simulated. A small share is sent twice or sent with an invalid rating on
purpose, to check that the streaming job drops them.
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer

from common.settings import DATA_DIR, KAFKA_BOOTSTRAP, KAFKA_TOPIC


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", default=str(DATA_DIR / "goodbooks" / "ratings.csv"))
    parser.add_argument("--rate", type=float, default=20, help="events per second")
    parser.add_argument("--limit", type=int, default=2000, help="number of ratings to replay")
    parser.add_argument("--dup", type=float, default=0.02, help="share of events sent twice")
    parser.add_argument("--bad", type=float, default=0.01, help="share of extra invalid events")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    df = pd.read_csv(args.file, usecols=["user_id", "book_id", "rating"])
    df = df[df["rating"].between(1, 5)].dropna()
    sample = df.sample(n=min(args.limit, len(df)), random_state=args.seed)
    rng = random.Random(args.seed)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=10,
    )
    counts = {"sent": 0, "duplicates": 0, "invalid": 0}
    started = time.time()
    for row in sample.itertuples(index=False):
        event = {"event_id": uuid.uuid4().hex, "user_id": int(row.user_id), "book_id": int(row.book_id),
                 "rating": int(row.rating), "event_time": now_utc(), "simulated": True}
        producer.send(KAFKA_TOPIC, key=event["user_id"], value=event)
        counts["sent"] += 1
        if rng.random() < args.dup:
            producer.send(KAFKA_TOPIC, key=event["user_id"], value=event)
            counts["duplicates"] += 1
        if rng.random() < args.bad:
            bad = dict(event, event_id=uuid.uuid4().hex, rating=9)
            producer.send(KAFKA_TOPIC, key=bad["user_id"], value=bad)
            counts["invalid"] += 1
        if counts["sent"] % 100 == 0:
            print(f"{counts} after {time.time() - started:.0f}s")
        time.sleep(1 / args.rate)
    producer.flush()
    print(f"done: {counts} to topic '{KAFKA_TOPIC}' in {time.time() - started:.0f}s")


if __name__ == "__main__":
    main()
