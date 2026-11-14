"""Event generator: replays real ratings from the dataset as simulated rating events into Kafka.

Every event gets a fresh event_id and event_time (the dataset has no rating timestamps), so all
events are labelled simulated. A small share is sent twice or sent with an invalid rating on
purpose, to check that the streaming job drops them. With --late, a share of events carries an
event_time that is --late-minutes old: older than the job's watermark, so Spark drops them.

Late events are only sent after --late-after-seconds. In Spark 3.5 a stateful query filters late rows
with the watermark of the previous micro-batch, so the first two batches after a fresh start have no
watermark yet and let late rows through. 30 s at a 5 s trigger leaves room for those first batches
(the first one takes 6-12 s on a cold JVM).

By default the replayed (user, book) pairs are already in the training data, so they are never on anyone's
recommendation list. For pairs the model has not seen, generate a fake file with scripts/make_fake_stream.py
and pass it with --file data/fake/stream_ratings.csv. To rate a book that IS on a user's list, use the app's
"Chấm sao" form or scripts/rate_from_recs.py.
"""
import argparse
import json
import random
import time
import uuid

import pandas as pd
from kafka import KafkaProducer

from common.events import rating_event
from common.settings import DATA_DIR, KAFKA_BOOTSTRAP, KAFKA_TOPIC


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", default=str(DATA_DIR / "goodbooks" / "ratings.csv"))
    parser.add_argument("--rate", type=float, default=20, help="events per second")
    parser.add_argument("--limit", type=int, default=2000, help="number of ratings to replay")
    parser.add_argument("--dup", type=float, default=0.02, help="share of events sent twice")
    parser.add_argument("--bad", type=float, default=0.01, help="share of extra invalid events")
    parser.add_argument("--late", type=float, default=0.0,
                        help="share of events sent with an old event_time, to show the watermark dropping them")
    parser.add_argument("--late-minutes", type=float, default=30, help="how old a late event is")
    parser.add_argument("--late-after-seconds", type=float, default=30,
                        help="send late events only after this many seconds, so Spark already has a watermark")
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
        # Survive a broker restart: keep unsent records in the buffer up to 2 minutes and retry them.
        # A retry can write a record twice; the stream job and clean.py drop that copy by event_id.
        retries=10,
        retry_backoff_ms=1000,
        request_timeout_ms=120_000,
    )
    counts = {"sent": 0, "duplicates": 0, "invalid": 0, "late": 0, "failed": 0}

    def failed(_exc):  # delivery callback: a record Kafka never acknowledged is counted, not silently lost
        counts["failed"] += 1
    started = time.time()
    for row in sample.itertuples(index=False):
        late = (args.late > 0 and time.time() - started >= args.late_after_seconds
                and rng.random() < args.late)
        event = rating_event(row.user_id, row.book_id, row.rating, minutes_ago=args.late_minutes if late else 0)
        counts["late"] += late
        producer.send(KAFKA_TOPIC, key=event["user_id"], value=event).add_errback(failed)
        counts["sent"] += 1
        if rng.random() < args.dup:
            producer.send(KAFKA_TOPIC, key=event["user_id"], value=event).add_errback(failed)
            counts["duplicates"] += 1
        if rng.random() < args.bad:
            bad = dict(event, event_id=uuid.uuid4().hex, rating=9)
            producer.send(KAFKA_TOPIC, key=bad["user_id"], value=bad).add_errback(failed)
            counts["invalid"] += 1
        if counts["sent"] % 100 == 0:
            print(f"{counts} after {time.time() - started:.0f}s")
        time.sleep(1 / args.rate)
    producer.flush()
    print(f"done: {counts} to topic '{KAFKA_TOPIC}' in {time.time() - started:.0f}s")


if __name__ == "__main__":
    main()
