"""Send rating events for books that are currently on a user's top-10, to show the app react live.

producer.py replays ratings that are already in the training data, and write_recs.py removes every
book a user has rated from their recommendations, so a replayed rating never touches a visible list.
This script plays a user who picks books from their own recommendations: it reads the active list
from MongoDB, skips books already hidden, and sends --n rating events for the top visible books.
Once the streaming job has processed them (one trigger, about 5-10 s), the app hides those books,
moves the next recommendations up, and counts a liked event that was a top-10 hit.

The app's "Chấm sao" form does the same for one book chosen in the UI; this script is the command-line
fallback. Run while streaming/stream_stats.py is running:
    docker compose exec spark python scripts/rate_from_recs.py --user 1 --n 2 --rating 5
"""
import argparse

from common.events import make_producer, rating_event
from common.settings import KAFKA_TOPIC, get_db

TOP_K = 10


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--user", type=int, default=1)
    parser.add_argument("--n", type=int, default=1, help="how many of the top visible books to rate")
    parser.add_argument("--rating", type=int, default=5, choices=range(1, 6))
    args = parser.parse_args()

    db = get_db()
    meta = db.meta.find_one({"_id": "active"})
    if meta is None:
        raise SystemExit("meta.active not found: run batch/write_recs.py first")
    doc = db[meta["recs_collection"]].find_one({"_id": args.user})
    if doc is None:
        raise SystemExit(f"user {args.user} has no recommendations (cold start)")
    seen = {d["book_id"] for d in db.user_seen.find({"user_id": args.user}, {"book_id": 1})}
    visible = [r["book_id"] for r in doc["recs"] if r["book_id"] not in seen][:TOP_K]
    picked = visible[:args.n]
    titles = {b["_id"]: b.get("title") for b in db.books.find({"_id": {"$in": picked}})}

    producer = make_producer()
    for book_id in picked:
        producer.send(KAFKA_TOPIC, key=args.user, value=rating_event(args.user, book_id, args.rating))
        print(f"user {args.user} rated book {book_id} ({titles.get(book_id)}) {args.rating} stars")
    producer.flush()
    print(f"sent {len(picked)} event(s) to '{KAFKA_TOPIC}'. After the next trigger the app hides them "
          "and moves the next recommendations up.")


if __name__ == "__main__":
    main()
