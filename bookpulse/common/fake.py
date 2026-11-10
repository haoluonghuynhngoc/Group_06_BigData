"""Fake "new" ratings for the stream, shared by scripts/make_fake_stream.py and the app's simulator.

The fake ratings are (user, book) pairs that are NOT in ratings.csv, so they carry information the model
has not seen. Users are picked in proportion to how many books they have rated, books in proportion to
their number of ratings, stars from the real distribution. The seed makes the file reproducible.
"""
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

from common.events import rating_event
from common.settings import DATA_DIR

RATINGS_CSV = DATA_DIR / "goodbooks" / "ratings.csv"
FAKE_CSV = DATA_DIR / "fake" / "stream_ratings.csv"


def load_ratings(path=RATINGS_CSV):
    ratings = pd.read_csv(path, usecols=["user_id", "book_id", "rating"]).dropna()
    return ratings[ratings["rating"].between(1, 5)].astype(int)


def make_fake(ratings, n, seed):
    rng = np.random.default_rng(seed)
    user_counts = ratings["user_id"].value_counts()
    book_counts = ratings["book_id"].value_counts()
    star_counts = ratings["rating"].value_counts().sort_index()
    existing = set(zip(ratings["user_id"].to_numpy(), ratings["book_id"].to_numpy()))

    rows, seen = [], set()
    while len(rows) < n:
        m = 2 * (n - len(rows))  # draw extra: some pairs are already rated and get dropped
        users = rng.choice(user_counts.index.to_numpy(), size=m, p=(user_counts / user_counts.sum()).to_numpy())
        books = rng.choice(book_counts.index.to_numpy(), size=m, p=(book_counts / book_counts.sum()).to_numpy())
        stars = rng.choice(star_counts.index.to_numpy(), size=m, p=(star_counts / star_counts.sum()).to_numpy())
        for u, b, r in zip(users, books, stars):
            pair = (u, b)
            if pair in existing or pair in seen:
                continue
            seen.add(pair)
            rows.append((int(u), int(b), int(r)))
            if len(rows) == n:
                break
    return pd.DataFrame(rows, columns=["user_id", "book_id", "rating"])


def write_fake(n=5000, seed=2026, ratings_path=RATINGS_CSV, out=FAKE_CSV):
    ratings = load_ratings(ratings_path)
    fake = make_fake(ratings, n, seed)
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    fake.to_csv(out, index=False)
    return ratings, fake


def events_for(row, rng, dup=0.0, bad=0.0):
    """The Kafka messages for one fake rating: the event, sometimes a resent copy (same event_id, dropped by
    the stream job) and sometimes an extra invalid event (rating 9, counted as rejected). rng: random.Random."""
    event = rating_event(row.user_id, row.book_id, row.rating)
    out = [event]
    if rng.random() < dup:
        out.append(event)
    if rng.random() < bad:
        out.append(dict(event, event_id=uuid.uuid4().hex, rating=9))
    return out
