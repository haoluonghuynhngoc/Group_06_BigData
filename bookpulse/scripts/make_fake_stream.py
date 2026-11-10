"""Fake "new" ratings for the stream: (user, book) pairs that are NOT in ratings.csv.

producer.py replays rows of ratings.csv by default, so every replayed pair is already in the training
data: it adds no new information. This script writes a CSV in the same format (user_id,book_id,rating)
whose pairs never appear in the dataset, so producer.py can send them as ratings the model has not seen:

    python scripts/make_fake_stream.py                      # -> data/fake/stream_ratings.csv
    python streaming/producer.py --file data/fake/stream_ratings.csv

How the fake ratings are drawn, so they look like the real data:
- users: existing users, picked in proportion to how many books they have rated (active users rate more);
- books: picked in proportion to their number of ratings (popular books are rated more often);
- stars: drawn from the real distribution of ratings in the dataset (mostly 4-5 stars).
Pairs already in the dataset and repeated pairs are dropped. The seed makes the file reproducible.

The events are still sent with simulated=true, so batch/clean.py --exclude-simulated keeps them out of
a retrain. The model, the train/test split and every reported number are unchanged by this file.
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from common.settings import DATA_DIR


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


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", default=str(DATA_DIR / "goodbooks" / "ratings.csv"))
    parser.add_argument("--output", default=str(DATA_DIR / "fake" / "stream_ratings.csv"))
    parser.add_argument("--n", type=int, default=5000, help="number of fake ratings")
    parser.add_argument("--seed", type=int, default=2026)
    args = parser.parse_args()

    ratings = pd.read_csv(args.input, usecols=["user_id", "book_id", "rating"]).dropna()
    ratings = ratings[ratings["rating"].between(1, 5)].astype(int)
    fake = make_fake(ratings, args.n, args.seed)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    fake.to_csv(out, index=False)

    share = lambda s: (s.value_counts(normalize=True).sort_index() * 100).round(1).to_dict()
    print(f"{len(fake)} fake ratings -> {out}")
    print(f"users: {fake['user_id'].nunique()}, books: {fake['book_id'].nunique()}, "
          f"pairs already in the dataset: 0 (checked against {len(ratings):,} ratings)")
    print(f"stars % fake: {share(fake['rating'])}")
    print(f"stars % real: {share(ratings['rating'])}")


if __name__ == "__main__":
    main()
