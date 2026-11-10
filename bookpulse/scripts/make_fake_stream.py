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
The app's sidebar ("Giả lập sự kiện") does the same without the command line: it creates this file
when missing and sends rows of it to Kafka.
"""
import argparse

from common.fake import FAKE_CSV, RATINGS_CSV, write_fake


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", default=str(RATINGS_CSV))
    parser.add_argument("--output", default=str(FAKE_CSV))
    parser.add_argument("--n", type=int, default=5000, help="number of fake ratings")
    parser.add_argument("--seed", type=int, default=2026)
    args = parser.parse_args()

    ratings, fake = write_fake(args.n, args.seed, args.input, args.output)
    out = args.output

    share = lambda s: (s.value_counts(normalize=True).sort_index() * 100).round(1).to_dict()
    print(f"{len(fake)} fake ratings -> {out}")
    print(f"users: {fake['user_id'].nunique()}, books: {fake['book_id'].nunique()}, "
          f"pairs already in the dataset: 0 (checked against {len(ratings):,} ratings)")
    print(f"stars % fake: {share(fake['rating'])}")
    print(f"stars % real: {share(ratings['rating'])}")


if __name__ == "__main__":
    main()
