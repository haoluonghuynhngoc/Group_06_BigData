"""Small synthetic dataset for testing the whole pipeline in a few minutes.

Users and books each belong to one of a few genres; users rate books of their own genre higher.
A few duplicate and invalid rows are mixed in so the cleaning step has something to remove.
Columns match Goodbooks-10k: ratings.csv (user_id,book_id,rating) and books.csv (book_id,title,authors).
"""
import argparse
import csv
import random

from common.settings import DATA_DIR

GENRES = ["Fantasy", "Romance", "Mystery", "Science"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--users", type=int, default=600)
    parser.add_argument("--books", type=int, default=300)
    parser.add_argument("--per-user", type=int, default=40)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    out = DATA_DIR / "sample"
    out.mkdir(parents=True, exist_ok=True)
    book_genre = {b: rng.randrange(len(GENRES)) for b in range(1, args.books + 1)}

    with open(out / "books.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["book_id", "title", "authors"])
        for b, g in book_genre.items():
            w.writerow([b, f"{GENRES[g]} book #{b}", f"Author {b % 50 + 1}"])

    rows = []
    for u in range(1, args.users + 1):
        taste = rng.randrange(len(GENRES))
        for b in rng.sample(range(1, args.books + 1), args.per_user):
            base = 4.3 if book_genre[b] == taste else 2.4
            rows.append([u, b, min(5, max(1, round(rng.gauss(base, 0.8))))])
    clean_rows = len(rows)
    rows += rng.sample(rows, clean_rows // 100)                             # duplicates
    rows += [[rng.randint(1, args.users), rng.randint(1, args.books), bad]  # invalid ratings
             for bad in rng.choices(["0", "9", ""], k=clean_rows // 200)]
    rng.shuffle(rows)

    with open(out / "ratings.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["user_id", "book_id", "rating"])
        w.writerows(rows)
    print(f"{len(rows)} rating rows ({clean_rows} clean) and {args.books} books -> {out}")


if __name__ == "__main__":
    main()
