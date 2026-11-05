#!/usr/bin/env python3
"""Reduce: add all vectors of one book and write the final statistics.

Output (tab separated, no header):
  book_id  n_ratings  sum_rating  avg_rating  n_liked  n1  n2  n3  n4  n5
avg_rating = sum_rating / n_ratings, rounded once here to 4 decimals; n_liked = ratings of 4 or 5 stars.
"""
import sys


def emit(book_id, totals):
    count, total, *stars = totals
    avg = total / count
    liked = stars[3] + stars[4]
    print("\t".join([book_id, str(count), str(total), f"{avg:.4f}", str(liked)] + [str(s) for s in stars]))


current_key = None
totals = None

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue
    key, value = line.split("\t", 1)
    vector = [int(v) for v in value.split(",")]
    if key != current_key:
        if current_key is not None:
            emit(current_key, totals)
        current_key, totals = key, vector
    else:
        totals = [a + b for a, b in zip(totals, vector)]

if current_key is not None:
    emit(current_key, totals)
