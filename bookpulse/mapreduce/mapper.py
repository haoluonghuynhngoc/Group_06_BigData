#!/usr/bin/env python3
"""Map: one line of ratings.csv (user_id,book_id,rating) -> book_id \t count,sum,n1,n2,n3,n4,n5

The value is a vector of counts and sums only, so the combiner and the reducer can add vectors in any
order and any grouping (associative and commutative). The average is NOT emitted here: an average of
averages is wrong, so it is computed once, in the reducer, from the final sum and count.
Invalid lines are skipped and counted per reason with Hadoop counters (group "BookPulse").
"""
import sys


def reject(reason):
    print(f"reporter:counter:BookPulse,{reason},1", file=sys.stderr)


for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue
    fields = line.split(",")
    if fields[0].strip().lstrip("﻿") == "user_id":
        continue  # header line
    if len(fields) != 3:
        reject("WrongColumnCount")
        continue
    try:
        user_id, book_id, rating = (int(f) for f in fields)
    except ValueError:
        reject("NotAnInteger")
        continue
    if not 1 <= rating <= 5:
        reject("RatingOutOfRange")
        continue
    stars = ["0"] * 5
    stars[rating - 1] = "1"
    print(f"{book_id}\t1,{rating},{','.join(stars)}")
