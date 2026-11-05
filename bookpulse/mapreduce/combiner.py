#!/usr/bin/env python3
"""Combine: add the value vectors of one key locally, before the shuffle.

Input and output have the same format as the mapper output (book_id \t count,sum,n1..n5), so Hadoop
may run this zero, one or several times without changing the final result. Only integer sums: no
rounding, no average (see mapper.py).
"""
import sys

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
            print(f"{current_key}\t{','.join(map(str, totals))}")
        current_key, totals = key, vector
    else:
        totals = [a + b for a, b in zip(totals, vector)]

if current_key is not None:
    print(f"{current_key}\t{','.join(map(str, totals))}")
