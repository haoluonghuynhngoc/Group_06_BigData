#!/usr/bin/env bash
# Simulate the MapReduce job with a Unix pipe (map -> sort -> combine -> sort -> reduce), without Hadoop.
# Quick check before submitting to YARN; needs only python3 and sort.
#   bash mapreduce/run_local.sh [ratings.csv]
set -euo pipefail
cd "$(dirname "$0")"
INPUT=${1:-../data/goodbooks/ratings.csv}
OUT_DIR=../reports/mapreduce
mkdir -p "$OUT_DIR"

# LC_ALL=C sorts bytes like Hadoop's shuffle sorts Text keys
python3 mapper.py < "$INPUT" 2> "$OUT_DIR/local_rejects.log" \
  | LC_ALL=C sort -t $'\t' -k1,1 \
  | python3 combiner.py \
  | LC_ALL=C sort -t $'\t' -k1,1 \
  | python3 reducer.py \
  | sort -n > "$OUT_DIR/local_output.tsv"

echo "$(wc -l < "$OUT_DIR/local_output.tsv") books -> reports/mapreduce/local_output.tsv"
echo "rejected lines by reason:"
sort "$OUT_DIR/local_rejects.log" | uniq -c || true
