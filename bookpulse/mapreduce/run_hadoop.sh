#!/usr/bin/env bash
# Run the BookPulse MapReduce job (rating statistics per book) on Hadoop: HDFS + YARN + Hadoop Streaming.
# Runs inside the Hadoop client container:
#   cd bookpulse/mapreduce/hadoop && docker compose exec client bash /bookpulse/mapreduce/run_hadoop.sh
# Then check it against Spark SQL from the BookPulse stack (bookpulse/):
#   docker compose exec spark python mapreduce/verify_with_spark.py
set -euo pipefail
cd "$(dirname "$0")"

LOCAL_INPUT=${1:-/bookpulse/data/goodbooks/ratings.csv}
HDFS_INPUT=/bookpulse/input/ratings.csv
HDFS_OUTPUT=/bookpulse/output/book_rating_stats
BLOCK_SIZE=${BLOCK_SIZE:-33554432}   # 32 MB: the 72 MB Goodbooks file spans 3 blocks -> 3 map tasks
REDUCERS=${REDUCERS:-2}              # 2 reduce tasks: HashPartitioner splits the book_ids between them
OUT_DIR=/bookpulse/reports/mapreduce
STAMP=$(date -u +%Y%m%d_%H%M%S)
JAR=$(find /opt/hadoop/share/hadoop/tools/lib -maxdepth 1 -name 'hadoop-streaming-*.jar' | head -n 1)
mkdir -p "$OUT_DIR"

echo "[1/4] Upload $LOCAL_INPUT to HDFS ($BLOCK_SIZE-byte blocks)"
hdfs dfs -mkdir -p "$(dirname "$HDFS_INPUT")"
hdfs dfs -D dfs.blocksize="$BLOCK_SIZE" -put -f "$LOCAL_INPUT" "$HDFS_INPUT"
hdfs fsck "$HDFS_INPUT" -files -blocks 2>/dev/null | grep -E "^$HDFS_INPUT|Total blocks" | tee "$OUT_DIR/hdfs_blocks_$STAMP.txt"

echo "[2/4] Remove previous output"
hdfs dfs -rm -r -f "$HDFS_OUTPUT" >/dev/null 2>&1 || true

echo "[3/4] Submit the Hadoop Streaming job to YARN (mapper -> combiner -> shuffle/sort -> reducer)"
started=$(date +%s)
hadoop jar "$JAR" \
  -D mapreduce.job.name="BookPulse-book-rating-stats" \
  -D mapreduce.job.reduces="$REDUCERS" \
  -files mapper.py,combiner.py,reducer.py \
  -mapper "python3 mapper.py" \
  -combiner "python3 combiner.py" \
  -reducer "python3 reducer.py" \
  -input "$HDFS_INPUT" \
  -output "$HDFS_OUTPUT" 2>&1 | tee "$OUT_DIR/job_$STAMP.log"
echo "elapsed_seconds=$(( $(date +%s) - started ))" | tee -a "$OUT_DIR/job_$STAMP.log"

echo "[4/4] Fetch the output from HDFS"
hdfs dfs -ls "$HDFS_OUTPUT"
hdfs dfs -cat "$HDFS_OUTPUT/part-*" | sort -n > "$OUT_DIR/hadoop_output_$STAMP.tsv"
echo "$(wc -l < "$OUT_DIR/hadoop_output_$STAMP.tsv") books -> reports/mapreduce/hadoop_output_$STAMP.tsv"
echo "Next (from bookpulse/): docker compose exec spark python mapreduce/verify_with_spark.py"
