#!/usr/bin/env bash
# Session 9, Card G mini-test G3, end to end (about 3 minutes), run from bookpulse/ on the host:
#   1. reset the streaming side (topic, checkpoints, archive, streaming collections; batch results kept)
#   2. start streaming/stream_stats.py for a fixed time, wait for its first micro-batch
#   3. scripts/session9/fresh_rating_trace.py: one new rating with a stable event_id, sent twice
#   4. when the job stops: scripts/kafka_audit.py (Kafka vs MongoDB vs Parquet archive)
# Logs and results go to evidence/session9/ (G3_fresh_rating_trace.json, G3_stream_job.log, G3_kafka_audit.json).
#   bash scripts/session9/run_g3.sh [user] [rating]
set -euo pipefail
cd "$(dirname "$0")/../.."
export MSYS_NO_PATHCONV=1
USER_ID=${1:-1}
RATING=${2:-5}
OUT=evidence/session9
mkdir -p "$OUT"
ts() { date +%T; }

echo "$(ts) reset streaming state"
docker compose exec -T spark python scripts/reset_streaming.py > "$OUT/G3_reset.log" 2>&1
echo "$(ts) start the stream job (runs 150 s)"
docker compose exec -T spark python streaming/stream_stats.py --run-seconds 150 > "$OUT/G3_stream_job.log" 2>&1 &
JOB=$!
for _ in $(seq 1 60); do grep -q "streaming from" "$OUT/G3_stream_job.log" 2>/dev/null && break; sleep 2; done
sleep 25   # first micro-batch on a cold JVM takes 7-12 s; wait so the event lands in a normal batch
echo "$(ts) send one fresh rating (user $USER_ID, $RATING stars) and trace it"
docker compose exec -T spark python scripts/session9/fresh_rating_trace.py --user "$USER_ID" --rating "$RATING" \
  | tee "$OUT/G3_trace.log" | grep -E '"latency|dropped|hidden|moved|hit_counted|unchanged|archive_parquet'
echo "$(ts) waiting for the stream job to stop"
wait $JOB || true
echo "$(ts) kafka_audit: Kafka vs MongoDB vs archive"
docker compose exec -T spark python scripts/kafka_audit.py > "$OUT/G3_kafka_audit.log" 2>&1
grep -E '"messages"|distinct_event_ids|repeated|valid_events_counted|expected_valid|no_loss|exactly_once|"rows"' "$OUT/G3_kafka_audit.log"
cp "$(ls -t reports/kafka_audit_*.json | head -1)" "$OUT/G3_kafka_audit.json"
echo "$(ts) done -> $OUT"
