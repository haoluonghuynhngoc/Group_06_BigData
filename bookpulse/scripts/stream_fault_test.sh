#!/usr/bin/env bash
# Streaming fault test: kill -9 the stream job in the middle of a run, restart it from its checkpoints,
# and check against Kafka that nothing was lost or counted twice.
#
#   1. reset the streaming side (empty topic, checkpoints, collections)
#   2. start the stream job and the producer (1500 events, 3% late after 30 s, duplicates, invalid)
#   3. after 45 s kill -9 the Python driver and the JVM (no clean shutdown, no final commit)
#   4. 15 s later restart the job from the same checkpoints; it runs until the producer is done
#   5. scripts/kafka_audit.py counts the topic and checks MongoDB:
#        valid counted == distinct valid - late,  rating_stats sum == valid counted,
#        rejected == invalid,  trending sum == 2 x (valid - late)
# Logs go to reports/streaming/fault_test_<timestamp>/, the check to reports/kafka_audit_<timestamp>.json.
# Takes about 3 minutes. Run from bookpulse/ on the host:   bash scripts/stream_fault_test.sh
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: do not rewrite container paths

OUT="reports/streaming/fault_test_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT"
ts() { date +%T; }
wait_for() { for _ in $(seq 1 90); do grep -q "$2" "$1" 2>/dev/null && return 0; sleep 2; done; echo "timeout: $2"; exit 1; }

bash scripts/reset_streaming.sh --yes > "$OUT/reset.log" 2>&1
echo "$(ts) streaming state reset"

docker compose exec -T spark python streaming/stream_stats.py > "$OUT/job1.log" 2>&1 &
wait_for "$OUT/job1.log" "streaming from"
echo "$(ts) job #1 running; starting producer"
docker compose exec -T spark python streaming/producer.py --limit 1500 --late 0.03 --seed 7 \
  > "$OUT/producer.log" 2>&1 &
PROD=$!

sleep 45
echo "$(ts) kill -9 the stream job (Python driver + JVM)"
# [s]/[p]: the pattern must not match this sh -c command line itself, or pkill kills its own shell
docker compose exec -T spark sh -c 'pkill -9 -f "[s]treaming/stream_stats.py"; pkill -9 -f "[p]yspark-shell"; sleep 1;
  ps -eo pid,args | grep -E "[s]treaming/stream_stats.py|[p]yspark-shell" || echo "no stream process left"' \
  | tee "$OUT/kill.log"

sleep 15
echo "$(ts) restart the job from the same checkpoints"
docker compose exec -T spark python streaming/stream_stats.py --run-seconds 100 > "$OUT/job2.log" 2>&1 &
JOB2=$!
wait $PROD
echo "$(ts) producer: $(tail -1 "$OUT/producer.log")"
wait $JOB2 || true
echo "$(ts) job #2 stopped; checking against Kafka"

docker compose exec -T spark python scripts/kafka_audit.py | tee "$OUT/audit.log" | grep -E \
  '"messages"|distinct_valid|invalid_messages|valid_events_counted|expected_|sum_n_ratings|rejected_events_counted|_match|no_loss'
grep -h "stream_progress_" "$OUT"/job*.log | sed 's/.*progress -> /progress file: /' || true
echo "$(ts) done -> $OUT"
