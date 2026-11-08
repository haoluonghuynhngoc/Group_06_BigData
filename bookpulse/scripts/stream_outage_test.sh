#!/usr/bin/env bash
# Outage test: stop Kafka, then MongoDB, while the producer and the stream job are running, and record
# what each component does. Answers "what happens when Kafka / MongoDB dies?" with a log, not a guess.
#
#   1. reset the streaming side, start the stream job, start the producer (3000 events, 20/s)
#   2. after 30 s:  docker compose stop kafka, wait 40 s, start it again
#   3. check whether the stream job survived; if it stopped, restart it from its checkpoints
#   4. after 30 s:  docker compose stop mongo, wait 30 s, start it again; same check
#   5. when the producer is done: stop the job, run scripts/kafka_audit.py (Kafka vs MongoDB vs archive)
# Logs go to reports/streaming/outage_test_<timestamp>/ (events.log is the timeline).
# Takes about 5 minutes. Run from bookpulse/ on the host:   bash scripts/stream_outage_test.sh
set -uo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: do not rewrite container paths

OUT="reports/streaming/outage_test_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT"
say() { echo "$(date +%T) $*" | tee -a "$OUT/events.log"; }
job_alive() { docker compose exec -T spark pgrep -f "[s]treaming/stream_stats.py" >/dev/null 2>&1; }
wait_for() { for _ in $(seq 1 90); do grep -q "$2" "$1" 2>/dev/null && return 0; sleep 2; done; say "timeout: $2"; exit 1; }
JOB=1
start_job() {
  docker compose exec -T spark python streaming/stream_stats.py > "$OUT/job$JOB.log" 2>&1 &
  wait_for "$OUT/job$JOB.log" "streaming from"
  say "stream job #$JOB running"
}
check_job() {
  if job_alive; then say "stream job #$JOB still running after $1"
  else
    say "stream job #$JOB STOPPED after $1; last error: $(grep -oE '(pymongo|kafka)\.errors\.[A-Za-z]+|StreamingQueryException' "$OUT/job$JOB.log" | tail -1)"
    JOB=$((JOB + 1)); start_job; say "restarted from the same checkpoints as job #$JOB"
  fi
}

bash scripts/reset_streaming.sh --yes > "$OUT/reset.log" 2>&1
say "streaming state reset"
start_job
docker compose exec -T spark python streaming/producer.py --limit 3000 --seed 11 > "$OUT/producer.log" 2>&1 &
PROD=$!
say "producer started (3000 events, 20/s)"

sleep 30
say "STOP kafka"; docker compose stop kafka >/dev/null 2>&1
sleep 40
say "producer during the Kafka outage: $(tail -1 "$OUT/producer.log")"
say "START kafka"; docker compose start kafka >/dev/null 2>&1
sleep 30
check_job "the Kafka outage"

sleep 20
say "STOP mongo"; docker compose stop mongo >/dev/null 2>&1
sleep 30
say "START mongo"; docker compose start mongo >/dev/null 2>&1
sleep 30
check_job "the MongoDB outage"

wait $PROD; say "producer exit code $?: $(tail -1 "$OUT/producer.log")"
sleep 20
docker compose exec -T spark pkill -INT -f "[s]treaming/stream_stats.py" || true
for _ in $(seq 1 30); do job_alive || break; sleep 1; done
say "stream job stopped; checking Kafka vs MongoDB vs archive"
docker compose exec -T spark python scripts/kafka_audit.py > "$OUT/audit.log" 2>&1
grep -E '"messages"|distinct_valid|valid_messages|invalid_messages|valid_events_counted|expected_|rows|distinct_event_ids|_match|no_loss|exactly_once|uncommitted' "$OUT/audit.log" | tee -a "$OUT/events.log"
say "done -> $OUT"
