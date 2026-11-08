#!/usr/bin/env bash
# Reset the streaming side of BookPulse to a clean state (e.g. before a demo):
#   - stop a running stream job
#   - delete and recreate the Kafka topic (all events in it are lost)
#   - delete the streaming checkpoints and the Parquet archive data/parquet/stream_events
#   - drop the MongoDB collections written by the stream job
# Batch results (recommendations, models, reports/) are kept. Ratings that came in through the stream
# stay in data/parquet/ratings until batch/clean.py runs again (run_pipeline.sh does that).
#
# Run from bookpulse/ on the host:   bash scripts/reset_streaming.sh [--yes]
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: do not rewrite container paths

if [ "${1:-}" != "--yes" ]; then
  read -r -p "Delete the Kafka topic, streaming checkpoints, archive and streaming collections? [y/N] " ok
  [ "$ok" = "y" ] || [ "$ok" = "Y" ] || { echo "cancelled"; exit 1; }
fi

echo "[1/5] stop a running stream job (if any)"
docker compose exec -T spark pkill -INT -f "streaming/stream_stats.py" || echo "  no stream job running"
# Wait until it has really stopped, so no batch commits into a checkpoint we are about to delete
for i in $(seq 1 30); do
  docker compose exec -T spark pgrep -f "streaming/stream_stats.py" >/dev/null || break
  [ "$i" = 30 ] && { echo "  still running after 30 s, killing it"; docker compose exec -T spark pkill -9 -f "streaming/stream_stats.py" || true; }
  sleep 1
done

echo "[2/5] delete the Kafka topic rating-events (the stream job recreates it with 3 partitions)"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic rating-events 2>/dev/null || echo "  topic did not exist"

echo "[3/5] delete streaming checkpoints"
docker compose exec -T spark rm -rf /work/checkpoints/stream_valid /work/checkpoints/stream_rejected \
  /work/checkpoints/stream_trending /work/checkpoints/stream_events_archive /work/checkpoints/stream_events

echo "[4/5] delete the Parquet archive of streamed events"
docker compose exec -T spark rm -rf /work/data/parquet/stream_events

echo "[5/5] drop the streaming collections in MongoDB"
docker compose exec -T mongo mongosh bookpulse --quiet --eval \
  '["rating_stats","user_seen","online_eval","stream_state","trending"].forEach(c => print("  " + c + ": " + db[c].drop()))'

echo "Streaming state is clean. Start it again with: docker compose exec spark python streaming/stream_stats.py"
