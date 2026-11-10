#!/usr/bin/env bash
# Reset the streaming side of BookPulse to a clean state (e.g. before a demo): stop the stream job, delete the
# Kafka topic, the streaming checkpoints, the Parquet archive and the streaming collections in MongoDB.
# The work is done by scripts/reset_streaming.py inside the spark container (see its docstring); the app page
# "Điều khiển demo" (/setting) runs the same script from a button.
#
# Run from bookpulse/ on the host:   bash scripts/reset_streaming.sh [--yes]
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: do not rewrite container paths

if [ "${1:-}" != "--yes" ]; then
  read -r -p "Delete the Kafka topic, streaming checkpoints, archive and streaming collections? [y/N] " ok
  [ "$ok" = "y" ] || [ "$ok" = "Y" ] || { echo "cancelled"; exit 1; }
fi

docker compose exec -T spark python scripts/reset_streaming.py
