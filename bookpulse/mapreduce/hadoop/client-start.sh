#!/usr/bin/env bash
# Client container entrypoint: wait for HDFS and YARN, prepare HDFS directories, then stay up.
set -euo pipefail

echo "[bookpulse] Python: $(python3 --version 2>&1) | $(hadoop version | head -n 1)"

echo "[bookpulse] Waiting for HDFS..."
for i in $(seq 1 90); do
  hdfs dfs -ls / >/dev/null 2>&1 && break
  [ "$i" -eq 90 ] && { echo "[bookpulse] ERROR: HDFS did not become ready." >&2; exit 1; }
  sleep 2
done

hdfs dfs -mkdir -p /tmp /bookpulse/input /mr-history/tmp /mr-history/done
hdfs dfs -chmod 1777 /tmp /mr-history/tmp /mr-history/done || true

echo "[bookpulse] Waiting for a YARN NodeManager..."
for i in $(seq 1 90); do
  yarn node -list 2>/dev/null | grep -q "RUNNING" && break
  [ "$i" -eq 90 ] && { echo "[bookpulse] WARNING: YARN not ready yet, give it another minute." >&2; break; }
  sleep 2
done

cat <<'MSG'
============================================================
 BOOKPULSE HADOOP READY
   HDFS NameNode UI : http://localhost:9870
   YARN RM UI       : http://localhost:8088
   JobHistory UI    : http://localhost:19888
 Run the MapReduce job:
   docker compose exec client bash /bookpulse/mapreduce/run_hadoop.sh
============================================================
MSG

tail -f /dev/null
