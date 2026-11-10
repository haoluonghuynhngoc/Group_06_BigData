"""Reset the streaming side of BookPulse to a clean state (e.g. before a demo). Runs inside the spark container:
  1. stop a running stream job (SIGINT, then SIGKILL after 30 s)
  2. delete the Kafka topic (all events in it are lost; it is created again on the next send, 3 partitions)
  3. delete the streaming checkpoints
  4. delete the Parquet archive data/parquet/stream_events
  5. drop the MongoDB collections written by the stream job
Batch results (recommendations, models, reports/) are kept. Ratings that came in through the stream stay in
data/parquet/ratings until batch/clean.py runs again (run_pipeline.sh does that).

    docker compose exec spark python scripts/reset_streaming.py     # or: bash scripts/reset_streaming.sh --yes
The app page "Điều khiển demo" (/setting) runs this through scripts/control_server.py.
"""
import shutil
import subprocess
import time

from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError

from common.settings import CHECKPOINT_DIR, KAFKA_BOOTSTRAP, KAFKA_TOPIC, STREAM_EVENTS_DIR, get_db

STREAM_JOB = "streaming/stream_stats.py"
CHECKPOINTS = ["stream_valid", "stream_rejected", "stream_trending", "stream_events_archive", "stream_events"]
COLLECTIONS = ["rating_stats", "user_seen", "online_eval", "stream_state", "trending"]


def running():
    return subprocess.run(["pgrep", "-f", STREAM_JOB], capture_output=True).returncode == 0


def main():
    print("[1/5] stop a running stream job (if any)")
    if running():
        subprocess.run(["pkill", "-INT", "-f", STREAM_JOB])
        # Wait until it has really stopped, so no batch commits into a checkpoint we are about to delete
        for _ in range(30):
            if not running():
                break
            time.sleep(1)
        else:
            print("  still running after 30 s, killing it")
            subprocess.run(["pkill", "-9", "-f", STREAM_JOB])
            time.sleep(1)
        print("  stopped")
    else:
        print("  no stream job running")

    print(f"[2/5] delete the Kafka topic {KAFKA_TOPIC}")
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        admin.delete_topics([KAFKA_TOPIC])
        for _ in range(20):  # deletion is asynchronous: wait until the broker no longer lists it
            if KAFKA_TOPIC not in admin.list_topics():
                break
            time.sleep(0.5)
        print("  deleted")
    except UnknownTopicOrPartitionError:
        print("  topic did not exist")
    finally:
        admin.close()

    print("[3/5] delete streaming checkpoints")
    for name in CHECKPOINTS:
        shutil.rmtree(CHECKPOINT_DIR / name, ignore_errors=True)

    print("[4/5] delete the Parquet archive of streamed events")
    shutil.rmtree(STREAM_EVENTS_DIR, ignore_errors=True)

    print("[5/5] drop the streaming collections in MongoDB")
    db = get_db()
    for name in COLLECTIONS:
        db.drop_collection(name)
        print(f"  {name}: dropped")

    print("Streaming state is clean. Start the stream job again (app page /setting, or "
          "docker compose exec spark python streaming/stream_stats.py).")


if __name__ == "__main__":
    main()
