"""Retrain ALS with the ratings that arrived through the stream and switch the app to the new version.

Until now the loop "stream -> archive -> retrain" had to be run by hand as three commands. This script runs
the whole loop once, inside the spark container, and records what changed:
  1. stop the stream job if it is running (two 4 GB Spark drivers do not fit in the Docker VM; events
     sent meanwhile wait in Kafka and are read from the checkpoint when the job starts again)
  2. batch/clean.py          CSV + archive data/parquet/stream_events -> Parquet (dedup by event_id,
                             one rating per (user, book), from_stream = true)
  3. batch/train_als.py      --implicit with the parameters chosen on validation on 06/11 (rank 50,
                             regParam 0.1, alpha 1): ONE fit, no grid, so the step takes minutes
  4. batch/write_recs.py     new collection recommendations_<version>, meta.active switched when complete
  5. start the stream job again, if it was running at step 1
The top-10 of --user, as the app shows it (stored recs minus user_seen), is read before and after, and the
seconds of every step go to reports/retrain_<timestamp>.json.

    docker compose exec spark python scripts/retrain.py [--user 1] [--eval-users 1000] [--exclude-simulated]
The app page /setting runs it from the button "Train lại với rating mới" (scripts/control_server.py).
Not a per-event retrain: one run took 164-179 s on 15/11 (12 cores: clean 26-28 s, fit 71-74 s, write_recs 65 s;
estimated 6-8 minutes on 4 cores), so it is run on demand (this button) or on a schedule / when the archive
holds enough new events.
"""
import argparse
import glob
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from common.recs import visible_top10
from common.settings import DATA_DIR, REPORT_DIR, ROOT, STREAM_EVENTS_DIR, get_db, save_report
from common.stream_job import STREAM_JOB, stop_stream, stream_running

STREAM_LOG = Path("/tmp/bookpulse-control/stream_restart.log")
PY = sys.executable


def start_stream():
    STREAM_LOG.parent.mkdir(parents=True, exist_ok=True)
    out = open(STREAM_LOG, "w", encoding="utf-8")
    subprocess.Popen([PY, STREAM_JOB], cwd=ROOT, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                     start_new_session=True)
    for _ in range(90):
        time.sleep(2)
        if "streaming from" in STREAM_LOG.read_text(encoding="utf-8", errors="replace"):
            return "started"
        if not stream_running():
            return "FAILED to start, see " + str(STREAM_LOG)
    return "started (no banner yet)"


def archive_rows():
    """Rows and distinct events in the stream archive (quick count with pyarrow; duplicates included)."""
    import pyarrow.parquet as pq

    rows, ids = 0, set()
    for path in glob.glob(str(STREAM_EVENTS_DIR / "**" / "*.parquet"), recursive=True):
        t = pq.read_table(path, columns=["event_id"])
        rows += t.num_rows
        ids.update(t.column("event_id").to_pylist())
    return {"rows": rows, "distinct_events": len(ids)}


def newest_report(prefix):
    files = sorted(glob.glob(str(REPORT_DIR / f"{prefix}_*.json")), key=os.path.getmtime)
    return json.loads(Path(files[-1]).read_text(encoding="utf-8")) if files else None


def step(name, argv, timings):
    print(f"\n=== {time.strftime('%H:%M:%S')} {name}: python {' '.join(argv)}", flush=True)
    started = time.time()
    subprocess.run([PY] + argv, cwd=ROOT, check=True)
    timings[name] = round(time.time() - started, 1)
    print(f"=== {name} done in {timings[name]} s", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--user", type=int, default=1, help="user whose top-10 is recorded before and after")
    parser.add_argument("--eval-users", type=int, default=1000,
                        help="users sampled for Precision@10 on the test set (fewer = faster)")
    parser.add_argument("--exclude-simulated", action="store_true",
                        help="drop streamed events marked simulated=true (demo ratings) from the training data")
    parser.add_argument("--no-stream-restart", action="store_true", help="leave the stream job stopped at the end")
    parser.add_argument("--books", default=str(DATA_DIR / "goodbooks" / "books.csv"))
    args = parser.parse_args()

    started = time.time()
    db = get_db()
    before = visible_top10(db, args.user)
    archive = archive_rows() if STREAM_EVENTS_DIR.exists() else {"rows": 0, "distinct_events": 0}
    print(f"archive: {archive['rows']} rows, {archive['distinct_events']} distinct events; "
          f"active model {before['model_version']}", flush=True)

    timings = {}
    was_running = stream_running()
    if was_running:
        print("stopping the stream job (it is started again at the end)", flush=True)
        t = time.time()
        stream_stop = stop_stream()
        timings["stop_stream"] = round(time.time() - t, 1)
    else:
        stream_stop = "was not running"

    clean = train = recs = None
    failed_step = None
    try:
        clean_args = ["batch/clean.py"] + (["--exclude-simulated"] if args.exclude_simulated else [])
        step("clean", clean_args, timings)
        clean = newest_report("clean")
        step("train_als_implicit", ["batch/train_als.py", "--implicit", "--ranks", "50", "--regs", "0.1",
                                    "--alphas", "1", "--eval-users", str(args.eval_users)], timings)
        train = newest_report("train_als_implicit")
        step("write_recs", ["batch/write_recs.py", "--books", args.books], timings)
        recs = newest_report("write_recs")
    except subprocess.CalledProcessError as exc:
        # a failed step must not leave the stream job stopped: the app keeps the old version, the job goes on
        failed_step = exc.cmd[1] if len(exc.cmd) > 1 else str(exc.cmd)
        print(f"=== FAILED: {failed_step} exited with {exc.returncode}; meta.active unchanged", flush=True)

    stream_start = "left stopped"
    if was_running and not args.no_stream_restart:
        t = time.time()
        stream_start = start_stream()
        timings["start_stream"] = round(time.time() - t, 1)

    after = visible_top10(db, args.user)
    before_ids = [r["book_id"] for r in before["top10"]]
    after_ids = [r["book_id"] for r in after["top10"]]
    save_report("retrain", {
        "started": datetime.fromtimestamp(started, timezone.utc).isoformat(),
        "total_seconds": round(time.time() - started, 1),
        "step_seconds": timings,
        "stream_job": {"was_running": was_running, "stop": stream_stop, "restart": stream_start},
        "archive_before_clean": archive,
        "clean": {k: clean.get(k) for k in ("output_rows", "output_rows_from_stream", "stream_events")} if clean else None,
        "train": {"rows": train.get("rows"), "best_params": train.get("best_params"),
                  "ranking_test": train.get("ranking_test"), "fit_seconds": train.get("fit_seconds")} if train else None,
        "write_recs": {k: recs.get(k) for k in ("model_version", "collection", "users_written", "previous_collection")}
        if recs else None,
        "user": args.user,
        "before": before,
        "after": after,
        "top10_books_changed": len(set(before_ids) ^ set(after_ids)) // 2 if before_ids and after_ids else None,
        "exclude_simulated": args.exclude_simulated,
        "failed_step": failed_step,
    })
    if failed_step:
        sys.exit(1)


if __name__ == "__main__":
    main()
