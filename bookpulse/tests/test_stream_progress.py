"""Unit tests for the observability part of streaming/stream_stats.py (ProgressLog).

ProgressLog is polled every 10 seconds and reads recentProgress, which repeats batches it has already
returned; totals must count every batch exactly once.
    docker compose exec spark python -m unittest tests.test_stream_progress -v
"""
import json
import tempfile
import unittest
from pathlib import Path

from streaming.stream_stats import ProgressLog


class FakeQuery:
    def __init__(self, progress):
        self.id, self.recentProgress = "q-1", progress


def batch(batch_id, rows, dropped_late=0, dropped_dups=0, watermark="2026-11-06T12:00:00.000Z"):
    return {"batchId": batch_id, "numInputRows": rows, "inputRowsPerSecond": rows / 5,
            "processedRowsPerSecond": rows / 2, "durationMs": {"triggerExecution": 2000},
            "eventTime": {"watermark": watermark},
            "stateOperators": [{"numRowsTotal": 100 + batch_id, "numRowsDroppedByWatermark": dropped_late,
                                "customMetrics": {"numDroppedDuplicateRows": dropped_dups}}]}


class ProgressLogTest(unittest.TestCase):
    def test_counts_each_batch_once_across_polls(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "progress.json"
            log = ProgressLog(path, {"watermark": "10 minutes"})
            query = FakeQuery([batch(0, 50), batch(1, 40, dropped_late=2)])
            log.collect({"valid": query})
            query.recentProgress = [batch(1, 40, dropped_late=2), batch(2, 30, dropped_late=1, dropped_dups=3)]
            log.collect({"valid": query})          # batch 1 is seen a second time
            log.write({"valid": query})
            q = json.loads(path.read_text(encoding="utf-8"))["queries"]["valid"]
        self.assertEqual(q["batches"], 3)
        self.assertEqual(q["input_rows"], 120)
        self.assertEqual(q["rows_dropped_by_watermark"], 3)
        self.assertEqual(q["duplicates_dropped"], 3)
        self.assertEqual(q["state_rows_now"], 102)
        self.assertEqual(q["max_batch_duration_ms"], 2000)


if __name__ == "__main__":
    unittest.main()


class FakeStateCollection:
    def __init__(self, doc):
        self.doc = doc

    def find_one(self, _query):
        return self.doc


class FakeDb:
    def __init__(self, last_batch=None):
        self.stream_state = FakeStateCollection(None if last_batch is None else {"last_batch": last_batch})


class AlreadyAppliedTest(unittest.TestCase):
    """stream_state.last_batch makes the non-idempotent $inc writes safe when Spark replays a batch."""

    def test_new_batch_is_applied(self):
        from streaming.stream_stats import already_applied
        self.assertFalse(already_applied(FakeDb(), "valid_events", 0))
        self.assertFalse(already_applied(FakeDb(last_batch=4), "valid_events", 5))

    def test_replayed_batch_is_skipped(self):
        from streaming.stream_stats import already_applied
        self.assertTrue(already_applied(FakeDb(last_batch=5), "valid_events", 5))

    def test_checkpoint_mismatch_stops_instead_of_skipping(self):
        # checkpoint deleted but MongoDB kept: batch ids restart at 0 while last_batch is 57
        from streaming.stream_stats import already_applied
        with self.assertRaises(RuntimeError):
            already_applied(FakeDb(last_batch=57), "valid_events", 0)
