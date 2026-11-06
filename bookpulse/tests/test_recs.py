"""Unit tests for common/recs.py (used by training evaluation and by write_recs.py). Needs PySpark:
    docker compose exec spark python -m unittest tests.test_recs -v
"""
import unittest

from pyspark.sql import SparkSession

from common.recs import explode_recs, top_k_unseen


class RecsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("test-recs")
                     .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_explode_recs_flattens_the_als_output(self):
        raw = self.spark.createDataFrame(
            [(1, [(10, 0.9), (11, 0.5)])],
            "user_id int, recommendations array<struct<book_id:int,rating:float>>")
        rows = sorted((r.user_id, r.book_id) for r in explode_recs(raw).collect())
        self.assertEqual(rows, [(1, 10), (1, 11)])

    def test_top_k_unseen_removes_read_books_and_keeps_the_best_k(self):
        candidates = self.spark.createDataFrame(
            [(1, 10, 0.9), (1, 11, 0.8), (1, 12, 0.7), (1, 13, 0.6), (2, 10, 0.5), (2, 14, 0.4)],
            "user_id int, book_id int, score double")
        seen = self.spark.createDataFrame([(1, 11), (2, 14)], "user_id int, book_id int")
        result = {(r.user_id, r.rank): r.book_id for r in top_k_unseen(candidates, seen, 2).collect()}
        # user 1 already read 11, so the top-2 is 10 then 12; user 2 already read 14, only 10 is left
        self.assertEqual(result, {(1, 1): 10, (1, 2): 12, (2, 1): 10})


if __name__ == "__main__":
    unittest.main()
