"""Unit tests for the MapReduce component (mapreduce/mapper.py, combiner.py, reducer.py).

The scripts are run as Hadoop Streaming would run them: text in on stdin, text out on stdout, counters
on stderr. Sorting between stages stands in for Hadoop's shuffle/sort.
    python -m unittest tests.test_mapreduce -v      (no Spark or Hadoop needed)
"""
import subprocess
import sys
import unittest
from pathlib import Path

MR = Path(__file__).resolve().parents[1] / "mapreduce"


def run(script, text):
    p = subprocess.run([sys.executable, str(MR / script)], input=text, capture_output=True, text=True, check=True)
    return p.stdout, p.stderr


def shuffle(text):
    """Hadoop sorts map output by key (bytes) before the reducer."""
    return "".join(sorted(text.splitlines(keepends=True)))


def job(csv_text, combine_splits=None):
    """mapper -> [combiner per split] -> shuffle -> reducer; returns {book_id: fields}."""
    lines = csv_text.splitlines(keepends=True)
    splits = combine_splits or [lines]
    mapped = ""
    for split in splits:
        out, _ = run("mapper.py", "".join(split))
        if combine_splits:
            out, _ = run("combiner.py", shuffle(out))
        mapped += out
    reduced, _ = run("reducer.py", shuffle(mapped))
    return {line.split("\t")[0]: line.split("\t")[1:] for line in reduced.splitlines()}


SAMPLE = "user_id,book_id,rating\n1,10,5\n2,10,4\n3,10,1\n1,20,3\n4,20,3\n5,20,4\n"


class MapperTest(unittest.TestCase):
    def test_emits_count_sum_and_star_vector(self):
        out, err = run("mapper.py", "user_id,book_id,rating\n7,42,4\n")
        self.assertEqual(out, "42\t1,4,0,0,0,1,0\n")
        self.assertEqual(err, "")

    def test_rejects_bad_lines_with_a_counter_per_reason(self):
        bad = "user_id,book_id,rating\n1,2\n1,x,5\n1,2,0\n1,2,9\n1,2,4.5\n"
        out, err = run("mapper.py", bad)
        self.assertEqual(out, "")
        counters = [line.split(",")[1] for line in err.splitlines()]
        self.assertEqual(counters, ["WrongColumnCount", "NotAnInteger", "RatingOutOfRange",
                                    "RatingOutOfRange", "NotAnInteger"])


class JobTest(unittest.TestCase):
    def test_statistics_per_book(self):
        result = job(SAMPLE)
        # book_id: n_ratings, sum, avg, n_liked, n1..n5
        self.assertEqual(result["10"], ["3", "10", "3.3333", "2", "1", "0", "0", "1", "1"])
        self.assertEqual(result["20"], ["3", "10", "3.3333", "1", "0", "0", "2", "1", "0"])

    def test_combiner_does_not_change_the_result(self):
        """Hadoop may run the combiner zero, one or several times; the output must not depend on it."""
        lines = SAMPLE.splitlines(keepends=True)
        without = job(SAMPLE)
        one_split = job(SAMPLE, combine_splits=[lines])
        three_splits = job(SAMPLE, combine_splits=[lines[:2], lines[2:4], lines[4:]])
        self.assertEqual(without, one_split)
        self.assertEqual(without, three_splits)

    def test_average_is_rounded_once_at_the_end(self):
        """An average of partial averages would be wrong: (5 + avg(1,1)) / 2 = 3, the true mean is 7/3."""
        data = "user_id,book_id,rating\n1,9,5\n2,9,1\n3,9,1\n"
        lines = data.splitlines(keepends=True)
        result = job(data, combine_splits=[lines[:2], lines[2:]])
        self.assertEqual(result["9"][2], "2.3333")


if __name__ == "__main__":
    unittest.main()
