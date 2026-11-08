Test of the write_recs.py quality gate and version keeping (08/11/2026), run on a separate database:
  docker compose exec -e MONGO_DB=bookpulse_test spark python batch/write_recs.py --books data/goodbooks/books.csv
Before: meta.active -> recommendations_20000101_000000 (fake active version),
        recommendations_20000102_000000 (fake leftover of a failed run).
After:  active -> recommendations_20261108_043709 (53.424 of 53.424 users written, gate passed),
        previous_collection -> recommendations_20000101_000000 (kept for rollback),
        recommendations_20000102_000000 dropped, books upserted (10.000) before the switch.
The real database "bookpulse" was not touched (still recommendations_20261106_125827). bookpulse_test was dropped.
