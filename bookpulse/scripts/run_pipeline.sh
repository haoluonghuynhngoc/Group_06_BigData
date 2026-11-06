#!/usr/bin/env bash
# Run the whole BookPulse batch pipeline on Goodbooks-10k with one command, in order, from bookpulse/:
#   download -> clean -> Spark SQL -> ALS explicit (RMSE + baselines) -> ALS implicit (serving model)
#   -> write recommendations to MongoDB -> read MongoDB back (NoSQL evidence)
# Every step writes its measurements to reports/. Takes about 25-35 minutes on a 4-core laptop.
#
#   docker compose up -d
#   bash scripts/run_pipeline.sh
# The MapReduce component runs on its own Hadoop cluster: see README section 8.
set -euo pipefail
cd "$(dirname "$0")/.."

run() {
  echo
  echo "=== $(date +%H:%M:%S) $*"
  docker compose exec -T spark "$@"
}

run python scripts/download_goodbooks.py
run python batch/clean.py
run python batch/analytics_sql.py --books data/goodbooks/books.csv --raw-csv data/goodbooks/ratings.csv
run python batch/train_als.py
run python batch/train_als.py --implicit --ranks 20,50 --regs 0.1 --alphas 1,10
run python batch/write_recs.py --books data/goodbooks/books.csv
run python scripts/nosql_evidence.py
echo
echo "=== $(date +%H:%M:%S) done. Results: reports/ ; app: http://localhost:8501"
