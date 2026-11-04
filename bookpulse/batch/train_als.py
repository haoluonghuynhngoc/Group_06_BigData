"""Batch step 2: baselines + ALS (tuned with TrainValidationSplit) + evaluation on a held-out test set.

RMSE is compared with the global-mean and item-mean baselines; Precision@10 / Recall@10 with the
most-popular baseline. A book is "relevant" when the user rated it 4 or 5 in the test set, and books
the user already rated in training are never recommended.
"""
import argparse
import os
import time

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import functions as F

from common.recs import explode_recs, top_k_unseen
from common.settings import CHECKPOINT_DIR, MODEL_DIR, PARQUET_DIR, get_spark, save_report

K = 10


def ranking_metrics(recs, relevant, k):
    """Average Precision@k and Recall@k over users that have at least one relevant test book."""
    n_rel = relevant.groupBy("user_id").agg(F.count("*").alias("n_rel"))
    hits = recs.join(relevant, ["user_id", "book_id"]).groupBy("user_id").agg(F.count("*").alias("hits"))
    row = (
        n_rel.join(hits, "user_id", "left").fillna(0, subset=["hits"])
        .agg(
            F.avg(F.col("hits") / k).alias("precision"),
            F.avg(F.col("hits") / F.col("n_rel")).alias("recall"),
            F.count("*").alias("users"),
        )
        .first()
    )
    return {f"precision@{k}": round(row["precision"], 4), f"recall@{k}": round(row["recall"], 4),
            "users_evaluated": row["users"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ranks", default="10,20")
    parser.add_argument("--regs", default="0.05,0.1")
    parser.add_argument("--max-iter", type=int, default=10)
    parser.add_argument("--test-ratio", type=float, default=0.2)
    parser.add_argument("--eval-users", type=int, default=5000, help="users sampled for Precision/Recall")
    parser.add_argument("--candidates", type=int, default=200, help="ALS candidates per user before removing seen books")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    spark = get_spark("bookpulse-train")
    spark.sparkContext.setCheckpointDir(str(CHECKPOINT_DIR / "als"))
    started = time.time()

    ratings = spark.read.parquet(str(PARQUET_DIR / "ratings"))
    # No reliable rating timestamps in the CSV, so the split is random; see README for the time-based variant.
    train, test = ratings.randomSplit([1 - args.test_ratio, args.test_ratio], seed=args.seed)
    train, test = train.cache(), test.cache()
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")

    # Baselines for RMSE
    global_mean = train.agg(F.avg("rating")).first()[0]
    item_mean = train.groupBy("book_id").agg(F.avg("rating").alias("item_mean"))
    with_mean = test.join(item_mean, "book_id", "left")
    rmse = {
        "baseline_global_mean": evaluator.evaluate(with_mean.withColumn("prediction", F.lit(float(global_mean)))),
        "baseline_item_mean": evaluator.evaluate(
            with_mean.withColumn("prediction", F.coalesce("item_mean", F.lit(float(global_mean))))),
    }

    # ALS is the Estimator; TrainValidationSplit picks rank/regParam on a validation slice of train
    als = ALS(userCol="user_id", itemCol="book_id", ratingCol="rating", maxIter=args.max_iter,
              coldStartStrategy="drop", nonnegative=True, seed=args.seed)
    grid = (ParamGridBuilder()
            .addGrid(als.rank, [int(x) for x in args.ranks.split(",")])
            .addGrid(als.regParam, [float(x) for x in args.regs.split(",")])
            .build())
    tvs = TrainValidationSplit(estimator=als, estimatorParamMaps=grid, evaluator=evaluator,
                               trainRatio=0.8, seed=args.seed)
    fit_started = time.time()
    tvs_model = tvs.fit(train)
    fit_seconds = time.time() - fit_started
    model = tvs_model.bestModel
    tried = [{**{p.name: v for p, v in params.items()}, "validation_rmse": round(m, 4)}
             for params, m in zip(grid, tvs_model.validationMetrics)]
    best = min(tried, key=lambda t: t["validation_rmse"])
    rmse["als"] = evaluator.evaluate(model.transform(test))
    rmse = {name: round(value, 4) for name, value in rmse.items()}

    # Ranking metrics on a sample of test users
    eval_users = test.select("user_id").distinct().orderBy(F.rand(args.seed)).limit(args.eval_users).cache()
    relevant = test.filter(F.col("rating") >= 4).join(eval_users, "user_id").select("user_id", "book_id")
    seen = train.select("user_id", "book_id")
    als_recs = top_k_unseen(explode_recs(model.recommendForUserSubset(eval_users, args.candidates)), seen, K)
    popular = (train.groupBy("book_id").agg(F.count("*").alias("score"))
               .orderBy(F.desc("score")).limit(args.candidates))
    pop_recs = top_k_unseen(eval_users.crossJoin(popular), seen, K)
    ranking = {"als": ranking_metrics(als_recs, relevant, K),
               "baseline_popular": ranking_metrics(pop_recs, relevant, K)}

    model.write().overwrite().save(str(MODEL_DIR / "als"))
    save_report("train_als", {
        "spark_master": os.getenv("SPARK_MASTER", "local[*]"),
        "rows": {"train": train.count(), "test": test.count()},
        "grid": tried,
        "best_params": best,
        "rmse_test": rmse,
        "ranking_test": ranking,
        "fit_seconds": round(fit_seconds, 1),
        "total_seconds": round(time.time() - started, 1),
        "model_path": str(MODEL_DIR / "als"),
    })
    spark.stop()


if __name__ == "__main__":
    main()
