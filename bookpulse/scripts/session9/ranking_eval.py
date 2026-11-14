"""Session 9, Card G mini-tests G1 (implicit semantics) and G2 (ranking test).

G2  Honest hold-out ranking test of the SERVING model (models/als_implicit, the one write_recs.py wrote
    to MongoDB) against the popularity baseline, with Precision@10, Recall@10 and NDCG@10:
      * split: the same seeded random 80/20 split as train_als.py (common.recs.split_train_test, seed 42),
        so the test rows are exactly the rows the serving model never saw;
      * relevance: a book the user rated 4 or 5 stars in the TEST set;
      * candidate universe: all 10,000 books minus the books the user rated in TRAIN (seen-item exclusion);
      * eligible users: users sampled from the test set that have >= 1 relevant test book (the denominator);
      * baseline: the 200 most-rated books in TRAIN, minus seen books, top 10.
    The explicit ALS model (models/als) is evaluated the same way for comparison.

G1  How 1-star and 5-star ratings enter implicit ALS, and what changes under a sensible alternative:
      * current scheme: every rating r is a positive interaction with confidence c = 1 + alpha * r
        (alpha = 1): a 1-star rating still says "the user chose this book", only with weight 2 instead of 6;
      * alternative "liked-only": only ratings >= 4 are positive interactions (1-3 stars are removed from
        the training signal, but stay in the seen set so they are never recommended again).
    The alternative is fitted with the same hyper-parameters (rank 50, regParam 0.1, alpha 1, 10 iterations,
    seed 42) on the same train split and scored on the same test users; it is NOT saved (models/ and MongoDB
    are untouched). One candidate user's top-10 is shown under both schemes.

    docker compose exec spark python scripts/session9/ranking_eval.py [--eval-users 5000] [--user 1]
Writes evidence/session9/E_model_validation.json (G2) and evidence/session9/G1_implicit_semantics.json.
"""
import argparse
import json
import time

from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import functions as F

from common.recs import explode_recs, split_train_test, top_k_unseen
from common.settings import CHECKPOINT_DIR, DATA_DIR, MODEL_DIR, PARQUET_DIR, ROOT, get_spark

K = 10
OUT_DIR = ROOT / "evidence" / "session9"


def metrics(recs, relevant, k):
    """Precision@k, Recall@k, NDCG@k averaged over users with >= 1 relevant test book (binary relevance)."""
    n_rel = relevant.groupBy("user_id").agg(F.count("*").alias("n_rel"))
    hits = recs.join(relevant, ["user_id", "book_id"])
    per_user = hits.groupBy("user_id").agg(
        F.count("*").alias("hits"),
        F.sum(F.lit(1.0) / F.log2(F.col("rank") + 1)).alias("dcg"))
    # ideal DCG: the min(n_rel, k) relevant books at ranks 1..min(n_rel, k)
    idcg = n_rel.withColumn("m", F.least(F.col("n_rel"), F.lit(k))).withColumn(
        "idcg", F.expr("aggregate(sequence(1, m), 0D, (acc, i) -> acc + 1D / log2(i + 1))"))
    row = (idcg.join(per_user, "user_id", "left").fillna(0, subset=["hits", "dcg"])
           .agg(F.avg(F.col("hits") / k).alias("precision"),
                F.avg(F.col("hits") / F.col("n_rel")).alias("recall"),
                F.avg(F.col("dcg") / F.col("idcg")).alias("ndcg"),
                F.count("*").alias("users"),
                F.sum(F.when(F.col("hits") > 0, 1).otherwise(0)).alias("users_with_hit"))
           .first())
    return {f"precision@{k}": round(row["precision"], 4), f"recall@{k}": round(row["recall"], 4),
            f"ndcg@{k}": round(row["ndcg"], 4), "users_evaluated": row["users"],
            "users_with_at_least_one_hit": row["users_with_hit"],
            "distinct_books_recommended": recs.select("book_id").distinct().count()}


def top10(model, user_id, seen, titles, candidates=200):
    users = seen.sparkSession.createDataFrame([(user_id,)], "user_id int")
    recs = top_k_unseen(explode_recs(model.recommendForUserSubset(users, candidates)), seen, K)
    return [{"rank": r["rank"], "book_id": r["book_id"], "score": round(float(r["score"]), 4),
             "title": titles.get(r["book_id"])} for r in recs.orderBy("rank").collect()]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--eval-users", type=int, default=5000)
    parser.add_argument("--user", type=int, default=1, help="candidate user shown for G1")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--candidates", type=int, default=200)
    parser.add_argument("--skip-alternative", action="store_true", help="G1: do not fit the liked-only model")
    args = parser.parse_args()

    spark = get_spark("session9-ranking-eval")
    spark.sparkContext.setCheckpointDir(str(CHECKPOINT_DIR / "als"))
    started = time.time()
    ratings = spark.read.parquet(str(PARQUET_DIR / "ratings"))
    train, test, n_streamed = split_train_test(ratings, 0.2, args.seed)
    train, test = train.cache(), test.cache()
    seen = train.select("user_id", "book_id").cache()

    eval_users = test.select("user_id").distinct().orderBy(F.rand(args.seed)).limit(args.eval_users).cache()
    relevant = test.filter(F.col("rating") >= 4).join(eval_users, "user_id").select("user_id", "book_id").cache()
    sampled = eval_users.count()
    eligible = relevant.select("user_id").distinct().count()

    books = spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(
        str(DATA_DIR / "goodbooks" / "books.csv"))
    titles = {int(r["book_id"]): r["title"] for r in books.select("book_id", "title").collect()}

    popular = (train.groupBy("book_id").agg(F.count("*").alias("score"))
               .orderBy(F.desc("score")).limit(args.candidates))
    pop_recs = top_k_unseen(eval_users.crossJoin(popular), seen, K).cache()

    implicit = ALSModel.load(str(MODEL_DIR / "als_implicit"))
    explicit = ALSModel.load(str(MODEL_DIR / "als"))
    imp_recs = top_k_unseen(explode_recs(implicit.recommendForUserSubset(eval_users, args.candidates)), seen, K).cache()
    exp_recs = top_k_unseen(explode_recs(explicit.recommendForUserSubset(eval_users, args.candidates)), seen, K).cache()

    g2 = {
        "test": "G2 ranking test (hold-out, Top-10 relevance vs popularity baseline)",
        "status": "measured",
        "measured_on": time.strftime("%Y-%m-%d %H:%M:%S"),
        "split": {"method": "seeded random 80/20 of CSV ratings (no timestamps in Goodbooks-10k, so no "
                            "temporal split); streamed ratings always in train", "seed": args.seed,
                  "train_rows": train.count(), "test_rows": test.count(), "streamed_in_train": n_streamed},
        "relevance_definition": "book rated 4 or 5 stars by the user in the TEST set",
        "candidate_universe": "10,000 books minus books the user rated in TRAIN (seen-item exclusion); "
                              f"ALS scores its top {args.candidates} candidates per user, then the 10 best unseen",
        "eligible_denominator": {"users_sampled_from_test": sampled, "users_with_ge1_relevant_test_book": eligible,
                                 "note": "metrics are averaged over the eligible users only"},
        "serving_model": {"path": str(MODEL_DIR / "als_implicit"), "implicitPrefs": True,
                          "rank": implicit.rank, "note": "the model write_recs.py wrote to MongoDB"},
        "results": {
            "als_implicit_serving": metrics(imp_recs, relevant, K),
            "als_explicit": metrics(exp_recs, relevant, K),
            "baseline_most_popular": metrics(pop_recs, relevant, K),
        },
    }
    lift = g2["results"]["als_implicit_serving"]["ndcg@10"] / max(g2["results"]["baseline_most_popular"]["ndcg@10"], 1e-9)
    g2["ndcg_lift_over_popularity"] = round(lift, 2)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "E_model_validation.json").write_text(json.dumps(g2, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(g2, indent=2, ensure_ascii=False), flush=True)

    # ---- G1: implicit semantics ----------------------------------------------------------------
    alpha = 1.0
    dist = {r["rating"]: r["n"] for r in train.groupBy("rating").agg(F.count("*").alias("n")).collect()}
    total = sum(dist.values())
    g1 = {
        "test": "G1 implicit semantics: how 1-star and 5-star records enter ALS",
        "status": "measured",
        "measured_on": time.strftime("%Y-%m-%d %H:%M:%S"),
        "serving_configuration": {"implicitPrefs": True, "alpha": alpha, "rank": 50, "regParam": 0.1,
                                  "maxIter": 10, "nonnegative": True, "coldStartStrategy": "drop",
                                  "ratingCol": "rating (1-5 stars, used as the implicit 'count')"},
        "transform_table_current_scheme": [
            {"stars": r, "preference_p": 1, "confidence_c=1+alpha*r": 1 + alpha * r,
             "share_of_train_rows_pct": round(100 * dist.get(r, 0) / total, 2)} for r in range(1, 6)],
        "reading": "Spark implicit ALS sets p = 1 for every observed (user, book) pair and weights the "
                   "squared error by c = 1 + alpha * rating; unobserved pairs have p = 0, c = 1. A 1-star "
                   "rating is therefore a POSITIVE signal with weight 2, a 5-star one weight 6: the ratio is "
                   "3x, but the sign is the same. 1-2 star rows are "
                   f"{round(100 * (dist.get(1, 0) + dist.get(2, 0)) / total, 2)}% of train.",
        "alternative_scheme": "liked-only: keep only ratings >= 4 as positive interactions (same hyper-parameters, "
                              "same split, same seen set and same test users); fitted here, not saved",
    }
    user_seen_rows = train.filter(F.col("user_id") == args.user)
    g1["candidate_user"] = {
        "user_id": args.user,
        "train_ratings": user_seen_rows.count(),
        "train_ratings_1_2_stars": user_seen_rows.filter(F.col("rating") <= 2).count(),
        "top10_current_scheme": top10(implicit, args.user, seen, titles, args.candidates),
    }
    if not args.skip_alternative:
        fit_started = time.time()
        liked = train.filter(F.col("rating") >= 4)
        alt = (ALS(userCol="user_id", itemCol="book_id", ratingCol="rating", implicitPrefs=True, alpha=alpha,
                   rank=50, regParam=0.1, maxIter=10, nonnegative=True, coldStartStrategy="drop", seed=args.seed)
               .fit(liked))
        g1["alternative_fit"] = {"train_rows_used": liked.count(), "fit_seconds": round(time.time() - fit_started, 1)}
        alt_recs = top_k_unseen(explode_recs(alt.recommendForUserSubset(eval_users, args.candidates)), seen, K).cache()
        g1["alternative_metrics_same_test_users"] = metrics(alt_recs, relevant, K)
        g1["current_metrics_same_test_users"] = g2["results"]["als_implicit_serving"]
        alt_top = top10(alt, args.user, seen, titles, args.candidates)
        g1["candidate_user"]["top10_alternative_scheme"] = alt_top
        cur_ids = [r["book_id"] for r in g1["candidate_user"]["top10_current_scheme"]]
        g1["candidate_user"]["books_in_both_top10"] = len(set(cur_ids) & {r["book_id"] for r in alt_top})
        # how many of the eligible users' top-10 lists change: overlap distribution
        both = (imp_recs.select("user_id", "book_id").join(alt_recs.select("user_id", "book_id"), ["user_id", "book_id"])
                .groupBy("user_id").agg(F.count("*").alias("overlap")))
        ov = (eval_users.join(both, "user_id", "left").fillna(0, subset=["overlap"])
              .agg(F.avg("overlap").alias("mean"), F.sum(F.when(F.col("overlap") == 10, 1).otherwise(0)).alias("identical"))
              .first())
        g1["top10_overlap_current_vs_alternative"] = {"mean_books_in_common_of_10": round(ov["mean"], 2),
                                                      "users_with_identical_top10": ov["identical"],
                                                      "users_compared": sampled}
    g1["total_seconds"] = round(time.time() - started, 1)
    (OUT_DIR / "G1_implicit_semantics.json").write_text(json.dumps(g1, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(g1, indent=2, ensure_ascii=False), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
