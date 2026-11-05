"""NoSQL evidence: read back what the batch and streaming jobs wrote to MongoDB and verify it.

Reports collection sizes, one sample recommendation document, the query plan of the app's main
access pattern (find by _id), read latency over random users, and consistency checks between
the streaming counters. Writes reports/nosql_evidence_<timestamp>.json.
"""
import argparse
import random
import time

from common.settings import get_db, save_report

TOP_K = 10


def percentile(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p / 100 * (len(values) - 1))))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reads", type=int, default=1000, help="random find-by-_id reads to time")
    parser.add_argument("--user", type=int, default=1, help="user shown as the sample document")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    db = get_db()
    meta = db.meta.find_one({"_id": "active"})
    if meta is None:
        raise SystemExit("meta.active not found: run batch/write_recs.py first")
    recs = db[meta["recs_collection"]]

    collections = {name: db[name].estimated_document_count() for name in sorted(db.list_collection_names())}

    sample = recs.find_one({"_id": args.user})
    if sample:
        titles = {b["_id"]: b.get("title") for b in db.books.find(
            {"_id": {"$in": [r["book_id"] for r in sample["recs"][:TOP_K]]}})}
        sample = {"_id": sample["_id"], "model_version": sample["model_version"],
                  "n_recs": len(sample["recs"]),
                  "top_5": [{**r, "title": titles.get(r["book_id"])} for r in sample["recs"][:5]]}

    # The app's access pattern: one document by primary key, top-10 slice only
    plan = recs.find({"_id": args.user}, {"recs": {"$slice": TOP_K}}).explain()
    winning = plan["queryPlanner"]["winningPlan"]
    winning = winning.get("queryPlan", winning)  # SBE plans nest the tree one level deeper
    stages = []
    while winning:
        stages.append(winning.get("stage"))
        winning = winning.get("inputStage")
    stats = plan.get("executionStats", {})

    ids = [d["_id"] for d in recs.find({}, {"_id": 1})]
    rng = random.Random(args.seed)
    latencies = []
    for user_id in rng.sample(ids, min(args.reads, len(ids))):
        t = time.perf_counter()
        recs.find_one({"_id": user_id}, {"recs": {"$slice": TOP_K}})
        latencies.append((time.perf_counter() - t) * 1000)

    # Streaming counters must agree with each other
    valid = db.stream_state.find_one({"_id": "valid_events"}) or {}
    rejected = db.stream_state.find_one({"_id": "rejected_events"}) or {}
    stats_sum = next(iter(db.rating_stats.aggregate(
        [{"$group": {"_id": None, "n": {"$sum": "$n_ratings"}}}])), {}).get("n", 0)
    online = db.online_eval.find_one({"_id": f"hit_rate@{TOP_K}"}) or {}

    save_report("nosql_evidence", {
        "active": {k: meta[k] for k in ("recs_collection", "model_version", "switched_at")},
        "collections": collections,
        "users_with_recs": recs.count_documents({}),
        "indexes_on_recs": list(recs.index_information()),
        "sample_document": sample,
        "find_by_id_plan": {"stage": " → ".join(reversed(stages)),
                            "docs_examined": stats.get("totalDocsExamined"),
                            "keys_examined": stats.get("totalKeysExamined")},
        "read_latency_ms": {"reads": len(latencies), "p50": round(percentile(latencies, 50), 2),
                            "p95": round(percentile(latencies, 95), 2),
                            "max": round(max(latencies), 2)},
        "streaming": {
            "valid_events": valid.get("events", 0),
            "rejected_events": rejected.get("events", 0),
            "last_batch": valid.get("last_batch"),
            "sum_n_ratings_in_rating_stats": stats_sum,
            "counts_agree": stats_sum == valid.get("events", 0),
            "books_with_live_stats": db.rating_stats.count_documents({}),
            "user_seen_pairs": db.user_seen.count_documents({}),
            "online_eval": {k: online.get(k) for k in ("liked_events", "hits", "cold_start_events")},
        },
    })


if __name__ == "__main__":
    main()
