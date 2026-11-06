"""Streamlit demo: user ID -> top-10 unread books, with live rating statistics from the stream."""
import pandas as pd
import streamlit as st

from common.settings import get_db

TOP_K = 10

st.set_page_config(page_title="BookPulse", page_icon="📚", layout="wide")
st.title("📚 BookPulse – gợi ý sách")


@st.cache_resource
def db():
    return get_db()


meta = db().meta.find_one({"_id": "active"})
if meta is None:
    st.warning("Chưa có gợi ý. Hãy chạy batch/write_recs.py trước (xem README).")
    st.stop()

user_id = int(st.number_input("Mã người dùng (user ID)", min_value=1, value=1, step=1))
if st.button("Làm mới"):
    st.rerun()

seen = {d["book_id"] for d in db().user_seen.find({"user_id": user_id}, {"book_id": 1})}
doc = db()[meta["recs_collection"]].find_one({"_id": user_id})
# Implicit ALS scores are preference scores (roughly 0-1), not stars, so the column is labelled differently
model_type = meta.get("model_type", "explicit")
score_label = "Mức phù hợp" if model_type == "implicit" else "Điểm dự đoán (sao)"

filled = 0
if doc:
    items = [{**r, "source": "ALS"} for r in doc["recs"] if r["book_id"] not in seen][:TOP_K]
    source = f"ALS {model_type} · phiên bản mô hình {meta['model_version']}"
    # The user has read through the stored recs: top up with popular books they have not rated.
    # "fallback" is per user (written by write_recs.py); documents from older versions lack it.
    refill = doc["fallback"] if "fallback" in doc else [p["book_id"] for p in meta["popular"]]
    shown = {r["book_id"] for r in items}
    for book_id in refill:
        if len(items) >= TOP_K:
            break
        if book_id not in seen and book_id not in shown:
            items.append({"book_id": book_id, "score": None, "source": "Phổ biến (bù)"})
            shown.add(book_id)
            filled += 1
else:
    items = [{"book_id": p["book_id"], "score": None, "source": "Phổ biến"}
             for p in meta["popular"] if p["book_id"] not in seen][:TOP_K]
    source = "Người dùng mới (cold start): gợi ý sách phổ biến"

ids = [r["book_id"] for r in items]
books = {b["_id"]: b for b in db().books.find({"_id": {"$in": ids}})}
stats = {s["_id"]: s for s in db().rating_stats.find({"_id": {"$in": ids}})}
table = pd.DataFrame([{
    "#": i + 1,
    "book_id": r["book_id"],
    "Tên sách": books.get(r["book_id"], {}).get("title", "—"),
    "Tác giả": books.get(r["book_id"], {}).get("authors", "—"),
    score_label: None if r.get("score") is None else round(r["score"], 2),
    "Điểm TB trực tiếp": round(stats[r["book_id"]]["avg_rating"], 2) if r["book_id"] in stats else None,
    "Lượt chấm (stream)": stats.get(r["book_id"], {}).get("n_ratings", 0),
    "Nguồn": r["source"],
} for i, r in enumerate(items)])

st.caption(source + f" · đã ẩn {len(seen)} cuốn người dùng vừa đọc"
           + (f" · bù {filled} cuốn phổ biến chưa đọc" if filled else ""))
st.dataframe(table, hide_index=True, use_container_width=True)

st.subheader("Luồng streaming")
valid = db().stream_state.find_one({"_id": "valid_events"}) or {}
rejected = db().stream_state.find_one({"_id": "rejected_events"}) or {}
online = db().online_eval.find_one({"_id": f"hit_rate@{TOP_K}"}) or {}
c1, c2, c3 = st.columns(3)
c1.metric("Sự kiện hợp lệ đã xử lý", valid.get("events", 0))
c2.metric("Sự kiện lỗi bị loại", rejected.get("events", 0))
liked = online.get("liked_events", 0)
c3.metric(f"Tỷ lệ trúng top-{TOP_K} (online)", f"{online.get('hits', 0) / liked:.1%}" if liked else "—",
          help="Trong các lượt chấm 4–5 sao của người dùng đã có gợi ý, bao nhiêu cuốn nằm trong top-10 của họ")

st.subheader("Sách đang hot")
# Sliding windows overlap: the two newest windows both contain the latest events. Show the older of the
# two, which has been collecting for longer and so is the more complete picture.
starts = sorted(db().trending.distinct("window_start"), reverse=True)[:2]
if starts:
    ws = starts[-1]
    hot = list(db().trending.find({"window_start": ws}).sort([("n_ratings", -1), ("avg_rating", -1)]).limit(TOP_K))
    titles = {b["_id"]: b.get("title", "—") for b in db().books.find({"_id": {"$in": [h["book_id"] for h in hot]}})}
    st.caption(f"Cửa sổ event time {hot[0]['window_start']:%Y-%m-%d %H:%M}–{hot[0]['window_end']:%H:%M} UTC · "
               "Spark Structured Streaming gom theo cửa sổ trượt, có watermark; sự kiện trễ quá watermark bị loại")
    st.dataframe(pd.DataFrame([{
        "#": i + 1, "Tên sách": titles.get(h["book_id"], "—"), "Lượt chấm trong cửa sổ": h["n_ratings"],
        "Điểm TB trong cửa sổ": h["avg_rating"], "Lượt 4–5 sao": h["n_liked"],
    } for i, h in enumerate(hot)]), hide_index=True, use_container_width=True)
else:
    st.info("Chưa có dữ liệu cửa sổ: chạy streaming/stream_stats.py và streaming/producer.py.")
