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
if doc:
    items = [r for r in doc["recs"] if r["book_id"] not in seen][:TOP_K]
    source = f"ALS · phiên bản mô hình {meta['model_version']}"
else:
    items = [{"book_id": p["book_id"], "score": None} for p in meta["popular"] if p["book_id"] not in seen][:TOP_K]
    source = "Người dùng mới (cold start): gợi ý sách phổ biến"

ids = [r["book_id"] for r in items]
books = {b["_id"]: b for b in db().books.find({"_id": {"$in": ids}})}
stats = {s["_id"]: s for s in db().rating_stats.find({"_id": {"$in": ids}})}
table = pd.DataFrame([{
    "#": i + 1,
    "book_id": r["book_id"],
    "Tên sách": books.get(r["book_id"], {}).get("title", "—"),
    "Tác giả": books.get(r["book_id"], {}).get("authors", "—"),
    "Điểm dự đoán": None if r.get("score") is None else round(r["score"], 2),
    "Điểm TB trực tiếp": round(stats[r["book_id"]]["avg_rating"], 2) if r["book_id"] in stats else None,
    "Lượt chấm (stream)": stats.get(r["book_id"], {}).get("n_ratings", 0),
} for i, r in enumerate(items)])

st.caption(source + f" · đã ẩn {len(seen)} cuốn người dùng vừa đọc")
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
