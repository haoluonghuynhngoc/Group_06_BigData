"""Streamlit demo: user ID -> top-10 unread books, with live rating statistics from the stream."""
import pandas as pd
import streamlit as st

from common.events import make_producer, rating_event
from common.settings import KAFKA_TOPIC, get_db

TOP_K = 10

st.set_page_config(page_title="BookPulse", page_icon="📚", layout="wide")
st.title("📚 BookPulse – gợi ý sách")


@st.cache_resource
def db():
    return get_db()


@st.cache_resource
def producer():
    return make_producer()


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

filled = hidden = 0
if doc:
    # Books the user just rated through the stream that were on their stored list (and are now hidden)
    hidden = sum(1 for r in doc["recs"] if r["book_id"] in seen)
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
    # Last resort: the per-user refill is used up too, so take globally popular books the user has not rated
    for p in meta["popular"]:
        if len(items) >= TOP_K:
            break
        if p["book_id"] not in seen and p["book_id"] not in shown:
            items.append({"book_id": p["book_id"], "score": None, "source": "Phổ biến (bù)"})
            shown.add(p["book_id"])
            filled += 1
    if not any(r["source"] == "ALS" for r in items):
        st.info("Người dùng này đã chấm hết danh sách gợi ý đã tính sẵn: tạm hiện sách phổ biến chưa đọc. "
                "Gợi ý cá nhân mới sẽ có sau lần huấn luyện lại tiếp theo (lượt chấm mới được đưa vào dữ liệu huấn luyện).")
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
    score_label: "—" if r.get("score") is None else f"{r['score']:.2f}",
    # Live stats cover only ratings streamed since the last reset (speed layer), not the batch history
    "Điểm TB (stream)": f'{stats[r["book_id"]]["avg_rating"]:.2f}' if r["book_id"] in stats else "—",
    "Lượt chấm (stream)": stats.get(r["book_id"], {}).get("n_ratings", 0),
    "Nguồn": r["source"],
} for i, r in enumerate(items)])

st.caption(source + f" · {len(seen)} lượt chấm mới qua streaming, {hidden} cuốn trong số đó đã bị ẩn khỏi danh sách"
           + (f" · bù {filled} cuốn phổ biến chưa đọc" if filled else ""))
st.dataframe(table, hide_index=True, use_container_width=True)

# Rate a book from the list: the app only sends the event to Kafka. The stream job (streaming/stream_stats.py,
# must be running) validates it and writes user_seen; on the next refresh the book is hidden and the next
# recommendation moves up. Nothing is written to MongoDB from here.
st.subheader("Chấm sao")
with st.form("rate", clear_on_submit=False):
    c_book, c_star, c_send = st.columns([6, 2, 2])
    labels = {r["book_id"]: f'#{i + 1} · {books.get(r["book_id"], {}).get("title", r["book_id"])}'
              for i, r in enumerate(items)}
    book_id = c_book.selectbox("Sách", list(labels), format_func=labels.get) if labels else None
    stars = c_star.selectbox("Số sao", [5, 4, 3, 2, 1], format_func=lambda n: "★" * n)
    c_send.write("")
    sent = c_send.form_submit_button("Gửi lượt chấm", use_container_width=True, disabled=book_id is None)
if sent and book_id is not None:
    try:
        producer().send(KAFKA_TOPIC, key=user_id, value=rating_event(user_id, book_id, stars)).get(timeout=10)
        st.session_state["last_rating"] = (user_id, labels[book_id], stars)
    except Exception as exc:  # Kafka down or unreachable: say so instead of failing the page
        st.error(f"Không gửi được vào Kafka ({type(exc).__name__}). Kafka có đang chạy không?")
if st.session_state.get("last_rating", (None,))[0] == user_id:
    _, label, n = st.session_state["last_rating"]
    st.success(f"Đã gửi vào Kafka: user {user_id} chấm {n} sao cho {label}. Job streaming xử lý trong khoảng "
               "5–10 giây; bấm **Làm mới** để thấy cuốn này bị ẩn và cuốn tiếp theo được đẩy lên.")

st.subheader("Luồng streaming")
valid = db().stream_state.find_one({"_id": "valid_events"}) or {}
rejected = db().stream_state.find_one({"_id": "rejected_events"}) or {}
online = db().online_eval.find_one({"_id": f"hit_rate@{TOP_K}"}) or {}
c1, c2, c3 = st.columns(3)
c1.metric("Sự kiện hợp lệ đã xử lý", valid.get("events", 0))
c2.metric("Sự kiện lỗi bị loại", rejected.get("events", 0))
liked = online.get("liked_events", 0)
c3.metric(f"Lượt 4–5 sao trúng top-{TOP_K}", f"{online.get('hits', 0)} / {liked}" if liked else "—",
          help="Trong các lượt chấm 4–5 sao của người dùng đã có gợi ý, bao nhiêu cuốn nằm trong top-10 của họ")
if liked:
    st.caption("producer.py phát lại rating đã có trong dữ liệu huấn luyện; những sách đó đã bị loại khỏi gợi ý "
               "nên không thể trúng. Một cuốn trúng top-10 chỉ khi người dùng chấm sách đang được gợi ý "
               "(mục Chấm sao ở trên, hoặc scripts/rate_from_recs.py).")

st.subheader("Sách đang hot")
# Sliding windows overlap: the two newest windows both contain the latest events. Show the older of the
# two, which has been collecting for longer and so is the more complete picture.
starts = sorted(db().trending.distinct("window_start"), reverse=True)[:2]
if starts:
    ws = starts[-1]
    window_end = db().trending.find_one({"window_start": ws})["window_end"]
    # "Hot" = liked right now: only books with at least one 4-5 star rating in the window are listed, ranked
    # by the number of 4-5 star ratings, then average. A 1-2 star rating adds to n_ratings but can never push
    # a book onto (or up) this list. The stream job still stores every rating per window (n_ratings).
    hot = list(db().trending.find({"window_start": ws, "n_liked": {"$gte": 1}})
               .sort([("n_liked", -1), ("avg_rating", -1), ("book_id", 1)]).limit(TOP_K))
    st.caption(f"Cửa sổ event time {ws:%Y-%m-%d %H:%M}–{window_end:%H:%M} UTC (thời điểm người dùng chấm) · "
               "cửa sổ 10 phút trượt 5 phút, watermark 10 phút · chỉ tính sách có lượt 4–5 sao, xếp theo số lượt "
               "4–5 sao rồi điểm TB; lượt 1–3 sao không đẩy sách lên bảng")
    if hot:
        titles = {b["_id"]: b.get("title", "—")
                  for b in db().books.find({"_id": {"$in": [h["book_id"] for h in hot]}})}
        st.dataframe(pd.DataFrame([{
            "#": i + 1, "Tên sách": titles.get(h["book_id"], "—"), "Lượt 4–5 sao": h["n_liked"],
            "Điểm TB trong cửa sổ": h["avg_rating"], "Lượt chấm trong cửa sổ": h["n_ratings"],
        } for i, h in enumerate(hot)]), hide_index=True, use_container_width=True)
    else:
        st.info("Trong cửa sổ này chưa có sách nào được chấm 4–5 sao.")
else:
    st.info("Chưa có dữ liệu cửa sổ: chạy streaming/stream_stats.py và streaming/producer.py.")
