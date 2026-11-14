"""Page "Điều khiển demo" (/setting): buttons for the commands the demo otherwise runs by hand.

Every button asks scripts/control_server.py (the spark container's main process, CONTROL_URL) to run one fixed
command there; the page shows its status and output. The commands themselves are unchanged:
  stream job      streaming/stream_stats.py   (start, and stop with SIGINT)
  reset           scripts/reset_streaming.py  (same as bash scripts/reset_streaming.sh --yes)
  rate recs       scripts/rate_from_recs.py --user U --n N --rating R
  late events     streaming/producer.py --file data/fake/stream_ratings.csv --late P
  checks          scripts/kafka_audit.py, scripts/nosql_evidence.py
  retrain         scripts/retrain.py --user U [--exclude-simulated]  (stop stream -> clean -> ALS -> write_recs -> stream)
"""
import json
import os
import urllib.error
import urllib.request

import streamlit as st

from common.fake import FAKE_CSV, write_fake

CONTROL_URL = os.getenv("CONTROL_URL", "http://spark:8600")


def call(path, payload=None, timeout=5):
    """GET (payload None) or POST JSON to the control server -> (status code, JSON reply)."""
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(CONTROL_URL + path, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as exc:  # 400/404/409: the server explains why in the body
        return exc.code, json.load(exc)


def run(name, **params):
    try:
        code, reply = call(f"/run/{name}", params)
    except OSError as exc:
        st.error(f"Không gọi được server điều khiển ({type(exc).__name__}).")
        return
    if code != 200:
        st.warning(reply.get("error", f"HTTP {code}"))


def task_box(name, done_text, lines=None):
    """Status and output of the last run of one command; refreshes itself while the command runs."""
    @st.fragment(run_every=2)
    def box():
        try:
            task = call("/status")[1]["tasks"].get(name)
            if task and lines is None and not task["running"]:
                task["log"] = call(f"/log/{name}")[1]["log"]
        except OSError:
            st.caption("Mất kết nối tới server điều khiển.")
            return
        if task is None:
            return
        if task["running"]:
            st.info(f"⏳ Đang chạy… {task['elapsed']} s")
        elif task["returncode"] == 0:
            st.success(done_text)
        else:
            st.error(f"Lỗi (mã thoát {task['returncode']})")
        with st.expander("Output", expanded=task["running"] or task["returncode"] != 0 or lines is None):
            log = task["log"].splitlines()
            st.code("\n".join(log[-lines:] if lines else log) or "(chưa có output)", language=None)
    box()


st.title("⚙️ Điều khiển demo")

try:
    call("/status", timeout=2)
except OSError:
    st.error("Chưa kết nối được container spark. Chạy lệnh sau rồi tải lại trang:")
    st.code("docker compose up -d spark", language="bash")
    st.stop()

# 1. Stream job -------------------------------------------------------------------------------------------------
st.subheader("1 · Job streaming")


@st.fragment(run_every=2)
def stream_panel():
    status = call("/status")[1]
    task = status["tasks"].get("start_stream")
    started_here = task is not None and task["running"]
    c1, c2, c3 = st.columns([3, 1, 1])
    if status["stream_running"]:
        c1.success("✅ Đang chạy" + (f" (bật từ trang này {task['elapsed']} s trước)" if started_here
                                     else " (bật ngoài trang này)"))
    else:
        c1.warning("⏹ Chưa chạy")
    if c2.button("Bật job", type="primary", use_container_width=True, disabled=status["stream_running"]):
        run("start_stream")
        st.rerun(scope="fragment")
    if c3.button("Dừng job", use_container_width=True, disabled=not status["stream_running"]):
        call("/stop_stream", {})
        st.toast("Đang dừng job…")
        st.rerun(scope="fragment")
    if task:
        with st.expander("Log của job (40 dòng cuối)"):
            st.code(task["log"] or "(chưa có output)", language=None)


stream_panel()

# 2. Reset -------------------------------------------------------------------------------------------------------
st.subheader("2 · Xoá dữ liệu streaming đã thử")
c1, c2 = st.columns([3, 1])
sure = c1.checkbox("Xác nhận xoá")
if c2.button("Xoá dữ liệu", disabled=not sure, use_container_width=True):
    run("reset")
task_box("reset", "Đã xoá.")

# 3. Rate books on a user's list -----------------------------------------------------------------------------------
st.subheader("3 · Chấm sách đang được gợi ý")
c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
user = c1.number_input("User ID", min_value=1, value=1, step=1)
n = c2.number_input("Số cuốn", min_value=1, max_value=10, value=2, step=1)
stars = c3.selectbox("Số sao", [5, 4, 3, 2, 1], format_func=lambda s: "★" * s)
c4.write("")
if c4.button("Gửi lượt chấm", type="primary", use_container_width=True):
    run("rate_recs", user=int(user), n=int(n), rating=int(stars))
task_box("rate_recs", "Đã gửi.")

# 4. Late events -------------------------------------------------------------------------------------------------
st.subheader("4 · Gửi sự kiện đến trễ")
if not FAKE_CSV.exists():
    if st.button("Tạo dữ liệu giả (5.000 lượt)"):
        with st.spinner("Đang tạo…"):
            write_fake()
        st.rerun()
else:
    c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
    limit = c1.number_input("Số sự kiện", min_value=50, max_value=5000, value=900, step=50)
    rate = c2.number_input("Tốc độ (sự kiện/giây)", min_value=5, max_value=100, value=20, step=5)
    late = c3.slider("Tỷ lệ trễ", 0.01, 0.20, 0.05, step=0.01, format="%.2f")
    c4.write("")
    if c4.button("Gửi", type="primary", use_container_width=True):
        run("late", limit=int(limit), rate=int(rate), late=float(late))
    task_box("late", "Đã gửi xong.", lines=15)

# 5. Checks ------------------------------------------------------------------------------------------------------
st.subheader("5 · Kiểm chứng")
c1, c2 = st.columns(2)
if c1.button("Đối chiếu Kafka ↔ MongoDB", use_container_width=True):
    run("kafka_audit")
if c2.button("Kiểm chứng MongoDB", use_container_width=True):
    run("nosql_evidence")
c1, c2 = st.columns(2)
with c1:
    task_box("kafka_audit", "Xong.")
with c2:
    task_box("nosql_evidence", "Xong.")

# 6. Retrain with the streamed ratings ------------------------------------------------------------------------------
st.subheader("6 · Train lại với rating mới")
st.caption("Streaming chỉ ẩn sách và cập nhật thống kê; mô hình ALS chỉ đổi khi train lại. Nút này chạy cả vòng: "
           "dừng job streaming → clean.py gộp kho sự kiện (bỏ trùng theo event_id) → train ALS implicit (rank 50, "
           "alpha 1, một bộ tham số) → write_recs.py ghi phiên bản mới và chuyển meta.active → bật lại job streaming. "
           "Đo 15/11: khoảng 3 phút (máy 12 core); trong lúc đó app vẫn phục vụ bản cũ, sự kiện mới chờ trong Kafka.")
c1, c2, c3 = st.columns([2, 3, 2])
retrain_user = c1.number_input("User so sánh trước/sau", min_value=1, value=1, step=1, key="retrain_user")
exclude_sim = c2.checkbox("Bỏ sự kiện mô phỏng (simulated=true) khỏi dữ liệu train", value=False)
c3.write("")
if c3.button("Train lại", type="primary", use_container_width=True):
    run("retrain", user=int(retrain_user), exclude_simulated=bool(exclude_sim))
task_box("retrain", "Xong: app đã chuyển sang phiên bản mô hình mới (report reports/retrain_*.json).", lines=60)
