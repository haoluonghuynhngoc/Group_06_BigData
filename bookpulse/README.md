# BookPulse – hệ thống gợi ý sách (BDA501)

Toàn bộ hệ thống chạy bằng Docker: Spark (batch + Structured Streaming), Kafka, MongoDB và app Streamlit.

```
Luồng batch:      CSV → clean.py → Parquet → train_als.py (ALS) → write_recs.py → MongoDB
Luồng streaming:  producer.py → Kafka (rating-events) → stream_stats.py → MongoDB
Phục vụ:          MongoDB → app/app.py (Streamlit, http://localhost:8501)
```

## 0. Yêu cầu

- Docker Desktop đang chạy, cấp **ít nhất 8 GB RAM** (Settings → Resources).
- **Ổ chứa dữ liệu Docker còn trống ít nhất 20 GB.** Nếu ổ C gần đầy, chuyển dữ liệu Docker sang ổ khác:
  Docker Desktop → Settings → Resources → Advanced → **Disk image location** → chọn thư mục trên ổ D → Apply & restart.

## 1. Khởi động

```bash
cd "D:\Big Data\Group\bookpulse"
docker compose up -d --build      # lần đầu mất vài phút để build image
docker compose ps                 # mongo, kafka, spark, app đều phải "running"
```

Mọi lệnh bên dưới chạy **bên trong container `spark`** bằng `docker compose exec spark ...`.

## 2. Chạy thử nhanh với dữ liệu mẫu (khoảng 5 phút)

```bash
docker compose exec spark python scripts/make_sample.py
docker compose exec spark python batch/clean.py --input data/sample/ratings.csv
docker compose exec spark python batch/train_als.py --eval-users 500
docker compose exec spark python batch/write_recs.py --books data/sample/books.csv
```

Mở http://localhost:8501, nhập user ID (ví dụ 1) để xem top-10.

Luồng streaming: mở **hai cửa sổ terminal**.

```bash
# Terminal 1: job Structured Streaming (Ctrl+C để dừng)
docker compose exec spark python streaming/stream_stats.py

# Terminal 2: bộ sinh sự kiện, 20 sự kiện/giây
docker compose exec spark python streaming/producer.py --file data/sample/ratings.csv --limit 500
```

Bấm "Làm mới" trong app: cột "Điểm TB trực tiếp" và các chỉ số luồng streaming thay đổi;
những cuốn người dùng vừa chấm biến mất khỏi top-10.

## 3. Chạy với Goodbooks-10k (dữ liệu thật)

```bash
docker compose exec spark python scripts/download_goodbooks.py
docker compose exec spark python batch/clean.py
docker compose exec spark python batch/train_als.py
docker compose exec spark python batch/write_recs.py --books data/goodbooks/books.csv
docker compose exec spark python streaming/producer.py --limit 2000
```

Với UCSD Book Graph: tải tay một file thể loại vào `data/ucsd/`, rồi
`python batch/clean.py --input data/ucsd/<file>.csv` (rating 0 = chưa chấm, tự động bị loại).

## 4. Kết quả đo

Mỗi bước ghi một file JSON vào `reports/`:

| File | Nội dung |
|---|---|
| `clean_*.json` | số dòng vào, số dòng lỗi, số dòng trùng, số dòng ra, thời gian |
| `train_als_*.json` | RMSE (ALS so với 2 baseline), Precision@10 / Recall@10 (ALS so với sách phổ biến), tham số tốt nhất, thời gian huấn luyện |
| `write_recs_*.json` | phiên bản mô hình, số người dùng đã ghi |

**Đo mức tăng tốc theo số core** (slide: "scaling with cores"):

```bash
docker compose exec -e SPARK_MASTER=local[1] spark python batch/train_als.py
docker compose exec -e SPARK_MASTER=local[2] spark python batch/train_als.py
docker compose exec -e SPARK_MASTER=local[4] spark python batch/train_als.py
```

Spark UI khi job đang chạy: http://localhost:4040

## 5. MongoDB có gì

| Collection | Ghi bởi | Nội dung |
|---|---|---|
| `recommendations_<phiên bản>` | write_recs.py | top-30 sách chưa đọc cho mỗi người dùng |
| `meta` (`_id: "active"`) | write_recs.py | phiên bản đang dùng + danh sách sách phổ biến (cold start) |
| `books` | write_recs.py | tên sách, tác giả |
| `rating_stats` | stream_stats.py | điểm trung bình và số lượt chấm từ luồng streaming |
| `user_seen` | stream_stats.py | sách người dùng vừa chấm (app ẩn khỏi top-10) |
| `online_eval` | stream_stats.py | tỷ lệ trúng top-10 trực tiếp |
| `stream_state` | stream_stats.py | batch cuối đã ghi (chống đếm hai lần khi restart), số sự kiện hợp lệ / bị loại |

Xem nhanh: `docker compose exec mongo mongosh bookpulse --eval "db.meta.findOne()"`

## 6. Thiết kế đã chọn

- **Top-30, app lọc còn 10:** người dùng không thấy lại cuốn vừa đọc mà không phải huấn luyện lại ALS.
- **Đổi phiên bản an toàn:** gợi ý mới ghi vào collection mới, xong hết mới chuyển `meta.active`.
- **Chống đếm hai lần:** Spark + Kafka + checkpoint là at-least-once; batch nào đã ghi thì bỏ qua khi bị phát lại.
  Giới hạn: nếu job dừng giữa lúc đang ghi một batch, batch đó có thể bị cộng lặp một phần.
- **Đánh giá online:** mỗi lượt chấm 4–5 sao được đối chiếu với top-10 hiện tại của người đó.
- **Chia train/test ngẫu nhiên** vì CSV không có thời điểm chấm. Nếu dùng dữ liệu UCSD bản JSON có ngày tháng,
  nên đổi sang chia theo thời gian (train trước một mốc, test sau mốc).

## 7. Dừng và làm lại từ đầu

```bash
docker compose down                 # dừng, giữ dữ liệu MongoDB và checkpoint
docker compose down -v              # dừng và XÓA dữ liệu MongoDB, checkpoint (làm lại từ đầu)
```

Khi xóa checkpoint streaming thì cũng nên xóa `stream_state` và `online_eval` trong MongoDB
(`docker compose down -v` đã xóa cả hai).
