# data/

Thư mục này không đưa lên Git (xem `.gitignore`).

| Thư mục | Nội dung | Tạo bằng |
|---|---|---|
| `sample/` | Dữ liệu mẫu nhỏ để chạy thử | `python scripts/make_sample.py` |
| `goodbooks/` | Goodbooks-10k: `ratings.csv`, `books.csv` | `python scripts/download_goodbooks.py` |
| `ucsd/` | Tập con UCSD Book Graph (tải tay từ mengtingwan.github.io/data/goodreads) | tải tay |
| `parquet/ratings` | Dữ liệu đã làm sạch (CSV + rating từ streaming) | `python batch/clean.py` |
| `parquet/book_stats` | Thống kê theo sách | `python batch/analytics_sql.py --books ...` |
| `parquet/stream_events` | Sự kiện hợp lệ từ Kafka, chia theo `event_date` | `python streaming/stream_stats.py` |
