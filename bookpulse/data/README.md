# data/

Thư mục này không đưa lên Git (xem `.gitignore`).

| Thư mục | Nội dung | Tạo bằng |
|---|---|---|
| `sample/` | Dữ liệu mẫu nhỏ để chạy thử | `python scripts/make_sample.py` |
| `goodbooks/` | Goodbooks-10k: `ratings.csv`, `books.csv` | `python scripts/download_goodbooks.py` |
| `ucsd/` | Tập con UCSD Book Graph (tải tay từ mengtingwan.github.io/data/goodreads) | tải tay |
| `parquet/` | Dữ liệu đã làm sạch | `python batch/clean.py` |
