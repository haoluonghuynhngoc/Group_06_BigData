# BookPulse – hệ thống gợi ý sách (BDA501)

Toàn bộ hệ thống chạy bằng Docker: Spark (batch + Structured Streaming), Kafka, MongoDB và app Streamlit;
thành phần MapReduce chạy trên một cụm Hadoop riêng (HDFS + YARN), xem mục 8.

```
MapReduce:        CSV → HDFS → Hadoop Streaming (mapper → combiner → reducer) → thống kê theo sách
                                                                          → đối chiếu với Spark SQL
Luồng batch:      CSV → clean.py → Parquet → analytics_sql.py (Spark SQL)
                                          → train_als.py (ALS) → write_recs.py → MongoDB
Luồng streaming:  producer.py → Kafka (rating-events) → stream_stats.py → MongoDB
                                                                     → Parquet (stream_events) → clean.py lần sau
Phục vụ:          MongoDB → app/app.py (Streamlit, http://localhost:8501)
```

## 0. Yêu cầu

- Docker Desktop đang chạy, cấp **ít nhất 8 GB RAM** (Settings → Resources).
- **Ổ chứa dữ liệu Docker còn trống ít nhất 20 GB.** Nếu ổ C gần đầy, chuyển dữ liệu Docker sang ổ khác:
  Docker Desktop → Settings → Resources → Advanced → **Disk image location** → chọn thư mục trên ổ D → Apply & restart.

## 1. Khởi động

```bash
cd "D:\Big Data\Group_06_BigData\bookpulse"
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
docker compose exec spark python batch/analytics_sql.py --books data/goodbooks/books.csv --raw-csv data/goodbooks/ratings.csv
docker compose exec spark python batch/train_als.py             # ALS explicit + baseline (RMSE, Precision@10)
docker compose exec spark python batch/train_als.py --implicit --ranks 20,50 --regs 0.1 --alphas 1,10
docker compose exec spark python batch/write_recs.py --books data/goodbooks/books.csv
docker compose exec spark python streaming/producer.py --limit 2000
docker compose exec spark python scripts/nosql_evidence.py     # đọc lại MongoDB và kiểm chứng
docker compose exec spark python scripts/kafka_audit.py        # đếm lại message trong Kafka để đối chiếu với streaming
```

**App dùng ALS implicit** (`models/als_implicit`), chọn theo Precision@10 chứ không theo RMSE; lý do và số liệu ở
[docs/chon-mo-hinh.md](docs/chon-mo-hinh.md). `--implicit` thử mọi tổ hợp rank × regParam × alpha trên một phần
validation tách từ tập train, rồi mới đánh giá một lần trên tập test. Muốn quay về mô hình explicit:
`write_recs.py --model als`.

Với UCSD Book Graph: tải tay một file thể loại vào `data/ucsd/`, rồi
`python batch/clean.py --input data/ucsd/<file>.csv` (rating 0 = chưa chấm, tự động bị loại).

## 4. Kết quả đo

Mỗi bước ghi một file JSON vào `reports/`:

| File | Nội dung |
|---|---|
| `clean_*.json` | số dòng vào, số dòng lỗi, số dòng trùng, số dòng ra, thời gian |
| `analytics_sql_*.json` | các câu hỏi phân tích bằng Spark SQL (profile, phân bố rating, top sách), kiểm tra SQL = DataFrame API, thời gian CSV so với Parquet |
| `sql_plan_*.txt` | physical plan của truy vấn join (BroadcastHashJoin, Exchange = shuffle) |
| `nosql_evidence_*.json` | số document mỗi collection, document mẫu, query plan và độ trễ đọc theo `_id`, đối chiếu số đếm streaming |
| `train_als_*.json` | RMSE (ALS so với 2 baseline), Precision@10 / Recall@10 và số đầu sách được gợi ý (ALS so với sách phổ biến), tham số tốt nhất, thời gian huấn luyện |
| `train_als_implicit_*.json` | ALS implicit: Precision@10 của từng tổ hợp tham số trên validation, kết quả trên tập test |
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
| `recommendations_<phiên bản>` | write_recs.py | top-30 sách chưa đọc cho mỗi người dùng, kèm `fallback`: 10 sách phổ biến người đó chưa chấm |
| `meta` (`_id: "active"`) | write_recs.py | phiên bản đang dùng + danh sách sách phổ biến (cold start) |
| `books` | write_recs.py | tên sách, tác giả |
| `rating_stats` | stream_stats.py | điểm trung bình và số lượt chấm từ luồng streaming |
| `user_seen` | stream_stats.py | sách người dùng vừa chấm (app ẩn khỏi top-10) |
| `online_eval` | stream_stats.py | tỷ lệ trúng top-10 trực tiếp |
| `stream_state` | stream_stats.py | batch cuối đã ghi (chống đếm hai lần khi restart), số sự kiện hợp lệ / bị loại |

Xem nhanh: `docker compose exec mongo mongosh bookpulse --eval "db.meta.findOne()"`

## 6. Thiết kế đã chọn

- **Hai tốc độ cập nhật:** thay đổi nhanh (vừa đọc, vừa chấm) do streaming xử lý trong vài giây;
  gu đọc mới, người dùng mới, sách mới chờ lần chạy batch sau (huấn luyện lại ALS rồi ghi phiên bản mới).
- **Top-30, app lọc còn 10:** người dùng không thấy lại cuốn vừa đọc mà không phải huấn luyện lại ALS.
  Khi đã đọc gần hết danh sách, app bù bằng `fallback` (sách phổ biến người đó chưa chấm) để luôn đủ 10 cuốn;
  cột "Nguồn" ghi rõ cuốn nào do ALS, cuốn nào là bù.
- **Rating từ streaming vào lần huấn luyện sau:** `stream_stats.py` ghi mọi sự kiện hợp lệ, đã khử trùng ra
  `data/parquet/stream_events` (file sink của Spark, exactly-once nhờ `_spark_metadata` + checkpoint).
  `clean.py` gộp chúng với CSV; cùng một cặp (user, book) thì lấy rating mới hơn. Report `clean_*.json`
  ghi số cặp được cập nhật và số cặp mới.
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
(`docker compose down -v` đã xóa cả hai). `data/parquet/stream_events` nằm ngoài volume nên phải tự xóa,
nếu không lần `clean.py` sau vẫn gộp các sự kiện cũ vào dữ liệu huấn luyện.

## 8. Thành phần MapReduce (Hadoop Streaming)

Job `mapreduce/` tính thống kê rating theo sách trên file `ratings.csv` gốc trong HDFS:

```
book_id  n_ratings  sum_rating  avg_rating  n_liked  n1  n2  n3  n4  n5
```

| File | Vai trò |
|---|---|
| `mapper.py` | Đọc từng dòng, kiểm tra hợp lệ, phát `book_id \t 1,rating,n1..n5`; dòng lỗi được đếm bằng Hadoop counter theo lý do (`WrongColumnCount`, `NotAnInteger`, `RatingOutOfRange`) |
| `combiner.py` | Cộng vector (count, sum, phân bố sao) ngay ở phía map trước khi shuffle. Chỉ cộng số nguyên, không tính trung bình, nên chạy 0 hay nhiều lần đều cho cùng kết quả |
| `reducer.py` | Cộng vector cuối cùng; tính `avg_rating = sum / count` và làm tròn **một lần** ở đây |
| `run_hadoop.sh` | Đưa CSV lên HDFS (block 32 MB → 3 block → 3 map task), chạy job trên YARN với 2 reducer, lấy kết quả về `reports/mapreduce/` |
| `run_local.sh` | Mô phỏng bằng pipe `map → sort → combine → sort → reduce`, không cần Hadoop |
| `verify_with_spark.py` | Tính cùng thống kê bằng Spark SQL trên cùng file, so bằng `exceptAll` theo cả hai chiều; đọc counter của Hadoop; ghi `reports/mapreduce_verify_*.json` |
| `hadoop/` | Cụm Hadoop 3.5.0 của môn học (cùng image và cấu hình với lab Session 2) |

Chạy (Docker Desktop cần đủ RAM cho cả hai cụm; nếu thiếu, tạm dừng `kafka` và `app` của BookPulse):

```bash
cd mapreduce/hadoop
docker compose up -d --build                  # lần đầu tải image Hadoop
docker compose logs -f client                 # chờ dòng "BOOKPULSE HADOOP READY", Ctrl+C
docker compose exec client bash /bookpulse/mapreduce/run_hadoop.sh

cd ../..                                      # về bookpulse/
docker compose exec spark python mapreduce/verify_with_spark.py
```

Giao diện web: HDFS http://localhost:9870 · YARN http://localhost:8088 · JobHistory http://localhost:19888

Job MapReduce không khử trùng cặp (user, book) vì việc này cần thêm một job khác (key là cặp user–book).
Goodbooks-10k không có dòng trùng (xem `clean_*.json`), nên kết quả bằng đúng thống kê trên dữ liệu đã làm sạch.
