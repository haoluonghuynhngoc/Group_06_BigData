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

# Terminal 2: bộ sinh sự kiện, 20 sự kiện/giây; --late 0.03 = 3% sự kiện trễ 30 phút (watermark sẽ loại),
# chỉ gửi sau --late-after-seconds (mặc định 30 giây) để Spark đã có watermark
docker compose exec spark python streaming/producer.py --file data/sample/ratings.csv --limit 500 --late 0.03

# Terminal 2, khi job đang chạy: user 1 chấm 5 sao 2 cuốn đầu trong top-10 hiện tại của mình
docker compose exec spark python scripts/rate_from_recs.py --user 1 --n 2 --rating 5
```

Bấm "Làm mới" trong app: cột "Điểm TB (stream)", các chỉ số luồng streaming và mục "Sách đang hot" (cửa sổ
event time 10 phút, trượt 5 phút) thay đổi. `producer.py` phát lại rating **đã có trong dữ liệu huấn luyện**,
mà gợi ý đã loại mọi sách người dùng đã chấm, nên sự kiện phát lại không bao giờ làm đổi top-10 và không thể
"trúng" (chỉ số "Lượt 4–5 sao trúng top-10" hiện dạng trúng / tổng; với riêng producer, số trúng luôn là 0). Muốn thấy sách vừa chấm
biến mất khỏi top-10 và cuốn tiếp theo được đẩy lên, dùng `scripts/rate_from_recs.py`: script đọc top-10 hiện
tại của người dùng trong MongoDB và gửi rating cho những cuốn đó; sau một trigger (5–10 giây) app ẩn chúng.
Trong lúc job chạy, Spark UI → tab **Structured Streaming** (http://localhost:4040) hiện 4 query có tên;
`reports/stream_progress_*.json` ghi tiến độ từng query. Chi tiết thiết kế và bằng chứng: [docs/streaming.md](docs/streaming.md).

## 3. Chạy với Goodbooks-10k (dữ liệu thật)

Chạy cả luồng batch bằng một lệnh (khoảng 25–35 phút): `bash scripts/run_pipeline.sh`. Hoặc từng bước:

```bash
docker compose exec spark python scripts/download_goodbooks.py
docker compose exec spark python batch/clean.py
docker compose exec spark python batch/analytics_sql.py --books data/goodbooks/books.csv --raw-csv data/goodbooks/ratings.csv
docker compose exec spark python batch/train_als.py             # ALS explicit + baseline (RMSE, Precision@10)
docker compose exec spark python batch/train_als.py --implicit --ranks 20,50 --regs 0.1 --alphas 1,10
docker compose exec spark python batch/write_recs.py --books data/goodbooks/books.csv
docker compose exec spark python streaming/producer.py --limit 2000
docker compose exec spark python scripts/nosql_evidence.py     # đọc lại MongoDB và kiểm chứng
docker compose exec spark python scripts/kafka_audit.py        # đếm lại Kafka và đối chiếu với số đếm streaming trong MongoDB
```

**App dùng ALS implicit** (`models/als_implicit`), chọn theo Precision@10 chứ không theo RMSE; lý do và số liệu ở
[docs/chon-mo-hinh.md](docs/chon-mo-hinh.md). `--implicit` thử mọi tổ hợp rank × regParam × alpha trên một phần
validation tách từ tập train, rồi mới đánh giá một lần trên tập test. Mô hình app dùng chính là mô hình đã đánh giá,
tức chỉ học trên 80% dữ liệu (tập train); khoảng 1,2 triệu rating của tập test không được học. Khi triển khai thật
nên fit lại trên 100% dữ liệu với tham số đã chọn (rank 50, alpha 1, regParam 0,1) rồi mới ghi gợi ý.
Muốn quay về mô hình explicit: `write_recs.py --model als`.

Với UCSD Book Graph: tải tay một file thể loại vào `data/ucsd/`, rồi
`python batch/clean.py --input data/ucsd/<file>.csv` (rating 0 = chưa chấm, tự động bị loại).

## 4. Kết quả đo

Mỗi bước ghi một file JSON vào `reports/`:

| File | Nội dung |
|---|---|
| `clean_*.json` | số dòng vào, số dòng lỗi, số dòng trùng, số dòng ra, thời gian |
| `analytics_sql_*.json` | các câu hỏi phân tích bằng Spark SQL (profile, phân bố rating, top sách), kiểm tra join giữ nguyên số dòng, SQL = DataFrame API (`exceptAll` hai chiều trên toàn bộ kết quả), thời gian CSV so với Parquet |
| `sql_plan_*.txt` | physical plan của truy vấn join: BroadcastHashJoin, một `Exchange` (shuffle) và một `BroadcastExchange` (gửi bảng sách nhỏ); rating đọc từ Parquet chỉ 2 cột, sách đọc từ `books.csv` |
| `nosql_evidence_*.json` | số document mỗi collection, document mẫu, query plan và độ trễ đọc theo `_id`, đối chiếu số đếm streaming; `recs_vs_training_ratings`: kiểm tra độc lập bằng pandas rằng không cặp (user, sách) nào trong gợi ý và danh sách bù đã có trong rating huấn luyện |
| `train_als_*.json` | RMSE (ALS so với 2 baseline), Precision@10 / Recall@10 và số đầu sách được gợi ý (ALS so với sách phổ biến), tham số tốt nhất, thời gian huấn luyện |
| `train_als_implicit_*.json` | ALS implicit: Precision@10 của từng tổ hợp tham số trên validation, kết quả trên tập test |
| `write_recs_*.json` | phiên bản mô hình, số người dùng đã ghi |
| `stream_progress_*.json` | tiến độ từng query streaming: số batch, dòng vào, tốc độ, thời gian batch, sự kiện bị watermark loại, bản trùng, trạng thái, watermark |
| `kafka_audit_*.json` | đếm lại toàn bộ topic Kafka (message, sự kiện hợp lệ, sự kiện trễ, message lỗi); khối `mongo_check` đọc lại MongoDB và ghi ba phép so: `no_loss_no_double_count` (đếm được = hợp lệ − trễ = tổng `rating_stats`), `rejected_match` (sự kiện lỗi), `trending_match` (tổng các cửa sổ = 2 × hợp lệ, vì mỗi sự kiện nằm trong 2 cửa sổ trượt). `--no-mongo` để chỉ đếm Kafka |
| `streaming/*.png` | ảnh Spark UI (tab Structured Streaming) và app "Sách đang hot" |
| `streaming/fault_test_*/` | log của `scripts/stream_fault_test.sh` (thử `kill -9` job streaming) |
| `scaling/docker_stats_*.csv` | `docker stats` mỗi 5 giây khi chạy batch (RAM đỉnh) |

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
| `rating_stats` | stream_stats.py | điểm trung bình và số lượt chấm **chỉ từ luồng streaming** kể từ lần reset (lớp nhanh); không gộp với thống kê batch. Cột "Điểm TB (stream)" trong app đọc từ đây |
| `user_seen` | stream_stats.py | sách người dùng vừa chấm (app ẩn khỏi top-10) |
| `online_eval` | stream_stats.py | số lượt chấm 4–5 sao và số lượt trong đó cuốn sách đang nằm trong top-10 của người đó; app hiện "Lượt 4–5 sao trúng top-10" dạng trúng / tổng |
| `stream_state` | stream_stats.py | batch cuối đã ghi (chống đếm hai lần khi restart), số sự kiện hợp lệ / bị loại |
| `trending` | stream_stats.py | số lượt chấm, điểm TB, lượt 4–5 sao theo (cửa sổ event time, sách); TTL index tự xóa sau 24 giờ |

Xem nhanh: `docker compose exec mongo mongosh bookpulse --eval "db.meta.findOne()"`

## 6. Thiết kế đã chọn

- **Hai tốc độ cập nhật:** thay đổi nhanh (vừa đọc, vừa chấm) do streaming xử lý trong vài giây;
  gu đọc mới, người dùng mới, sách mới chờ lần chạy batch sau (huấn luyện lại ALS rồi ghi phiên bản mới).
- **Top-30, app lọc còn 10:** người dùng không thấy lại cuốn vừa đọc mà không phải huấn luyện lại ALS.
  Khi đã đọc gần hết danh sách, app bù bằng `fallback` (sách phổ biến người đó chưa chấm) để luôn đủ 10 cuốn;
  cột "Nguồn" ghi rõ cuốn nào do ALS, cuốn nào là bù.
- **Rating từ streaming vào lần huấn luyện sau:** `stream_stats.py` lưu **mọi** sự kiện hợp lệ (kể cả trùng
  và đến trễ) ra `data/parquet/stream_events` (file sink, không trạng thái, exactly-once nhờ `_spark_metadata`
  + checkpoint). `clean.py` bỏ trùng chính xác theo `event_id` rồi gộp với CSV; cùng một cặp (user, book) thì
  lấy rating mới hơn. Report `clean_*.json` ghi số dòng lưu trữ, số sự kiện sau bỏ trùng, số cặp cập nhật/mới.
- **Lớp nhanh bỏ sự kiện trễ, lớp batch giữ lại:** thống kê trực tiếp và "sách đang hot" dùng watermark 10 phút
  để giới hạn bộ nhớ trạng thái, nên bỏ sự kiện trễ hơn thế; kho lưu trữ thì giữ lại để không mất rating thật.
  Spark 3.5 lọc sự kiện trễ bằng watermark của batch **trước**, nên 1–2 batch đầu sau khi khởi động mới chưa lọc;
  vì vậy producer chỉ gửi sự kiện trễ sau 30 giây (`--late-after-seconds`).
- **Lớp nhanh không gộp với lớp batch:** `rating_stats` chỉ chứa rating từ streaming; thống kê theo sách của
  MapReduce / Spark SQL dùng để phân tích và đối chiếu, không được nạp vào MongoDB.
- **Đổi phiên bản an toàn:** gợi ý mới ghi vào collection mới, xong hết mới chuyển `meta.active`.
- **Chống đếm hai lần:** Spark + Kafka + checkpoint là at-least-once; batch nào đã ghi thì bỏ qua khi bị phát lại
  (`stream_state.last_batch`). Lượt thử `kill -9` (`scripts/stream_fault_test.sh`): 1.474 = 1.500 − 26 sự kiện trễ,
  0 mất, 0 trùng. Còn một khe hở: nếu job chết đúng lúc đang ghi MongoDB (`rating_stats` và `online_eval` cộng dồn
  trước khi ghi `last_batch`), batch phát lại có thể cộng lặp một phần; sửa triệt để bằng transaction (replica set)
  hoặc ghi idempotent theo `batch_id`. Kho Parquet exactly-once (file sink); "sách đang hot" idempotent vì mỗi lần
  ghi thay cả document (ReplaceOne).
- **Ghi MongoDB chạy ở driver:** query `valid` gom mỗi micro-batch về driver rồi ghi bằng một client MongoDB. Đủ cho
  tốc độ sự kiện của đồ án; `--max-offsets` (`maxOffsetsPerTrigger`, mặc định 20.000) giới hạn một batch sau khi
  job tắt lâu. Ở quy mô lớn cần chuyển việc ghi sang executor (`foreachPartition` hoặc MongoDB Spark connector,
  gộp theo sách trong Spark trước khi ghi).
- **Đánh giá online:** mỗi lượt chấm 4–5 sao được đối chiếu với top-10 hiện tại của người đó. Với `producer.py`
  kết quả luôn là 0 trúng (sách phát lại đã bị loại khỏi gợi ý); chỉ `rate_from_recs.py` tạo được lượt trúng.
- **Một máy:** Kafka 1 broker (replication 1), MongoDB standalone, Spark `local[*]`, Hadoop 1 DataNode (replication 1),
  không có restart policy: mỗi thành phần là một điểm lỗi đơn. Thiết kế cluster nằm trong báo cáo.
- **Chia train/test ngẫu nhiên** vì CSV không có thời điểm chấm. Nếu dùng dữ liệu UCSD bản JSON có ngày tháng,
  nên đổi sang chia theo thời gian (train trước một mốc, test sau mốc).

## 7. Dừng và làm lại từ đầu

Chỉ làm sạch phần streaming (trước khi demo): `bash scripts/reset_streaming.sh` (hỏi xác nhận, `--yes` để bỏ qua;
dừng job streaming và chờ nó dừng hẳn, xóa topic Kafka, checkpoint streaming, kho `stream_events` và các collection
streaming, giữ nguyên kết quả batch).

```bash
docker compose down                 # dừng, giữ dữ liệu MongoDB, topic Kafka (volume kafka-data) và checkpoint
docker compose down -v              # dừng và XÓA mọi volume: MongoDB, Kafka, checkpoint
bash scripts/reset_streaming.sh --yes   # bắt buộc chạy sau `down -v` (sau khi `up -d` lại)
```

Trạng thái streaming nằm ở ba nơi: checkpoint (volume `spark-checkpoints`), `stream_state` trong MongoDB và
`data/parquet/stream_events/_spark_metadata` (thư mục trên máy, không thuộc volume nào). Phải xóa cả ba cùng lúc.
Sau `down -v`, checkpoint mới đếm batch lại từ 0, nhưng `stream_events` vẫn còn `_spark_metadata` cũ, nên query
lưu trữ âm thầm bỏ qua các batch có số nhỏ hơn batch cũ (sự kiện mới không được lưu cho lần huấn luyện sau), và
`clean.py` lần sau vẫn gộp sự kiện cũ. `reset_streaming.sh` xóa đúng cả ba.

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
export MSYS_NO_PATHCONV=1                     # Git Bash: không đổi /bookpulse/... thành C:/Program Files/Git/bookpulse/...
cd mapreduce/hadoop
docker compose up -d --build                  # lần đầu tải image Hadoop
docker compose logs client | grep "BOOKPULSE HADOOP READY"   # chờ đến khi có dòng này
docker compose exec client bash /bookpulse/mapreduce/run_hadoop.sh

cd ../..                                      # về bookpulse/
docker compose exec spark python mapreduce/verify_with_spark.py | grep -E 'rows_only|"match"|combiner_reduction'
```

Không có `MSYS_NO_PATHCONV=1`, lệnh `run_hadoop.sh` trong Git Bash báo lỗi "C:/Program Files/Git/bookpulse/...: No such file".

Giao diện web: HDFS http://localhost:9870 · YARN http://localhost:8088 · JobHistory http://localhost:19888

Số map task và thời gian thay đổi theo lần chạy: 05/11 có 4 map task (3 split + 1 bản speculative) và 134 giây
(`reports/mapreduce_verify_20261105_170058.json`); lần tập 07/11 có 3 map task, job 52 giây; kết quả giống hệt nhau.
Mọi map task đều là rack-local, không có data-local, vì DataNode và NodeManager chạy ở hai container riêng
(cluster thật đặt chung một node).

Job MapReduce không khử trùng cặp (user, book) vì việc này cần thêm một job khác (key là cặp user–book).
Goodbooks-10k không có dòng trùng (xem `clean_*.json`), nên kết quả bằng đúng thống kê trên dữ liệu đã làm sạch.

## 9. Kiểm thử tự động

```bash
python -m unittest tests.test_mapreduce -v                                  # không cần Spark/Hadoop
docker compose exec spark python -m unittest discover -s tests -t . -v      # tất cả, gồm phần cần Spark
```

| File | Kiểm tra |
|---|---|
| `tests/test_mapreduce.py` | mapper loại đúng từng loại dòng lỗi; combiner chạy 0/1/nhiều lần cho cùng kết quả; điểm TB chỉ làm tròn một lần |
| `tests/test_recs.py` | không gợi ý sách đã đọc; xếp hạng top-k đúng |
| `tests/test_stream_progress.py` | file tiến độ streaming cộng đúng mỗi batch một lần dù đọc lại nhiều lần |

8/8 test đều qua (chạy trong container `spark`; trên máy host chỉ chạy được `test_mapreduce` vì thiếu pyspark/pymongo).

Ngoài unit test, mỗi thành phần có kiểm chứng trên dữ liệu thật: `mapreduce/verify_with_spark.py` (MapReduce so với Spark SQL),
`scripts/kafka_audit.py` (đếm Kafka và so với số đếm streaming trong MongoDB, khối `mongo_check`),
`scripts/nosql_evidence.py` (đọc lại MongoDB; kiểm tra gợi ý không chứa sách đã chấm).

Thử sự cố streaming (khoảng 3 phút, chạy từ `bookpulse/`): `bash scripts/stream_fault_test.sh`. Script reset streaming,
chạy job + producer (1.500 sự kiện, 3% trễ sau 30 giây, seed 7), `kill -9` cả Python lẫn JVM sau 45 giây, 15 giây sau
chạy lại từ checkpoint, rồi chạy `kafka_audit.py`. Log ở `reports/streaming/fault_test_<thời điểm>/`. Lần chạy 08/11
(`reports/kafka_audit_20261107_220738.json`, tên file theo giờ UTC): 1.474 = 1.500 − 26 sự kiện trễ, 15 sự kiện lỗi,
tổng cửa sổ 2.948 = 2 × 1.474; cả ba phép so đều `true`.
