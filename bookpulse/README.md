# BookPulse – hệ thống gợi ý sách (BDA501)

Toàn bộ hệ thống chạy bằng Docker trên một máy: Spark (batch + Structured Streaming), Kafka, MongoDB và app Streamlit;
thành phần MapReduce chạy trên một cụm Hadoop riêng (HDFS + YARN), là nhánh kiểm chứng, xem mục 8. Hệ thống tổ chức
theo ý tưởng Lambda: lớp batch, lớp tốc độ, lớp phục vụ; chưa hợp nhất batch view và speed view thành một số liệu,
app chỉ gộp lúc đọc (ẩn sách trong `user_seen`). Sơ đồ: tổng quan [docs/img/architecture_overview.png](docs/img/architecture_overview.png)
(slide 3), chi tiết [docs/img/architecture_detail.png](docs/img/architecture_detail.png) (slide 17, phụ lục) và bản
tương tác [docs/kien-truc.html](docs/kien-truc.html).

```
MapReduce:        CSV → HDFS → Hadoop Streaming (mapper → combiner → reducer) → thống kê theo sách
(kiểm chứng)                                                              → đối chiếu với Spark SQL (app không dùng)
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

# Terminal 2: bộ sinh sự kiện, 20 sự kiện/giây; --late 0.03 = 3% số sự kiện gửi sau --late-after-seconds
# (mặc định 30 giây, khi Spark đã có watermark) là sự kiện trễ 30 phút (watermark sẽ loại)
docker compose exec spark python streaming/producer.py --file data/sample/ratings.csv --limit 1000 --late 0.05

# Chấm sao: trong app, mục "Chấm sao" (chọn sách trong top-10, chọn số sao, bấm "Gửi lượt chấm"),
# hoặc bằng dòng lệnh khi job đang chạy: user 1 chấm 5 sao 2 cuốn đầu trong top-10 hiện tại của mình
docker compose exec spark python scripts/rate_from_recs.py --user 1 --n 2 --rating 5
```

Bấm "Làm mới" trong app: cột "Điểm TB (stream)", các chỉ số luồng streaming và mục "Sách đang hot" (chỉ sách có lượt
4–5 sao, xếp theo số lượt 4–5 sao rồi điểm TB, nên lượt 1–3 sao không đẩy sách lên bảng; cửa sổ
event time 10 phút, trượt 5 phút) thay đổi. `producer.py` phát lại rating **đã có trong dữ liệu huấn luyện**,
mà gợi ý đã loại mọi sách người dùng đã chấm, nên sự kiện phát lại không bao giờ làm đổi top-10 và không thể
"trúng" (chỉ số "Lượt 4–5 sao trúng top-10" hiện dạng trúng / tổng; với riêng producer, số trúng luôn là 0). Muốn thấy sách vừa chấm
biến mất khỏi top-10 và cuốn tiếp theo được đẩy lên, dùng mục **Chấm sao** của app (app chỉ gửi sự kiện vào Kafka,
không ghi MongoDB) hoặc `scripts/rate_from_recs.py` (đọc top-10 hiện tại trong MongoDB và gửi rating cho `--n` cuốn
đầu); sau một trigger (5–10 giây) job streaming ghi `user_seen` và app ẩn chúng.
Trong lúc job chạy, Spark UI → tab **Structured Streaming** (http://localhost:4040) hiện 4 query có tên;
`reports/stream_progress_*.json` ghi tiến độ từng query. Chi tiết thiết kế và bằng chứng: [docs/streaming.md](docs/streaming.md).

**Không chạy luồng batch song song với streaming.** Chỉ cổng 4040 của Spark UI được mở ra máy host (job thứ hai
sang 4041, không xem được), và hai driver 4 GB vượt 7,7 GiB RAM của Docker. Dừng job streaming (Ctrl+C) trước khi
chạy batch. Vì vậy trên máy này hai lớp chạy lần lượt: trong 25–35 phút chạy `run_pipeline.sh`, sự kiện chờ trong
Kafka (giữ 7 ngày), app vẫn phục vụ gợi ý và speed view cũ; khi job streaming chạy lại, nó đọc bù tối đa 20.000
offset mỗi batch. Trên cluster: batch chạy trên cụm riêng, streaming chạy thường trực.

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
# hoặc phát dữ liệu giả "mới": các cặp user–sách KHÔNG có trong ratings.csv (không cần train lại, số liệu không đổi)
docker compose exec spark python scripts/make_fake_stream.py   # -> data/fake/stream_ratings.csv (5000 dòng, seed 2026)
docker compose exec spark python streaming/producer.py --file data/fake/stream_ratings.csv --limit 2000
docker compose exec spark python scripts/nosql_evidence.py     # đọc lại MongoDB và kiểm chứng
docker compose exec spark python scripts/kafka_audit.py        # đếm lại Kafka và đối chiếu với số đếm streaming trong MongoDB
```

**App dùng ALS implicit** (`models/als_implicit`), chọn theo Precision@10 chứ không theo RMSE; lý do và số liệu ở
[docs/chon-mo-hinh.md](docs/chon-mo-hinh.md). `--implicit` thử mọi tổ hợp rank × regParam × alpha trên một phần
validation tách từ tập train (lưới tham số fit trên 80% của tập train), rồi fit lại trên toàn bộ tập train và đánh
giá một lần trên tập test. Mô hình app dùng chính là mô hình đã đánh giá, tức fit trên toàn bộ tập train (80% dữ liệu,
4.780.968 rating); khoảng 1,2 triệu rating của tập test không được học. Khi triển khai thật
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
| `kafka_audit_*.json` | đếm lại toàn bộ topic Kafka (message, sự kiện hợp lệ, sự kiện trễ, message lỗi); khối `mongo_check` đọc lại MongoDB và ghi ba phép so: `no_loss_no_double_count` (đếm được = hợp lệ − trễ = tổng `rating_stats`), `rejected_match` (sự kiện lỗi), `trending_match` (tổng các cửa sổ = 2 × (hợp lệ − trễ), vì mỗi sự kiện đúng hạn nằm trong 2 cửa sổ trượt); khối `archive_check` đếm kho `data/parquet/stream_events` qua log `_spark_metadata` (chỉ file đã commit): số dòng = số message hợp lệ (kho giữ cả bản lặp và sự kiện trễ), `event_id` khác nhau = sự kiện hợp lệ khác nhau, số file chưa commit trên đĩa, `exactly_once`. `--no-mongo` để chỉ đếm Kafka, `--no-archive` để bỏ qua kho |
| `streaming/*.png` | ảnh Spark UI (tab Structured Streaming) và app "Sách đang hot" |
| `streaming/fault_test_*/` | log của `scripts/stream_fault_test.sh` (thử `kill -9` job streaming) |
| `streaming/outage_test_*/` | log của `scripts/stream_outage_test.sh` (thử ngắt Kafka và MongoDB); `events.log` là dòng thời gian |
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
| `meta` (`_id: "active"`) | write_recs.py | phiên bản đang dùng, phiên bản trước (`previous_collection`, để quay lại) + danh sách sách phổ biến (cold start) |
| `books` | write_recs.py | tên sách, tác giả (không có phiên bản; upsert theo `_id` trước khi chuyển `meta.active`) |
| `rating_stats` | stream_stats.py | điểm trung bình và số lượt chấm **chỉ từ luồng streaming** kể từ lần reset (lớp nhanh); không gộp với thống kê batch. Cột "Điểm TB (stream)" trong app đọc từ đây |
| `user_seen` | stream_stats.py | sách người dùng vừa chấm (app ẩn khỏi top-10) |
| `online_eval` | stream_stats.py | số lượt chấm 4–5 sao và số lượt trong đó cuốn sách nằm trong 10 gợi ý đầu **đã lưu** của phiên bản active (không trừ sách đã ẩn, không tính sách bù); cộng dồn qua các phiên bản, `model_version` là phiên bản mới nhất; app hiện "Lượt 4–5 sao trúng top-10" dạng trúng / tổng |
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
  Vòng này đã chạy thật ngày 06/11 (`reports/clean_20261106_122043.json` gộp 4.901 dòng), nhưng `reset_streaming.sh`
  xóa kho `stream_events` trước mỗi lượt demo/thử, nên lần làm sạch của mô hình cuối (`clean_20261106_123904.json`)
  có `stream_events` = null: mô hình cuối (P@10 0,204) được huấn luyện không có sự kiện streaming. Kho giữ sự kiện cho
  lần huấn luyện sau cho đến khi chạy `reset_streaming.sh`; production: kho sự kiện là dữ liệu gốc bất biến, không reset.
  `producer.py`, `rate_from_recs.py` và nút Chấm sao của app đánh dấu `simulated=true`. `clean.py` mặc định gộp cả sự
  kiện mô phỏng (để demo được vòng chấm sao → huấn luyện lại → gợi ý mới) và bỏ chúng khi chạy với `--exclude-simulated`;
  report `clean_*.json` đếm riêng `real_events` và `simulated_events`. Kết quả làm sạch có cột `from_stream`, và
  `train_als.py` luôn đưa rating đến từ streaming vào tập train (rating CSV mới chia ngẫu nhiên 80/20), nên mô hình phục vụ
  học được mọi lượt chấm mới; trước 10/11, chia ngẫu nhiên cả bảng làm khoảng 1/5 lượt chấm mới rơi vào tập test và không
  được học (thử 10/11: 1/11 lượt, `A Game of Thrones` của user 22). Kho là file sink trigger 5 giây, chia theo
  `event_date`, không compaction, nên sinh nhiều file nhỏ (production: trigger dài hơn cho query này, hoặc compaction /
  Delta Lake / Iceberg).
- **Lớp nhanh bỏ sự kiện trễ, lớp batch giữ lại:** thống kê trực tiếp và "sách đang hot" dùng watermark 10 phút
  để giới hạn bộ nhớ trạng thái, nên bỏ sự kiện trễ hơn thế; kho lưu trữ thì giữ lại để không mất rating thật.
  Spark 3.5 lọc sự kiện trễ bằng watermark của batch **trước**, nên 1–2 batch đầu sau khi khởi động mới (checkpoint
  trống) chưa lọc; chạy lại từ checkpoint thì watermark được khôi phục. Vì vậy producer chỉ gửi sự kiện trễ sau 30 giây
  (`--late-after-seconds`). `user_seen` cũng được ghi sau watermark, nên một rating thật trễ hơn 10 phút không bị ẩn
  khỏi app cho tới lần batch sau (có thể sửa bằng cách ghi `user_seen`, vốn là upsert idempotent, từ một nhánh không
  trạng thái như kho lưu trữ).
- **Theo ý tưởng Lambda, chưa hợp nhất hai view:** `rating_stats` chỉ chứa rating từ streaming; thống kê theo sách
  của MapReduce / Spark SQL dùng để phân tích và đối chiếu, không được nạp vào MongoDB; app chỉ gộp lúc đọc. Hai lớp
  **không** cho cùng kết quả: `clean.py` giữ một rating mới nhất cho mỗi cặp (user, book), lớp tốc độ đếm mọi
  `event_id` khác nhau (chấm lại bị tính hai lần trong `rating_stats`) và bỏ sự kiện trễ hơn watermark; vì vậy hai view
  không được cộng với nhau. Muốn hợp nhất cần lớp tốc độ theo (user, book) và một mốc cắt (ghi trong `meta` mốc mà batch
  đã hấp thụ, chỉ cộng speed view sau mốc); hiện chưa có, nên `rating_stats` và `user_seen` tăng tới khi reset. Không dùng Kappa vì log Kafka không phải nguồn gốc: 6 triệu rating lịch sử chưa từng đi qua Kafka, và
  Kafka chỉ giữ 7 ngày; muốn Kappa phải nạp lịch sử vào topic, giữ log lâu dài rồi huấn luyện lại bằng cách đọc lại log.
- **Đổi phiên bản gợi ý an toàn (chỉ phần gợi ý):** gợi ý mới ghi vào collection mới; cổng kiểm tra số document ghi
  được phải bằng số người dùng cần ghi (nếu không: xóa collection mới, job báo lỗi, `meta.active` giữ bản cũ); `books`
  upsert theo `_id` **trước** khi chuyển; rồi mới chuyển `meta.active`. Giữ đúng phiên bản đang dùng và phiên bản trước
  (`previous_collection` trong `meta`; quay lại bằng cách đặt `recs_collection` về giá trị này), xóa collection sót lại
  của lần chạy hỏng. Đã thử trên database riêng `bookpulse_test` (`reports/superseded/write_recs_gate_test_20261108/`:
  53.424/53.424 người dùng, nhánh cho qua; nhánh chặn chỉ có trong code). Còn lại: cổng chỉ kiểm tra đủ số lượng,
  không so chất lượng (Precision@10) trước khi chuyển; `model_version` là thời điểm `write_recs.py` chạy;
  `models/als_implicit` và `data/parquet/ratings` bị ghi đè tại chỗ, không có phiên bản.
- **Chống đếm hai lần:** Spark + Kafka + checkpoint là at-least-once; batch nào đã ghi thì bỏ qua khi bị phát lại
  (`stream_state.last_batch`). Lượt thử `kill -9` (`scripts/stream_fault_test.sh`): 1.474 = 1.500 − 26 sự kiện trễ,
  0 mất, 0 trùng (26 sự kiện trễ bị loại là suy từ kafka_audit; file progress chỉ ghi 0 + 23 vì batch commit ngay
  trước `kill -9` không nằm trong file nào). Còn một khe hở: nếu job chết đúng lúc đang ghi MongoDB (`rating_stats` và
  `online_eval` cộng dồn trước khi ghi `last_batch`), batch phát lại có thể cộng lặp một phần. `kill -9` ở thời điểm
  bất kỳ gần như chắc chắn không rơi vào khe vài mili giây này, nên "0 trùng" không chứng minh khe đã đóng (muốn thử cần
  chèn lỗi ngay sau `bulk_write`). Cách sửa, chưa làm: lọc `{_id: book, last_batch: {$lt: batch_id}}` rồi `$inc` cùng
  `$set last_batch` (nguyên tử trên một document, không cần transaction). Nếu batch id nhỏ hơn `last_batch`
  (checkpoint bị xóa nhưng MongoDB còn), job dừng với lỗi thay vì âm thầm bỏ qua (sửa 08/11). Kho Parquet exactly-once (file sink); "sách đang hot" idempotent vì mỗi lần
  ghi thay cả document (ReplaceOne). "0 mất" là kết quả đo, không phải bảo đảm: `stream_stats.py` đặt
  `failOnDataLoss=false`, nên nếu job tắt lâu hơn thời gian giữ của Kafka (mặc định 168 giờ) hoặc topic bị tạo lại,
  Spark chỉ ghi cảnh báo và bỏ qua offset đã mất (production: `failOnDataLoss=true` + cảnh báo theo lag).
- **Producer chịu được Kafka dừng ngắn:** trước khi sửa, lượt ngắt Kafka 40 giây làm producer mất 571 message, vì
  kafka-python 2.0.2 hủy bản ghi chờ trong buffer quá `request_timeout_ms` (mặc định 30 giây) và không có callback
  kiểm tra. Thay đổi quyết định là `request_timeout_ms=120000` (chờ tới 2 phút); thêm `retries=10`,
  `retry_backoff_ms=1000` cho request lỗi giữa chừng, và callback đếm bản ghi Kafka không xác nhận (`failed` trong dòng
  tiến độ). Kafka dừng lâu hơn khoảng 2 phút thì vẫn mất, nhưng được đếm. Retry có thể ghi một bản ghi hai lần; bỏ trùng
  theo `event_id` xử lý được. Không có idempotent producer, nên sau retry thứ tự trong partition không được bảo đảm (job
  dùng `event_time`). `acks=all` với replication 1 tương đương `acks=1`; chỉ có ý nghĩa khi replication 3,
  `min.insync.replicas=2`.
- **Ghi MongoDB chạy ở driver:** query `valid` và `trending` đều gom mỗi micro-batch về driver rồi ghi bằng một
  client MongoDB (`rejected` chỉ đếm). Đủ cho
  tốc độ sự kiện của đồ án; `--max-offsets` (`maxOffsetsPerTrigger`, mặc định 20.000) giới hạn một batch sau khi
  job tắt lâu. Ở quy mô lớn cần chuyển việc ghi sang executor (`foreachPartition` hoặc MongoDB Spark connector,
  gộp theo sách trong Spark trước khi ghi). Mặc định nguồn Kafka đọc mỗi partition bằng một task (`minPartitions` chia
  nhỏ thêm được); phần có trạng thái chạy trên 8 shuffle partition. **Chưa thử tải**: ở 20 sự kiện/giây batch mất 3–6
  giây, phần lớn là chi phí cố định (4 query chia 4 core); 20.000 offset/batch chưa được hiệu chỉnh.
- **Đánh giá online:** mỗi lượt chấm 4–5 sao được đối chiếu với 10 gợi ý đầu đã lưu của phiên bản active (không trừ
  sách đã ẩn, không tính sách bù); bộ đếm cộng dồn qua các phiên bản. Với `producer.py`
  kết quả luôn là 0 trúng (sách phát lại đã bị loại khỏi gợi ý); chỉ lượt chấm sách đang được gợi ý (nút Chấm sao hoặc `rate_from_recs.py`) tạo được lượt trúng.
- **Một máy:** Kafka 1 broker (replication 1), MongoDB standalone, Spark `local[*]`, Hadoop 1 DataNode (replication 1),
  không có restart policy hay supervisor (khi MongoDB dừng, job streaming dừng theo và phải chạy lại bằng tay): mỗi
  thành phần là một điểm lỗi đơn. Spark đọc ổ đĩa local, không đọc HDFS: `local[*]` là một JVM trên một máy, nên HDFS
  1 DataNode chỉ thêm độ trễ RPC. Lên cluster không chỉ đổi đường dẫn: logic xử lý (DataFrame, SQL, ALS) giữ nguyên,
  nhưng phải sửa lớp cấu hình (`common/settings.py` dùng pathlib, biến `hdfs://` thành `hdfs:/`, nên phải đổi sang
  chuỗi URI; đặt `SPARK_MASTER`; checkpoint lên HDFS/S3; gửi `common/` bằng `--py-files`; `spark.sql.shuffle.partitions`
  = 8 và driver 4 GB là giá trị cho laptop, đổi qua `SPARK_SHUFFLE_PARTITIONS`, `SPARK_DRIVER_MEMORY`; gói
  `spark-sql-kafka` đang tải từ Maven lúc chạy nên phải đóng sẵn vào image) và viết lại vài đoạn I/O (`has_parquet()`
  trong `clean.py`, phần ghi MongoDB của `valid` và `trending` sang executor). Thiết kế cluster nằm trong báo cáo.
- **Chưa có bảo mật:** cổng MongoDB (27017) và Kafka (9094) mở ra máy host, MongoDB không xác thực, Kafka PLAINTEXT;
  chỉ chấp nhận được trên laptop. Sự kiện lỗi chỉ được đếm, không lưu (chưa có dead-letter topic).
- **Chia train/test ngẫu nhiên** vì CSV không có thời điểm chấm (seed 42: 4.780.968 / 1.195.511 dòng; rating từ
  streaming luôn vào train). Nếu dùng dữ liệu UCSD bản JSON có ngày tháng, nên đổi sang chia theo thời gian (train trước
  một mốc, test sau mốc). Khi có thêm rating từ streaming, cách chia của `randomSplit` xáo lại một phần tập test của CSV,
  nên Precision@10 dao động nhẹ giữa các lần (thử 10/11: 0,201–0,208); chia theo hash của (user, sách) sẽ ổn định hơn.

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
`clean.py` lần sau vẫn gộp sự kiện cũ. `reset_streaming.sh` xóa đúng cả ba. Nếu checkpoint bị xóa mà `stream_state`
còn, job streaming nay dừng với lỗi (batch id nhỏ hơn `last_batch`) thay vì âm thầm bỏ qua các batch đó.

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
| `run_hadoop.sh` | Đưa CSV lên HDFS (file 72 MB, blocksize 32 MB → 3 block → 3 split), chạy job trên YARN với 2 reducer, lấy kết quả về `reports/mapreduce/` |
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

Số map task và thời gian thay đổi theo lần chạy: 05/11 có 4 map task (3 split + bản dự phòng do ApplicationMaster của
MapReduce chạy, speculative execution; log `reports/mapreduce/job_20261105_165718.log`: Launched map tasks=4, Killed
map tasks=2, Killed reduce tasks=1; mỗi split chỉ hoàn tất một lần) và 134 giây
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
| `tests/test_recs.py` | không gợi ý sách đã đọc; xếp hạng top-k đúng; rating từ streaming luôn nằm trong tập train (2 test) |
| `tests/test_stream_progress.py` | file tiến độ streaming cộng đúng mỗi batch một lần dù đọc lại nhiều lần; `AlreadyAppliedTest` (3 test): bỏ qua batch đã ghi, dừng với lỗi khi batch id nhỏ hơn `last_batch` |

13/13 test đều qua (chạy trong container `spark`; trên máy host chỉ chạy được `test_mapreduce` vì thiếu pyspark/pymongo).

Ngoài unit test, mỗi thành phần có kiểm chứng trên dữ liệu thật: `mapreduce/verify_with_spark.py` (MapReduce so với Spark SQL),
`scripts/kafka_audit.py` (đếm Kafka và so với số đếm streaming trong MongoDB, khối `mongo_check`),
`scripts/nosql_evidence.py` (đọc lại MongoDB; kiểm tra gợi ý không chứa sách đã chấm). Cổng kiểm tra của
`write_recs.py` được thử trên database riêng: `docker compose exec -e MONGO_DB=bookpulse_test spark python
batch/write_recs.py --books data/goodbooks/books.csv` (`reports/superseded/write_recs_gate_test_20261108/`).

Thử sự cố streaming (khoảng 3 phút, chạy từ `bookpulse/`): `bash scripts/stream_fault_test.sh`. Script reset streaming,
chạy job + producer (1.500 sự kiện, 3% trễ sau 30 giây, seed 7), `kill -9` cả Python lẫn JVM sau 45 giây, 15 giây sau
chạy lại từ checkpoint, rồi chạy `kafka_audit.py`. Log ở `reports/streaming/fault_test_<thời điểm>/`. Lần chạy 08/11
(`reports/kafka_audit_20261107_220738.json`, tên file theo giờ UTC): 1.474 = 1.500 − 26 sự kiện trễ (26 sự kiện trễ
bị loại là suy từ phép trừ này; file progress chỉ ghi 0 + 23), 15 sự kiện lỗi,
tổng cửa sổ 2.948 = 2 × 1.474; cả ba phép so đều `true`. Lần chạy thứ hai, có thêm kiểm tra kho Parquet
(`reports/kafka_audit_20261108_040236.json`): 1.475 = 1.500 − 25, kho 1.537 dòng = 1.537 message hợp lệ, 1.500
`event_id` khác nhau, 0 file chưa commit, `exactly_once` = `true`.

Thử ngắt Kafka và MongoDB (khoảng 5 phút, chạy từ `bookpulse/`): `bash scripts/stream_outage_test.sh`. Script reset
streaming, chạy job + producer (3.000 sự kiện, 20 sự kiện/giây), sau 30 giây dừng Kafka 40 giây rồi bật lại, sau đó
dừng MongoDB 30 giây rồi bật lại; sau mỗi lần ngắt, nếu job đã dừng thì chạy lại từ checkpoint; cuối cùng chạy
`kafka_audit.py` (Kafka, MongoDB và kho Parquet). Log ở `reports/streaming/outage_test_<thời điểm>/`, `events.log`
là dòng thời gian. Lần chạy 08/11 (`reports/kafka_audit_20261108_041203.json`): Kafka dừng thì job vẫn chạy và tự
kết nối lại, producer `failed` = 0; MongoDB dừng thì job dừng (`ServerSelectionTimeoutError`) và script thử nghiệm
chạy lại job từ cùng checkpoint sau 11 giây (chưa có restart policy, ngoài thử nghiệm phải chạy lại bằng tay); kết quả 3.078 message, 3.000 = 3.000 sự kiện hợp lệ, tổng cửa sổ 6.000, 19 sự kiện lỗi, kho 3.059 dòng
= 3.059 message hợp lệ; 0 mất, 0 trùng.
