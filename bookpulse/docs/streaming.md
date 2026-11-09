# Streaming trong BookPulse: thiết kế và bằng chứng

Tài liệu này tổng hợp phần streaming: kiến trúc, cách áp dụng từng khái niệm của môn học (Session 7), số đo kiểm chứng và các giới hạn. Mọi số liệu lấy từ `reports/` (dữ liệu Goodbooks-10k phát lại thành sự kiện). Các lượt kiểm thử ở mục 3 chạy ngày 08/11/2026; tên file trong container theo giờ UTC, nên `20261107_22xx` là 08/11 lúc 05:xx giờ Việt Nam.

## 1. Kiến trúc

```
producer.py ──► Kafka topic rating-events (3 partition, key = user_id)
                    │
                    ▼  Spark Structured Streaming (stream_stats.py), trigger 5 giây
     ┌──────────────┼──────────────────────┬─────────────────────────────┐
     ▼              ▼                      ▼                             ▼
  "valid"        "rejected"         "events_to_parquet"             "trending"
  bỏ trùng +     đếm sự kiện        lưu MỌI sự kiện hợp lệ          cửa sổ event time 10 phút,
  watermark      lỗi                (không trạng thái)              trượt 5 phút, theo sách
     │              │                      │                             │
     ▼              ▼                      ▼                             ▼
  MongoDB:       MongoDB:           Parquet stream_events           MongoDB: trending
  rating_stats,  stream_state       → clean.py (bỏ trùng theo       → app "Sách đang hot"
  user_seen,                          event_id) → huấn luyện lại
  online_eval
  (ghi ở driver)                                                    (ghi ở driver)
```

Ngoài `producer.py` (phát lại rating), mục **Chấm sao** của app và `scripts/rate_from_recs.py` gửi rating cho sách đang nằm trong top-10 của một người dùng (mục 3); cả hai chỉ gửi sự kiện vào Kafka (cùng định dạng, `common/events.py`), không ghi MongoDB. Kafka lưu log trên volume `kafka-data`, giữ 7 ngày theo mặc định (docker-compose không đặt lại). Đây là lớp tốc độ của hệ thống, tổ chức theo ý tưởng Lambda: lớp batch, lớp tốc độ, lớp phục vụ; chưa hợp nhất batch view và speed view thành một số liệu, app chỉ gộp lúc đọc (ẩn sách trong `user_seen`).

`producer.py` đặt `request_timeout_ms=120000` (bản ghi được chờ trong buffer tới 2 phút; đây là thay đổi then chốt, vì kafka-python 2.0.2 hủy bản ghi chờ quá `request_timeout_ms`, mặc định 30 giây), `retries=10` và `retry_backoff_ms=1000` cho request lỗi giữa chừng, và đếm bản ghi Kafka không xác nhận qua callback (`failed` trong dòng tiến độ), để không mất sự kiện âm thầm khi broker khởi động lại (mục 4). Kafka dừng lâu hơn khoảng 2 phút thì vẫn mất bản ghi, nhưng chúng được đếm. Retry có thể ghi một bản ghi hai lần; bỏ trùng theo `event_id` xử lý được bản này. kafka-python 2.0.2 không có idempotent producer, nên sau retry thứ tự trong một partition không được bảo đảm (job dùng `event_time`, nên chấp nhận được). Producer đặt `acks=all`, nhưng với replication 1 hiện tại thì tương đương `acks=1`; chỉ có ý nghĩa khi replication 3 và `min.insync.replicas=2`.

| Query | Kiểu | Output mode / sink | Có trạng thái? |
|---|---|---|---|
| `valid` | Lọc hợp lệ → watermark 10 phút → `dropDuplicatesWithinWatermark(event_id)` | `foreachBatch` → MongoDB | Có (trạng thái bỏ trùng) |
| `rejected` | Lọc sự kiện lỗi | `foreachBatch` → chỉ `count()`, ghi số đếm vào `stream_state` | Không |
| `events_to_parquet` (archive trong sơ đồ) | Lọc hợp lệ, **không** watermark, **không** bỏ trùng | File sink Parquet, append, chia theo `event_date` | Không |
| `trending` | Cùng nhánh `valid` → `groupBy(window(event_time, 10 phút, 5 phút), book_id)` | `update` + `foreachBatch` → MongoDB | Có (bỏ trùng + cửa sổ) |

Mỗi query có **checkpoint riêng** trong `checkpoints/` (`stream_valid`, `stream_rejected`, `stream_events_archive`, `stream_trending`). `valid` và `trending` gom mỗi micro-batch về driver (`collect`) rồi ghi MongoDB từ driver.

## 2. Đối chiếu với các khái niệm của môn học

| Khái niệm | Áp dụng trong BookPulse |
|---|---|
| Kafka pub/sub, partition, partition key | Topic `rating-events` 3 partition; key = `user_id`, nên mọi sự kiện của một người vào cùng partition (Kafka giữ thứ tự trong một partition) và tải chia đều. Job không dựa vào thứ tự này: thống kê theo `event_time`, `clean.py` giữ rating có `event_time` mới nhất. Theo mặc định, nguồn Kafka của Spark đọc mỗi partition bằng một task (tùy chọn `minPartitions` chia nhỏ thêm được); phần có trạng thái sau shuffle chạy trên 8 partition (`spark.sql.shuffle.partitions`). Job tự tạo topic nếu chưa có |
| DataFrame API, schema tường minh | `SCHEMA` (6 trường) + `from_json`, không có schema registry: trường thừa bị bỏ qua; trường thiếu, đổi tên hoặc sai kiểu thành null và bị chuyển sang nhánh `rejected`. Sự kiện lỗi chỉ được đếm, không được lưu, nên hết thời gian giữ của Kafka là mất (production: dead-letter topic, Avro/Protobuf + Schema Registry) |
| Source / sink | Nguồn Kafka; sink: `foreachBatch` (MongoDB) và file sink (Parquet) |
| Event time và processing time | Thống kê và cửa sổ dựa trên `event_time` (thời điểm rating xảy ra), không dựa trên lúc Spark xử lý |
| Watermark | 10 phút: sự kiện có `event_time` cũ hơn (event time lớn nhất đã thấy − 10 phút) bị loại khỏi các phép tính có trạng thái, và trạng thái cũ được giải phóng |
| Cửa sổ có trạng thái | Cửa sổ trượt 10 phút / 5 phút theo sách: số lượt chấm, điểm TB, số lượt 4–5 sao |
| Checkpoint và khôi phục | Mỗi query một checkpoint: `offsets/N` ghi khoảng offset của batch N **trước** khi chạy (write-ahead log); `commits/N` ghi sau khi xong và chỉ là dấu "batch N đã xong" (chứa `nextBatchWatermarkMs`, không chứa offset); trạng thái ở `state/`. Batch có trong `offsets/` mà chưa có trong `commits/` được chạy lại với cùng batch id và cùng offset. Đã thử dừng nhẹ (SIGINT), **dừng đột ngột (`kill -9`)** hai lần (`scripts/stream_fault_test.sh`) và ngắt Kafka, MongoDB (`scripts/stream_outage_test.sh`) |
| Idempotent | `stream_state.last_batch` bỏ qua batch bị phát lại (còn một khe hở, mục 6); nếu batch id nhỏ hơn `last_batch` (checkpoint bị xóa hoặc chuyển đi nhưng MongoDB còn), job dừng với lỗi thay vì âm thầm bỏ qua (sửa 08/11, 3 unit test `AlreadyAppliedTest`); `trending` ghi đè cả document (ReplaceOne) nên phát lại vẫn ra cùng kết quả; file sink exactly-once nhờ `_spark_metadata` |
| Backpressure | `maxOffsetsPerTrigger` = 20.000 (`--max-offsets`): sau khi job tắt lâu, mỗi micro-batch đọc tối đa 20.000 bản ghi thay vì dồn cả backlog vào một batch |
| Observability | `reports/stream_progress_*.json` (mục 5) và tab Structured Streaming của Spark UI |

## 3. Bằng chứng kiểm thử

Đếm độc lập bằng `scripts/kafka_audit.py`, đọc lại toàn bộ topic: tổng message, sự kiện hợp lệ khác nhau, sự kiện trễ (Kafka nhận sau `event_time` hơn 10 phút), message lỗi. Cùng lệnh đó đọc lại MongoDB và lưu khối `mongo_check` vào file report, gồm ba phép so:

- `no_loss_no_double_count`: số sự kiện hợp lệ đã đếm (`stream_state`) = hợp lệ khác nhau − trễ = tổng `n_ratings` trong `rating_stats`;
- `rejected_match`: số sự kiện lỗi đã đếm = số message lỗi trong Kafka;
- `trending_match`: tổng lượt trong các cửa sổ = 2 × (hợp lệ − trễ), vì cửa sổ 10 phút trượt 5 phút nên mỗi sự kiện đúng giờ nằm trong đúng 2 cửa sổ.

Từ ngày 08/11, cùng lệnh còn đếm kho Parquet `data/parquet/stream_events` và lưu khối `archive_check` (bỏ qua bằng `--no-archive`). Script đọc log `_spark_metadata` của file sink và chỉ tính file đã có trong log, đúng như cách Spark đọc kho (file của một batch bị kill giữa chừng còn trên đĩa nhưng không được tính). Kho không có watermark, không bỏ trùng, nên phải có: số dòng = số message hợp lệ (gồm bản lặp và sự kiện trễ), số `event_id` khác nhau = số sự kiện hợp lệ khác nhau; kèm số file chưa commit trên đĩa. Cả hai đúng thì `exactly_once` = `true`.

Mỗi lượt dưới đây bắt đầu từ trạng thái sạch (`reset_streaming.sh`), nên số đếm chỉ gồm lượt đó.

### Lượt 1: sự kiện trễ + `kill -9` giữa chừng (`bash scripts/stream_fault_test.sh`)

Producer gửi 1.500 sự kiện (seed 7), trong đó 32 bản trùng, 15 sự kiện lỗi và **26 sự kiện trễ 30 phút** (gửi sau giây thứ 30). Sau 45 giây, script `kill -9` cả tiến trình Python lẫn JVM; 15 giây sau chạy lại từ checkpoint. Bằng chứng: `reports/kafka_audit_20261107_220738.json`, log ở `reports/streaming/fault_test_20261108_050413/`, tiến độ ở `stream_progress_20261107_220431.json` (trước khi kill) và `_220545.json` (sau khi chạy lại).

Kafka: 1.547 message (partition 537 / 496 / 514), 1.500 sự kiện hợp lệ khác nhau, 26 trễ, 32 message lặp, 15 message lỗi.

| Kiểm tra | Kỳ vọng (từ Kafka) | MongoDB | |
|---|---|---|---|
| Sự kiện hợp lệ được tính (`stream_state`) | 1.500 − 26 trễ = 1.474 | 1.474 | ✅ |
| Tổng `n_ratings` trong `rating_stats` | 1.474 | 1.474 | ✅ |
| Sự kiện lỗi bị loại | 15 | 15 | ✅ |
| Tổng lượt trong các cửa sổ | 2 × 1.474 = 2.948 | 2.948 | ✅ |

Kết luận: lượt `kill -9` này **0 mất, 0 trùng**, và 26 sự kiện trễ bị loại khỏi thống kê trực tiếp (suy từ kafka_audit: 1.474 = 1.500 − 26, không phải Spark đếm). Hai file progress chỉ ghi 0 + 23 = 23 dòng bị watermark loại: file progress được ghi mỗi 10 giây, và batch 8 commit sau lần ghi progress cuối, ngay trước `kill -9`, nên không nằm trong file nào. Lượt này có trước khi `kafka_audit.py` có `archive_check`. Đây là kết quả đo, không phải bảo đảm: vẫn còn khe hở khi job chết đúng lúc đang ghi MongoDB, và `failOnDataLoss=false` (mục 6). `kill -9` ở giây thứ 45 gần như chắc chắn không rơi vào khe hở vài mili giây đó, nên "0 trùng" chứng minh đường khôi phục bình thường, không chứng minh khe hở đã đóng.

### Lượt 2: chạy demo, đọc file progress

Khởi động mới, producer `--limit 1000 --late 0.05` (khoảng 52 giây), rồi `scripts/rate_from_recs.py --user 1 --n 2`. Bằng chứng: `reports/kafka_audit_20261107_221105.json`, `reports/stream_progress_20261107_220918.json`.

Kafka: 1.028 message, 1.002 sự kiện hợp lệ khác nhau (1.000 từ producer + 2 từ `rate_from_recs.py`), 19 trễ, 16 message lặp, 10 message lỗi.

| Query | Batch | Dòng đầu vào | Bị watermark loại | Bản trùng bị bỏ | Dòng trạng thái |
|---|---|---|---|---|---|
| `valid` | 16 | 1.028 | **20** | **15** | 983 |
| `trending` | 17 | 1.028 | **20** | **15** | 2.624 |
| `events_to_parquet` | 14 | 1.028 | 0 | 0 | 0 (không trạng thái) |

20 dòng bị watermark loại = 19 sự kiện trễ + 1 bản gửi lại của một sự kiện trễ (bản này vì thế không còn nằm trong số 15 bản trùng bị bỏ). Cả `valid` lẫn `trending` loại hết sự kiện trễ dù khởi động mới, vì sự kiện trễ chỉ đến sau giây thứ 30. MongoDB: **983** = 1.002 − 19 sự kiện hợp lệ, tổng cửa sổ 1.966 = 2 × 983, **10** sự kiện lỗi; cả ba phép so `true`.

Counter trong file progress là số đếm theo lần chạy và theo từng query, nên có thể lệch vài dòng so với Kafka: bản gửi lại của một sự kiện trễ được tính là "bị watermark loại", và khi job bị `kill -9`, batch commit sau lần ghi file progress cuối (mỗi 10 giây) không nằm trong file nào (counter **thiếu**, không thừa). Số liệu dữ liệu được kiểm bằng `mongo_check`, không bằng counter.

### Lượt 3: `kill -9` lần 2, thêm kiểm tra kho Parquet (`bash scripts/stream_fault_test.sh`)

Cùng kịch bản lượt 1. Producer gửi 1.500 sự kiện, 37 bản trùng, 13 sự kiện lỗi, 25 sự kiện trễ. Bằng chứng: `reports/kafka_audit_20261108_040236.json`, log ở `reports/streaming/fault_test_20261108_105833/`.

Kafka: 1.550 message (536 / 500 / 514), 1.500 sự kiện hợp lệ khác nhau, 25 trễ, 37 message lặp, 13 message lỗi, 1.537 message hợp lệ.

| Kiểm tra | Kỳ vọng (từ Kafka) | Thực tế | |
|---|---|---|---|
| Sự kiện hợp lệ được tính (MongoDB) | 1.500 − 25 = 1.475 | 1.475 | ✅ |
| Tổng `n_ratings` trong `rating_stats` | 1.475 | 1.475 | ✅ |
| Sự kiện lỗi bị loại | 13 | 13 | ✅ |
| Tổng lượt trong các cửa sổ | 2 × 1.475 = 2.950 | 2.950 | ✅ |
| Kho Parquet: số dòng | 1.537 message hợp lệ | 1.537 | ✅ |
| Kho Parquet: `event_id` khác nhau | 1.500 | 1.500 | ✅ |
| Kho Parquet: file chưa commit trên đĩa | 0 | 0 | ✅ |

Kho giữ cả 37 bản lặp và 25 sự kiện trễ mà lớp tốc độ đã bỏ; `clean.py` bỏ trùng theo `event_id` ở lần batch sau. File progress lượt này (`stream_progress_20261108_035851.json`, `_040005.json`) cũng chỉ ghi 0 + 23 dòng bị watermark loại so với 25 sự kiện trễ, cùng lý do như lượt 1.

### Lượt 4: ngắt Kafka và MongoDB (`bash scripts/stream_outage_test.sh`)

Script (khoảng 5 phút, chạy trên máy host từ `bookpulse/`): reset streaming, chạy job và producer (3.000 sự kiện, 20 sự kiện/giây); sau 30 giây dừng Kafka 40 giây rồi bật lại; sau đó dừng MongoDB 30 giây rồi bật lại; sau mỗi lần ngắt, nếu job đã dừng thì chạy lại từ cùng checkpoint; khi producer xong thì dừng job và chạy `kafka_audit.py`. Log ở `reports/streaming/outage_test_<thời điểm>/`, `events.log` là dòng thời gian.

**Lượt đầu tìm ra lỗi ở producer** (`reports/superseded/outage_test_20261108_110251_producer_no_retry/`). kafka-python 2.0.2 hủy bản ghi đã chờ trong buffer quá `request_timeout_ms` (mặc định 30 giây), trong khi Kafka dừng hơn 40 giây (`events.log`: 11:03:43 → 11:04:26); và không có callback nào kiểm tra việc gửi. Producer gửi 3.078 message nhưng topic chỉ có 2.507: mất 571. Phía Spark không mất gì so với Kafka. Đã sửa `producer.py` (retry + đếm `failed`, mục 1).

**Lượt chạy lại với producer đã sửa** (`reports/streaming/outage_test_20261108_110750/`, `reports/kafka_audit_20261108_041203.json`):

- Kafka dừng 40 giây: producer giữ sự kiện trong buffer, `failed` = 0; job streaming #1 vẫn chạy và tự kết nối lại, không cần khởi động lại.
- MongoDB dừng 30 giây: job #1 dừng với `pymongo.errors.ServerSelectionTimeoutError` (hết 30 giây chờ trong `foreachBatch`). Script thử nghiệm chạy lại job (#2) từ cùng checkpoint 11 giây sau khi job dừng (`events.log`: 11:11:17 → 11:11:28); batch chưa ghi `last_batch` được chạy lại. Chưa có supervisor hay restart policy: ngoài thử nghiệm phải chạy lại bằng tay (production: restart policy hoặc orchestrator, kèm cảnh báo).

| Kiểm tra | Kỳ vọng (từ Kafka) | Thực tế | |
|---|---|---|---|
| Message trong topic | producer gửi 3.000 + 59 trùng + 19 lỗi = 3.078 | 3.078 (1.019 / 1.016 / 1.043) | ✅ |
| Sự kiện hợp lệ được tính (MongoDB) | 3.000 − 0 trễ = 3.000 | 3.000 | ✅ |
| Tổng `n_ratings` trong `rating_stats` | 3.000 | 3.000 | ✅ |
| Sự kiện lỗi bị loại | 19 | 19 | ✅ |
| Tổng lượt trong các cửa sổ | 2 × 3.000 = 6.000 | 6.000 | ✅ |
| Kho Parquet: số dòng | 3.059 message hợp lệ | 3.059 | ✅ |
| Kho Parquet: `event_id` khác nhau | 3.000 | 3.000 | ✅ |
| Kho Parquet: file chưa commit trên đĩa | 0 | 0 | ✅ |

Kết luận: 0 mất, 0 đếm trùng từ đầu đến cuối, qua một lần ngắt Kafka và một lần ngắt MongoDB có khởi động lại job.

### Kho lưu trữ cho lần huấn luyện sau (06/11, `clean_20261106_122043.json`)

| | Số dòng |
|---|---|
| Dòng trong kho Parquet (mọi sự kiện hợp lệ, gồm cả trùng và trễ) | 4.901 |
| Sau khi `clean.py` bỏ trùng theo `event_id` | 4.805 (bỏ 96 bản trùng) |
| Cặp (user, book) mới vào dữ liệu huấn luyện | 5 |

Report `clean_*.json` không đếm riêng số sự kiện trễ trong kho. Lần đếm topic gần nhất trước đó, `reports/kafka_audit_20261106_121247.json` (3.924 message, trước khi gửi thêm sự kiện), có 43 sự kiện trễ: lớp nhanh bỏ, kho giữ.

Vòng streaming → huấn luyện lại đã chạy thật ở lượt này. Tuy nhiên `scripts/reset_streaming.sh` xóa kho `data/parquet/stream_events` cùng topic trước mỗi lượt demo/thử, nên lần làm sạch của mô hình cuối (`clean_20261106_123904.json`) có `stream_events` = null: mô hình cuối (P@10 0,204) được huấn luyện **không có** sự kiện streaming. Kho giữ sự kiện cho lần huấn luyện sau cho đến khi chạy `reset_streaming.sh`; ở production, kho sự kiện là dữ liệu gốc bất biến, không reset. Ngoài ra `producer.py` và `rate_from_recs.py` đánh dấu `simulated=true` nhưng `clean.py` gộp mà không lọc, nên một lượt 5 sao demo từ `rate_from_recs.py` sẽ vào lần huấn luyện sau; production cần lọc sự kiện mô phỏng.

### Sách vừa chấm biến khỏi top-10 (`scripts/rate_from_recs.py`)

`producer.py` phát lại cặp (user, sách) lấy từ `ratings.csv`, tức dữ liệu huấn luyện, mà `write_recs.py` đã loại mọi cặp đó khỏi gợi ý và danh sách bù. Vì vậy sự kiện phát lại **không bao giờ** làm đổi top-10 đang hiện, và số lượt trúng online luôn là 0. Để thấy app phản ứng, `rate_from_recs.py` đóng vai người dùng chọn sách ngay trong danh sách của mình: đọc top-10 hiện tại (collection gợi ý đang active trừ `user_seen`) rồi gửi rating cho `--n` cuốn đầu. Từ 09/11 app có mục **Chấm sao** làm cùng việc cho một cuốn chọn trên giao diện: chọn sách, số sao, bấm "Gửi lượt chấm"; app gửi sự kiện vào Kafka, job streaming xử lý như mọi sự kiện khác (thử ngày 09/11: user 22 chấm 5 sao cuốn #1 *Misery* trên app, sau một trigger cuốn này bị ẩn và *Carrie* lên #1).

Lượt 08/11: user 1 chấm 5 sao cuốn #1 *The Guernsey Literary and Potato Peel Pie Society* (195) và #2 *The Great Gatsby* (5). Sau một trigger, cả hai bị ẩn, *The Catcher in the Rye* (8) lên #1, hai cuốn mới vào #9–10 (*The Immortal Life of Henrietta Lacks*, 208; *The Da Vinci Code*, 26). Chỉ số "Lượt 4–5 sao trúng top-10" hiện **2 / 672**: 2 lượt trúng là 2 sự kiện này; 670 lượt còn lại từ producer không thể trúng. Ảnh: `docs/img/app_after_rate_from_recs.png`.

### Ảnh chụp (`reports/streaming/`)

- `spark_ui_streaming_queries.png`: 4 query có tên, đang chạy.
- `spark_ui_trending_statistics.png`: query `trending`: tốc độ đầu vào khoảng 20 sự kiện/giây, tốc độ xử lý, thời gian mỗi batch (batch đầu khoảng 12 giây, các batch sau khoảng 2,8–6,1 giây), khoảng cách watermark khoảng 600 giây (đúng 10 phút), số dòng trạng thái tăng dần.
- `app_trending.png`: cả trang app sau lượt 2: 983 sự kiện hợp lệ, 10 sự kiện lỗi, 2 / 672 lượt trúng và mục "Sách đang hot".

## 4. Phát hiện khi kiểm thử và cách sửa

**Watermark là riêng cho từng query, và chỉ cập nhật giữa các batch.** Phát hiện ở một lượt `kill -9` ngày 06/11, khi kho lưu trữ còn theo **thiết kế cũ** (dùng chung bước bỏ trùng có watermark), không phải các lượt ở mục 3: query lưu Parquet chưa kịp hoàn tất batch nào trước khi bị `kill -9`, nên khi chạy lại, batch đầu tiên vẫn dùng watermark cũ (ngày 05/11), và **12 sự kiện trễ lọt qua**.

Phát hiện này cho thấy một vấn đề thiết kế. Vì kho lưu trữ dùng chung bước bỏ trùng có watermark, nên:
- rating trễ hơn 10 phút bị mất khỏi dữ liệu huấn luyện dù là rating thật;
- kết quả còn phụ thuộc vào thời điểm restart.

**Cách sửa (theo ý tưởng Lambda: lớp tốc độ và lớp batch xử lý cùng sự kiện theo hai cách):**
- **Lớp xử lý nhanh** (`valid`, `trending`) giữ watermark để giới hạn bộ nhớ trạng thái; bỏ sự kiện trễ là đánh đổi chấp nhận được cho số liệu "trực tiếp".
- **Lớp batch** (`events_to_parquet`) lưu mọi sự kiện hợp lệ, không trạng thái; `clean.py` bỏ trùng chính xác theo `event_id`. Không mất rating nào, và kết quả không phụ thuộc vào thời điểm restart.

Kho cũ được giữ lại ở `data/parquet/stream_events_v1_deduped`; kho mới dựng lại từ toàn bộ topic Kafka và khớp đúng số liệu Kafka (06/11).

**Watermark chưa có ở 1–2 batch đầu.** Trong Spark 3.5, query có trạng thái lọc sự kiện trễ bằng watermark của batch **trước**. Sau khi khởi động **mới** (checkpoint trống), batch 0 và 1 chưa có watermark nên chưa lọc gì; khi chạy lại từ checkpoint thì watermark được khôi phục (job thứ hai của lượt 1 loại 23 dòng trễ). Lần tập 07/11, producer bắt đầu gửi sự kiện trễ sau 200 sự kiện (khoảng 10 giây), lúc batch đầu còn đang chạy trên JVM lạnh: `trending` chỉ loại 9 trên 13 dòng trễ mà `valid` loại (`reports/superseded/rehearsal_20261107/`). Đây là đặc tính của Spark, không phải lỗi của job. Cách sửa: producer chỉ gửi sự kiện trễ sau `--late-after-seconds` (mặc định 30 giây, thay cho `--late-after` đếm theo số sự kiện). Với cách này, không sự kiện trễ nào lọt vào số liệu trực tiếp ở các lượt 08/11 (lượt 1: 26 sự kiện trễ bị loại, suy từ kafka_audit; lượt 2: 19).

**Topic Kafka từng không được lưu bền.** Trước đây Kafka không có volume, topic nằm trong container, nên `docker compose down` xóa topic trong khi checkpoint (volume khác) vẫn còn; khi chạy lại, offset trong checkpoint lớn hơn topic mới và `failOnDataLoss=false` chỉ ghi cảnh báo. Nay Kafka có volume `kafka-data` (`KAFKA_LOG_DIRS=/var/lib/kafka/data`): `down` giữ topic; `down -v` xóa mọi volume, sau đó phải chạy `bash scripts/reset_streaming.sh --yes` (xóa luôn `data/parquet/stream_events`, nằm trên thư mục máy, ngoài volume; nếu để lại, `_spark_metadata` cũ làm query lưu trữ bỏ qua batch mới).

**Producer mất sự kiện khi Kafka dừng.** Lượt ngắt Kafka đầu tiên (lượt 4) cho thấy producer gửi 3.078 message nhưng topic chỉ có 2.507: kafka-python 2.0.2 hủy bản ghi chờ trong buffer quá `request_timeout_ms` (mặc định 30 giây) trong khi Kafka dừng hơn 40 giây, và producer không kiểm tra kết quả gửi. Cách sửa: thay đổi then chốt là đặt `request_timeout_ms=120000` (chờ tới 2 phút); thêm `retries=10`, `retry_backoff_ms=1000` cho request lỗi giữa chừng, và callback đếm `failed`. Lượt chạy lại: `failed` = 0, topic có đủ 3.078 message. Kafka dừng lâu hơn khoảng 2 phút thì vẫn mất, nhưng được đếm.

**Phát lại không thể làm đổi gợi ý.** Sự kiện của `producer.py` là cặp đã có trong dữ liệu huấn luyện, nên không bao giờ nằm trong top-10. Nhóm thêm `scripts/rate_from_recs.py` để thấy hiệu ứng ẩn sách (mục 3) và đổi chỉ số online trên app thành "trúng / tổng" kèm giải thích.

## 5. Observability: file progress

`stream_stats.py` gom `StreamingQueryProgress` của từng query theo batch và ghi `reports/stream_progress_<thời điểm>.json` mỗi 10 giây và khi dừng. Mỗi query có:

| Trường | Ý nghĩa |
|---|---|
| `batches`, `input_rows` | Số micro-batch và số dòng đã đọc từ Kafka |
| `max_input_rows_per_second`, `max_processed_rows_per_second` | Tốc độ vào và tốc độ xử lý; xử lý thấp hơn vào kéo dài nghĩa là đang tụt lại |
| `max_batch_duration_ms` | Thời gian lâu nhất của một batch, để so với trigger 5 giây |
| `rows_dropped_by_watermark`, `duplicates_dropped` | Sự kiện trễ bị loại, bản trùng bị bỏ |
| `state_rows_now`, `watermark_now` | Kích thước trạng thái và watermark hiện tại |
| `last_progress` | Bản đầy đủ của batch cuối (offset Kafka, thời gian từng bước, `avgOffsetsBehindLatest` của nguồn Kafka) |

Spark không commit offset vào consumer group của Kafka (offset nằm trong checkpoint), nên công cụ đo lag của Kafka (lag exporter) không thấy lag của job. Độ tụt lại đo bằng metric `avgOffsetsBehindLatest` trong `last_progress`, hoặc qua `StreamingQueryListener` khi đẩy metric lên Prometheus.

Chỉ cổng 4040 của Spark UI được mở ra máy host. **Không chạy job batch trong lúc job streaming đang chạy**: UI thứ hai sang cổng 4041 (không mở), và hai driver 4 GB vượt 7,7 GiB RAM của Docker.

## 6. Giới hạn và vấn đề hiệu năng

- **Thời gian batch sát trigger.** Batch đầu sau khi khởi động mất 7–12 giây (JVM còn lạnh); các batch sau thường 3–6 giây, sát trigger 5 giây (ảnh Spark UI: khoảng 2,8–6,1 giây sau batch đầu khoảng 12 giây; `max_batch_duration_ms` của query `valid` và `trending` ở ba file progress `20261107_22xx` của lượt 1 và lượt 2: 6,3–10,6 giây; trong lượt thử ngắt Kafka/MongoDB, batch kéo dài gần 1 phút, tối đa 57,7 giây, `stream_progress_20261108_040808.json`; lượt đầu với producer chưa sửa: 61 giây). Nguyên nhân: 4 query chạy song song và **mỗi query tự đọc Kafka riêng** trên 4 core. Ở 20 sự kiện/giây hệ thống vẫn theo kịp, nhưng nếu tốc độ cao hơn sẽ tụt lại. Hướng cải thiện: gộp các nhánh vào một query `foreachBatch` ghi ra nhiều nơi, tăng trigger, hoặc thêm core/executor.
- **Chưa thử tải.** Ở 20 sự kiện/giây, thời gian batch (3–6 giây) phần lớn là chi phí cố định (4 query chia 4 core), không phải do lượng dữ liệu; `maxOffsetsPerTrigger` 20.000 chưa được hiệu chỉnh. Thử tải đúng cách: tăng dần tốc độ producer cho tới khi `avgOffsetsBehindLatest` bắt đầu tăng.
- **Ghi MongoDB chạy ở driver.** `process_valid` và `process_trending` đều gom mỗi micro-batch về driver (`collect`) rồi ghi bằng một client MongoDB (mỗi tiến trình một client); `process_rejected` chỉ `count()`. Đủ cho tốc độ sự kiện của đồ án; `maxOffsetsPerTrigger` 20.000 giới hạn kích thước một batch sau khi job tắt lâu. Thêm executor **không** làm phần ghi này nhanh hơn; ở quy mô lớn cần chuyển việc ghi sang executor (`foreachPartition`, hoặc MongoDB Spark connector) và gộp theo sách trong Spark trước khi ghi.
- **Exactly-once có giới hạn: có thể cộng lặp một phần nếu job chết đúng lúc đang ghi MongoDB.** `rating_stats` (pipeline `$add`) và `online_eval` (`$inc`) cộng dồn trước khi ghi `last_batch`, không nằm trong transaction; nếu job chết giữa các bước đó (vài mili giây giữa `bulk_write` và `mark_applied`), batch phát lại có thể cộng lặp một phần. Hai lượt `kill -9` và lượt ngắt MongoDB cho 0 mất, 0 trùng, nhưng `kill -9` ở một thời điểm bất kỳ gần như chắc chắn không rơi vào khe này: chúng chứng minh đường khôi phục bình thường, không chứng minh khe đã đóng; muốn thử cần chèn lỗi có chủ đích (thoát ngay sau `bulk_write`). Cách sửa (chưa làm): chặn theo từng document, lọc `{_id: book, last_batch: {$lt: batch_id}}` rồi `$inc` cùng `$set last_batch` (MongoDB nguyên tử trên một document, không cần transaction), hoặc transaction trên replica set. `trending` (ReplaceOne) và kho Parquet (file sink) không gặp vấn đề này. `last_batch` cũng dựa vào số batch của checkpoint, nên checkpoint và `stream_state` phải được xóa cùng nhau (`reset_streaming.sh`); từ 08/11, nếu batch id nhỏ hơn `last_batch` (checkpoint bị xóa nhưng MongoDB còn), job dừng với lỗi thay vì âm thầm bỏ qua mọi batch tới `last_batch`.
- **Counter trong file progress có thể lệch** vài dòng so với Kafka: file được ghi mỗi 10 giây, nên batch commit ngay trước `kill -9` không nằm trong file nào (counter thiếu: 23 so với 26 và 25 sự kiện trễ ở hai lượt `kill -9`), và bản gửi lại của một sự kiện trễ được tính là "bị watermark loại". Số liệu dữ liệu vẫn đúng (`mongo_check`), chỉ counter đếm theo lần chạy.
- **"0 mất" là kết quả đo, không phải bảo đảm: `failOnDataLoss=false`.** Nếu job tắt lâu hơn thời gian giữ của Kafka (mặc định 168 giờ; docker-compose không đặt lại) hoặc topic bị tạo lại, Spark chỉ ghi cảnh báo và bỏ qua các offset đã mất. Production: `failOnDataLoss=true` và cảnh báo theo lag.
- **`user_seen` ghi sau watermark.** `user_seen` được ghi từ query `valid`, nên một rating thật đến trễ hơn 10 phút (ví dụ client offline) không bị ẩn khỏi app cho tới lần batch sau, dù kho Parquet vẫn giữ nó. `user_seen` là upsert idempotent, nên có thể ghi từ một nhánh không trạng thái như kho lưu trữ.
- **Sự kiện lỗi chỉ được đếm.** Nhánh `rejected` không lưu sự kiện, nên hết thời gian giữ của Kafka là mất. Production: dead-letter topic, Avro/Protobuf + Schema Registry.
- **Kho sự kiện sinh nhiều file nhỏ.** File sink, trigger 5 giây, chia theo `event_date`, không compaction: lượt 4 có 66 file cho 3.059 dòng. Production: trigger dài hơn cho riêng query lưu trữ, hoặc compaction định kỳ, Delta Lake, Iceberg.
- **Theo ý tưởng Lambda, chưa hợp nhất hai view.** `rating_stats` chỉ chứa rating từ streaming kể từ lần reset; thống kê theo sách của MapReduce / Spark SQL không được nạp vào MongoDB; app chỉ gộp lúc đọc (ẩn sách trong `user_seen`). Cột "Điểm TB (stream)" của app vì vậy có thể là 5,0 từ một lượt chấm duy nhất. Hai lớp **không** cho cùng kết quả: `clean.py` giữ một rating mới nhất cho mỗi cặp (user, book) theo `event_time`, còn lớp tốc độ đếm mọi `event_id` khác nhau (một lượt chấm lại bị tính hai lần trong `rating_stats`) và bỏ sự kiện trễ hơn watermark mà batch vẫn giữ; vì vậy hai view không được cộng với nhau. Muốn hợp nhất cần: lớp tốc độ giữ trạng thái theo (user, book) để trừ rating cũ, và một mốc cắt (ghi vào `meta` `event_time`/offset lớn nhất mà batch đã hấp thụ, giữ speed view theo khoảng thời gian, chỉ cộng các khoảng sau mốc, xóa các khoảng đã hấp thụ). Hiện chưa có mốc này nên `rating_stats` và `user_seen` tăng tới khi reset bằng tay. Không dùng Kappa vì log Kafka không phải nguồn gốc: 6 triệu rating lịch sử chưa từng đi qua Kafka, và Kafka chỉ giữ 7 ngày.
- **Một máy, không có restart policy hay supervisor.** Kafka 1 broker (replication 1), MongoDB standalone, Spark `local[*]`: mỗi thành phần là một điểm lỗi đơn; job streaming chạy bằng `docker compose exec` trong terminal. Khi MongoDB dừng, job dừng theo (lượt 4) và phải chạy lại bằng tay hoặc bằng script. Batch và streaming không chạy cùng lúc được (hai driver 4 GB không vừa 7,7 GiB RAM): trong 25–35 phút chạy `run_pipeline.sh`, job streaming dừng, sự kiện chờ trong Kafka (giữ 7 ngày), app phục vụ gợi ý và speed view cũ; khi chạy lại, job đọc bù tối đa 20.000 offset mỗi batch. Trên cluster: batch chạy trên cụm riêng (tạm thời), streaming chạy thường trực. Cổng MongoDB (27017) và Kafka (9094) mở ra máy host, MongoDB không xác thực, Kafka PLAINTEXT: chỉ chấp nhận được trên laptop.
- **Sự kiện là mô phỏng.** Goodbooks không có thời điểm chấm, nên `event_time` là lúc producer gửi. Đánh giá online với `producer.py` luôn ra 0 trúng (sách phát lại đã bị loại khỏi gợi ý); kế hoạch Session 8 phát lại tập test giữ riêng chưa được làm, nên app hiện "trúng / tổng" kèm giải thích thay vì một tỷ lệ.

## 7. Chạy demo

```bash
export MSYS_NO_PATHCONV=1   # Git Bash
bash scripts/reset_streaming.sh --yes
# Terminal 1
docker compose exec spark python streaming/stream_stats.py
# Terminal 2: 5% số sự kiện gửi sau giây thứ 30 là sự kiện trễ 30 phút (lượt demo: 19/1.000), để thấy watermark loại chúng; khoảng 55 giây
docker compose exec spark python streaming/producer.py --limit 1000 --late 0.05
# User 1 chấm 2 cuốn đầu trong top-10 của mình, rồi bấm "Làm mới" trong app
docker compose exec spark python scripts/rate_from_recs.py --user 1 --n 2 --rating 5
# Đếm lại Kafka và đối chiếu MongoDB (một lệnh, kết quả lưu vào reports/kafka_audit_*.json)
docker compose exec spark python scripts/kafka_audit.py | grep -E 'distinct_valid|invalid_messages|counted|expected|_match|no_loss'
```

Mở app (http://localhost:8501) để xem "Sách đang hot", và Spark UI (http://localhost:4040) → tab Structured Streaming.

Thử sự cố, lặp lại được (khoảng 3 phút): `bash scripts/stream_fault_test.sh`. Script reset, chạy job + producer (1.500 sự kiện, 3% trễ, seed 7), `kill -9` Python và JVM sau 45 giây, chạy lại từ checkpoint sau 15 giây, rồi chạy `kafka_audit.py`. Log ở `reports/streaming/fault_test_<thời điểm>/`.

Thử ngắt Kafka và MongoDB (khoảng 5 phút): `bash scripts/stream_outage_test.sh`. Script reset, chạy job + producer (3.000 sự kiện, 20 sự kiện/giây), dừng Kafka 40 giây rồi MongoDB 30 giây, chạy lại job từ checkpoint nếu nó dừng, rồi chạy `kafka_audit.py`. Log ở `reports/streaming/outage_test_<thời điểm>/` (`events.log` là dòng thời gian).

## 8. Trả lời nhanh khi bị hỏi

| Câu hỏi | Trả lời |
|---|---|
| Vì sao watermark 10 phút? | Chọn theo kịch bản, không đo (sự kiện là mô phỏng); trigger 5 giây và cửa sổ 10 / 5 phút cũng vậy. 10 phút đủ rộng cho độ trễ mạng/xử lý bình thường, mà vẫn cho Spark giải phóng trạng thái cũ. Production: đo độ trễ (thời điểm xử lý − `event_time`) và đặt watermark gần p99; kho Parquet giữ sự kiện trễ cho batch, nên watermark ngắn hơn chỉ làm số liệu tức thời kém đầy đủ hơn |
| Vì sao cửa sổ 10 phút, trượt 5 phút? | Đủ dài để có số liệu ổn định với 20 sự kiện/giây; trượt 5 phút để danh sách "hot" cập nhật thường xuyên. Mỗi sự kiện thuộc 2 cửa sổ |
| Vì sao output mode `update`? | `append` chỉ ghi cửa sổ khi đã đóng (sau watermark), tức trễ khoảng 20 phút; `update` ghi ngay giá trị mới của cửa sổ đang mở, hợp với "sách đang hot" |
| Hệ thống có exactly-once không? | Kafka + checkpoint là at-least-once. Kho Parquet là exactly-once (file sink; `archive_check` khớp ở lượt 3 và 4). `trending` idempotent (ghi đè). `rating_stats` chống phát lại bằng `last_batch`: hai lượt `kill -9` và lượt ngắt Kafka/MongoDB 0 mất, 0 trùng, nhưng còn khe hở nếu chết đúng lúc đang ghi MongoDB (các lượt `kill -9` gần như chắc chắn không thử tới khe này), và `failOnDataLoss=false` (mục 6) |
| Kafka hay MongoDB chết thì sao? | Lượt 4: Kafka dừng 40 giây thì producer giữ sự kiện trong buffer (có retry), job vẫn chạy và tự kết nối lại. MongoDB dừng 30 giây thì job dừng (`ServerSelectionTimeoutError`); chạy lại từ checkpoint thì batch chưa ghi được chạy lại, số đếm vẫn khớp. Chưa có restart policy: trong thử nghiệm script chạy lại job sau 11 giây, ngoài thử nghiệm phải chạy lại bằng tay |
| Sự kiện trễ thì sao? | Lớp nhanh bỏ (lượt 1: 26 sự kiện trễ, suy từ kafka_audit 1.474 = 1.500 − 26; lượt 2: 19; với điều kiện sự kiện trễ đến sau khi job đã có watermark); kho lưu trữ giữ lại để lần huấn luyện sau vẫn học được |
| Theo dõi hệ thống thế nào? | File progress (tốc độ vào/xử lý, thời gian batch, sự kiện trễ, bản trùng, trạng thái, watermark, `avgOffsetsBehindLatest`), tab Structured Streaming của Spark UI, và `kafka_audit.py` đối chiếu Kafka với MongoDB và kho Parquet. Lag exporter của Kafka không thấy lag của job vì Spark không commit offset vào consumer group |
| Vì sao app hiện 2 / 672 lượt trúng? | 670 lượt 4–5 sao từ producer là rating đã có trong dữ liệu huấn luyện, đã bị loại khỏi gợi ý nên không thể trúng; 2 lượt trúng là 2 cuốn user 1 chấm bằng `rate_from_recs.py` |
| Streaming có mở rộng theo executor không? | Đọc Kafka và tính toán thì có: mặc định mỗi partition Kafka một task đọc (`minPartitions` chia nhỏ thêm được), phần có trạng thái chạy trên 8 shuffle partition; nhưng chưa thử tải, và phần ghi MongoDB của `valid` và `trending` đang chạy ở driver, cần chuyển sang `foreachPartition` / connector khi tải lớn |
| Tắt Docker thì mất topic không? | `docker compose down` giữ topic (volume `kafka-data`); `down -v` xóa, sau đó chạy `reset_streaming.sh --yes` |
| Vì sao key là `user_id`? | Mọi sự kiện của một người vào cùng partition và tải chia đều. Job không cần thứ tự: thống kê theo `event_time`, `clean.py` giữ rating có `event_time` mới nhất |
