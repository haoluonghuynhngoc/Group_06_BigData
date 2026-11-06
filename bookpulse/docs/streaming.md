# Streaming trong BookPulse: thiết kế và bằng chứng

Tài liệu này tổng hợp phần streaming: kiến trúc, cách áp dụng từng khái niệm của môn học (Session 7), số đo kiểm chứng và các giới hạn. Mọi số liệu lấy từ `reports/` (chạy ngày 06/11/2026, dữ liệu Goodbooks-10k phát lại thành sự kiện).

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
```

| Query | Kiểu | Output mode / sink | Có trạng thái? |
|---|---|---|---|
| `valid` | Lọc hợp lệ → watermark 10 phút → `dropDuplicatesWithinWatermark(event_id)` | `foreachBatch` → MongoDB | Có (trạng thái bỏ trùng) |
| `rejected` | Lọc sự kiện lỗi | `foreachBatch` → MongoDB | Không |
| `events_to_parquet` | Lọc hợp lệ, **không** watermark, **không** bỏ trùng | File sink Parquet, append, chia theo `event_date` | Không |
| `trending` | Cùng nhánh `valid` → `groupBy(window(event_time, 10 phút, 5 phút), book_id)` | `update` + `foreachBatch` → MongoDB | Có (bỏ trùng + cửa sổ) |

Mỗi query có **checkpoint riêng** trong `checkpoints/` (`stream_valid`, `stream_rejected`, `stream_events_archive`, `stream_trending`).

## 2. Đối chiếu với các khái niệm của môn học

| Khái niệm | Áp dụng trong BookPulse |
|---|---|
| Kafka pub/sub, partition, partition key | Topic `rating-events` 3 partition; key = `user_id`, nên mọi sự kiện của một người vào cùng partition và giữ thứ tự. Job tự tạo topic nếu chưa có |
| DataFrame API, schema tường minh | `SCHEMA` (6 trường) + `from_json`; sự kiện không đọc được hoặc thiếu trường bị chuyển sang nhánh `rejected` |
| Source / sink | Nguồn Kafka; sink: `foreachBatch` (MongoDB) và file sink (Parquet) |
| Event time và processing time | Thống kê và cửa sổ dựa trên `event_time` (thời điểm rating xảy ra), không dựa trên lúc Spark xử lý |
| Watermark | 10 phút: sự kiện có `event_time` cũ hơn (event time lớn nhất đã thấy − 10 phút) bị loại khỏi các phép tính có trạng thái, và trạng thái cũ được giải phóng |
| Cửa sổ có trạng thái | Cửa sổ trượt 10 phút / 5 phút theo sách: số lượt chấm, điểm TB, số lượt 4–5 sao |
| Checkpoint và khôi phục | Mỗi query một checkpoint; đã thử dừng nhẹ (SIGINT) và **dừng đột ngột (`kill -9`)** |
| Idempotent | `stream_state.last_batch` bỏ qua batch bị phát lại; `trending` ghi đè cả document nên phát lại vẫn ra cùng kết quả; file sink exactly-once nhờ `_spark_metadata` |
| Observability | `reports/stream_progress_*.json` (mục 5) và tab Structured Streaming của Spark UI |

## 3. Bằng chứng kiểm thử

Đếm độc lập bằng `scripts/kafka_audit.py`, đọc lại toàn bộ topic: tổng message, sự kiện hợp lệ khác nhau, sự kiện trễ (Kafka nhận sau `event_time` hơn 10 phút), message lỗi.

### Lượt 1: sự kiện trễ + `kill -9` giữa chừng

Producer gửi 1.500 sự kiện, trong đó 32 bản trùng, 15 sự kiện lỗi và **43 sự kiện trễ 30 phút**. Khoảng 40 giây sau khi bắt đầu, nhóm `kill -9` cả tiến trình Python lẫn JVM; 15 giây sau chạy lại từ checkpoint.

| Kiểm tra | Kỳ vọng (từ Kafka) | Thực tế | |
|---|---|---|---|
| Sự kiện hợp lệ được tính (`stream_state`) | 3.805 khác nhau − 43 trễ = 3.762 | 3.762 | ✅ |
| Tổng `n_ratings` trong `rating_stats` | 3.762 | 3.762 | ✅ |
| Sự kiện lỗi bị loại | 42 | 42 | ✅ |
| Tổng lượt trong các cửa sổ từ lúc test | 2 × (1.500 − 43) = 2.914 (mỗi sự kiện thuộc 2 cửa sổ trượt) | 2.914 | ✅ |

Kết luận: qua một lần sập đột ngột, **không mất, không đếm trùng**, và **cả 43 sự kiện trễ đều bị loại** khỏi thống kê trực tiếp.

### Lượt 2: chạy demo, đọc file progress

Producer gửi 1.000 sự kiện: 19 bản trùng, 4 sự kiện lỗi, **13 sự kiện trễ** (`reports/stream_progress_20261106_121739.json`).

| Query | Batch | Dòng đầu vào | Bị watermark loại | Bản trùng bị bỏ | Dòng trạng thái |
|---|---|---|---|---|---|
| `valid` | 14 | 1.023 | **13** | **19** | 2.444 |
| `trending` | 12 | 1.023 | **13** | **19** | 6.285 |
| `events_to_parquet` | 12 | 1.023 | 0 | 0 | 0 (không trạng thái) |

Số bị watermark loại và số bản trùng bị bỏ **khớp đúng** với số producer đã gửi. MongoDB: 3.762 + (1.000 − 13) = **4.749** sự kiện hợp lệ; 42 + 4 = **46** sự kiện lỗi.

### Kho lưu trữ cho lần huấn luyện sau (`clean_20261106_122043.json`)

| | Số dòng |
|---|---|
| Dòng trong kho Parquet (mọi sự kiện hợp lệ, gồm cả trùng và trễ) | 4.901 |
| Sau khi `clean.py` bỏ trùng theo `event_id` | 4.805 (bỏ 96 bản trùng) |
| Sự kiện trễ **được giữ lại** cho huấn luyện (lớp nhanh đã bỏ) | 56 |
| Cặp (user, book) mới vào dữ liệu huấn luyện | 5 |

### Ảnh chụp (`reports/streaming/`)

- `spark_ui_streaming_queries.png`: 4 query có tên, đang chạy.
- `spark_ui_trending_statistics.png`: query `trending`: tốc độ đầu vào khoảng 20 sự kiện/giây, tốc độ xử lý, thời gian mỗi batch, khoảng cách watermark khoảng 600 giây (đúng 10 phút), số dòng trạng thái tăng dần.
- `app_trending.png`: app với mục "Sách đang hot".

## 4. Phát hiện khi kiểm thử và cách sửa

**Watermark là riêng cho từng query, và chỉ cập nhật giữa các batch.** Ở lượt 1, query lưu Parquet chưa kịp hoàn tất batch nào trước khi bị `kill -9`, nên khi chạy lại, batch đầu tiên vẫn dùng watermark cũ (ngày 05/11), và **12 sự kiện trễ lọt qua**.

Phát hiện này cho thấy một vấn đề thiết kế. Thiết kế cũ cho kho lưu trữ dùng chung bước bỏ trùng có watermark, nên:
- rating trễ hơn 10 phút bị mất khỏi dữ liệu huấn luyện dù là rating thật;
- kết quả còn phụ thuộc vào thời điểm restart.

**Cách sửa (theo kiến trúc Lambda):**
- **Lớp xử lý nhanh** (`valid`, `trending`) giữ watermark để giới hạn bộ nhớ trạng thái; bỏ sự kiện trễ là đánh đổi chấp nhận được cho số liệu "trực tiếp".
- **Lớp batch** (`events_to_parquet`) lưu mọi sự kiện hợp lệ, không trạng thái; `clean.py` bỏ trùng chính xác theo `event_id`. Không mất rating nào, và kết quả không phụ thuộc vào thời điểm restart.

Kho cũ được giữ lại ở `data/parquet/stream_events_v1_deduped`; kho mới dựng lại từ toàn bộ topic Kafka và khớp đúng số liệu Kafka (mục 3).

## 5. Observability: file progress

`stream_stats.py` gom `StreamingQueryProgress` của từng query theo batch và ghi `reports/stream_progress_<thời điểm>.json` mỗi 10 giây và khi dừng. Mỗi query có:

| Trường | Ý nghĩa |
|---|---|
| `batches`, `input_rows` | Số micro-batch và số dòng đã đọc từ Kafka |
| `max_input_rows_per_second`, `max_processed_rows_per_second` | Tốc độ vào và tốc độ xử lý; xử lý thấp hơn vào kéo dài nghĩa là đang tụt lại |
| `max_batch_duration_ms` | Thời gian lâu nhất của một batch, để so với trigger 5 giây |
| `rows_dropped_by_watermark`, `duplicates_dropped` | Sự kiện trễ bị loại, bản trùng bị bỏ |
| `state_rows_now`, `watermark_now` | Kích thước trạng thái và watermark hiện tại |
| `last_progress` | Bản đầy đủ của batch cuối (offset Kafka, thời gian từng bước) |

## 6. Giới hạn và vấn đề hiệu năng

- **Thời gian batch dài hơn trigger.** Batch mất 7–14 giây trong khi trigger là 5 giây, vì 4 query chạy song song và **mỗi query tự đọc Kafka riêng** trên 4 core. Ở 20 sự kiện/giây hệ thống vẫn theo kịp, nhưng nếu tốc độ cao hơn sẽ tụt lại. Hướng cải thiện: gộp các nhánh vào một query `foreachBatch` ghi ra nhiều nơi, tăng trigger, hoặc thêm core/executor.
- **Có thể cộng lặp một phần nếu job chết đúng lúc đang ghi MongoDB.** `rating_stats` dùng `$inc`, nên nếu job chết sau khi ghi mà trước khi lưu `last_batch`, batch đó có thể bị cộng hai lần. Lần `kill -9` đã thử không rơi vào khoảnh khắc này. Cách sửa triệt để: transaction (cần replica set) hoặc ghi theo `batch_id`. `trending` và kho Parquet không gặp vấn đề này.
- **Counter `numRowsDroppedByWatermark` có thể đếm lại** batch bị phát lại sau khi sập (lượt 1 ghi nhận 46 trong khi có 43 sự kiện trễ). Số liệu dữ liệu vẫn đúng, chỉ counter đếm theo lần chạy.
- **Sự kiện là mô phỏng.** Goodbooks không có thời điểm chấm, nên `event_time` là lúc producer gửi; đánh giá online vì vậy chưa có ý nghĩa.

## 7. Chạy demo

```bash
# Terminal 1
docker compose exec spark python streaming/stream_stats.py
# Terminal 2: 3% sự kiện trễ 30 phút để thấy watermark loại chúng
docker compose exec spark python streaming/producer.py --limit 1000 --late 0.03
# Sau đó: đếm lại Kafka để đối chiếu
docker compose exec spark python scripts/kafka_audit.py
```

Mở app (http://localhost:8501) để xem "Sách đang hot", và Spark UI (http://localhost:4040) → tab Structured Streaming.

## 8. Trả lời nhanh khi bị hỏi

| Câu hỏi | Trả lời |
|---|---|
| Vì sao watermark 10 phút? | Sự kiện đến trễ chủ yếu do độ trễ mạng/xử lý, vài giây. 10 phút đủ rộng để không mất sự kiện bình thường, mà vẫn cho Spark giải phóng trạng thái cũ, nên bộ nhớ không tăng mãi |
| Vì sao cửa sổ 10 phút, trượt 5 phút? | Đủ dài để có số liệu ổn định với 20 sự kiện/giây; trượt 5 phút để danh sách "hot" cập nhật thường xuyên. Mỗi sự kiện thuộc 2 cửa sổ |
| Vì sao output mode `update`? | `append` chỉ ghi cửa sổ khi đã đóng (sau watermark), tức trễ khoảng 20 phút; `update` ghi ngay giá trị mới của cửa sổ đang mở, hợp với "sách đang hot" |
| Hệ thống có exactly-once không? | Kafka + checkpoint là at-least-once. Kho Parquet là exactly-once (file sink). `trending` idempotent (ghi đè). `rating_stats` chống phát lại bằng `last_batch`, chỉ còn rủi ro nếu chết đúng lúc đang ghi (mục 6) |
| Sự kiện trễ thì sao? | Lớp nhanh bỏ (đếm được trong file progress); kho lưu trữ giữ lại để lần huấn luyện sau vẫn học được |
| Theo dõi hệ thống thế nào? | File progress (tốc độ vào/xử lý, thời gian batch, sự kiện trễ, bản trùng, trạng thái, watermark) và tab Structured Streaming của Spark UI |
