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
  (ghi ở driver)
```

Ngoài `producer.py` (phát lại rating), `scripts/rate_from_recs.py` gửi rating cho sách đang nằm trong top-10 của một người dùng (mục 3). Kafka lưu log trên volume `kafka-data`.

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
| Checkpoint và khôi phục | Mỗi query một checkpoint; đã thử dừng nhẹ (SIGINT) và **dừng đột ngột (`kill -9`)**, lặp lại được bằng `scripts/stream_fault_test.sh` |
| Idempotent | `stream_state.last_batch` bỏ qua batch bị phát lại (còn một khe hở, mục 6); `trending` ghi đè cả document (ReplaceOne) nên phát lại vẫn ra cùng kết quả; file sink exactly-once nhờ `_spark_metadata` |
| Backpressure | `maxOffsetsPerTrigger` = 20.000 (`--max-offsets`): sau khi job tắt lâu, mỗi micro-batch đọc tối đa 20.000 bản ghi thay vì dồn cả backlog vào một batch |
| Observability | `reports/stream_progress_*.json` (mục 5) và tab Structured Streaming của Spark UI |

## 3. Bằng chứng kiểm thử

Đếm độc lập bằng `scripts/kafka_audit.py`, đọc lại toàn bộ topic: tổng message, sự kiện hợp lệ khác nhau, sự kiện trễ (Kafka nhận sau `event_time` hơn 10 phút), message lỗi. Cùng lệnh đó đọc lại MongoDB và lưu khối `mongo_check` vào file report, gồm ba phép so:

- `no_loss_no_double_count`: số sự kiện hợp lệ đã đếm (`stream_state`) = hợp lệ khác nhau − trễ = tổng `n_ratings` trong `rating_stats`;
- `rejected_match`: số sự kiện lỗi đã đếm = số message lỗi trong Kafka;
- `trending_match`: tổng lượt trong các cửa sổ = 2 × (hợp lệ − trễ), vì cửa sổ 10 phút trượt 5 phút nên mỗi sự kiện đúng giờ nằm trong đúng 2 cửa sổ.

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

Kết luận: lượt `kill -9` này **0 mất, 0 trùng**, và cả 26 sự kiện trễ đều bị loại khỏi thống kê trực tiếp. Đây là kết quả đo của một lượt, không phải bảo đảm: vẫn còn khe hở khi job chết đúng lúc đang ghi MongoDB (mục 6).

### Lượt 2: chạy demo, đọc file progress

Khởi động mới, producer `--limit 1000 --late 0.05` (khoảng 52 giây), rồi `scripts/rate_from_recs.py --user 1 --n 2`. Bằng chứng: `reports/kafka_audit_20261107_221105.json`, `reports/stream_progress_20261107_220918.json`.

Kafka: 1.028 message, 1.002 sự kiện hợp lệ khác nhau (1.000 từ producer + 2 từ `rate_from_recs.py`), 19 trễ, 16 message lặp, 10 message lỗi.

| Query | Batch | Dòng đầu vào | Bị watermark loại | Bản trùng bị bỏ | Dòng trạng thái |
|---|---|---|---|---|---|
| `valid` | 16 | 1.028 | **20** | **15** | 983 |
| `trending` | 17 | 1.028 | **20** | **15** | 2.624 |
| `events_to_parquet` | 14 | 1.028 | 0 | 0 | 0 (không trạng thái) |

20 dòng bị watermark loại = 19 sự kiện trễ + 1 bản gửi lại của một sự kiện trễ (bản này vì thế không còn nằm trong số 15 bản trùng bị bỏ). Cả `valid` lẫn `trending` loại hết sự kiện trễ dù khởi động mới, vì sự kiện trễ chỉ đến sau giây thứ 30. MongoDB: **983** = 1.002 − 19 sự kiện hợp lệ, tổng cửa sổ 1.966 = 2 × 983, **10** sự kiện lỗi; cả ba phép so `true`.

Counter trong file progress là số đếm theo lần chạy và theo từng query, nên có thể lệch một hai dòng so với Kafka (bản trùng của sự kiện trễ, batch bị phát lại sau khi sập). Số liệu dữ liệu được kiểm bằng `mongo_check`, không bằng counter.

### Kho lưu trữ cho lần huấn luyện sau (06/11, `clean_20261106_122043.json`)

| | Số dòng |
|---|---|
| Dòng trong kho Parquet (mọi sự kiện hợp lệ, gồm cả trùng và trễ) | 4.901 |
| Sau khi `clean.py` bỏ trùng theo `event_id` | 4.805 (bỏ 96 bản trùng) |
| Sự kiện trễ **được giữ lại** cho huấn luyện (lớp nhanh đã bỏ) | 56 |
| Cặp (user, book) mới vào dữ liệu huấn luyện | 5 |

### Sách vừa chấm biến khỏi top-10 (`scripts/rate_from_recs.py`)

`producer.py` phát lại cặp (user, sách) lấy từ `ratings.csv`, tức dữ liệu huấn luyện, mà `write_recs.py` đã loại mọi cặp đó khỏi gợi ý và danh sách bù. Vì vậy sự kiện phát lại **không bao giờ** làm đổi top-10 đang hiện, và số lượt trúng online luôn là 0. Để thấy app phản ứng, `rate_from_recs.py` đóng vai người dùng chọn sách ngay trong danh sách của mình: đọc top-10 hiện tại (collection gợi ý đang active trừ `user_seen`) rồi gửi rating cho `--n` cuốn đầu.

Lượt 08/11: user 1 chấm 5 sao cuốn #1 *The Guernsey Literary and Potato Peel Pie Society* (195) và #2 *The Great Gatsby* (5). Sau một trigger, cả hai bị ẩn, *The Catcher in the Rye* (8) lên #1, hai cuốn mới vào #9–10 (*The Immortal Life of Henrietta Lacks*, 208; *The Da Vinci Code*, 26). Chỉ số "Lượt 4–5 sao trúng top-10" hiện **2 / 672**: 2 lượt trúng là 2 sự kiện này; 670 lượt còn lại từ producer không thể trúng. Ảnh: `docs/img/app_after_rate_from_recs.png`.

### Ảnh chụp (`reports/streaming/`)

- `spark_ui_streaming_queries.png`: 4 query có tên, đang chạy.
- `spark_ui_trending_statistics.png`: query `trending`: tốc độ đầu vào khoảng 20 sự kiện/giây, tốc độ xử lý, thời gian mỗi batch (batch đầu khoảng 12 giây, các batch sau khoảng 2,8–6,1 giây), khoảng cách watermark khoảng 600 giây (đúng 10 phút), số dòng trạng thái tăng dần.
- `app_trending.png`: cả trang app sau lượt 2: 983 sự kiện hợp lệ, 10 sự kiện lỗi, 2 / 672 lượt trúng và mục "Sách đang hot".

## 4. Phát hiện khi kiểm thử và cách sửa

**Watermark là riêng cho từng query, và chỉ cập nhật giữa các batch.** Ở lượt 1, query lưu Parquet chưa kịp hoàn tất batch nào trước khi bị `kill -9`, nên khi chạy lại, batch đầu tiên vẫn dùng watermark cũ (ngày 05/11), và **12 sự kiện trễ lọt qua**.

Phát hiện này cho thấy một vấn đề thiết kế. Thiết kế cũ cho kho lưu trữ dùng chung bước bỏ trùng có watermark, nên:
- rating trễ hơn 10 phút bị mất khỏi dữ liệu huấn luyện dù là rating thật;
- kết quả còn phụ thuộc vào thời điểm restart.

**Cách sửa (theo kiến trúc Lambda):**
- **Lớp xử lý nhanh** (`valid`, `trending`) giữ watermark để giới hạn bộ nhớ trạng thái; bỏ sự kiện trễ là đánh đổi chấp nhận được cho số liệu "trực tiếp".
- **Lớp batch** (`events_to_parquet`) lưu mọi sự kiện hợp lệ, không trạng thái; `clean.py` bỏ trùng chính xác theo `event_id`. Không mất rating nào, và kết quả không phụ thuộc vào thời điểm restart.

Kho cũ được giữ lại ở `data/parquet/stream_events_v1_deduped`; kho mới dựng lại từ toàn bộ topic Kafka và khớp đúng số liệu Kafka (06/11).

**Watermark chưa có ở 1–2 batch đầu.** Trong Spark 3.5, query có trạng thái lọc sự kiện trễ bằng watermark của batch **trước**. Sau khi khởi động mới (checkpoint trống), batch 0 và 1 chưa có watermark nên chưa lọc gì. Lần tập 07/11, producer bắt đầu gửi sự kiện trễ sau 200 sự kiện (khoảng 10 giây), lúc batch đầu còn đang chạy trên JVM lạnh: `trending` chỉ loại 9 trên 13 dòng trễ mà `valid` loại (`reports/superseded/rehearsal_20261107/`). Đây là đặc tính của Spark, không phải lỗi của job. Cách sửa: producer chỉ gửi sự kiện trễ sau `--late-after-seconds` (mặc định 30 giây, thay cho `--late-after` đếm theo số sự kiện). Với cách này, hai lượt 08/11 đều loại hết sự kiện trễ (26/26 và 19/19).

**Topic Kafka từng không được lưu bền.** Trước đây Kafka không có volume, topic nằm trong container, nên `docker compose down` xóa topic trong khi checkpoint (volume khác) vẫn còn; khi chạy lại, offset trong checkpoint lớn hơn topic mới và `failOnDataLoss=false` chỉ ghi cảnh báo. Nay Kafka có volume `kafka-data` (`KAFKA_LOG_DIRS=/var/lib/kafka/data`): `down` giữ topic; `down -v` xóa mọi volume, sau đó phải chạy `bash scripts/reset_streaming.sh --yes` (xóa luôn `data/parquet/stream_events`, nằm trên thư mục máy, ngoài volume; nếu để lại, `_spark_metadata` cũ làm query lưu trữ bỏ qua batch mới).

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
| `last_progress` | Bản đầy đủ của batch cuối (offset Kafka, thời gian từng bước) |

## 6. Giới hạn và vấn đề hiệu năng

- **Thời gian batch sát trigger.** Batch đầu sau khi khởi động mất 7–12 giây (JVM còn lạnh); các batch sau thường 3–6 giây, sát trigger 5 giây (ảnh Spark UI: khoảng 2,8–6,1 giây sau batch đầu khoảng 12 giây; `max_batch_duration_ms` các lượt 08/11: 6,5–10,6 giây). Nguyên nhân: 4 query chạy song song và **mỗi query tự đọc Kafka riêng** trên 4 core. Ở 20 sự kiện/giây hệ thống vẫn theo kịp, nhưng nếu tốc độ cao hơn sẽ tụt lại. Hướng cải thiện: gộp các nhánh vào một query `foreachBatch` ghi ra nhiều nơi, tăng trigger, hoặc thêm core/executor.
- **Ghi MongoDB chạy ở driver.** `process_valid` gom mỗi micro-batch về driver (`collect`) rồi ghi bằng một client MongoDB (mỗi tiến trình một client). Đủ cho tốc độ sự kiện của đồ án; `maxOffsetsPerTrigger` 20.000 giới hạn kích thước một batch sau khi job tắt lâu. Thêm executor **không** làm phần ghi này nhanh hơn; ở quy mô lớn cần chuyển việc ghi sang executor (`foreachPartition`, hoặc MongoDB Spark connector) và gộp theo sách trong Spark trước khi ghi.
- **Exactly-once có giới hạn: có thể cộng lặp một phần nếu job chết đúng lúc đang ghi MongoDB.** `rating_stats` (pipeline `$add`) và `online_eval` (`$inc`) cộng dồn trước khi ghi `last_batch`, không nằm trong transaction; nếu job chết giữa các bước đó, batch phát lại có thể cộng lặp một phần. Lượt `kill -9` đã thử không rơi vào khoảnh khắc này (0 mất, 0 trùng). Cách sửa triệt để: transaction (cần replica set) hoặc ghi idempotent theo `batch_id`. `trending` (ReplaceOne) và kho Parquet (file sink) không gặp vấn đề này. `last_batch` cũng dựa vào số batch của checkpoint, nên checkpoint và `stream_state` phải được xóa cùng nhau (`reset_streaming.sh`).
- **Counter trong file progress có thể lệch** vài dòng so với Kafka: batch bị phát lại sau khi sập được đếm lại, và bản gửi lại của một sự kiện trễ được tính là "bị watermark loại". Số liệu dữ liệu vẫn đúng (`mongo_check`), chỉ counter đếm theo lần chạy.
- **Lớp nhanh không gộp với lớp batch.** `rating_stats` chỉ chứa rating từ streaming kể từ lần reset; thống kê theo sách của MapReduce / Spark SQL không được nạp vào MongoDB. Cột "Điểm TB (stream)" của app vì vậy có thể là 5,0 từ một lượt chấm duy nhất.
- **Một máy.** Kafka 1 broker (replication 1), MongoDB standalone, Spark `local[*]`: mỗi thành phần là một điểm lỗi đơn; job streaming chạy bằng `docker compose exec` trong terminal, không có restart policy.
- **Sự kiện là mô phỏng.** Goodbooks không có thời điểm chấm, nên `event_time` là lúc producer gửi. Đánh giá online với `producer.py` luôn ra 0 trúng (sách phát lại đã bị loại khỏi gợi ý); kế hoạch Session 8 phát lại tập test giữ riêng chưa được làm, nên app hiện "trúng / tổng" kèm giải thích thay vì một tỷ lệ.

## 7. Chạy demo

```bash
export MSYS_NO_PATHCONV=1   # Git Bash
bash scripts/reset_streaming.sh --yes
# Terminal 1
docker compose exec spark python streaming/stream_stats.py
# Terminal 2: 5% sự kiện trễ 30 phút (gửi sau giây thứ 30) để thấy watermark loại chúng; khoảng 55 giây
docker compose exec spark python streaming/producer.py --limit 1000 --late 0.05
# User 1 chấm 2 cuốn đầu trong top-10 của mình, rồi bấm "Làm mới" trong app
docker compose exec spark python scripts/rate_from_recs.py --user 1 --n 2 --rating 5
# Đếm lại Kafka và đối chiếu MongoDB (một lệnh, kết quả lưu vào reports/kafka_audit_*.json)
docker compose exec spark python scripts/kafka_audit.py | grep -E 'distinct_valid|invalid_messages|counted|expected|_match|no_loss'
```

Mở app (http://localhost:8501) để xem "Sách đang hot", và Spark UI (http://localhost:4040) → tab Structured Streaming.

Thử sự cố, lặp lại được (khoảng 3 phút): `bash scripts/stream_fault_test.sh`. Script reset, chạy job + producer (1.500 sự kiện, 3% trễ, seed 7), `kill -9` Python và JVM sau 45 giây, chạy lại từ checkpoint sau 15 giây, rồi chạy `kafka_audit.py`. Log ở `reports/streaming/fault_test_<thời điểm>/`.

## 8. Trả lời nhanh khi bị hỏi

| Câu hỏi | Trả lời |
|---|---|
| Vì sao watermark 10 phút? | Sự kiện đến trễ chủ yếu do độ trễ mạng/xử lý, vài giây. 10 phút đủ rộng để không mất sự kiện bình thường, mà vẫn cho Spark giải phóng trạng thái cũ, nên bộ nhớ không tăng mãi |
| Vì sao cửa sổ 10 phút, trượt 5 phút? | Đủ dài để có số liệu ổn định với 20 sự kiện/giây; trượt 5 phút để danh sách "hot" cập nhật thường xuyên. Mỗi sự kiện thuộc 2 cửa sổ |
| Vì sao output mode `update`? | `append` chỉ ghi cửa sổ khi đã đóng (sau watermark), tức trễ khoảng 20 phút; `update` ghi ngay giá trị mới của cửa sổ đang mở, hợp với "sách đang hot" |
| Hệ thống có exactly-once không? | Kafka + checkpoint là at-least-once. Kho Parquet là exactly-once (file sink). `trending` idempotent (ghi đè). `rating_stats` chống phát lại bằng `last_batch`: lượt `kill -9` 0 mất, 0 trùng, nhưng còn khe hở nếu chết đúng lúc đang ghi MongoDB (mục 6) |
| Sự kiện trễ thì sao? | Lớp nhanh bỏ (26/26 và 19/19 ở hai lượt 08/11, với điều kiện sự kiện trễ đến sau khi job đã có watermark); kho lưu trữ giữ lại để lần huấn luyện sau vẫn học được |
| Theo dõi hệ thống thế nào? | File progress (tốc độ vào/xử lý, thời gian batch, sự kiện trễ, bản trùng, trạng thái, watermark), tab Structured Streaming của Spark UI, và `kafka_audit.py` đối chiếu Kafka với MongoDB |
| Vì sao app hiện 2 / 672 lượt trúng? | 670 lượt 4–5 sao từ producer là rating đã có trong dữ liệu huấn luyện, đã bị loại khỏi gợi ý nên không thể trúng; 2 lượt trúng là 2 cuốn user 1 chấm bằng `rate_from_recs.py` |
| Streaming có mở rộng theo executor không? | Đọc Kafka và tính toán thì có; phần ghi MongoDB của `valid` đang chạy ở driver, cần chuyển sang `foreachPartition` / connector khi tải lớn |
| Tắt Docker thì mất topic không? | `docker compose down` giữ topic (volume `kafka-data`); `down -v` xóa, sau đó chạy `reset_streaming.sh --yes` |
