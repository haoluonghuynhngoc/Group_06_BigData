# H. Card G: BookPulse Goodreads Book Recommendation (Session 9, mục 7)

**Card result: [x] Verified (G1, G2, G3 đều chạy được, có số đo) · [ ] Failed / corrected · [ ] Design-only**
Evidence files: `E_model_validation.json`, `G1_implicit_semantics.json`, `G3_fresh_rating_trace.json`, `G3_kafka_audit.json`, `G3_stream_job.log`, `G3_trace.log`, `G3_run.log`; script: `scripts/session9/ranking_eval.py`, `scripts/session9/fresh_rating_trace.py`, `scripts/session9/run_g3.sh`.

## Kiểm lại "project context" của card so với code

| Card nói | Thực tế trong repo |
|---|---|
| Goodreads-10k ratings (~6M explicit 1–5 stars) | Đúng: 5.976.479 rating, 53.424 user, 10.000 sách |
| batch Spark SQL/ALS | Đúng: `batch/analytics_sql.py`, `batch/train_als.py` (explicit **và** implicit; app dùng implicit) |
| Hadoop Streaming comparison | Đúng: `mapreduce/` chạy trên HDFS + YARN, so với Spark SQL bằng `exceptAll` |
| Kafka replay | Đúng, và còn thêm dữ liệu **giả** (cặp chưa có trong dataset) + nút "Chấm sao" của app |
| Structured Streaming statistics | Đúng: 4 query (valid, trending, rejected, archive) |
| MongoDB và Streamlit | Đúng |
| "Implicit ALS on explicit ratings" là rủi ro chính | Đúng là cách nhóm làm: `implicitPrefs=True`, rating 1–5 làm confidence. Đã đo ảnh hưởng (G1) |
| "timely relevance" | Nhóm **không** claim gợi ý real-time; streaming chỉ ẩn sách và cập nhật thống kê (G3) |

## Trả lời 7 câu chất vấn

**1. ALS mode: 1 sao vẫn là tín hiệu dương?** Đúng. Spark `ALS(implicitPrefs=True, alpha=1.0, rank=50, regParam=0.1, maxIter=10, nonnegative=True)` đặt p = 1 cho mọi cặp đã chấm và c = 1 + α·r: 1 sao → c = 2, 5 sao → c = 6 (bảng ở G1). Không có biến đổi nào đưa 1–2 sao về phía "không thích". Nhóm biết và đã đo thay thế (G1): bỏ 1–3 sao khỏi tín hiệu cho NDCG@10 0,2356 so với 0,2348, tức không đổi chất lượng đo được, nhưng đổi 5/10 cuốn trong danh sách. Lý do chấp nhận scheme hiện tại: 1–2 sao chỉ 8,1 % dữ liệu, trọng số 2–3 so với 5–6; và việc **đã đọc** (dù chê) vẫn là tín hiệu về gu (người đọc Twilight 1 sao vẫn thuộc nhóm đọc YA). Cấu hình ghi trong `train_als_implicit_20261106_125816.json` (`best_params`) và `G1_implicit_semantics.json` (`serving_configuration`).

**2. Metric fit.** Relevance = sách 4–5 sao trong tập test; candidate universe = 10.000 sách trừ sách user đã chấm trong train (seen-item exclusion; ALS chấm 200 ứng viên rồi lấy 10 chưa đọc tốt nhất); mẫu số = user mẫu có ≥ 1 sách liên quan trong test (4.995/5.000). Kết quả (13/11): ALS implicit P@10 0,2008, R@10 0,1330, **NDCG@10 0,2348**; popularity 0,0777 / 0,0511 / 0,0881; explicit 0,0078 / 0,0063 / 0,0074. RMSE chỉ còn dùng để báo cáo explicit (0,828 so với baseline 0,955). Chi tiết `E_model_validation.md`.

**3. Timestamp availability.** Goodbooks-10k `ratings.csv` **không có** timestamp. Nhóm **không** claim temporal split hay chronology: split ngẫu nhiên seed 42, ghi rõ trong báo cáo mục 6.2, 12 và Q&A 39. `event_time` của sự kiện streaming là lúc producer gửi (gán khi mô phỏng), chỉ dùng cho cửa sổ/watermark của lớp tốc độ, không dùng để chia train/test (rating stream luôn vào train).

**4. Batch vs speed: gì đổi trong 5 giây, gì cần retrain.** Trong 1–2 trigger (đo G3: 2,0 s): `user_seen` (+1 cặp) → app **ẩn** cuốn vừa chấm và dồn cuốn #11 từ 30 gợi ý đã lưu lên; `rating_stats` (n, avg của cuốn đó); `trending` (cửa sổ 10/5 phút); `online_eval` (+1 liked, +1 hit nếu cuốn nằm trong top-10 đã lưu). **Không đổi**: factor ALS, điểm của 30 gợi ý, `meta.active` (`als_recommendations_unchanged_until_retrain: true`). Để top-10 thật sự được tính lại phải chạy `clean.py` (gộp kho `stream_events`, dedup `event_id`, `from_stream=true`) → `train_als.py --implicit` (rating stream luôn trong train) → `write_recs.py` (collection mới, chuyển `meta.active`); từ 15/11 là nút "Train lại" (`scripts/retrain.py`, 164–179 s): đo 10/11 (Q&A 36): user 16470 chấm Harry Potter 1–5 → sau retrain nhận tập 6, 7 ở #1–#2.

**5. Replay idempotency.** `event_id = uuid4().hex` sinh **một lần** ở producer/app và giữ nguyên qua retry (`retries=10`); watermark scope = query `valid` và `trending` (10 phút trên `event_time`, state dedup riêng mỗi query); sau Kafka restart: Spark đọc tiếp từ offset trong checkpoint (`offsets/N` WAL), batch có offsets mà chưa commits chạy lại cùng id → `stream_state.last_batch` bỏ qua phần MongoDB đã ghi, `trending` ReplaceOne, `user_seen` upsert theo `"user_book"`; kho Parquet exactly-once nhờ `_spark_metadata`, **giữ cả bản trùng** (G3: 2 dòng cho 1 event_id), và `clean.py` `dropDuplicates(event_id)` rồi `row_number` 1 rating/cặp ưu tiên `event_time` mới nhất trước khi vào ALS batch. Đo: G3 gửi cùng event_id 2 lần → `rating_stats` +1, `valid_events_counted` 1, `kafka_audit` `messages 2, distinct 1, no_loss_no_double_count true, exactly_once true`. Giới hạn: khe hở ms giữa `bulk_write` và `mark_applied`; `failOnDataLoss=false`.

**6. Cold start.** *User mới* (không có document): app hiện `meta.popular` (100 sách phổ biến nhất, bỏ sách trong `user_seen`); sau khi user đó chấm vài cuốn qua Kafka, lần batch sau ALS có factor cho user. *Sách mới* (không có rating): không bao giờ vào ALS; không có fallback theo nội dung/thể loại (chưa dùng `books.csv` metadata cho gợi ý) → chỉ được gợi ý sau khi có rating và retrain. *Hết danh sách*: `fallback` 10 sách phổ biến chưa chấm của riêng user, rồi `meta.popular`. Đây là giới hạn đã ghi (Q&A K13, 38).

**7. MapReduce count comparisons.** Workload rõ: key = `book_id`, value = vector `(count, sum, n1..n5)`; combiner cộng vector (kết hợp, giao hoán; **không** cộng trung bình); reducer tính avg một lần; 2 reducer HashPartitioner; HDFS 3 block 32 MB → 3 split. Counter: Map output 5.976.479 → Combine output 47.087 (−99,2 %), Reduce shuffle 1,2 MB. Spark SQL reference trên **cùng file** (`verify_with_spark.py`): 0/0 dòng lệch trên 10.000 sách, avg lệch ≤ 0,00005 (làm tròn). Nói rõ: measured trên toàn bộ dataset, nhưng 1 DataNode, map task rack-local, 52–134 s.

## Mini-tests

### G1 – implicit semantics (MEASURED, 13/11, `G1_implicit_semantics.json`)

Transform table và cấu hình: xem câu 1 và `E_model_validation.md`. So sánh trên **cùng** 4.995 user test:

| Scheme | Dòng train | Fit | P@10 | R@10 | NDCG@10 | Đầu sách |
|---|---|---|---|---|---|---|
| Hiện tại: mọi rating, c = 1 + r | 4.780.731 | ~138 s (06/11) | 0,2008 | 0,1330 | 0,2348 | 1.208 |
| Thay thế: chỉ rating ≥ 4 | 3.297.772 | 47,6 s | 0,1995 | 0,1320 | 0,2356 | 1.086 |

Top-10 trung bình trùng 5,0/10 cuốn; 28/5.000 user trùng hết. User ứng viên 1 (89 rating train, 8 lượt 1–2 sao): 6/10 trùng; scheme hiện tại xếp Life of Pi, The Red Tent, The Great Gatsby, The Catcher in the Rye, The Joy Luck Club…; liked-only xếp The Guernsey Literary…, The Red Tent, The Diary of a Young Girl, Cutting for Stone, The Great Gatsby, Life of Pi, Angels & Demons, The Other Boleyn Girl, A Tree Grows in Brooklyn, Unbroken. **Thay đổi định tính**: liked-only đẩy các cuốn "được thích rộng rãi" lên, bỏ The Da Vinci Code (user 1 thuộc nhóm có nhiều lượt chê); không có scheme nào thắng rõ. Quyết định: giữ scheme hiện tại, ghi rõ hạn chế; nếu muốn đổi phải retrain và đổi số báo cáo.

### G2 – ranking test (MEASURED, 13/11, `E_model_validation.json`)

Split seed 42 (4.780.731 / 1.195.748); relevance 4–5 sao test; 4.995 user eligible / 5.000 mẫu; ALS implicit NDCG@10 **0,2348** so với popularity **0,0881** (2,67×), P@10 0,2008 so với 0,0777, R@10 0,1330 so với 0,0511; 3.938/4.995 user có ≥ 1 hit (popularity 2.179). Không temporal split (không có timestamp). Chi tiết `E_model_validation.md`.

### G3 – fresh rating (MEASURED, 13/11, `G3_fresh_rating_trace.json`, `G3_kafka_audit.json`)

| Bước | Kết quả |
|---|---|
| Event | `event_id e7b82b7f11634e3281b08cd8429e1fef`, user 1, book 5 *The Great Gatsby* (#1 trong top-10 hiển thị), 5 sao, `event_time 2026-11-13 12:07:09.495`, gửi **2 lần** (partition 0, offset 0 và 1) |
| Trước (MongoDB) | `meta.active = recommendations_20261110_212851`; user_seen của user 1: 0; rating_stats[5]: null; valid_events 0 |
| Kafka → MongoDB | `user_seen "1_5"` xuất hiện sau **2,0 s** (gửi 12:07:09.6 → thấy 12:07:11.65); log job: `[valid] batch 1: 2 rows, duplicates dropped 1` |
| Sau | rating_stats[5] = {n 1, avg 5,0} (không phải 2); valid_events 1; online_eval liked 1, hits 1; user_seen 1 |
| UI (theo đúng quy tắc app) | Trước: #1 Gatsby, #2 Catcher in the Rye, #3 Joy Luck Club … #10 Anna Karenina. Sau: Gatsby **ẩn**, Catcher lên #1, … #10 mới = The Other Boleyn Girl (cuốn #11 trong 30 đã lưu) |
| Kho Parquet | 2 dòng với event_id này (archive giữ bản lặp); `kafka_audit`: messages 2, distinct 1, repeated 1, `no_loss_no_double_count true`, `exactly_once true` |
| Khi nào ALS đổi | `meta.active`/`model_version` **không đổi** (20261110_212851). Chỉ đổi sau `clean.py → train_als.py --implicit → write_recs.py` (nút "Train lại", 164–179 s đo 15/11); bằng chứng lần 10/11 (Q&A 36) |

Lệnh chạy lại (3 phút): `bash scripts/session9/run_g3.sh 1 5` (reset streaming trước; sau khi chạy, user 1 có 1 cặp trong `user_seen`, chạy `reset_streaming` trước demo).

## One-minute defense

> Streaming trong BookPulse **không** tạo gợi ý cá nhân hóa mới; nó làm ba việc trong 2–5 giây: ẩn cuốn vừa chấm và dồn cuốn tiếp theo từ 30 gợi ý đã tính sẵn, cập nhật thống kê theo sách và "sách đang hot" theo cửa sổ 10 phút, và lưu sự kiện vào kho để lần huấn luyện sau học. Gợi ý mới thật sự chỉ xuất hiện sau khi batch chạy lại ALS, đã đo ngày 10/11: user chấm Harry Potter 1–5 thì sau retrain nhận tập 6–7. Nhóm nói rõ điều này trên slide và trong app thay vì gọi hệ thống là "real-time recommendation".
