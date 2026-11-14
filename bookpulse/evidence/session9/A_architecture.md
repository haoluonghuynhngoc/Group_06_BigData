# A. Architecture truth audit (Session 9, mục 2)

Trạng thái: **measured / inspected** (đọc code, docker-compose, report trong `reports/`), ngày 13/11/2026.
Sơ đồ gốc: `docs/img/architecture_overview.png` (tổng quan), `docs/img/architecture_detail.png` (chi tiết), `docs/kien-truc.html`.

## Deliverable A: sơ đồ kiến trúc đã đánh dấu [I] / [P] / [N/A]

Mũi tên `───►` = luồng dữ liệu **đã chạy thật** (có report); `- - ►` = luồng **đề xuất / thiết kế**.

```
NGUỒN                 THU NHẬN                 XỬ LÝ                                   PHỤC VỤ                 TRÌNH BÀY
[I] Goodbooks-10k ───────────────────────────► [I] Hadoop 3.5.0 HDFS + YARN              (app không dùng)
    ratings.csv 72 MB                             Hadoop Streaming: mapper→combiner→reducer
    books.csv 3,3 MB                              → book_rating_stats.tsv ──► [I] verify_with_spark.py (đối chiếu Spark SQL)
        │
        ├──────────────────────────────────────► [I] LỚP BATCH · Spark 3.5.3 local[*] (scripts/run_pipeline.sh, theo đợt)
        │                                           B1 clean.py ──► [I] Parquet ratings (8 file, from_stream)
        │                                           B2 analytics_sql.py ──► [I] Parquet book_stats · sql_plan_*.txt
        │                                           B3 train_als.py (explicit + implicit, split seed 42)
        │                                           B4 write_recs.py ──► [I] MongoDB recommendations_<v> → meta.active
        │                                                   ▲
        │                                                   │ [I] S5: clean.py gộp kho stream_events ở lần batch sau
        │                                                   │     (đã chạy 06/11; mô hình đang phục vụ chưa học sự kiện stream)
        ▼                                                   │
[I] producer.py / make_fake_stream.py      [I] Kafka 3.8     [I] LỚP TỐC ĐỘ · Structured Streaming (trigger 5 s)
    app "Chấm sao" / rate_from_recs.py ──► 1 broker, topic ──► valid (watermark 10 min, dropDuplicatesWithinWatermark)
    (sự kiện MÔ PHỎNG, simulated=true)      rating-events        ──► [I] MongoDB rating_stats, user_seen, online_eval
                                            3 partition,          trending (window 10/5 min, update) ──► [I] MongoDB trending
                                            key=user_id           rejected (chỉ đếm) ──► stream_state
                                            volume kafka-data     events_to_parquet ──► [I] Parquet stream_events (archive)
                                                                  checkpoints/ (volume spark-checkpoints)

[I] MongoDB 7.0.14 standalone ─────────────────────────────────────────────────────────► [I] Streamlit 1.39 app
    batch view: recommendations_<v>, meta, books                                            home.py: top-10 = recs − user_seen (+fallback)
    speed view: rating_stats, user_seen, trending, online_eval, stream_state                setting.py → [I] control_server.py (spark:8600)
                                                                                            "Chấm sao" ──► Kafka (không ghi MongoDB)

- - - - - - - - - - - - - - - - - - - - -  [P] THIẾT KẾ, CHƯA CHẠY (báo cáo mục 11)  - - - - - - - - - - - - - - - - - - - - -
[P] Spark trên Kubernetes/YARN, nhiều executor      [P] Kafka 3 broker, RF 3, min.insync.replicas 2   [P] MongoDB replica set 3 node
[P] S3/MinIO cho raw, Parquet, checkpoint, kho sự kiện bất biến (Kafka Connect S3 sink)             [P] API chỉ đọc giữa app và MongoDB
[P] Airflow/cron lập lịch batch, model theo run_id, cổng chất lượng P@10 trước khi chuyển meta.active
[P] Prometheus/Grafana qua StreamingQueryListener (lag = avgOffsetsBehindLatest)    [P] SCRAM + TLS, secret manager, mạng nội bộ
[P] RocksDB state store, Delta Lake/Iceberg compaction cho kho sự kiện               [P] hợp nhất batch view + speed view (mốc cắt)
[P] fit lại ALS trên 100% dữ liệu trước khi phục vụ (hiện fit trên tập train = 80%)

[N/A] Cassandra, Redis (không dùng) · Spark đọc HDFS / chạy trên YARN (không: Spark đọc đĩa local, local[*]) ·
      UCSD Book Graph (không chạy) · online learning (không có: ALS chỉ học theo đợt) · dead-letter topic · schema registry
```

### Bảng thành phần

| Thành phần | Nhãn | Bằng chứng đã chạy / nơi ghi thiết kế |
|---|---|---|
| Goodbooks-10k CSV (5.976.479 rating, 10.000 sách) | [I] | `scripts/download_goodbooks.py`; `reports/clean_20261106_123904.json` |
| Hadoop 3.5.0 HDFS + YARN + Hadoop Streaming (compose riêng `mapreduce/hadoop/`) | [I] | `reports/mapreduce/job_20261105_165718.log`, `hdfs_blocks_*.txt`, `reports/mapreduce_verify_20261105_170058.json` (match: true) |
| Spark batch `local[*]`: clean → Spark SQL → ALS → write_recs | [I] | `reports/clean_*`, `analytics_sql_*`, `sql_plan_*`, `train_als_*`, `train_als_implicit_*`, `write_recs_*` |
| Parquet `ratings`, `book_stats`, `stream_events` | [I] | `data/parquet/` (8 file ratings; book_stats 10.000 dòng) |
| Kafka 3.8.0 KRaft, 1 broker, topic 3 partition, volume | [I] | `docker-compose.yml`; `reports/kafka_audit_*.json` |
| Structured Streaming 4 query + checkpoint + archive | [I] | `reports/stream_progress_*.json`, `reports/streaming/fault_test_*`, `outage_test_*` |
| MongoDB 7.0.14 standalone, batch view + speed view | [I] | `reports/nosql_evidence_20261107_221150.json` (IDHACK, p50 0,51 ms) |
| Streamlit app + control server | [I] | `app/home.py`, `app/setting.py`, `scripts/control_server.py`; ảnh `docs/img/app_*.png` |
| Vòng stream → kho Parquet → clean.py → retrain | [I] (đã chạy 06/11 và 10/11) | `reports/clean_20261106_122043.json` (4.901 dòng gộp); Q&A 36 (10/11: 11/11 lượt vào train) |
| Cluster/cloud (K8s/YARN, 3 broker, replica set, S3, Airflow, Prometheus, TLS) | [P] | Báo cáo cuối kỳ mục 11, Bảng 17; Q&A K10, K18 |
| Fit lại trên 100% dữ liệu; cổng chất lượng P@10; hợp nhất 2 view | [P] | `docs/chon-mo-hinh.md` Kết luận; README mục 6 |
| Cassandra/Redis, UCSD, Spark trên HDFS/YARN, online learning | [N/A] | không có trong code |

## 2.1 Trace một bản ghi qua hệ thống: rating của user 1

| Stage | Artifact thật (input → output) | Công nghệ | Bằng chứng |
|---|---|---|---|
| Source / ingestion | Dòng `1,<book_id>,<rating>` trong `data/goodbooks/ratings.csv` (user 1 có 117 rating, 93 trong train sau split) | CSV tải bằng `download_goodbooks.py` | `reports/clean_20261106_123904.json`: input_rows 5.976.479 |
| Raw / distributed storage | `ratings.csv` → HDFS `/bookpulse/input/ratings.csv`, blocksize 32 MB → 3 block (chỉ cho nhánh MapReduce) | HDFS 3.5.0, replication 1 | `reports/mapreduce/hdfs_blocks_20261105_165718.txt`: "3 block(s): OK" |
| Cleaning / transformation | `clean.py`: cast int, lọc rating 1–5, bỏ trùng (user, book) theo window; 0 dòng bị loại | Spark DataFrame | `clean_20261106_123904.json`: rejected_invalid 0, rejected_duplicates 0, output 5.976.479 |
| Curated / columnar output | `data/parquet/ratings/part-0000{0..7}` (user_id, book_id, rating, from_stream) | Parquet snappy, 8 file | `pyarrow` schema đọc 13/11: `from_stream: bool not null`, 5.976.479 dòng |
| Spark SQL / analytics | Q4: sách user 1 góp vào COUNT(*) của book_id; Q6 → `parquet/book_stats` 10.000 dòng | Spark SQL temp view, BroadcastHashJoin | `analytics_sql_20261106_123956.json`, `sql_plan_20261106_123938.txt` |
| ML / large-scale analytics | 93 rating train của user 1 → vector 50 chiều `userFactors` → `recommendForAllUsers(200)` → top-30 chưa đọc | Spark MLlib ALS implicit (rank 50, alpha 1, reg 0,1) | `train_als_implicit_20261106_125816.json` (P@10 0,204); `models/als_implicit/userFactors` |
| NoSQL serving / read-back | `recommendations_20261110_212851` doc `_id: 1` (30 recs + 10 fallback); đọc lại `find_one({_id: 1})` IDHACK | MongoDB 7.0 | `nosql_evidence_20261107_221150.json` (sample_document user 1: 195, 5, 8, 15, 291) |
| Streaming path | Sự kiện `{event_id, user_id: 1, book_id: 195, rating: 5, event_time, simulated: true}` → Kafka → `valid` → `user_seen` `_id "1_195"`; `rating_stats` 195; archive Parquet | Kafka + Structured Streaming | **G3** `G3_fresh_rating_trace.json` (Session 9); 08/11: `docs/img/app_after_rate_from_recs.png` |
| Final user / dashboard | App user 1: top-10 = recs − user_seen; cuốn 195 ẩn, cuốn #11 dồn lên; "Điểm TB (stream)" từ rating_stats | Streamlit | `docs/img/app_implicit_user1.png`, `app_after_rate_from_recs.png` |

## 2.2 Câu hỏi thách thức kiến trúc

1. **Luồng quan trọng nhất:** CSV → clean.py → Parquet → train_als.py (ALS implicit) → write_recs.py → MongoDB `recommendations_<v>` → app `find_one({_id: user})`. Đầu ra kiểm chứng được: top-10 user 1 (195, 5, 8, …), P@10 0,204 trên tập test, 0/1.602.720 cặp gợi ý trùng sách đã chấm. Cả ba thành viên trình bày được theo slide 4–5 (Q&A K1).
2. **Thành phần có mặt vì "tên công nghệ":** Hadoop MapReduce/HDFS/YARN. App không dùng; bỏ đi hệ thống vẫn giải quyết bài toán. Nhóm giữ vì (a) yêu cầu môn học, (b) là cách tính **độc lập** để kiểm chứng Spark SQL (0/0 dòng lệch trên 10.000 sách). Nói thẳng trong báo cáo mục 3 và Q&A Hảo 2. Xem 2.3.
3. **Ranh giới:** *Batch processing* = `run_pipeline.sh` (theo đợt, 25–35 phút, huấn luyện lại ALS). *Streaming ingestion* = Kafka + 4 query Structured Streaming, trigger 5 s, chỉ cập nhật số liệu và `user_seen`. *Online serving* = MongoDB + app đọc theo `_id` (~1 ms). *Online learning* = **không có**: factor ALS không đổi theo sự kiện; sự kiện chỉ vào kho Parquet chờ lần batch sau (xem G3 và câu bảo vệ Card G).
4. **Proposed được gắn nhãn chưa?** Có: báo cáo mục 11 mở đầu bằng "Phần này là thiết kế, chưa triển khai thật"; Q&A Hảo 11; sơ đồ ở trên tách khối [P] bằng nét đứt. Số liệu UCSD (×38, 2×10¹² tích vô hướng, 650 MB factor) được ghi là ước tính.
5. **Một thành phần chết thì mất gì, khôi phục được gì** (đo bằng `stream_fault_test.sh`, `stream_outage_test.sh`, 08/11):

| Chết | Mất gì | Khôi phục được |
|---|---|---|
| Job streaming (kill -9) | Speed view đứng; sự kiện nằm lại Kafka | Chạy lại từ checkpoint: 1.474 = 1.500 − 26 trễ, 0 mất 0 trùng (`kafka_audit_20261107_220738.json`) |
| Kafka (tắt 40 s) | Producer giữ buffer 120 s; job Spark vẫn chạy, tự nối lại | `failed = 0`, 3.078/3.078 message (`outage_test_20261108_110750/events.log`) |
| MongoDB (tắt 30 s) | App không đọc được; job streaming **dừng** (ServerSelectionTimeoutError) | Chạy lại tay từ checkpoint sau 11 s: 3.000 = 3.000; chưa có restart policy |
| Job batch giữa chừng | Không ảnh hưởng app (bản cũ vẫn phục vụ) | Cổng write_recs: đủ 53.424 user mới chuyển `meta.active`; thiếu thì drop collection mới |
| Cụm Hadoop | Không ảnh hưởng luồng chính | Chỉ là nhánh kiểm chứng |
| Cả máy (một host) | Mọi thứ: Kafka RF 1, Mongo standalone, Spark local, HDFS 1 DataNode | Không có dự phòng; volume Docker giữ topic/checkpoint/Mongo; [P] replica set, RF 3, S3 |

## 2.3 One-technology deletion test: bỏ cụm Hadoop (HDFS + YARN + MapReduce)

| Khía cạnh | Tác động nếu bỏ |
|---|---|
| Functionality | **Không đổi**: app, gợi ý, streaming không đọc gì từ Hadoop; thống kê theo sách đã có bằng Spark SQL (Q6 `book_stats`). |
| Scalability | Không đổi cho Goodbooks; mất đường chạy MapReduce trên cluster YARN khi dữ liệu lớn (nhưng Spark trên YARN/K8s thay được). |
| Latency | Giảm: không phải chờ cụm Hadoop khởi động (~40 s) và job 52–134 s; demo ngắn hơn ~2 phút; trả lại RAM cho Spark/Kafka. |
| Complexity | Giảm rõ: bớt 1 compose, 6 container, image 3.5.0, lỗi `MSYS_NO_PATHCONV`, safe mode, rack-local. |
| Assessment evidence | **Mất** mục "Project-related MapReduce" của Final Project 40% và mất phép kiểm chứng độc lập (hai engine, hai cách viết, cùng kết quả 10.000/10.000; combiner giảm 99,2% shuffle). |

Kết luận: về sản phẩm, Hadoop có thể bỏ; về đồ án, giữ vì nó là nhánh kiểm chứng và là yêu cầu đầu ra. Nhóm gắn đúng nhãn "kiểm chứng, app không dùng" chứ không vẽ nó vào đường dữ liệu chính.

Phương án **đơn giản hóa** thứ hai (không bỏ công nghệ): gộp 4 query streaming thành 1 `foreachBatch` ghi nhiều sink. Được: Kafka đọc 1 lần thay vì 4, batch đầu ngắn hơn; mất: kho Parquet không còn là file sink exactly-once độc lập, `trending` update mode và kho không-watermark phải xử lý trong cùng một plan (Q&A K12). Chưa làm.
