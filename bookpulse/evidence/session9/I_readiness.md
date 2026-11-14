# I. Final completeness check, readiness self-assessment, handoff (Session 9, mục 8–10)

Ngày 13/11/2026. Các số liệu dẫn từ `reports/` và `evidence/session9/`.

## 8. Final technical completeness check

| Required item | Ready? | Evidence standard → bằng chứng của nhóm |
|---|---|---|
| Problem & Big Data justification | [x] | Báo cáo mục 1–2: 6 triệu rating là bộ phát triển; đo giới hạn một máy (RAM 4,6/7,7 GiB, 4 core 1,95×); UCSD ×38 cần cluster (`B_benchmark.md`) |
| End-to-end architecture | [x] | `A_architecture.md`: mọi khối [I]/[P]/[N/A], trace một bản ghi, deletion test |
| Project-related MapReduce | [x] | mapper/combiner/reducer, HDFS 3 block, 2 reducer, counter 5.976.479 → 47.087, combiner validity (unit test 0/1/n lần), match Spark SQL 0/0 (`D_data_validation.md`) |
| PySpark DataFrames / Spark SQL | [x] | `clean.py` schema + cleaning + dedup window; `analytics_sql.py` 6 câu hỏi, LEFT JOIN enrichment, HAVING, SQL = DataFrame |
| Curated Parquet / columnar output | [x] | `parquet/ratings` (8 file, 4 cột), `book_stats` (coalesce 1), `stream_events` (partition event_date); lý do không partition theo book_id (`C_query_plan.md`) |
| At least two analytical queries | [x] | Q1–Q6 + kết quả trong `analytics_sql_20261106_123956.json`; plan `C_query_plan.txt` |
| Performance/scalability experiment | [x] | `B_benchmark.md`: join strategy 0,83 → 1,29 s (2 Exchange), shuffle partitions 1/8/64 không đổi; core 1/2/4 = 1,00/1,50/1,95× |
| Spark ML / large-scale analytics | [x] | ALS implicit vs explicit vs popularity, P@10/R@10/**NDCG@10**, tham số chọn trên validation (`E_model_validation.md`) |
| NoSQL serving | [x] | Document per user, `find_one({_id})` IDHACK, p50 0,51 ms, 0/1,6 triệu cặp trùng sách đã chấm, cổng chuyển phiên bản (`F_serving_stream.md`) |
| Streaming consideration | [x] | Kafka 3 partition, 4 query, event time + watermark 10 min + cửa sổ 10/5 min + checkpoint; kill -9 ×2, outage; G3 2,0 s (`F_serving_stream.md`, `H_project_card.md`) |
| Cloud/cluster deployment design | [x] DESIGN | `G_deployment.md` 6.1: K8s/YARN, executor scaling, S3, RF 3, replica set, Prometheus, SCRAM/TLS, trade-off cụm tạm thời |
| Validation | [x] | `D_data_validation.md` 7/7 pass; row-count reconciliation 5.976.479 xuyên suốt; sample-known-answer (MapReduce vs Spark, pandas vs Spark) |
| Reproducibility | [x] (1 việc còn lại) | `G_deployment.md` 6.2: phiên bản ghim, seed 42, `run_pipeline.sh`; **chưa thử clone sạch trên máy lạ**, chưa có `requirements.lock` |
| Final report + README + code package | [x] (cần cập nhật số) | `Present/BookPulse - Bao cao cuoi ky BDA501 Nhom 06.docx`, `README.md`, repo; cần thêm NDCG và số Session 9 (xem Top 3) |
| Team contribution statement | [x] (xác nhận lại) | Báo cáo Bảng 18: Hảo (dữ liệu, batch, SQL, MapReduce, triển khai), Tuấn (streaming, đánh giá), Kiệt (ALS, MongoDB, app); Q&A 34 ghi "cần thống nhất" |
| Demo evidence | [x] | `Present/BookPulse - Kich ban demo Session 10.md`: MapReduce → SQL → ML/Mongo → streaming, mỗi bước có lệnh kiểm chứng; dự phòng bằng ảnh/report |

### Red-flag checklist

| Cờ đỏ | Có? | Ghi chú |
|---|---|---|
| Kiến trúc có công nghệ nhóm không giải thích/không dùng | [ ] Không | Hadoop là nhánh kiểm chứng, gắn nhãn rõ, bỏ được (A 2.3) |
| "Real-time" chỉ dựa trên phát lại file tĩnh mà không nêu độ trễ | [ ] Không | Nguồn mô phỏng nói rõ; độ trễ đo: 2,0 s (G3), batch 3–6 s, trigger 5 s |
| Target tính tất định từ feature | [ ] Không | ALS không có feature; relevance = rating test |
| Split/feature có thể lộ thông tin tương lai | [~] Một phần | Không có timestamp → split ngẫu nhiên; nêu là giới hạn, không claim temporal (E 4.2) |
| Metric không khớp task (RMSE cho top-K) | [ ] Không | Chọn mô hình theo P@10; Session 9 thêm NDCG@10 |
| NoSQL chỉ "nhanh hơn" không có access pattern | [ ] Không | F 5.1: find_one theo _id, workload ghi theo đợt |
| Không có kết quả hiệu năng có kiểm soát | [ ] Không | B: join strategy, shuffle partitions, cores |
| Không tái tạo được từ README | [ ] Không (chưa thử máy lạ) | Hai lần chạy cùng split; hành động: clone sạch |
| Implemented và proposed lẫn lộn trên sơ đồ | [ ] Không | A: khối [P] nét đứt riêng |

## 9. Readiness self-assessment (chẩn đoán, không thay điểm giảng viên)

| Criterion | Weight | Full-readiness evidence | Self-score | Evidence gap / action |
|---|---|---|---|---|
| Accuracy & Correctness | 25 | Kết quả chạy đúng như claim; validation thuyết phục | 19 | Số trong deck/Q&A là của bản 06/11, DB đang phục vụ là bản 10/11 không còn report → đồng bộ (Top 3 #1); khe hở exactly-once nêu đúng |
| Creativity & Problem-Solving | 15 | Lựa chọn thích nghi, trade-off rõ | 12 | Chuyển explicit → implicit theo đúng metric; Lambda với kho không-watermark; cổng chuyển phiên bản. Thiếu: xử lý cold start sách mới, popularity penalty |
| Completeness of Product | 25 | Đủ thành phần, có bằng chứng | 22 | Đủ MapReduce/SQL/ML/NoSQL/Streaming/Deploy; thiếu fit 100 %, cổng chất lượng, API, hợp nhất view (đều là [P]) |
| Presentation & Expression | 10 | Sơ đồ/demo rõ, thuật ngữ chính xác | 8 | Sơ đồ có nhãn [I]/[P] trong A_architecture_labeled.png; slide 4 của deck chưa có; demo 8–10 phút đã tập; thuật ngữ "real-time" đã tránh |
| Application of Theory | 20 | MapReduce, DAG/shuffle, layout, CAP, ML eval, streaming/deploy | 17 | Có: combiner, Exchange/broadcast, column pruning, event time/watermark, at-least-once + idempotent, NDCG. Yếu: CAP chỉ ở mức design (standalone) |
| Timeliness & Requirements | 5 | Đúng gói, khai báo, đặt tên | 5 | Gói đã nộp cấu trúc đúng |
| **Tổng** | 100 | | **83 / 100** | |

### Top three corrections before Session 10

| Priority | Weakness found this morning | Concrete fix | Owner | Done? |
|---|---|---|---|---|
| 1 | Deck, Q&A và kịch bản demo trích số của bản mô hình 20261106_125827 (user 1 #1 = *Guernsey*), nhưng `meta.active` đang là **20261110_212851** (user 1 #1 = *The Great Gatsby*, G3) và `reports/` không còn report train/write_recs của lần 10/11 | Chọn một: (a) chạy lại `bash scripts/run_pipeline.sh` từ sạch để có report mới khớp DB, hoặc (b) sửa kịch bản demo/Q&A theo số Session 9 (`E_model_validation.json`: P@10 0,2008, NDCG@10 0,2348, user 1 top-10 bắt đầu Gatsby, Catcher). Sau đó `reset_streaming` (G3 để lại 1 cặp `user_seen` của user 1) | Kiệt | [ ] |
| 2 | Deck chưa có NDCG@10, mẫu số eligible, và câu trả lời rõ về "1 sao là tín hiệu dương" | Thêm vào slide 10: bảng 4 dòng (popular / explicit / implicit / liked-only) với P@10, R@10, NDCG@10, 4.995 user; một dòng "c = 1 + r: 1★ = 2, 5★ = 6; liked-only cho NDCG 0,2356 ≈ 0,2348"; thêm slide "Session 9 Technical Readiness Summary" (mục 10) | Kiệt + Tuấn | [ ] |
| 3 | Reproducibility chưa thử trên máy lạ; thiếu lock file; demo script phụ thuộc trạng thái streaming | Một thành viên clone sạch repo trên máy khác, chạy README mục 2 (dữ liệu mẫu, 5 phút) và `tests`; thêm `requirements.lock` (`pip freeze` trong container); ghi vào README | Hảo | [ ] |

### Likely Q&A (câu khó + trả lời có bằng chứng)

| Question the lecturer may ask | Evidence-based answer |
|---|---|
| "Streaming của nhóm có sinh gợi ý mới không, hay chỉ ẩn sách?" | Chỉ ẩn sách, dồn danh sách đã lưu và cập nhật thống kê: G3 đo 2,0 s từ Kafka tới MongoDB, `meta.active` không đổi. Gợi ý mới chỉ sau retrain (10/11: Harry Potter 6–7 lên #1–#2 sau `clean → train → write_recs`). Nhóm không gọi đây là real-time recommendation. |
| "Precision 0,20 với split ngẫu nhiên có bị thổi phồng không?" | Có thể cao hơn triển khai thật vì không có timestamp (temporal leakage tập thể); nhưng so sánh **cùng split** cho thấy thứ hạng mô hình ổn định (implicit 0,2348 NDCG, popularity 0,0881, explicit 0,0074) và 3.938/4.995 user có ít nhất 1 hit. Cách sửa: dữ liệu UCSD có ngày → split theo thời gian. |
| "1 sao mà cũng là tín hiệu dương, mô hình có gợi ý sách người ta ghét không?" | c = 1 + r nên 1★ có trọng số 2 so với 6; 1–2★ chỉ 8,1 % dữ liệu. Đã đo scheme chỉ ≥ 4★: NDCG 0,2356 so với 0,2348, tức chất lượng không đổi, nhưng đổi 5/10 cuốn mỗi danh sách; nhóm giữ scheme hiện tại và ghi rõ đây là lựa chọn. |
| "Exactly-once có thật không?" | Không hoàn toàn: at-least-once + idempotent (`last_batch`, ReplaceOne, upsert) + file sink exactly-once; G3 gửi cùng event_id 2 lần → đếm 1. Còn khe hở ms giữa `bulk_write` và `mark_applied` và `failOnDataLoss=false`; 3 lượt thử 0 mất 0 trùng chứng minh đường phục hồi thường, không chứng minh khe đã đóng. |
| "Vì sao sort-merge join chậm hơn và khi nào broadcast hết dùng được?" | Đo 13/11: SMJ 1,29 s so với 0,83 s vì thêm 1 Exchange trên 6 triệu dòng; broadcast chỉ khi bảng nhỏ vừa mọi executor (< 10 MB mặc định); 2,36 triệu sách UCSD (~300–500 MB) thì không, lúc đó join trên bảng đã gom theo sách hoặc bucketing. |

## 10. Handoff: slide "Session 9 Technical Readiness Summary"

| Required line | What to report (một dòng trên slide) |
|---|---|
| 1. Architecture truth | Sơ đồ gắn nhãn: **[I]** CSV → Spark batch (clean/SQL/ALS) → MongoDB → Streamlit; Kafka → 4 query Structured Streaming → MongoDB + kho Parquet; Hadoop MapReduce = nhánh kiểm chứng (app không dùng). **[P]** K8s/YARN, 3 broker, replica set, S3, Airflow, Prometheus, TLS, fit 100 %. Không có online learning. |
| 2. Performance | Join strategy trên Q4 (6 triệu dòng): broadcast 0,83 s (1 Exchange) → sort-merge 1,29 s (2 Exchange), **1,55×**; shuffle partitions 1/8/64 không đổi; 4 core = 1,95× (Amdahl). |
| 3. Validation | Dữ liệu: 5.976.479 dòng khớp CSV = Parquet = MapReduce = Spark SQL, join giữ 100 % dòng, 0/1,6 triệu cặp gợi ý trùng sách đã chấm. ML: ALS implicit **NDCG@10 0,235** so với popularity 0,088 (2,67×), explicit 0,007; 4.995 user; liked-only 0,236 (≈). |
| 4. Operations | Streaming = phát lại/sinh giả 20 sự kiện/giây, trigger 5 s; rating mới → ẩn khỏi top-10 sau **2,0 s**, bản trùng bị bỏ, ALS chỉ đổi khi retrain; kill -9 ×2 + tắt Kafka/MongoDB: 0 mất 0 trùng (còn khe hở ms, `failOnDataLoss=false`); chạy lại bằng `run_pipeline.sh`, seed 42, phiên bản ghim; cluster = design-only. |
| 5. Limitation | Không có timestamp → split ngẫu nhiên, không chứng minh được chất lượng theo thời gian; mô hình phục vụ fit trên 80 % dữ liệu; một máy, mọi thành phần là single point of failure, chưa thử tải. |

### Demo evidence sequence (ánh xạ vào kịch bản Session 10)

1. Kiến trúc (slide 4–5) + nhãn [I]/[P] → `A_architecture.md`.
2. Một bản ghi từ CSV → Parquet → gợi ý user 1 → `A_architecture.md` 2.1.
3. MapReduce: `run_hadoop.sh` + `verify_with_spark.py` (match true, combiner −99,2 %).
4. Spark SQL + plan: `sql_plan_*.txt` (BroadcastHashJoin, 1 Exchange) + `B_benchmark.json` (SMJ 2 Exchange).
5. ML: app user 1/2/3 + bảng NDCG (`E_model_validation.json`).
6. NoSQL: `mongosh` IDHACK + `nosql_evidence`.
7. Streaming: producer dữ liệu giả → app → "Chấm sao" → `kafka_audit.py` (G3 là bản thu gọn, 3 phút).
8. Đóng: `B_benchmark.md`, `G_deployment.md` 6.3, `G_deployment.md` 6.2.

### Team sign-off (ký trước khi rời Session 9)

| Statement | Initials |
|---|---|
| We can reproduce the main result from our documented steps. | |
| We can distinguish implemented components from proposed/design-only components. | |
| We have measured at least one performance/scalability factor. | |
| We have validated our key analytical/model claims against evidence or a baseline. | |
| Every member can defend their attributed technical component. | |
