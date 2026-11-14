# Session 9 Lab: Integrated Big Data Analytics & Final Project Readiness — Nhóm 06 BookPulse

| Field | Team entry |
|---|---|
| Team / Class | Nhóm 06 · BDA501 Big Data · MSE, FPT School of Business & Technology |
| Project title | BookPulse: hệ thống gợi ý sách trên nền tảng Big Data (MapReduce · Spark SQL · Spark ML · MongoDB · Kafka + Structured Streaming) |
| Members | Lương Huỳnh Ngọc Hảo (dữ liệu, batch, Spark SQL, MapReduce, triển khai) · Đinh Trần Quốc Tuấn (streaming, kiểm thử) · Nguyễn Tấn Kiệt (ALS, MongoDB, app) |
| Primary dataset(s) | Goodbooks-10k: `ratings.csv` 5.976.479 rating (53.424 user × 10.000 sách, 1–5 sao, không timestamp), `books.csv` 10.000 sách; sự kiện streaming mô phỏng (`make_fake_stream.py`, `producer.py`, nút "Chấm sao") |
| Challenge card | **G – BookPulse Goodreads** (`H_project_card.md`) |
| Ngày làm lab | 13/11/2026 |

Quy ước: mỗi số liệu ghi rõ **measured** (đã chạy, có file), **inferred** (suy từ số đo) hoặc **design-only** (thiết kế, chưa chạy). Số liệu cũ lấy từ `reports/` (lần chạy cuối 06/11, streaming 08/11, góp ý 10/11); số liệu mới của Session 9 do script trong `scripts/session9/` sinh ra ngày 13/11.

## Sáu đầu ra bắt buộc của Session 9

| # | Đầu ra | Ở đâu | Trạng thái |
|---|---|---|---|
| 1 | Sơ đồ kiến trúc có nhãn [I]/[P]/[N/A], trace một bản ghi, deletion test | `A_architecture.md` | measured (code + report) |
| 2 | Một thí nghiệm hiệu năng có kiểm soát + một giải thích query plan | `B_benchmark.md`, `B_benchmark.json`, `B_benchmark_plans.txt`, `C_query_plan.md`, `C_query_plan.txt` | measured 13/11 (join strategy, shuffle partitions) + 04/11 (cores) |
| 3 | Bảng validation xử lý dữ liệu + ML, bảng claim/evidence | `D_data_validation.md`, `E_model_validation.md`, `E_model_validation.json` | measured |
| 4 | NoSQL access pattern, streaming design, cloud/cluster design | `F_serving_stream.md`, `G_deployment.md` 6.1 | measured (NoSQL, streaming) · design-only (cluster) |
| 5 | Reproducibility check, limitation/risk list, completeness audit | `G_deployment.md` 6.2–6.3, `I_readiness.md` mục 8 | measured / self-assessed |
| 6 | Slide "Session 9 Technical Readiness Summary" (5 dòng) + self-score + top-3 + Q&A | `I_readiness.md` mục 9–10 | ready to paste into deck |

Card G (mục 7): `H_project_card.md` với G1 `G1_implicit_semantics.json`, G2 `E_model_validation.json`, G3 `G3_fresh_rating_trace.json`.

## Evidence manifest (quy ước 6.4)

| File | Nội dung | Sample / data period | Command / setting | Measured / inferred / design |
|---|---|---|---|---|
| `A_architecture.md` | Sơ đồ nhãn I/P, bảng thành phần, trace user 1, câu hỏi 2.2, deletion test 2.3 | toàn bộ repo, reports 04–10/11 | đọc code, `docker-compose.yml`, `reports/` | measured (I), design (P) |
| `B_benchmark.json`, `B_benchmark_plans.txt`, `B_benchmark.md` | Join strategy & shuffle partitions trên Q4; cores 1/2/4 (04/11) | 5.976.479 dòng Parquet, 10.000 sách; 13/11 12:0x UTC | `docker compose exec spark python scripts/session9/benchmark_join.py` (local[*] 12 core, driver 4 GB, 3 lần sau warm-up) | measured |
| `C_query_plan.txt`, `C_query_plan.md` | Physical plan Q4 + bảng audit 3.2 | plan 06/11 (`reports/sql_plan_20261106_123938.txt`) và 13/11 | `batch/analytics_sql.py --books ... --raw-csv ...` (`EXPLAIN FORMATTED`) | measured |
| `D_data_validation.md` | 7 mục validation + claim vs evidence | reports 04–08/11; schema đọc lại 13/11 | `clean.py`, `analytics_sql.py`, `verify_with_spark.py`, `nosql_evidence.py`, `kafka_audit.py` | measured |
| `E_model_validation.json`, `E_model_validation.md` | G2: P@10, R@10, NDCG@10 của serving model vs explicit vs popularity; bảng 4.2 | 5.000 user mẫu từ test (4.995 eligible), split seed 42: 4.780.731/1.195.748 | `docker compose exec spark python scripts/session9/ranking_eval.py` (105 s) | measured |
| `G1_implicit_semantics.json` | G1: transform table c = 1 + r, cấu hình, so sánh scheme liked-only, user 1 | cùng user/test như G2; alt model 3.297.772 dòng, fit 47,6 s | cùng lệnh trên | measured |
| `G3_fresh_rating_trace.json`, `G3_trace.log`, `G3_stream_job.log`, `G3_stream_progress.json`, `G3_kafka_audit.json/.log`, `G3_reset.log`, `G3_run.log` | G3: 1 rating mới (event_id ổn định, gửi 2 lần) → Kafka → Spark → MongoDB → view app; audit Kafka/Mongo/archive | user 1, book 5, 13/11 12:07 UTC; job chạy 150 s | `bash scripts/session9/run_g3.sh 1 5` (reset → job → trace → kafka_audit) | measured |
| `F_serving_stream.md` | Bảng 5.1 NoSQL và 5.2 streaming | reports 06–08/11, G3 | `nosql_evidence.py`, `kafka_audit.py`, `stream_fault_test.sh`, `stream_outage_test.sh` | measured; replica set/sharding: design |
| `G_deployment.md` | 6.1 cluster design, 6.2 reproducibility, 6.3 limitations | — | kiểm tra `requirements.txt`, `docker-compose.yml`, grep credential 13/11 | 6.1 design-only; 6.2 measured; 6.3 measured/inferred |
| `H_project_card.md` | Card G: 7 câu, G1/G2/G3, defense 60 giây | như trên | như trên | measured |
| `I_readiness.md` | Mục 8 completeness + red flags, 9 self-score 83/100 + top-3 + Q&A, 10 slide + demo sequence + sign-off | — | — | self-assessed |

## Chạy lại toàn bộ bằng chứng Session 9 (~6 phút, stack `docker compose up -d` đang chạy, không chạy job streaming khác cùng lúc)

```bash
cd bookpulse
docker compose exec spark python scripts/session9/benchmark_join.py       # ~1 phút  -> B_benchmark.*
docker compose exec spark python scripts/session9/ranking_eval.py         # ~2 phút  -> E_model_validation.json, G1_implicit_semantics.json
bash scripts/session9/run_g3.sh 1 5                                       # ~3 phút  -> G3_*  (reset streaming trước; reset lại trước demo)
```

Lưu ý sau khi chạy G3: topic Kafka có 2 message, `user_seen` có cặp (1, 5), nên app ẩn *The Great Gatsby* của user 1. Trước demo chạy `bash scripts/reset_streaming.sh --yes` (hoặc nút "Xoá dữ liệu" ở trang /setting).

## Những gì KHÔNG làm trong lab này (và vì sao)

- Không huấn luyện lại / ghi đè `models/als_implicit` và `meta.active`: G1 fit mô hình thay thế trong bộ nhớ và không lưu, để không đổi trạng thái demo.
- Không chạy lại `run_pipeline.sh` (25–35 phút): `reports/` của lần 10/11 không còn, nên số trong deck là của bản 06/11; xem Top 3 #1 trong `I_readiness.md`.
- Không chạy trên cluster: mục 6.1 là DESIGN REVIEW.
- Không chạy lại Hadoop MapReduce: dùng report 05/11 và lần tập 07/11.
