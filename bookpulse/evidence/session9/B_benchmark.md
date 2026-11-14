# B. Performance & scalability lab (Session 9, mục 3.1)

Hai thí nghiệm có kiểm soát, mỗi thí nghiệm đổi **đúng một yếu tố**. Thí nghiệm 1 đo mới ngày 13/11/2026 (`B_benchmark.json`, `B_benchmark_plans.txt`, script `scripts/session9/benchmark_join.py`). Thí nghiệm 2 đo ngày 04/11/2026 (`reports/train_als_20261104_0*.json`, `reports/scaling/docker_stats_20261104_batch.csv`).

## Thí nghiệm 1 (mới): join strategy trên truy vấn Q4 của đồ án — MEASURED

| Experiment field | Team entry |
|---|---|
| Question / hypothesis | Broadcast hash join (bảng sách 10.000 dòng gửi tới mọi task) nhanh hơn sort-merge join (shuffle cả hai bảng) cho truy vấn `ratings LEFT JOIN books GROUP BY book_id`; kỳ vọng SMJ chậm hơn vì có thêm một Exchange trên 6 triệu dòng. |
| Environment / hardware | Docker Desktop trên Windows 11; container `bookpulse-spark`, Spark 3.5.3 `local[*]`, 12 core nhìn thấy, driver 4 GB; dữ liệu trên đĩa local (page cache ấm sau warm-up); mongo, kafka, app cùng chạy nhưng rảnh. |
| Dataset / input size | `data/parquet/ratings`: 5.976.479 dòng, 8 file Parquet (chỉ đọc 2 cột `book_id`, `rating`); `books.csv` 10.000 dòng. |
| Factor changed | `spark.sql.autoBroadcastJoinThreshold`: 10 MB (mặc định, baseline) → −1 (tắt broadcast, variant). Yếu tố phụ (thí nghiệm B): `spark.sql.shuffle.partitions` 8 → 1 → 64, giữ broadcast. |
| Baseline configuration | BroadcastHashJoin, shuffle.partitions = 8 (giá trị `common/settings.py`). |
| Variant configuration | SortMergeJoin (broadcast tắt), shuffle.partitions = 8. |
| Metric measured | Thời gian wall-clock của `.collect()` (median của 3 lần sau 1 lần warm-up); số operator `Exchange` (shuffle) và `BroadcastExchange` trong physical plan. |
| Expected trend before running | SMJ chậm hơn 1,5–3 lần và có 2 Exchange thay vì 1; số shuffle partition hầu như không ảnh hưởng vì sau partial aggregation chỉ còn ~10.000 nhóm đi qua shuffle. |

### Measured results

| Run | Configuration | Input size / partitions | Runtime (median, 3 lần) | Other metric | Observation |
|---|---|---|---|---|---|
| Baseline | BroadcastHashJoin, shuffle.partitions 8 | 5.976.479 dòng / 8 file / 8 shuffle part. | **0,829 s** (0,829 · 0,862 · 0,771) | 1 Exchange + 1 BroadcastExchange | Bảng sách 3,3 MB < 10 MB → broadcast; bảng rating không bị shuffle để join |
| Variant 1 | SortMergeJoin (broadcast tắt), shuffle.partitions 8 | như trên | **1,287 s** (1,287 · 1,179 · 1,583) | **2 Exchange** (hashpartitioning(book_id) cả hai bảng) + 0 Broadcast | Chậm hơn **1,55×**; phương sai lớn hơn (1,18–1,58 s) |
| Variant 2 (yếu tố phụ) | BroadcastHashJoin, shuffle.partitions 1 | như trên | 0,782 s | 1 Exchange | Không chậm hơn baseline |
| Variant 2b | BroadcastHashJoin, shuffle.partitions 64 | như trên | 0,716 s | 1 Exchange | Không nhanh hơn có ý nghĩa (chênh < 0,12 s, trong nhiễu) |

Plan đầy đủ của từng cấu hình: `B_benchmark_plans.txt` (dòng `(9) SortMergeJoin` ở variant 1, `Exchange hashpartitioning(book_id#1, 8)` và `hashpartitioning(book_id#54, 8)` cho hai phía).

### Interpretation

1. **What changed, and why did the result move in that direction?** Tắt broadcast buộc Spark shuffle **cả hai** phía theo `book_id` rồi sort trước khi join: thêm một Exchange trên 6 triệu dòng (hash + ghi/đọc file shuffle + sort) và mất khả năng kết hợp partial aggregation ngay sau scan. Thời gian tăng 0,83 → 1,29 s (1,55×). Yếu tố shuffle partitions không đổi kết quả vì lượng dữ liệu qua Exchange rất nhỏ (partial aggregate đã gom còn ≤ 10.000 nhóm × 8 task): chia 1 hay 64 phần đều là chi phí cố định; NHƯNG plan chỉ là plan trước khi chạy (isFinalPlan=false) và AQE coalescePartitions bật mặc định nên cấu hình 64 có thể đã bị gộp lúc chạy, chưa lấy số task thật. Kết luận đúng mức: với truy vấn này số shuffle partition không phải yếu tố quyết định.
2. **At what point would the chosen configuration stop scaling well?** Broadcast chỉ hợp lệ khi bảng nhỏ vừa bộ nhớ của **mọi** executor và dưới ngưỡng (10 MB mặc định, có thể nâng tới vài trăm MB). Với bộ UCSD đầy đủ (2,36 triệu sách, metadata ước ~300–500 MB) bảng sách không còn broadcast được: Spark sẽ rơi về SortMergeJoin, tức chính variant 1, và chi phí shuffle lúc đó tỷ lệ với 229 triệu dòng rating, trên mạng thật chứ không phải RAM một máy. Khi đó cách giảm là bucketing theo `book_id` cho cả hai bảng, hoặc join trên bảng đã gom (`book_stats`) thay vì trên từng rating.
3. **What limitation prevents this experiment from proving production-scale performance?** Chạy `local[*]` trên một máy: shuffle đi qua đĩa/RAM local, không qua mạng; Parquet chỉ 16,6 MB (8 file × ~2 MB; 72 MB là CSV) nằm trong page cache sau warm-up; thời gian 0,7–1,3 s phần lớn là chi phí cố định (JIT, lập kế hoạch, 12 thread) nên tỉ lệ 1,55× không ngoại suy được; chỉ 3 lần lặp; không đo shuffle bytes qua Spark UI. Kết quả chứng minh **xu hướng và cơ chế** (số Exchange), không chứng minh thông lượng production.

## Thí nghiệm 2 (04/11/2026): số core cho huấn luyện ALS — MEASURED

| Experiment field | Team entry |
|---|---|
| Question / hypothesis | Huấn luyện ALS (TrainValidationSplit, rank 10) có tăng tốc tuyến tính theo số core không? Kỳ vọng: không, vì phần tuần tự ở driver và shuffle giữa các vòng lặp. |
| Environment | Docker 4 CPU / 7,7 GiB (cấu hình tại thời điểm 04/11); `SPARK_MASTER=local[1|2|4]`; cùng dữ liệu, cùng seed. |
| Factor changed | Số core: 1 → 2 → 4. |
| Metric | `fit_seconds` của TrainValidationSplit; RAM đỉnh container (`docker stats` mỗi 5 s). |

| Run | Configuration | Train rows | Runtime | Speed-up | Observation |
|---|---|---|---|---|---|
| Baseline | local[1] | 4.781.442 | 159,3 s | 1,00× | |
| Variant 1 | local[2] | 4.780.152 | 106,5 s | 1,50× | |
| Variant 2 | local[4] | 4.780.993 | 81,9 s | 1,95× | hiệu suất song song ~49% (Amdahl); RAM đỉnh container Spark trong từng lần train 2,19 / 2,26 / 2,52 GiB (đỉnh 4,594 GiB của cả lượt batch rơi vào write_recs lúc 12:13, không phải ALS); local[N] không giới hạn CPU của JVM (docker stats tới 451 % ngay ở local[1]) |

Bằng chứng: `reports/train_als_20261104_045928.json` (1 core), `_050249.json` (2), `_050535.json` (4); `reports/scaling/docker_stats_20261104_batch.csv`. Đây là số liệu báo cáo cuối kỳ Bảng 15. Giới hạn: một máy, core chia chung với Kafka/MongoDB, mỗi điểm đo 1 lần.

## Thí nghiệm bổ sung đã có: CSV vs Parquet (cùng GROUP BY, 06/11)

4,22 s (CSV) → 1,08 s (Parquet) = 3,9×; các lần đo khác 3,9–8,4× (dao động theo page cache). Bằng chứng chắc hơn thời gian là plan: `Scan parquet` chỉ đọc 2 cột (`ReadSchema: struct<book_id:int,rating:int>`). File: `reports/analytics_sql_20261106_123956.json` (`csv_vs_parquet_seconds`).

## Không claim

- Không nói "Spark nhanh": nói "broadcast tránh một Exchange trên 6 triệu dòng, đo được 1,55× trên máy này".
- Không nói "nhiều partition nhanh hơn": đo 1/8/64 không khác nhau ở truy vấn này.
- Không nói "cache giúp": truy vấn chạy một lần, không có cache/persist; `train_als.py` cache `train`/`test` vì ALS lặp 10 vòng (xem C_query_plan.md).
