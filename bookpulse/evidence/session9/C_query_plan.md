# C. Spark query-plan / shuffle audit (Session 9, mục 3.2)

Plan đọc từ `C_query_plan.txt` (bản sao của `reports/sql_plan_20261106_123938.txt`, sinh bởi `batch/analytics_sql.py` bằng `EXPLAIN FORMATTED`), đối chiếu với các plan trong `B_benchmark_plans.txt` (13/11). Trạng thái: **measured**.

Truy vấn được kiểm tra (Q4, sách được chấm nhiều nhất):

```sql
SELECT r.book_id, COALESCE(b.title, 'UNKNOWN') AS title,
       COUNT(*) AS n_ratings, ROUND(AVG(r.rating), 3) AS avg_rating
FROM ratings r LEFT JOIN books b ON r.book_id = b.book_id
GROUP BY r.book_id, COALESCE(b.title, 'UNKNOWN')
ORDER BY n_ratings DESC, r.book_id LIMIT 10
```

```
AdaptiveSparkPlan (12)
+- TakeOrderedAndProject (11)                       -- ORDER BY ... LIMIT 10: mỗi task giữ top-10 rồi driver gộp
   +- HashAggregate (10)                            -- final count/avg theo (book_id, title)
      +- Exchange (9)  hashpartitioning(book_id, title, 8)   <-- SHUFFLE duy nhất
         +- HashAggregate (8)  partial_count, partial_avg   -- gom sơ bộ trong từng partition (như combiner)
            +- Project (7)
               +- BroadcastHashJoin LeftOuter BuildRight (6)
                  :- Scan parquet (1)  ReadSchema: struct<book_id:int,rating:int>   -- chỉ 2 cột
                  +- BroadcastExchange (5)          -- bảng sách 10.000 dòng gửi tới mọi task
                     +- Project (4) / Filter (3)
                        +- Scan csv (2)  books.csv
```

| Check | Evidence / answer |
|---|---|
| Which query or DataFrame operation did you inspect? | Q4 ở trên (`batch/analytics_sql.py`), và cùng kết quả qua DataFrame API (`ratings.join(books, "left").groupBy(...).agg(...)`), hai cách viết cho **cùng plan** và cùng 10.000 dòng (`sql_vs_dataframe: only_in_sql 0, only_in_dataframe 0`). |
| Where is Exchange / shuffle visible? | Operator `(9) Exchange hashpartitioning(book_id#1, _groupingexpression, 8), ENSURE_REQUIREMENTS`: shuffle **một lần**, giữa partial và final HashAggregate. Không có Exchange cho join. `shuffle_exchanges: 1`, `broadcast_exchanges: 1` trong `analytics_sql_20261106_123956.json`. Khi tắt broadcast (B_benchmark variant A1) plan có **2** Exchange (`hashpartitioning(book_id#1, 8)` và `hashpartitioning(book_id#54, 8)`) + Sort hai phía + `SortMergeJoin LeftOuter`. |
| Which join strategy is used (if any)? | `BroadcastHashJoin LeftOuter BuildRight`: bảng sách (3,3 MB CSV, 10.000 dòng) nhỏ hơn `spark.sql.autoBroadcastJoinThreshold` = 10 MB nên được broadcast (BuildRight); bảng rating 6 triệu dòng đứng yên trong partition của nó. LEFT OUTER để không mất rating của sách thiếu metadata (0 dòng với Goodbooks, kiểm tra `rows_preserved: true`). |
| What determines partitioning at this point? | Trước shuffle: số file Parquet (8 file Parquet, tổng 16,6 MB, ~2 MB/file < openCostInBytes 4 MB → mỗi file một partition = 8 task; `maxPartitionBytes` 128 MB không có vai trò ở kích thước này). Tại Exchange: `spark.sql.shuffle.partitions` = 8 (đặt trong `common/settings.py`, env `SPARK_SHUFFLE_PARTITIONS`), hash theo khóa GROUP BY `(book_id, title)`. AQE (`AdaptiveSparkPlan isFinalPlan=false`) có thể gộp partition shuffle nhỏ lúc chạy. Thí nghiệm B: đổi 8 → 1 → 64 không đổi thời gian (0,78 / 0,83 / 0,72 s) vì dữ liệu qua shuffle chỉ còn ≤ 10.000 nhóm/partition sau partial aggregate. |
| Would cache/persist help? Why or why not? | Theo quy tắc của nhóm thì **có thể** cho `analytics_sql.py`: `ratings` được quét hơn 10 lần trong cùng job (Q1–Q6, join_check, exceptAll hai chiều) và chỉ ~80 MB khi cache (6 triệu × ~13 byte), không phải vài GB; nhóm chưa cache và chưa đo, lý do chấp nhận được là Parquet 16,6 MB đã nằm trong page cache nên chi phí decode nhỏ (cả job 43 s). **Có** cho ALS: `train_als.py` gọi `train.cache()` và `test.cache()` vì ALS lặp 10 vòng trên cùng DataFrame, và `clean.py` cache `valid` vì nó được dùng cho 4 phép đếm. Quy tắc: cache khi một DataFrame được dùng lại nhiều lần trong cùng job và vừa bộ nhớ. |
| Could broadcast join help? Under what data-size condition? | Đã dùng. Điều kiện: phía build (sách) phải vừa bộ nhớ driver **và** mỗi executor, dưới ngưỡng (10 MB mặc định; nâng được tới ~vài trăm MB bằng `autoBroadcastJoinThreshold`; hint `broadcast()` bỏ qua ngưỡng chứ không nâng ngưỡng). Với 2,36 triệu sách UCSD (ước 300–500 MB metadata) không nên broadcast: chi phí gửi N bản tới N executor và GC; lúc đó SortMergeJoin, hoặc join trên bảng đã gom theo sách (10.000 → 2,36 triệu dòng nhỏ) thay vì trên từng rating. `write_recs.py` cũng dùng `F.broadcast(pool)` cho bảng 200 sách phổ biến khi cross join với 53.424 user. |
| Is Parquet/data layout/partitioning helping the actual access pattern? | **Column pruning có**: `Scan parquet Output [2]: [book_id, rating]` bỏ `user_id`, `from_stream`; CSV phải đọc cả dòng (4,22 s so với 1,08 s). **Partition theo thư mục: không có, và đúng là không nên** với mẫu truy cập hiện tại: mọi truy vấn quét toàn bảng (profile, phân bố, GROUP BY sách, ALS đọc hết); partition theo `book_id` tạo 10.000 thư mục nhỏ (small files), theo `user_id` còn tệ hơn. Kho `stream_events` partition theo `event_date` hợp với cách `clean.py` đọc theo thời gian, nhưng trigger 5 s sinh nhiều file nhỏ (66 file / 3.059 dòng ở lượt 08/11) → cần compaction (thiết kế). `book_stats` được `coalesce(1)` thành 1 file vì chỉ 10.000 dòng. |

## Shuffle ở các job khác (để trả lời khi bị hỏi)

| Job | Shuffle | Vì sao |
|---|---|---|
| `clean.py` | 1 Exchange cho `Window.partitionBy(user_id, book_id)` (row_number bỏ trùng) + các `distinct()/count` | Khử trùng cần gom theo khóa |
| `train_als.py` ALS | Mỗi vòng lặp (10 maxIter) shuffle factor user ↔ item theo block; `recommendForUserSubset` là cross product theo block | Bản chất ALS phân tán; phần tuần tự này là lý do 4 core chỉ nhanh 1,95× |
| `write_recs.py` | `recommendForAllUsers(200)` + `Window.partitionBy(user_id)` + `groupBy(user_id)` + `crossJoin(broadcast(pool))` | Broadcast pool 200 sách để không shuffle 53.424 user |
| MapReduce | Reduce shuffle bytes 1.206.873 B sau combiner (47.087 record thay vì 5.976.479; giảm 99,2%) | Combiner = partial aggregate, cùng ý tưởng với HashAggregate (8) |

Nguyên tắc nhóm nêu khi bảo vệ: hiệu năng gắn với **workload** (GROUP BY theo sách), **layout** (Parquet 2 cột, 8 file), **tài nguyên** (local[*], 4 GB driver) và **access pattern** (quét toàn bảng theo đợt; đọc điểm theo `_id` thì ở MongoDB, không ở Spark).
