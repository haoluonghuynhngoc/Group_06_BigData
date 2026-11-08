# BookPulse – Hỏi đáp Session 10 (Final Assessment)

Ôn hỏi đáp cho bài cuối kỳ. Số liệu theo `bookpulse/reports/` (lần chạy lại ngày 06/11/2026; streaming đo lại ngày 08/11/2026) và báo cáo Word. Syllabus ghi giảng viên hỏi về: **quyết định triển khai, xử lý phân tán, khả năng mở rộng và chuẩn đầu ra môn học**.

## Nguyên tắc

- **Ai trình bày phần nào trả lời phần đó**; Kiệt điều phối.
- **Trả lời bằng số đo và chỉ ra file bằng chứng** (`reports/…`).
- **Nói thẳng giới hạn** (một máy, sự kiện mô phỏng, thiết kế cluster chưa chạy thật). Giảng viên đánh giá cao việc tự chỉ ra vấn đề.
- **Không biết thì ghi nhận**, không đoán.

## Trả lời nhanh

| Nếu bị hỏi | Một câu |
|---|---|
| MapReduce làm gì? | Thống kê rating theo sách trên HDFS + YARN; combiner giảm 99,2% dữ liệu shuffle; khớp 100% Spark SQL |
| Spark SQL ở đâu? | `batch/analytics_sql.py`: temp view, 6 câu hỏi, query plan, kiểm tra join và SQL = DataFrame |
| Mô hình nào, tốt cỡ nào? | ALS implicit, Precision@10 0,204 (sách phổ biến 0,078; ALS explicit 0,008); mô hình phục vụ học trên 80% dữ liệu (tập train) |
| Vì sao MongoDB? | Một truy vấn duy nhất theo user_id → document mỗi người; trung vị khoảng 0,9 ms, p95 khoảng 2,8 ms |
| Streaming có gì? | Kafka 3 partition; 4 query: watermark + bỏ trùng, cửa sổ trượt 10/5 phút, kho lưu trữ, đếm lỗi; lượt kill -9: 0 mất, 0 trùng (còn khe hở khi chết giữa lúc ghi MongoDB) |
| App hiện "2 / 672" là sao? | Rating phát lại đã có trong dữ liệu huấn luyện nên không thể trúng; 2 lượt trúng do `rate_from_recs.py` |
| Có phải Big Data không? | Goodbooks là bộ phát triển; đã đo giới hạn một máy; bộ đầy đủ ×38 cần cluster (slide 13) |

---

## Kiến trúc – phần giảng viên chú trọng (Hảo trả lời, cả nhóm cần nắm)

Mở slide 3 (hoặc `bookpulse/docs/kien-truc.html`) khi trả lời, chỉ tay theo đường đi của dữ liệu.

**K1. Trình bày kiến trúc trong 30 giây.**
Năm lớp. **Nguồn**: Goodbooks (CSV) và sự kiện rating do producer mô phỏng. **Thu nhận**: Kafka (topic 3 partition, key user_id) cho sự kiện; HDFS cho MapReduce. **Xử lý**: Spark batch (làm sạch, Spark SQL, ALS) chạy theo đợt; Spark Structured Streaming (4 query) chạy liên tục; MapReduce trên YARN để đối chiếu. **Lưu trữ**: Parquet cho dữ liệu sạch và kho sự kiện; MongoDB cho gợi ý và số liệu trực tiếp. **Phục vụ**: Streamlit đọc MongoDB theo `_id`. Vòng lặp: sự kiện streaming được lưu ra Parquet để lần huấn luyện sau học tiếp.

**K2. Vì sao chọn từng công nghệ?**
Kafka: log bền, đọc lại được theo offset và chia partition, nên Spark tắt rồi chạy lại vẫn không mất sự kiện. Spark: một engine cho batch, SQL, ML và streaming, chạy trong bộ nhớ. Parquet: lưu theo cột, có nén, chỉ đọc cột cần dùng. MongoDB: truy vấn chính là "gợi ý của một người" nên mỗi người một document, đọc theo khóa chính. HDFS + YARN: yêu cầu môn học, đồng thời là cách tính độc lập để kiểm chứng Spark SQL.

**K3. Đây là Lambda hay Kappa?**
Gần Lambda: lớp batch huấn luyện lại ALS trên toàn bộ dữ liệu theo đợt, lớp tốc độ (streaming) cập nhật trong vài giây. Chưa đầy đủ: lớp phục vụ chưa gộp số liệu batch với số liệu stream (`rating_stats` chỉ chứa rating đến qua streaming). Không chọn Kappa vì ALS cần học lại trên toàn bộ dữ liệu, không cập nhật từng sự kiện được.

**K4. Một thành phần chết thì sao?**

| Chết | Hệ quả | Phục hồi |
|---|---|---|
| Job streaming | Sự kiện vẫn nằm trong Kafka | Chạy lại từ checkpoint; đã thử kill -9: 1.474 = 1.500 − 26, không mất, không trùng |
| Kafka | Producer không gửi được; Spark chờ | Topic và sự kiện nằm trên volume `kafka-data`, khởi động lại là còn; Spark đọc tiếp từ offset trong checkpoint |
| MongoDB | App không đọc được | Một node, chưa có dự phòng; production dùng replica set |
| Job batch giữa chừng | App không bị ảnh hưởng | `meta.active` vẫn trỏ bản cũ; chỉ chuyển khi ghi xong |
| Cụm Hadoop | Không ảnh hưởng luồng chính | MapReduce chỉ để thống kê và đối chiếu |

Nói thẳng: chạy một máy nên mọi thành phần là điểm lỗi duy nhất; thiết kế cluster ở slide 13.

**K5. Tải tăng 100 lần thì nghẽn ở đâu trước?**
(1) Phần ghi MongoDB của streaming đang chạy ở driver: chuyển sang executor (foreachPartition hoặc MongoDB Spark connector), gom theo sách trước khi ghi. (2) Kafka 1 broker: thêm broker, replication 3, tăng partition. (3) Spark local: chạy trên YARN/Kubernetes với nhiều executor. (4) Huấn luyện ALS đã dùng 4,6/7,7 GiB RAM: cần cluster. Đã có `maxOffsetsPerTrigger` (20.000) để một batch không kéo quá nhiều sau khi job ngừng lâu.

**K6. Dữ liệu lưu ở đâu, giữ bao lâu?**
Kafka: mặc định 7 ngày, trên volume. Kho sự kiện Parquet: giữ lâu dài, dùng cho lần huấn luyện sau. Checkpoint: trên volume `spark-checkpoints`, mỗi query một thư mục. MongoDB: gợi ý giữ 2 phiên bản để quay lại; `trending` tự xóa sau 24 giờ (TTL).

**K7. Vì sao app không gọi Spark mà đọc MongoDB?**
Gợi ý được tính trước theo đợt, app chỉ đọc một document theo `_id` (trung vị khoảng 0,9 ms). App không phụ thuộc Spark đang chạy, và đổi mô hình chỉ là chuyển `meta.active` (nguyên tử).

**K8. Batch và stream có mâu thuẫn nhau không?**
Không: gợi ý đã loại sách người dùng từng chấm; app ẩn thêm sách vừa chấm qua streaming (`user_seen`); `clean.py` gộp kho sự kiện vào dữ liệu huấn luyện, cùng một cặp (người dùng, sách) thì giữ rating mới hơn. Kiểm tra độc lập: 0 trong 1.602.720 cặp gợi ý trùng sách đã chấm.

---

## Hảo – dữ liệu, kiến trúc, MapReduce, Spark SQL, cluster

**1. 6 triệu dòng thì có phải Big Data không?**
Không, và nhóm nói rõ đây là bộ phát triển. Nhóm đã đo giới hạn của một máy: RAM đỉnh 4,6/7,7 GiB (`reports/scaling/docker_stats_20261104_batch.csv`), 4 core chỉ nhanh 1,95 lần. Bộ UCSD đầy đủ có 229 triệu tương tác (gấp 38 lần) sẽ không vừa; phần xử lý Spark giữ nguyên khi lên cluster, nhưng cần đổi đường dẫn sang HDFS/S3, đặt master qua `spark-submit` hoặc biến `SPARK_MASTER`, và thay phép kiểm tra thư mục bằng `pathlib` trong `clean.py` bằng API FileSystem của Hadoop; thiết kế triển khai ở slide 13.

**2. Vì sao vẫn cần MapReduce khi đã có Spark?**
Môn học yêu cầu, và nó cho thấy mô hình map → shuffle/sort → reduce mà Spark kế thừa. Nhóm dùng nó như một cách tính độc lập để kiểm chứng Spark SQL: hai hệ thống, hai cách viết, cùng kết quả. Trên thực tế Spark nhanh hơn vì giữ dữ liệu trong bộ nhớ giữa các bước.

**3. Combiner có luôn dùng được không?**
Chỉ với phép toán có tính kết hợp và giao hoán (tổng, đếm). Trung bình thì không: trung bình của trung bình là sai. Nhóm mang theo (tổng, số lượt) và chia ở reducer; unit test kiểm tra kết quả không đổi khi combiner chạy 0, 1 hay nhiều lần.

**4. Vì sao có 4 map task khi chỉ có 3 block?**
Số map task thay đổi theo lần chạy: 05/11 có 4 (134 giây), lần tập 07/11 có 3 (job 52 giây), kết quả giống hệt nhau. Task thứ tư là bản speculative execution: Hadoop chạy thêm bản dự phòng cho task chậm, bản nào xong trước thì giữ, bản kia bị hủy; đây là cơ chế chịu lỗi của MapReduce.

**5. Có data locality không?**
Không: mọi map task đều rack-local, 0 data-local (`Rack-local map tasks` trong `reports/mapreduce/job_*.log`), vì DataNode và NodeManager chạy ở hai container riêng. Cluster thật đặt hai dịch vụ chung một node để map đọc block tại chỗ.

**6. Shuffle là gì, sao quan trọng?**
Là bước gom dữ liệu cùng key về cùng reducer/partition, phải truyền qua mạng và sắp xếp, nên tốn kém nhất. Nhóm giảm shuffle bằng combiner (MapReduce) và broadcast join (Spark SQL); query plan chỉ có một `Exchange` shuffle, cộng một `BroadcastExchange` gửi bảng sách nhỏ.

**7. Vì sao BroadcastHashJoin?**
Bảng sách 10.000 dòng (đọc từ `books.csv`) nhỏ hơn ngưỡng `spark.sql.autoBroadcastJoinThreshold` (10 MB), nên Spark gửi nguyên bảng tới mọi task, không phải shuffle bảng rating 6 triệu dòng.

**8. Vì sao LEFT JOIN chứ không phải inner join?**
Inner join âm thầm bỏ rating của sách không có trong `books.csv`. Nhóm dùng LEFT JOIN, gắn nhãn UNKNOWN và kiểm tra số dòng không đổi (5.976.479 trước và sau; 0 dòng UNKNOWN với Goodbooks).

**9. Parquet nhanh hơn CSV bao nhiêu? Sao số thay đổi?**
3,9–8,4 lần qua các lần đo (lần chạy cuối 4,22 s so với 1,08 s = 3,9 lần); dao động do bộ nhớ đệm của hệ điều hành. Bằng chứng chắc chắn là query plan: Parquet chỉ đọc 2 cột cần dùng (column pruning), có nén và schema sẵn.

**10. Có partition dữ liệu không?**
Không: mọi truy vấn quét toàn bộ, và partition theo book_id sẽ sinh 10.000 thư mục nhỏ. Với dữ liệu lớn hơn, partition theo thể loại (cách lấy tập con) hợp lý hơn.

**11. Triển khai lên cloud thế nào? Đã chạy thật chưa?**
Chưa chạy thật, đây là thiết kế: Kafka 3 broker, object storage (S3/MinIO) cho dữ liệu và checkpoint, Spark trên Kubernetes/YARN mở rộng theo executor, MongoDB replica set, Prometheus/Grafana, secret manager và TLS. Hiện mọi thành phần chạy một bản trên một máy (Kafka 1 broker, Mongo standalone, Spark `local[*]`, 1 DataNode), nên mỗi thành phần là một điểm lỗi đơn.

## Kiệt – Spark ML, MongoDB, hiệu năng

**12. Vì sao không chọn mô hình có RMSE tốt nhất?**
Người dùng thấy 10 cuốn, không thấy số sao. Explicit có RMSE 0,828 nhưng Precision@10 chỉ 0,008, thua cả sách phổ biến, vì đẩy lên sách ít người chấm mà toàn 5 sao. Implicit đạt 0,204. Chi tiết trong `docs/chon-mo-hinh.md`.

**13. ALS implicit khác explicit thế nào?**
Implicit coi mỗi lượt chấm là tín hiệu "quan tâm", số sao là độ tin cậy (c = 1 + alpha × rating), và học cả từ những ô trống, nên học được người dùng sẽ chọn cuốn nào, tức học xếp hạng.

**14. Có data leakage không?**
Không: baseline và độ phổ biến chỉ tính trên train; tham số chọn trên validation tách từ train; tập test dùng một lần. Gợi ý không chứa sách đã chấm: kiểm tra độc lập bằng pandas trên 1.602.720 cặp gợi ý và 534.240 cặp bù, 0 cặp đã chấm (`reports/nosql_evidence_20261107_221150.json`).

**15. Precision@10 0,204 có thấp không?**
Mỗi người chỉ có khoảng 15 cuốn "liên quan" trong 10.000 cuốn; ngẫu nhiên chỉ được khoảng 0,0015. 0,204 nghĩa là trung bình 2 trong 10 gợi ý trúng sách người dùng thật sự thích.

**16. Mô hình implicit có chỉ gợi ý sách phổ biến không?**
Không hẳn: gợi ý 1.224 đầu sách khác nhau, so với 39 của baseline, và user 1, 2, 3 nhận danh sách khác nhau. Nhưng có nghiêng về sách nổi tiếng hơn explicit (Animal Farm có trong cả ba danh sách); có thể thêm hình phạt độ phổ biến khi xếp hạng.

**17. Mô hình phục vụ học trên bao nhiêu dữ liệu?**
80%: app dùng chính mô hình đã đánh giá, fit trên tập train (4.780.968 rating); khoảng 1,2 triệu rating của tập test không được học. Khi triển khai thật nên fit lại trên 100% dữ liệu với tham số đã chọn (rank 50, alpha 1, regParam 0,1) rồi mới ghi gợi ý; nhóm chưa làm bước này.

**18. Dữ liệu trong MongoDB tổ chức thế nào? Vì sao không Cassandra?**
Mỗi người dùng một document (`_id` = user_id, mảng `recs` 30 phần tử, `fallback` 10 sách). Truy vấn theo `_id` dùng index có sẵn (IDHACK), trung vị 0,94 ms, p95 2,81 ms. Cassandra hợp khi ghi rất nhiều; tải ghi ở đây nhỏ.

**19. Đổi mô hình mới thì app có bị gián đoạn không?**
Danh sách gợi ý thì không: gợi ý mới ghi vào collection mới; ghi xong mới chuyển `meta.active` (cập nhật một document là nguyên tử); giữ 2 phiên bản để quay lại. Riêng collection `books` chưa có phiên bản: được xóa rồi nạp lại sau khi chuyển, nên trong vài giây tên sách có thể hiện "—".

**20. Vì sao 4 core chỉ nhanh 1,95 lần?**
Định luật Amdahl: phần tuần tự ở driver và shuffle giữa các vòng lặp ALS không tăng tốc được; 4 core còn chia cho Kafka và MongoDB.

## Tuấn – streaming, kiểm thử

**21. Event time khác processing time thế nào? Watermark để làm gì?**
Event time là lúc rating xảy ra, processing time là lúc Spark xử lý. Thống kê dựa trên event time. Watermark (10 phút) là ngưỡng chờ sự kiện trễ: cũ hơn thì bị loại khỏi phép tính có trạng thái, và trạng thái cũ được giải phóng nên bộ nhớ không tăng mãi.

**22. Vì sao output mode `update` cho cửa sổ?**
`append` chỉ ghi khi cửa sổ đã đóng (sau watermark), tức chậm khoảng 20 phút; `update` ghi ngay giá trị mới của cửa sổ đang mở, hợp với "sách đang hot".

**23. Có exactly-once không?**
Không hoàn toàn. Kafka + checkpoint là at-least-once; kho Parquet exactly-once nhờ file sink; `trending` ghi đè document (ReplaceOne) nên phát lại vẫn đúng. `rating_stats` bỏ qua batch đã ghi bằng `last_batch`, nhưng còn một khe hở: nếu job chết đúng lúc đang ghi MongoDB (`rating_stats` và `online_eval` cộng dồn trước khi ghi `last_batch`), batch phát lại có thể cộng lặp một phần; sửa triệt để bằng transaction (replica set) hoặc ghi idempotent theo `batch_id`.

**24. Đã thử sự cố thế nào?**
`bash scripts/stream_fault_test.sh` (khoảng 3 phút, lặp lại được): 1.500 sự kiện, `kill -9` cả Python lẫn JVM sau 45 giây, chạy lại từ checkpoint. `kafka_audit.py` đối chiếu Kafka với MongoDB: 1.474 = 1.500 − 26 sự kiện trễ, tổng cửa sổ 2.948 = 2 × 1.474, 15 sự kiện lỗi; lượt này 0 mất, 0 trùng (`reports/kafka_audit_20261107_220738.json`).

**25. Sự kiện trễ thì sao?**
Lớp nhanh bỏ: 26/26 ở lượt kill -9 và 19/19 ở lượt demo, với điều kiện sự kiện trễ đến sau khi job đã có watermark; Spark 3.5 lọc bằng watermark của batch trước, nên 1–2 batch đầu sau khi khởi động mới chưa lọc, vì vậy producer chỉ gửi sự kiện trễ sau 30 giây. Kho lưu trữ giữ lại, nên lần huấn luyện sau vẫn học được rating đó.

**26. Vì sao key Kafka là user_id?**
Mọi sự kiện của một người vào cùng một partition nên giữ đúng thứ tự; 3 partition cho phép đọc song song. Đánh đổi: người dùng hoạt động nhiều có thể làm lệch tải giữa các partition.

**27. Theo dõi hệ thống thế nào?**
Mỗi query có tên trên Spark UI (tab Structured Streaming); file `reports/stream_progress_*.json` ghi tốc độ vào/xử lý, thời gian batch, sự kiện trễ, bản trùng, kích thước trạng thái, watermark; `kafka_audit.py` lưu phép đối chiếu Kafka – MongoDB. Batch đầu sau khi khởi động mất 7–12 giây (JVM còn lạnh), các batch sau thường 3–6 giây, sát trigger 5 giây.

**28. Tỷ lệ trúng online trên app bằng 0? Vì sao hiện "2 / 672"?**
`producer.py` phát lại rating đã có trong dữ liệu huấn luyện, mà gợi ý đã loại mọi sách người dùng đã chấm, nên 670 lượt 4–5 sao từ producer không thể trúng. 2 lượt trúng là 2 cuốn user 1 chấm từ chính top-10 của mình bằng `scripts/rate_from_recs.py`; đánh giá chất lượng mô hình vẫn dựa trên tập test offline.

**29. Ghi MongoDB của streaming có phân tán không?**
Không: query `valid` gom mỗi micro-batch về driver rồi ghi bằng một client MongoDB, đủ cho tốc độ sự kiện của đồ án; `maxOffsetsPerTrigger` 20.000 giới hạn một batch sau khi job tắt lâu. Khi tải lớn, chuyển việc ghi sang executor (`foreachPartition` hoặc MongoDB Spark connector) và gộp theo sách trong Spark trước khi ghi.

**30. Tắt Docker thì Kafka có mất dữ liệu không?**
`docker compose down` không mất: Kafka lưu log trên volume `kafka-data` (trước đây topic nằm trong container và bị xóa, nhóm đã sửa). `down -v` xóa mọi volume, khi đó phải chạy `bash scripts/reset_streaming.sh --yes` để xóa luôn checkpoint, `stream_state` và kho `stream_events` cho khớp nhau.

**31. Kiểm thử gồm những gì?**
8 unit test, đều qua (MapReduce, hàm gợi ý, file tiến độ) và các phép đối chiếu độc lập: MapReduce so với Spark SQL, SQL so với DataFrame, Kafka so với MongoDB (`kafka_audit.py`, khối `mongo_check`), gợi ý không chứa sách đã chấm (`nosql_evidence.py`), và thử `kill -9` bằng `stream_fault_test.sh`.

## Câu chung

**32. Dữ liệu thay đổi thì hệ thống xử lý thế nào?** (câu giảng viên đã hỏi ở Session 8)
Hai tốc độ: thay đổi nhanh do streaming xử lý trong vài giây: ẩn cuốn vừa chấm, lấy tiếp từ 30 gợi ý, bù bằng sách chưa đọc, cập nhật điểm trung bình (chỉ từ stream). Hiệu ứng ẩn sách chỉ thấy khi người dùng chấm sách đang nằm trong top-10 (demo bằng `rate_from_recs.py`); rating phát lại bằng producer đã có trong dữ liệu huấn luyện nên không bao giờ nằm trong danh sách. Gu đọc, người dùng mới, sách mới chờ lần huấn luyện sau; rating từ streaming được lưu ra Parquet nên lần huấn luyện sau học được. Chi tiết: `Present/BookPulse - Cau hoi giang vien Session 8.md`.

**33. Điều khó nhất / bài học lớn nhất?**
Kiểm chứng: nhiều vấn đề chỉ lộ ra khi chạy thật và đọc ngược kết quả. Ví dụ: mô hình RMSE tốt nhưng gợi ý kém; sự kiện trễ mất khỏi dữ liệu huấn luyện; watermark chưa lọc ở batch đầu; topic Kafka mất khi `docker compose down`; sự kiện phát lại không thể làm đổi gợi ý; image MongoDB và pyarrow lỗi do không ghim phiên bản.

**34. Ai làm gì?** *(cần thống nhất)*
Hảo: dữ liệu, batch, Spark SQL, MapReduce, thiết kế triển khai. Tuấn: streaming và đánh giá. Kiệt: Spark ML, MongoDB, app. Nhóm xác nhận lại trước khi nộp.

**35. Chạy lại được không?**
Có: `docker compose up -d`, rồi `bash scripts/run_pipeline.sh` (khoảng 30 phút); MapReduce theo README mục 8 (trong Git Bash cần `export MSYS_NO_PATHCONV=1`); thử sự cố streaming bằng `bash scripts/stream_fault_test.sh`; mọi số đo tự ghi vào `reports/`.
