# BookPulse – Hỏi đáp Session 10 (Final Assessment)

Ôn hỏi đáp cho bài cuối kỳ. Số liệu theo `bookpulse/reports/` (lần chạy lại ngày 06/11/2026) và báo cáo Word. Syllabus ghi giảng viên hỏi về: **quyết định triển khai, xử lý phân tán, khả năng mở rộng và chuẩn đầu ra môn học**.

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
| Mô hình nào, tốt cỡ nào? | ALS implicit, Precision@10 0,204 (sách phổ biến 0,078; ALS explicit 0,008) |
| Vì sao MongoDB? | Một truy vấn duy nhất theo user_id → document mỗi người; đọc dưới 1 ms |
| Streaming có gì? | Kafka 3 partition; 4 query: watermark + bỏ trùng, cửa sổ trượt 10/5 phút, kho lưu trữ, đếm lỗi; chịu được kill -9 |
| Có phải Big Data không? | Goodbooks là bộ phát triển; đã đo giới hạn một máy; bộ đầy đủ ×38 cần cluster (slide 13) |

---

## Hảo – dữ liệu, kiến trúc, MapReduce, Spark SQL, cluster

**1. 6 triệu dòng thì có phải Big Data không?**
Không, và nhóm nói rõ đây là bộ phát triển. Nhóm đã đo giới hạn của một máy: 4,6/7,7 GiB RAM, 4 core chỉ nhanh 1,95 lần. Bộ UCSD đầy đủ có 229 triệu tương tác (gấp 38 lần) sẽ không vừa; vì vậy code viết trên Spark và Hadoop để chạy trên cluster không phải viết lại, và có thiết kế triển khai ở slide 13.

**2. Vì sao vẫn cần MapReduce khi đã có Spark?**
Môn học yêu cầu, và nó cho thấy mô hình map → shuffle/sort → reduce mà Spark kế thừa. Nhóm dùng nó như một cách tính độc lập để kiểm chứng Spark SQL: hai hệ thống, hai cách viết, cùng kết quả. Trên thực tế Spark nhanh hơn vì giữ dữ liệu trong bộ nhớ giữa các bước.

**3. Combiner có luôn dùng được không?**
Chỉ với phép toán có tính kết hợp và giao hoán (tổng, đếm). Trung bình thì không: trung bình của trung bình là sai. Nhóm mang theo (tổng, số lượt) và chia ở reducer; unit test kiểm tra kết quả không đổi khi combiner chạy 0, 1 hay nhiều lần.

**4. Vì sao có 4 map task khi chỉ có 3 block?**
Speculative execution: Hadoop chạy thêm bản dự phòng cho task chậm, bản nào xong trước thì giữ, bản kia bị hủy (counter "Killed map tasks"). Đây là cơ chế chịu lỗi của MapReduce.

**5. Shuffle là gì, sao quan trọng?**
Là bước gom dữ liệu cùng key về cùng reducer/partition, phải truyền qua mạng và sắp xếp, nên tốn kém nhất. Nhóm giảm shuffle bằng combiner (MapReduce) và broadcast join (Spark SQL); query plan chỉ có một Exchange.

**6. Vì sao BroadcastHashJoin?**
Bảng sách 10.000 dòng nhỏ hơn ngưỡng `spark.sql.autoBroadcastJoinThreshold` (10 MB), nên Spark gửi nguyên bảng tới mọi task, không phải shuffle bảng rating 6 triệu dòng.

**7. Vì sao LEFT JOIN chứ không phải inner join?**
Inner join âm thầm bỏ rating của sách không có trong `books.csv`. Nhóm dùng LEFT JOIN, gắn nhãn UNKNOWN và kiểm tra số dòng không đổi (5.976.479 trước và sau; 0 dòng UNKNOWN với Goodbooks).

**8. Parquet nhanh hơn CSV bao nhiêu? Sao số thay đổi?**
3,9–8,4 lần qua các lần đo; dao động do bộ nhớ đệm của hệ điều hành. Bằng chứng chắc chắn là query plan: Parquet chỉ đọc 2 cột cần dùng (column pruning), có nén và schema sẵn.

**9. Có partition dữ liệu không?**
Không: mọi truy vấn quét toàn bộ, và partition theo book_id sẽ sinh 10.000 thư mục nhỏ. Với dữ liệu lớn hơn, partition theo thể loại (cách lấy tập con) hợp lý hơn.

**10. Triển khai lên cloud thế nào? Đã chạy thật chưa?**
Chưa chạy thật, đây là thiết kế: Kafka 3 broker, object storage (S3/MinIO) cho dữ liệu và checkpoint, Spark trên Kubernetes/YARN mở rộng theo executor, MongoDB replica set, Prometheus/Grafana, secret manager và TLS. Huấn luyện chạy theo lịch trên cụm tạm thời để tiết kiệm chi phí.

## Kiệt – Spark ML, MongoDB, hiệu năng

**11. Vì sao không chọn mô hình có RMSE tốt nhất?**
Người dùng thấy 10 cuốn, không thấy số sao. Explicit có RMSE 0,828 nhưng Precision@10 chỉ 0,008, thua cả sách phổ biến, vì đẩy lên sách ít người chấm mà toàn 5 sao. Implicit đạt 0,204. Chi tiết trong `docs/chon-mo-hinh.md`.

**12. ALS implicit khác explicit thế nào?**
Implicit coi mỗi lượt chấm là tín hiệu "quan tâm", số sao là độ tin cậy (c = 1 + alpha × rating), và học cả từ những ô trống, nên học được người dùng sẽ chọn cuốn nào, tức học xếp hạng.

**13. Có data leakage không?**
Không: baseline và độ phổ biến chỉ tính trên train; tham số chọn trên validation tách từ train; tập test dùng một lần. Gợi ý không chứa sách đã chấm trong train.

**14. Precision@10 0,204 có thấp không?**
Mỗi người chỉ có khoảng 15 cuốn "liên quan" trong 10.000 cuốn; ngẫu nhiên chỉ được khoảng 0,0015. 0,204 nghĩa là trung bình 2 trong 10 gợi ý trúng sách người dùng thật sự thích.

**15. Mô hình implicit có chỉ gợi ý sách phổ biến không?**
Không hẳn: gợi ý 1.224 đầu sách khác nhau, so với 39 của baseline. Nhưng có nghiêng về sách nổi tiếng hơn explicit; có thể thêm hình phạt độ phổ biến khi xếp hạng.

**16. Dữ liệu trong MongoDB tổ chức thế nào? Vì sao không Cassandra?**
Mỗi người dùng một document (`_id` = user_id, mảng `recs` 30 phần tử, `fallback` 10 sách). Truy vấn theo `_id` dùng index có sẵn (IDHACK). Cassandra hợp khi ghi rất nhiều; tải ghi ở đây nhỏ.

**17. Đổi mô hình mới thì app có bị gián đoạn không?**
Không: gợi ý mới ghi vào collection mới; ghi xong mới chuyển `meta.active` (cập nhật một document là nguyên tử). Giữ 2 phiên bản để quay lại.

**18. Vì sao 4 core chỉ nhanh 1,95 lần?**
Định luật Amdahl: phần tuần tự ở driver và shuffle giữa các vòng lặp ALS không tăng tốc được; 4 core còn chia cho Kafka và MongoDB.

## Tuấn – streaming, kiểm thử

**19. Event time khác processing time thế nào? Watermark để làm gì?**
Event time là lúc rating xảy ra, processing time là lúc Spark xử lý. Thống kê dựa trên event time. Watermark (10 phút) là ngưỡng chờ sự kiện trễ: cũ hơn thì bị loại khỏi phép tính có trạng thái, và trạng thái cũ được giải phóng nên bộ nhớ không tăng mãi.

**20. Vì sao output mode `update` cho cửa sổ?**
`append` chỉ ghi khi cửa sổ đã đóng (sau watermark), tức chậm khoảng 20 phút; `update` ghi ngay giá trị mới của cửa sổ đang mở, hợp với "sách đang hot".

**21. Có exactly-once không?**
Kafka + checkpoint là at-least-once. Kho Parquet exactly-once nhờ file sink. `trending` ghi đè document nên phát lại vẫn đúng. `rating_stats` bỏ qua batch đã ghi bằng `last_batch`; rủi ro còn lại là job chết đúng lúc đang ghi MongoDB (sửa triệt để bằng transaction với replica set).

**22. Đã thử sự cố thế nào?**
`kill -9` cả Python lẫn JVM giữa chừng rồi chạy lại từ checkpoint; đối chiếu với `kafka_audit.py`: 3.762 = 3.805 − 43 sự kiện trễ, không mất, không trùng.

**23. Sự kiện trễ thì sao?**
Lớp nhanh bỏ (đếm được qua `numRowsDroppedByWatermark` trong file progress). Kho lưu trữ giữ lại, nên lần huấn luyện sau vẫn học được rating đó.

**24. Vì sao key Kafka là user_id?**
Mọi sự kiện của một người vào cùng một partition nên giữ đúng thứ tự; 3 partition cho phép đọc song song. Đánh đổi: người dùng hoạt động nhiều có thể làm lệch tải giữa các partition.

**25. Theo dõi hệ thống thế nào?**
Mỗi query có tên trên Spark UI (tab Structured Streaming). File `reports/stream_progress_*.json` ghi tốc độ vào/xử lý, thời gian batch, sự kiện trễ, bản trùng, kích thước trạng thái, watermark. Đang thấy batch 7–14 giây trong khi trigger là 5 giây: 4 query cùng đọc Kafka trên 4 core.

**26. Kiểm thử gồm những gì?**
8 unit test (MapReduce, hàm gợi ý, file tiến độ) và các phép đối chiếu độc lập: MapReduce so với Spark SQL, SQL so với DataFrame, streaming so với Kafka, đọc lại MongoDB.

## Câu chung

**27. Dữ liệu thay đổi thì hệ thống xử lý thế nào?** (câu giảng viên đã hỏi ở Session 8)
Hai tốc độ: thay đổi nhanh (vừa đọc, vừa chấm) do streaming xử lý trong vài giây: ẩn cuốn đã đọc, lấy tiếp từ 30 gợi ý, bù bằng sách chưa đọc, cập nhật điểm trung bình. Gu đọc, người dùng mới, sách mới chờ lần huấn luyện sau; rating từ streaming được lưu ra Parquet nên lần huấn luyện sau học được. Chi tiết: `Present/BookPulse - Cau hoi giang vien Session 8.md`.

**28. Điều khó nhất / bài học lớn nhất?**
Kiểm chứng: nhiều vấn đề chỉ lộ ra khi chạy thật và đọc ngược kết quả. Ví dụ: mô hình RMSE tốt nhưng gợi ý kém; sự kiện trễ mất khỏi dữ liệu huấn luyện; topic Kafka chưa tồn tại làm job dừng; image MongoDB và pyarrow lỗi do không ghim phiên bản.

**29. Ai làm gì?** *(cần thống nhất)*
Hảo: dữ liệu, batch, Spark SQL, MapReduce, thiết kế triển khai. Tuấn: streaming và đánh giá. Kiệt: Spark ML, MongoDB, app. Nhóm xác nhận lại trước khi nộp.

**30. Chạy lại được không?**
Có: `docker compose up -d`, rồi `bash scripts/run_pipeline.sh` (khoảng 30 phút); MapReduce theo README mục 8; mọi số đo tự ghi vào `reports/`.
