# BookPulse – Hỏi đáp Session 8

Tài liệu ôn hỏi đáp cho Team Presentation 2 (Final Project Progress Review). Số slide theo deck Session 8 (13 slide). Mọi số liệu lấy từ `bookpulse/reports/`.

## Nguyên tắc khi trả lời

- **Ai trình bày phần nào thì trả lời phần đó.** Kiệt điều phối: nghe câu hỏi, chỉ định người trả lời, bổ sung nếu cần.
- **Trả lời bằng số đo.** Buổi 5 nhóm nói "sẽ đo"; buổi này phải nói "đã đo, kết quả là …, file report là …".
- **Nói thẳng giới hạn.** Một máy, dữ liệu 6 triệu dòng, sự kiện mô phỏng, đo một lần. Giảng viên đánh giá cao việc nhóm tự chỉ ra vấn đề (slide 11) hơn là che đi.
- **Không biết thì ghi nhận.** "Nhóm chưa đo điểm này, sẽ bổ sung trong báo cáo cuối kỳ."

## Phần 1 – Trả lời nhanh trong một câu

| Nếu bị hỏi | Trả lời một câu |
|---|---|
| Đã chạy được gì? | Toàn bộ batch (CSV → Parquet → Spark SQL → ALS → MongoDB) và streaming (Kafka → Structured Streaming → MongoDB) trên Goodbooks-10k, 5.976.479 rating. |
| Spark SQL ở đâu? | `batch/analytics_sql.py`: temporary view `ratings`, `books`; 6 câu hỏi phân tích; query plan lưu ở `reports/sql_plan_*.txt`. |
| Kết quả ML? | RMSE 0,827 so với 0,954 của baseline; nhưng Precision@10 của ALS explicit chỉ 0,008. ALS implicit đạt 0,167, gấp 2,1 lần baseline sách phổ biến (0,080). |
| Bằng chứng NoSQL? | 53.424 document đọc lại được; query plan IDHACK đọc 1 document; p50 0,50 ms, p95 1,11 ms trên 1.000 lần đọc. |
| Streaming có đúng không? | 2.059 message trong Kafka → đúng 2.000 sự kiện hợp lệ được tính, 22 lỗi bị loại, 37 trùng bị bỏ, kể cả khi restart giữa chừng. |
| Hiệu năng? | 4 core nhanh hơn 1 core 1,95 lần (159 → 82 giây); RAM đỉnh 4,6 GiB; Parquet nhanh hơn CSV 8,4 lần. |
| Còn thiếu gì cho Buổi 10? | MapReduce, tập con UCSD, đổi sang mô hình theo Precision@10, sửa đánh giá online, thiết kế cluster/cloud. |

## Câu nhóm cần chốt trước buổi thuyết trình

- **Phân công thật** (slide 12 ghi theo phân công Buổi 5) – xác nhận còn đúng không.
- **Có demo trực tiếp không.** Nếu có, bật Docker trước 10 phút (cuối file Script).
- **Có đổi mô hình phục vụ sang ALS implicit trước Buổi 10 không** (câu 13). Hiện app vẫn dùng ALS explicit.
- **Push code mới lên GitHub trước buổi thuyết trình**, vì slide 13 dẫn link repo.

## Phần 2 – Câu hỏi dự đoán theo người trả lời

### Hảo – dữ liệu, Spark SQL, kiến trúc

**1. Goodbooks-10k chỉ 72 MB, sao gọi là Big Data?**
Nhóm đồng ý Goodbooks-10k không phải Big Data; nó là tập phát triển. Buổi này nhóm chứng minh pipeline chạy đúng và đo được giới hạn của một máy: RAM đỉnh 4,6/7,7 GiB, 4 core chỉ nhanh 1,95 lần. Bộ UCSD lớn hơn khoảng 38 lần (229 triệu tương tác) và sẽ chạy theo tập con ở Buổi 10. Kết luận nào cũng ghi rõ phạm vi dữ liệu đã chạy.

**2. Vì sao bước làm sạch không loại dòng nào?**
Bản Goodbooks-10k hiện tại đã được tác giả khử trùng và không có rating ngoài 1–5. Để chứng minh bước làm sạch hoạt động, nhóm chạy trên dữ liệu mẫu có chèn 1% dòng trùng và 0,5% dòng lỗi: bước làm sạch loại đúng 120 dòng lỗi và 240 dòng trùng (`reports/sample_smoke/clean_*.json`).

**3. Vì sao Spark chọn BroadcastHashJoin?**
Bảng books chỉ có 10.000 dòng, nhỏ hơn ngưỡng `spark.sql.autoBroadcastJoinThreshold` (mặc định 10 MB). Spark gửi nguyên bảng nhỏ tới mọi task nên không phải shuffle bảng ratings 6 triệu dòng. Plan chỉ còn một Exchange, là bước gom theo book_id của GROUP BY.

**4. Có partition dữ liệu Parquet không? Vì sao?**
Chưa partition. Các truy vấn hiện tại đều quét toàn bộ dữ liệu, và nếu partition theo book_id sẽ sinh 10.000 thư mục nhỏ, chậm hơn. Bảng book_stats chỉ 10.000 dòng nên ghi một file (`coalesce(1)`). Với UCSD, nhóm cân nhắc partition theo thể loại, vì đó là cách nhóm lấy tập con.

**5. Số shuffle partition là bao nhiêu, vì sao?**
`spark.sql.shuffle.partitions = 8` (trong `common/settings.py`), thay cho mặc định 200. Với 6 triệu dòng trên 4 core, 200 partition tạo quá nhiều task nhỏ. Khi chạy UCSD sẽ tăng lên và đo lại.

**6. So sánh CSV với Parquet có công bằng không?**
Cùng một truy vấn GROUP BY trên cùng dữ liệu, cùng phiên Spark. CSV mất 11,97 giây, Parquet 1,43 giây. Giới hạn: mới đo một lần. Báo cáo cuối kỳ sẽ đo 3 lần và lấy trung vị. Plan cũng cho thấy lý do: Parquet chỉ đọc 2 cột cần dùng.

**7. Spark SQL và DataFrame API khác nhau thế nào?**
Cùng đi qua bộ tối ưu Catalyst và ra cùng physical plan; chỉ khác cách viết. Nhóm kiểm tra bằng cách viết câu Q4 theo cả hai cách: 10/10 dòng kết quả giống hệt (`sql_equals_dataframe_api: true`).

**8. AdaptiveSparkPlan trong plan là gì?**
Adaptive Query Execution, bật mặc định từ Spark 3.2. Spark có thể đổi kế hoạch trong lúc chạy dựa trên số liệu thật, ví dụ gộp bớt partition sau shuffle hoặc đổi sang broadcast join.

**9. Hệ thống chạy trên hạ tầng nào? Lên cluster thế nào?**
Hiện chạy bằng Docker trên một máy: Spark local mode, Kafka, MongoDB và app. Thiết kế Buổi 10: dữ liệu lên object storage (S3 hoặc MinIO), Spark chạy trên Standalone, YARN hoặc Kubernetes, tách compute khỏi storage; MongoDB dùng replica set. Phần này mới là thiết kế, nhóm chưa chạy thật trên cluster.

### Kiệt – Spark ML, hiệu năng, MongoDB

**10. RMSE 0,827 là tốt hay xấu?**
Tự nó chưa nói lên gì; phải so với baseline. Đoán bằng điểm trung bình của từng cuốn sai 0,954, nên ALS giảm sai số 13,3%. Dữ liệu lệch về 4–5 sao (69%), nên ngay cả baseline đơn giản cũng không sai nhiều.

**11. Vì sao Precision@10 của ALS explicit thấp như vậy?**
ALS explicit học để đoán đúng số sao trên những cặp người–sách đã có rating, không học cuốn nào người dùng sẽ chọn đọc. Nó cho điểm cao những cuốn ít người chấm nhưng ai chấm cũng cho 5 sao, như các tuyển tập Calvin and Hobbes. Những cuốn đó hiếm khi nằm trong tập test của người dùng, nên Precision@10 thấp. Đây là hiện tượng đã biết của mô hình tối ưu RMSE.

**12. ALS implicit khác gì?**
Mỗi lượt chấm được coi là tín hiệu "có quan tâm", số sao thành độ tin cậy (c = 1 + alpha × rating). Mô hình học trên cả những ô trống, nên phân biệt được sách người dùng sẽ đọc với sách họ không đọc, tức là học xếp hạng. Cấu hình đã chạy: rank 20, regParam 0,1, alpha 1,0, fit 59 giây.

**13. So sánh implicit với explicit có công bằng không? Sao chưa dùng luôn?**
Cùng tập train/test, cùng 4.987 người dùng đánh giá, cùng định nghĩa "liên quan" (chấm 4–5 sao trong test), cùng loại sách đã đọc. Chưa đưa vào app vì mới thử một bộ tham số: Buổi 10 nhóm tinh chỉnh alpha và rank trên tập validation (không chọn trên test), rồi mới đổi mô hình phục vụ. **(cần thống nhất)**

**14. Precision@10 0,167 nghe vẫn thấp?**
Mỗi người chỉ có khoảng 15 cuốn "liên quan" trong tập test, trên tổng 10.000 cuốn. Đoán ngẫu nhiên chỉ được khoảng 0,0015, nên 0,167 nghĩa là trung bình gần 2 trên 10 gợi ý trúng sách người dùng thật sự thích.

**15. Vì sao RMSE của các lần đo theo core lệch nhau một chút (0,833 / 0,832 / 0,833)?**
Số core đổi thì cách chia partition đổi, nên kết quả `randomSplit` và cách ALS chia block cũng đổi theo, dù seed cố định. Mức lệch dưới 0,001, không ảnh hưởng kết luận. Các lần đo tăng tốc chỉ dùng để so thời gian.

**16. Vì sao 4 core chỉ nhanh 1,95 lần?**
Theo định luật Amdahl, phần chạy tuần tự (driver điều phối, shuffle giữa các vòng lặp ALS) không tăng tốc được. Ngoài ra Docker chỉ có 4 CPU cho cả Spark, Kafka và MongoDB. Hiệu suất song song khoảng 49%.

**17. Có data leakage không?**
Baseline và độ phổ biến chỉ tính trên tập train. Tham số chọn trên phần validation tách từ train (TrainValidationSplit). Tập test chỉ dùng để đánh giá cuối. Sách đã chấm trong train bị loại khỏi danh sách gợi ý khi đánh giá.

**18. Vì sao TrainValidationSplit mà không phải CrossValidator?**
4 tổ hợp tham số, mỗi lần fit trên 4,8 triệu dòng. CrossValidator 3 fold sẽ tốn gấp 3 thời gian. TrainValidationSplit đủ để chọn tham số ở giai đoạn này.

**19. Dữ liệu trong MongoDB được tổ chức thế nào? Vì sao?**
Mỗi người dùng một document: `_id` là user_id, mảng `recs` 30 phần tử (rank, book_id, score), kèm phiên bản mô hình. App chỉ có một truy vấn: đọc theo `_id` và `$slice` 10 phần tử. Plan IDHACK chỉ đọc đúng 1 document, nên không cần thêm index nào.

**20. Vì sao lưu 30 mà app chỉ hiện 10?**
Streaming ghi những cuốn người dùng vừa chấm vào `user_seen`; app bỏ các cuốn đó rồi lấy 10 cuốn tiếp theo, không phải huấn luyện lại mô hình.

**21. Đổi phiên bản mô hình thế nào để không lỗi?**
Gợi ý mới ghi vào collection mới `recommendations_<phiên bản>`. Ghi xong mới cập nhật document `meta.active`; một lần cập nhật một document trong MongoDB là nguyên tử. Nhóm giữ 2 phiên bản gần nhất để có thể quay lại.

**22. Sao không dùng MongoDB Connector for Spark?**
Nhóm ghi bằng `foreachPartition` và `bulk_write` của pymongo (upsert theo `_id`, 1.000 thao tác mỗi lần). Cách này không cần thêm jar và dễ kiểm soát. Connector là phương án thay thế tương đương.

### Tuấn – streaming, rủi ro, kế hoạch

**23. Hệ thống có exactly-once không?**
Spark đọc Kafka kèm checkpoint đảm bảo at-least-once. Nhóm thêm hai lớp: bỏ trùng theo `event_id` (trạng thái nằm trong checkpoint), và ghi lại batch cuối đã áp dụng vào `stream_state` để bỏ qua batch bị phát lại. Bài test restart cho kết quả khớp tuyệt đối. Giới hạn: nếu job chết đúng lúc đang ghi một batch, batch đó có thể bị cộng lặp một phần. Hướng sửa là dùng transaction (cần replica set) hoặc ghi theo batch_id.

**24. Thử restart là dừng "nhẹ nhàng" hay dừng đột ngột?**
Dừng có kiểm soát (gửi SIGINT, như bấm Ctrl+C). Dừng đột ngột (kill -9) là trường hợp xấu hơn và nằm trong kế hoạch Buổi 10.

**25. Vì sao watermark 10 phút?**
event_time do producer gán bằng thời điểm gửi, nên độ trễ chỉ đến từ thời gian xử lý, thường vài giây. 10 phút là ngưỡng rộng rãi, vẫn đủ để Spark giải phóng trạng thái bỏ trùng cũ, không để bộ nhớ tăng mãi.

**26. Lỗi topic Kafka là gì, sửa thế nào?**
Kafka chỉ tự tạo topic khi producer gửi tin đầu tiên. Nếu job streaming khởi động trước, Spark báo `UnknownTopicOrPartitionException` và dừng. Nhóm gặp lỗi này ở lần chạy đầu. Đã sửa: `stream_stats.py` tạo topic 3 partition bằng KafkaAdminClient trước khi subscribe.

**27. 168 dòng/giây có phải giới hạn không?**
Không. Producer chỉ gửi 20 sự kiện/giây nên hệ thống đang chờ dữ liệu. Muốn đo giới hạn cần tăng `--rate` của producer; việc này nằm trong kế hoạch đo hiệu năng Buổi 10.

**28. Vì sao topic có 3 partition, key là gì?**
Key là user_id, nên mọi sự kiện của một người dùng vào cùng một partition và giữ đúng thứ tự. 3 partition cho phép Spark đọc song song 3 luồng. Số sự kiện chia khá đều: 681 / 656 / 722.

**29. Đánh giá online đo gì, vì sao bằng 0%?**
Khi người dùng chấm 4–5 sao, app kiểm tra cuốn đó có nằm trong top-10 hiện tại của họ không. Bằng 0/1.385 vì producer phát lại rating đã có trong tập train, mà top-10 đã loại sách người dùng đã chấm. Nghĩa là chỉ số này không đo được gì với dữ liệu phát lại. Hướng sửa: phát lại phần test giữ lại, và mô hình phục vụ chỉ học trên phần train.

**30. Đã đo độ trễ end-to-end chưa?**
Chưa. Trigger là 5 giây nên độ trễ ước tính vài giây, nhưng nhóm chưa đo. Kế hoạch: so `event_time` với thời điểm ghi vào MongoDB.

**31. Kế hoạch Buổi 10 có kịp không?**
Hai việc đã sửa xong (topic Kafka, ghim phiên bản). ALS implicit đã chạy được, chỉ còn tinh chỉnh. MapReduce là job đơn giản (đếm rating theo sách) và có sẵn kết quả Spark SQL để đối chiếu. Nếu thiếu thời gian, UCSD là việc lùi lại trước.

### Câu hỏi chung

**32. Từ Buổi 5 đến nay thay đổi gì so với đề xuất?**
Thêm bước Spark SQL; lưu top-30 thay vì top-10; job streaming tự tạo topic; ghim phiên bản phần mềm; thêm thử nghiệm ALS implicit sau khi phát hiện vấn đề top-10. Kiến trúc tổng thể giữ nguyên.

**33. Phần khó nhất là gì?**
Không phải viết code mà là kiểm chứng: số đúng chưa, có đếm trùng không. Ba lỗi thật (topic Kafka, MongoDB, pyarrow) và vấn đề top-10 chỉ lộ ra khi chạy end-to-end và đọc ngược lại kết quả.

**34. Làm sao biết các con số không bị chọn lọc?**
Mỗi bước tự ghi file JSON vào `reports/` kèm thời điểm chạy; lần chạy thử trên dữ liệu mẫu và lần chạy bị thay thế cũng được giữ lại (`reports/sample_smoke/`, `reports/superseded/`). Ai cũng chạy lại được theo README.
