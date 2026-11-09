# BookPulse – Script thuyết trình Session 8

Team Presentation 2 (10%): Final Project Progress Review · CLO1–CLO5

Mọi con số trong script lấy từ `bookpulse/reports/` (lần chạy ngày 04/11/2026, Goodbooks-10k, Docker trên một máy 4 CPU, 7,7 GiB RAM). Nếu chạy lại, số có thể lệch nhẹ; khi đó đọc theo số trên slide.

## Phân công và thời lượng

| Người trình bày | Slide | Nội dung | Thời lượng |
| --- | --- | --- | --- |
| Lương Huỳnh Ngọc Hảo | 1–6 | Mở đầu, tiến độ, kiến trúc cập nhật, ingestion, Spark SQL, query plan | ~5 phút 30 giây |
| Nguyễn Tấn Kiệt | 7–9 | Spark ML, hiệu năng theo số core, MongoDB; điều phối hỏi đáp | ~3 phút 30 giây + hỏi đáp |
| Đinh Trần Quốc Tuấn | 10–13 | Streaming, bên trong job streaming, rủi ro và khắc phục, kế hoạch Buổi 10 | ~4 phút 30 giây |
| Lương Huỳnh Ngọc Hảo | 14–16 | Cập nhật sau Buổi 8, câu hỏi của giảng viên, cảm ơn | ~2 phút 30 giây |

Slide 17 là phụ lục (kiến trúc theo từng luồng, bản cũ của slide 3), chỉ mở khi được hỏi.

Tổng thời gian nói khoảng 16 phút. Nếu lớp quy định 10–12 phút: Hảo rút slide 6 còn 30 giây (chỉ nói câu Parquet nhanh hơn 8,4 lần), Kiệt rút slide 8 còn 30 giây, Tuấn nói slide 11 trong 30 giây (chỉ hai nhánh hợp lệ / lỗi và last_batch), Hảo nói slide 14 trong 45 giây (chỉ đọc cột trạng thái).

So với Session 5, thông điệp chính của buổi này là: **hệ thống đã chạy thật end-to-end, mọi số trên slide đều là số đo, và khi chạy thật nhóm tìm ra 6 vấn đề: 2 đã sửa xong, 1 đã có thử nghiệm khắc phục kèm số liệu, 3 còn lại có kế hoạch cho Buổi 10.**

---

## Phần 1 – Lương Huỳnh Ngọc Hảo (slide 1–6)

### Slide 1 – BookPulse (~20 giây)

Xin chào thầy cô và các bạn. Nhóm 06 gồm Lương Huỳnh Ngọc Hảo, Đinh Trần Quốc Tuấn và Nguyễn Tấn Kiệt.

Ở Buổi 5, nhóm trình bày đề xuất BookPulse, khi đó chưa có phần nào chạy. Hôm nay là Team Presentation 2: báo cáo tiến độ. Toàn bộ pipeline batch và streaming đã chạy end-to-end trên bộ Goodbooks-10k, gần 6 triệu rating. Mọi con số nhóm trình bày hôm nay đều là số đo thật, lưu trong thư mục reports của repo.

### Slide 2 – Từ đề xuất đến hệ thống chạy được (~1 phút)

Bốn con số tóm tắt tiến độ.

Một: gần 6 triệu rating, chính xác 5.976.479 dòng, đã được làm sạch và lưu sang Parquet, gồm 53.424 người dùng và 10.000 cuốn sách.

Hai: mô hình ALS đoán số sao với sai số RMSE 0,827, tốt hơn baseline tốt nhất là 0,954.

Ba: 53.424 người dùng đều đã có danh sách gợi ý trong MongoDB. App đọc một người mất khoảng 1 mili giây.

Bốn: luồng streaming xử lý 2.000 sự kiện, có tắt và bật lại giữa chừng, mà không mất hay đếm trùng sự kiện nào.

Khung xanh bên trái là những gì đã chạy và đo được trong Buổi 6 đến 8. Khung cam bên phải là phần còn lại đến Buổi 10: chạy dữ liệu lớn hơn là bộ UCSD, thêm thành phần MapReduce mà bài cuối kỳ yêu cầu, sửa cách đánh giá online, và thiết kế triển khai trên cluster.

### Slide 3 – Kiến trúc cập nhật: đã chạy và kế hoạch (~1 phút 15 giây)

Đây là kiến trúc của Buổi 5, nay ghi rõ phần nào đã chạy và phần nào còn là kế hoạch. Đọc từ trái sang phải theo năm cột: nguồn, thu nhận, xử lý bằng Spark, lưu trữ và phục vụ, rồi ứng dụng. Nét liền là đã chạy và đo ở Buổi 8; khung tím nét đứt là kế hoạch đến Buổi 10.

Khung xanh phía trên là luồng batch: CSV Goodbooks đi qua làm sạch, lưu Parquet, phân tích bằng Spark SQL, huấn luyện ALS, rồi ghi top-30 gợi ý cho mỗi người vào MongoDB. So với Buổi 5 có hai thay đổi. Thứ nhất, nhóm thêm bước Spark SQL để trả lời các câu hỏi phân tích; lát nữa em trình bày. Thứ hai, MongoDB lưu sẵn 30 gợi ý thay vì 10: khi người dùng vừa chấm một cuốn, app chỉ cần bỏ cuốn đó đi và lấy cuốn tiếp theo, không phải huấn luyện lại.

Khung xanh phía dưới là luồng streaming: producer phát lại rating thật thành sự kiện, gửi vào Kafka 3 partition; Spark Structured Streaming cứ 5 giây xử lý một micro-batch, lọc lỗi, bỏ trùng, có watermark và checkpoint, rồi ghi số liệu mới vào MongoDB. App Streamlit đọc cả hai phần từ MongoDB.

Các khung tím là việc còn lại: tập con UCSD để chạy dữ liệu lớn hơn, job Hadoop MapReduce đối chiếu với Spark SQL, bản thiết kế cluster hoặc cloud, và mũi tên giữa hai luồng: lưu sự kiện streaming để lần huấn luyện sau học được, chạy theo lịch. Hiện mọi thứ chạy bằng Docker trên một máy.

### Slide 4 – Ingestion: CSV → làm sạch → Parquet (~1 phút)

Bước đầu tiên là đọc và làm sạch dữ liệu. Bảng bên trái lấy từ file report của bước clean.

Với Goodbooks-10k: đọc vào 5.976.479 dòng, loại 0 dòng lỗi, 0 dòng trùng, ghi ra đúng 5.976.479 dòng, mất 63 giây.

Không loại dòng nào là đúng chứ không phải bước làm sạch không chạy: tác giả bộ Goodbooks đã khử trùng sẵn ở bản hiện tại. Để chứng minh bước này hoạt động, nhóm chạy thêm trên dữ liệu mẫu, trong đó cố ý chèn 1% dòng trùng và 0,5% dòng lỗi. Cột bên phải cho thấy bước làm sạch loại đúng 120 dòng lỗi và 240 dòng trùng.

Biểu đồ bên phải là phân bố số sao: 4 và 5 sao chiếm gần 69%, 1 sao chỉ 2,1%. Người dùng Goodreads chủ yếu chấm sách họ thích, nên dữ liệu lệch về điểm cao. Điểm này giải thích vì sao đoán điểm trung bình đã cho sai số không lớn lắm.

### Slide 5 – Pipeline Spark SQL (~1 phút 15 giây)

Sau khi làm sạch, nhóm đăng ký dữ liệu thành hai temporary view là ratings và books, rồi trả lời các câu hỏi phân tích bằng Spark SQL. Bên trái là một truy vấn mẫu: nối hai bảng, đếm số lượt chấm và tính điểm trung bình của từng cuốn, lấy 10 cuốn được chấm nhiều nhất.

Bên phải là kết quả: The Hunger Games đứng đầu với 22.806 lượt, tiếp theo là Harry Potter tập 1 và Giết con chim nhại. Twilight được chấm rất nhiều nhưng điểm trung bình chỉ 3,21, tức là phổ biến chưa chắc đã được thích.

Ba ô bên dưới là ba con số từ các truy vấn khác. Độ phủ ma trận chỉ 1,12%, nghĩa là gần 99% ô trong bảng người dùng nhân sách là trống, đúng loại dữ liệu mà ALS được thiết kế để xử lý. Mỗi người dùng chấm trung vị 111 cuốn. Và nhóm viết cùng câu hỏi bằng cả SQL lẫn DataFrame API: hai cách cho ra 10 dòng giống hệt nhau.

Bảng kết quả theo từng cuốn còn được ghi ra Parquet thành bảng book_stats, để dùng cho báo cáo và app.

### Slide 6 – Query plan (~1 phút)

Slide này là physical plan của truy vấn vừa rồi, Spark in ra bằng lệnh EXPLAIN. Đọc từ trên xuống.

Bước một, Spark đọc file Parquet nhưng chỉ đọc 2 cột cần dùng là book_id và rating. Đây là ưu điểm của định dạng cột.

Bước hai, phép join. Bảng sách chỉ có 10 nghìn dòng, nên Spark tự chọn BroadcastHashJoin: gửi nguyên bảng nhỏ tới mọi task, tránh phải shuffle bảng 6 triệu dòng.

Bước ba và bốn: mỗi partition đếm sơ bộ trước, rồi mới có đúng một lần shuffle, tức bước Exchange, để gom theo book_id. Cuối cùng gộp kết quả và lấy top 10.

Bên phải là lý do nhóm lưu Parquet: cùng một truy vấn GROUP BY trên 6 triệu dòng, đọc từ CSV gốc mất 12 giây, từ Parquet chỉ 1,4 giây, nhanh hơn 8,4 lần.

*Chuyển lượt:* Tiếp theo, Kiệt trình bày kết quả Spark ML và phần lưu trữ MongoDB.

---

## Phần 2 – Nguyễn Tấn Kiệt (slide 7–9)

### Slide 7 – Spark ML: RMSE tốt, nhưng top-10 chưa tốt (~1 phút 30 giây)

Cảm ơn Hảo. Em trình bày kết quả của mô hình ALS.

Nhóm chia ngẫu nhiên 80% dữ liệu để huấn luyện, 20% để kiểm tra. ALS là Estimator; nhóm dùng TrainValidationSplit để thử 4 tổ hợp tham số trên một phần của tập train. Bộ tốt nhất là rank 20, regParam 0,1, huấn luyện mất 4 phút.

Biểu đồ trái là RMSE, thấp hơn là tốt hơn. Đoán mọi rating bằng điểm trung bình chung sai 0,991 sao. Đoán bằng điểm trung bình của từng cuốn sai 0,954. ALS sai 0,827, giảm 13,3% so với baseline tốt nhất. Vậy về đoán số sao, ALS tốt hơn rõ.

Nhưng người dùng không thấy số sao dự đoán, họ thấy danh sách 10 cuốn. Biểu đồ phải đo đúng điều đó bằng Precision@10. Kết quả ngược lại: ALS chỉ đạt 0,008, trong khi gợi ý cho ai cũng là 10 cuốn phổ biến nhất lại đạt 0,080, cao gấp 10 lần.

Lý do: ALS dạng explicit học cách đoán đúng số sao, không học cách xếp hạng. Nó cho điểm rất cao những cuốn ít người chấm mà ai chấm cũng cho 5 sao. Ở slide sau các bạn sẽ thấy app gợi ý cho người dùng số 1 toàn tuyển tập truyện tranh Calvin and Hobbes.

Nhóm đã thử hướng sửa là ALS implicit: coi mỗi lượt chấm là một tín hiệu quan tâm, số sao là độ tin cậy. Cột cuối cho thấy Precision@10 lên 0,167, gấp 2,1 lần baseline sách phổ biến. Đây là kết quả thử nghiệm; Tuấn sẽ nói kế hoạch đưa vào hệ thống ở slide rủi ro.

### Slide 8 – Hiệu năng: tăng số core (~1 phút)

Để đo khả năng song song, nhóm chạy lại cùng một lần huấn luyện ALS, rank 10, trên 4,78 triệu dòng, với Spark dùng 1, 2 rồi 4 core.

1 core mất 159 giây, 2 core 107 giây, 4 core 82 giây. Tức là nhanh hơn 1,95 lần với 4 core, chứ không phải 4 lần; hiệu suất song song khoảng 49%.

Có hai lý do chính. Một, mỗi vòng lặp ALS phải shuffle dữ liệu giữa các partition, và một phần công việc chạy tuần tự ở driver. Hai, 4 core của Docker còn phải chia cho Kafka và MongoDB.

Về bộ nhớ, container Spark dùng tối đa 4,6 GiB. Nhóm lấy số này bằng docker stats, 5 giây đo một lần.

Nhóm nói rõ đây là local mode trên một máy: đo được mức tăng theo số core, chưa phải mở rộng theo số máy. Mở rộng theo cluster nằm trong thiết kế triển khai cho Buổi 10.

### Slide 9 – NoSQL: MongoDB phục vụ gợi ý cho app (~1 phút)

Kết quả gợi ý được ghi vào MongoDB, mỗi người dùng một document. Bên trái là document thật của người dùng số 1: _id là user_id, kèm phiên bản mô hình và mảng recs gồm 30 phần tử, mỗi phần tử có thứ hạng, mã sách và điểm.

Truy vấn duy nhất của app là đọc một document theo _id và chỉ lấy 10 phần tử đầu bằng $slice. Nhóm đọc ngược lại từ MongoDB để kiểm chứng. Đủ 53.424 document. Query plan là IDHACK, tức MongoDB đi thẳng vào index _id và chỉ đọc đúng 1 document. Đo 1.000 lần đọc ngẫu nhiên: trung vị 0,5 mili giây, p95 là 1,1 mili giây.

Gợi ý mới được ghi vào một collection mới. Ghi xong hết mới chuyển con trỏ meta.active sang, nên app không bao giờ đọc phải danh sách đang ghi dở.

Bên phải là app Streamlit đọc trực tiếp từ MongoDB. Đây cũng là bằng chứng cho vấn đề em vừa nói: 9 trên 10 cuốn gợi ý cho người dùng số 1 là tuyển tập Calvin and Hobbes.

*Chuyển lượt:* Tiếp theo, Tuấn trình bày luồng streaming, các rủi ro và kế hoạch đến Buổi 10.

---

## Phần 3 – Đinh Trần Quốc Tuấn (slide 10–13)

### Slide 10 – Streaming: Kafka → Spark → MongoDB, có restart (~1 phút 15 giây)

Cảm ơn Kiệt. Phần của em là luồng streaming.

Hàng trên là đường đi của sự kiện. Producer phát lại 2.000 rating thật dưới dạng sự kiện mô phỏng, 20 sự kiện mỗi giây. Producer cố ý gửi thêm 37 sự kiện trùng và 22 sự kiện lỗi có số sao bằng 9. Kafka nhận tất cả vào topic rating-events, chia 3 partition. Spark Structured Streaming cứ 5 giây xử lý một micro-batch, có watermark 10 phút. Kết quả là 19 micro-batch, ghi vào MongoDB.

Để kiểm chứng, nhóm đếm lại toàn bộ message trong Kafka làm mốc, rồi đối chiếu với những gì MongoDB ghi nhận. Bảng cho thấy cả bốn dòng đều khớp: 2.000 sự kiện hợp lệ được tính đúng 2.000, 22 sự kiện lỗi bị loại đủ 22, 37 sự kiện trùng bị bỏ đủ 37, và tổng số lượt chấm trong rating_stats đúng bằng 2.000.

Phần quan trọng là thử restart. Nhóm dừng job streaming sau khoảng 45 giây trong khi producer vẫn tiếp tục gửi, 21 giây sau bật lại. Spark đọc tiếp từ checkpoint, và kết quả cuối cùng không mất, không đếm trùng sự kiện nào.

Về thông lượng, một micro-batch xử lý tối đa 168 dòng mỗi giây. Đầu vào chỉ có 20 sự kiện mỗi giây, nên đây chưa phải giới hạn của hệ thống.

### Slide 11 – Bên trong job streaming (~1 phút)

Slide này giải thích vì sao kết quả vừa rồi khớp. Sự kiện từ Kafka được đọc theo schema JSON, rồi qua bước kiểm tra: đủ các trường và số sao từ 1 đến 5.

Nhánh trên là sự kiện hợp lệ. Watermark 10 phút cho phép sự kiện đến trễ tối đa 10 phút và giới hạn bộ nhớ trạng thái; bước bỏ trùng theo event_id loại các bản producer gửi lặp. Sau đó ghi vào ba collection của MongoDB: rating_stats là điểm trung bình theo sách, user_seen là sách người dùng vừa chấm để app ẩn đi, và online_eval là phép đo trúng top-10.

Nhánh dưới là sự kiện lỗi: chỉ đếm, không tính vào điểm.

Hai cơ chế chống mất và chống trùng. Checkpoint lưu offset Kafka và trạng thái, nên job chết thì đọc tiếp đúng chỗ, không mất; nhưng một batch có thể bị chạy lại, tức at-least-once. Vì vậy mỗi lần ghi xong, job lưu số batch vào stream_state; batch nào bị phát lại sẽ bị bỏ qua, nên không đếm trùng. Đó là lý do thử restart ra đúng 2.000 trên 2.000.

### Slide 12 – Rủi ro, vấn đề hiệu năng và hướng khắc phục (~1 phút 30 giây)

Đây là các vấn đề nhóm phát hiện khi chạy thật. Mỗi dòng có bằng chứng và hướng khắc phục.

Một, top-10 của ALS explicit kém, như Kiệt đã nói. Nhóm đã thử ALS implicit và đạt Precision@10 0,167. Buổi 10 nhóm sẽ chọn mô hình theo Precision@10 chứ không theo RMSE.

Hai, đánh giá online đang bằng 0%. App đo xem khi người dùng chấm 4 hoặc 5 sao, cuốn đó có nằm trong top-10 của họ không. Kết quả là 0 trên 1.385 lượt. Nguyên nhân không phải mô hình: sự kiện phát lại là những cuốn người dùng đã chấm trong tập train, mà gợi ý thì đã loại sách người dùng đã đọc. Hướng sửa: chỉ phát lại phần dữ liệu test được giữ lại, và mô hình phục vụ chỉ học trên phần train.

Ba, nếu topic Kafka chưa tồn tại khi job streaming khởi động, job dừng luôn với lỗi UnknownTopicOrPartition. Nhóm gặp lỗi này ở lần chạy đầu. Đã sửa: job tự tạo topic 3 partition trước khi đọc.

Bốn, phiên bản phần mềm không được ghim. Image mongo:7.0 mới nhất bị lỗi, MongoDB không khởi động được. Thư viện pyarrow bản mới đòi NumPy 2 nên app báo lỗi. Đã sửa bằng cách ghim mongo 7.0.14 và pyarrow 17.0.0.

Năm, giới hạn của một máy: 4 core chỉ nhanh 1,95 lần, RAM đã dùng 4,6 trên 7,7 GiB với 6 triệu dòng. Bộ UCSD lớn hơn khoảng 38 lần nên có thể không vừa. Nhóm sẽ chạy tập con theo thể loại và thiết kế phương án cluster.

Sáu, nhóm chưa có thành phần MapReduce mà bài cuối kỳ yêu cầu. Kế hoạch là một job Hadoop Streaming đếm rating theo sách, rồi đối chiếu với kết quả Spark SQL.

### Slide 13 – Kế hoạch hoàn thiện đến Buổi 10 (~45 giây)

Kế hoạch đến Buổi 10 chia theo người.

Hảo phụ trách dữ liệu và batch: chạy tập con UCSD và đo thời gian, bộ nhớ; viết job MapReduce; và bản thiết kế triển khai cluster hoặc cloud.

Kiệt phụ trách Spark ML và NoSQL: chốt mô hình theo Precision@10 với ALS implicit, tinh chỉnh tham số, so lại với baseline, và hoàn thiện app.

Em phụ trách streaming và đánh giá: sửa cách đánh giá online bằng dữ liệu giữ lại, thử dừng đột ngột để kiểm tra trường hợp xấu nhất, đo độ trễ end-to-end, và tổng hợp số đo vào báo cáo.

Sản phẩm nộp Buổi 10 gồm báo cáo cuối kỳ, repo kèm README và các file đo, và demo trực tiếp. Nếu thiếu thời gian, nhóm ưu tiên MapReduce và mô hình theo Precision@10 trước, UCSD sau.

*Chuyển lượt:* Hảo cập nhật kế hoạch này đã làm được đến đâu.

---

## Phần 4 – Lương Huỳnh Ngọc Hảo (slide 14–16)

### Slide 14 – Cập nhật sau Buổi 8 (~1 phút 15 giây)

Cảm ơn Tuấn. Slide này cập nhật kế hoạch vừa rồi đã làm được đến đâu, tính đến ngày 9/11.

Đã xong. Job MapReduce trên Hadoop cho kết quả khớp 100% với Spark SQL, combiner giảm 99,2% bản ghi phải shuffle. Mô hình được chọn theo Precision@10: ALS implicit đạt 0,204, gấp 2,6 lần gợi ý sách phổ biến, và app đang dùng mô hình này. Thử giết đột ngột job streaming bằng kill -9: 1.474 bằng 1.500 trừ 26 sự kiện trễ, không mất, không trùng. Streaming có thêm cửa sổ trượt cho sách đang hot và kho Parquet lưu sự kiện, ghi đúng một lần. Thử tắt Kafka rồi MongoDB khi đang gửi 3.000 sự kiện: 3.000 bằng 3.000; lượt đầu của thử nghiệm này lộ ra lỗi producer làm mất 571 tin, nhóm đã sửa. Và đã có bản thiết kế cluster, cloud.

Làm một phần: đưa sự kiện streaming vào lần huấn luyện sau đã chạy thử, gộp 4.901 dòng, nhưng chưa chạy tự động theo lịch.

Chưa làm, nhóm nói thẳng: chạy tập con UCSD, và đánh giá online bằng dữ liệu giữ lại. Hai mục này được ghi rõ trong phần giới hạn của báo cáo cuối kỳ.

### Slide 15 – Câu hỏi của giảng viên: dữ liệu thay đổi thì sao? (~1 phút)

Ở Buổi 8, thầy cô hỏi: dữ liệu lưu sẵn 10 cuốn gợi ý cho mỗi người, nhưng dữ liệu sẽ thay đổi, vậy hệ thống xử lý thế nào. Nhóm chia thay đổi thành hai tốc độ.

Thay đổi nhanh đi qua streaming, xử lý trong vài giây. Người dùng chấm một cuốn đang nằm trong top-10 thì app ẩn cuốn đó và lấy cuốn tiếp theo trong 30 cuốn đã lưu. Đọc gần hết danh sách thì app bù bằng sách phổ biến người đó chưa đọc, nên luôn đủ 10 cuốn. Điểm trung bình của sách được cộng dồn tổng điểm và số lượt rồi mới chia.

Thay đổi chậm như gu đọc đổi, người dùng mới, sách mới thì cần huấn luyện lại, và việc này chạy theo đợt. Sự kiện streaming được lưu ra Parquet, bước làm sạch gộp vào dữ liệu huấn luyện, lấy rating mới nhất, rồi huấn luyện lại. Mô hình mới ghi sang collection mới, xong mới chuyển meta.active, nên app không gián đoạn.

Sau câu hỏi của thầy cô, nhóm phát hiện rating từ streaming chưa được đưa vào lần huấn luyện sau, nên đã bổ sung và kiểm thử: gửi 5 rating mới qua Kafka thì dữ liệu huấn luyện tăng đúng 5 dòng và 5 cuốn đó biến khỏi gợi ý. Không huấn luyện lại sau mỗi sự kiện vì một lần huấn luyện mất khoảng 4 phút: cập nhật số liệu rẻ nên làm ngay, học lại mô hình đắt nên làm theo đợt.

### Slide 16 – Xin cảm ơn (~15 giây, sau đó hỏi đáp)

Phần trình bày của nhóm đến đây là hết. Mã nguồn và toàn bộ số đo nằm trên GitHub của nhóm. Cảm ơn thầy cô và các bạn đã lắng nghe, nhóm rất mong nhận câu hỏi và góp ý.

*Kiệt điều phối hỏi đáp: ai trình bày phần nào thì trả lời phần đó (xem file Hỏi đáp Session 8 và file Câu hỏi giảng viên Session 8).*

---

## Phụ lục

### Slide 17 – Phụ lục · Kiến trúc theo từng luồng

Bản cũ của slide 3, vẽ mỗi luồng thành một hàng hộp. Chỉ mở khi được hỏi về thứ tự các bước trong từng luồng.

---

## Chuẩn bị demo (nếu giảng viên yêu cầu)

Chạy trước buổi thuyết trình ít nhất 10 phút:

```bash
cd "D:\Big Data\Group_06_BigData\bookpulse"
docker compose up -d
docker compose ps                    # 4 service đều "running"
```

- App: mở http://localhost:8501, nhập user 1 (top-10 Calvin and Hobbes) rồi thử user 2, 3.
- Streaming trực tiếp (hai terminal): `docker compose exec spark python streaming/stream_stats.py` và `docker compose exec spark python streaming/producer.py --limit 300`. Bấm "Làm mới" trong app để thấy số sự kiện tăng.
- Bằng chứng nhanh: `docker compose exec mongo mongosh bookpulse --eval "db.meta.findOne({}, {popular: 0})"`.
- Spark UI: http://localhost:4040, chỉ có khi một job Spark đang chạy.
