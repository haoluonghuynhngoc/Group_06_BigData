# BookPulse – Script thuyết trình Session 10 (Final Assessment)

Final Assessment (40%): Final Project Report, Presentation, Demo & Q&A · CLO1–CLO5

Số liệu lấy từ `bookpulse/reports/` (lần chạy lại toàn bộ bằng `scripts/run_pipeline.sh` ngày 06/11/2026, dữ liệu Goodbooks-10k). Báo cáo Word dùng cùng bộ số liệu này.

## Phân công và thời lượng

| Người trình bày | Slide | Nội dung | Thời lượng |
| --- | --- | --- | --- |
| Lương Huỳnh Ngọc Hảo | 1–6 | Mở đầu, kết quả chính, kiến trúc, dữ liệu, MapReduce, Spark SQL | ~6 phút |
| Nguyễn Tấn Kiệt | 7–9 | Spark ML, MongoDB, hiệu năng | ~3 phút 30 giây |
| Đinh Trần Quốc Tuấn | 10–12 | Streaming (thiết kế, kiểm chứng), kiểm thử | ~3 phút 30 giây |
| Lương Huỳnh Ngọc Hảo | 13–14 | Thiết kế cluster/cloud, giới hạn | ~2 phút |
| Cả nhóm | 15 | Demo trực tiếp (theo file Kịch bản demo) | 8–10 phút |
| Hảo; Kiệt điều phối hỏi đáp | 16 | Cảm ơn, hỏi đáp | — |

Thông điệp xuyên suốt: **hệ thống chạy end-to-end, và mỗi thành phần đều được kiểm chứng bằng một cách tính độc lập.** Nếu bị giới hạn thời gian, rút ngắn slide 4 và 12 trước.

---

## Phần 1 – Lương Huỳnh Ngọc Hảo (slide 1–6)

### Slide 1 – BookPulse (~20 giây)

Xin chào thầy cô và các bạn. Nhóm 06 gồm Lương Huỳnh Ngọc Hảo, Đinh Trần Quốc Tuấn và Nguyễn Tấn Kiệt. Hôm nay nhóm trình bày bài cuối kỳ: BookPulse, hệ thống gợi ý sách trên nền tảng Big Data, gồm đủ năm phần mà môn học yêu cầu: MapReduce, Spark SQL, Spark ML, NoSQL và streaming. Cuối bài nhóm sẽ demo trực tiếp.

### Slide 2 – Kết quả chính (~1 phút)

Bốn con số tóm tắt hệ thống. Một: gần 6 triệu rating được xử lý bằng MapReduce trên Hadoop, kết quả khớp 100% với Spark SQL. Hai: mô hình gợi ý đạt Precision@10 là 0,204, tức trung bình 10 gợi ý thì khoảng 2 cuốn đúng là sách người dùng thích, gấp 2,6 lần cách gợi ý sách phổ biến. Ba: app đọc gợi ý từ MongoDB trong chưa tới 1 mili giây. Bốn: luồng streaming bị giết đột ngột giữa chừng mà không mất hay đếm trùng sự kiện nào.

Bảng bên dưới đối chiếu với năm yêu cầu của bài cuối kỳ trong syllabus; mỗi dòng sẽ có một slide riêng và một phần trong demo.

### Slide 3 – Kiến trúc hệ thống (~1 phút)

Đây là kiến trúc đã chạy thật. Hàng trên cùng màu tím là cụm Hadoop: dữ liệu đưa lên HDFS, job MapReduce chạy trên YARN. Khung xanh là container Spark với luồng batch: làm sạch ra Parquet, phân tích bằng Spark SQL, huấn luyện ALS, rồi ghi gợi ý vào MongoDB. Khung cam là luồng streaming: producer gửi sự kiện vào Kafka, Spark Structured Streaming cập nhật số liệu trực tiếp và lưu sự kiện ra Parquet để lần huấn luyện sau dùng; đó là mũi tên vòng ngược lên clean.py. Bên phải là MongoDB và app Streamlit, app chỉ đọc kết quả tính sẵn.

Nguyên tắc: streaming không huấn luyện lại mô hình sau mỗi sự kiện. Thay đổi nhanh xử lý trong vài giây, thay đổi về gu đọc chờ lần chạy batch sau. Đây là cách tổ chức của kiến trúc Lambda.

### Slide 4 – Dữ liệu và lý do dùng Big Data (~1 phút)

Dữ liệu là Goodbooks-10k từ Goodreads: gần 6 triệu rating của hơn 53 nghìn người cho 10 nghìn cuốn sách. Ma trận người dùng nhân sách gần 99% ô trống, và dữ liệu lệch về 4–5 sao.

Nhóm nói thẳng: 6 triệu dòng vẫn chạy được trên một máy, nên đây là bộ phát triển. Nhưng nhóm đã đo giới hạn: với 6 triệu dòng, Spark dùng 4,6 trên 7,7 GiB RAM, và tăng lên 4 core chỉ nhanh gần 2 lần. Bộ đầy đủ UCSD lớn hơn 38 lần sẽ không vừa một máy. Vì vậy toàn bộ code viết trên Spark và Hadoop để chạy được trên cluster, và slide 13 là thiết kế đó.

### Slide 5 – MapReduce trên Hadoop (~1 phút 15 giây)

Thành phần MapReduce thống kê rating theo từng cuốn sách. File 72 MB được chia thành 3 block trên HDFS. Mapper kiểm tra từng dòng và phát ra cặp key là mã sách, value là một vector: số lượt, tổng điểm và phân bố 1–5 sao. Combiner cộng các vector ngay phía map. Điểm quan trọng: combiner không được cộng điểm trung bình, vì trung bình của trung bình là sai; nó chỉ cộng tổng và số lượt, và điểm trung bình chỉ được tính và làm tròn một lần ở reducer.

Kết quả: combiner giảm 99,2% số bản ghi phải shuffle, dữ liệu qua mạng chỉ còn 1,2 MB. Để kiểm chứng, nhóm tính lại cùng thống kê bằng Spark SQL và so bằng exceptAll theo hai chiều: 0 dòng khác biệt trên 10.000 sách.

### Slide 6 – Spark DataFrame và Spark SQL (~1 phút 15 giây)

Sau khi làm sạch và lưu Parquet, nhóm đăng ký dữ liệu thành temporary view và trả lời sáu câu hỏi phân tích bằng Spark SQL, ví dụ truy vấn bên trái tìm sách được chấm nhiều nhất.

Bên phải là bốn điểm kiểm tra. Query plan cho thấy Spark chỉ đọc 2 cột cần dùng từ Parquet, bảng sách nhỏ được broadcast nên chỉ có một lần shuffle. Join dùng LEFT JOIN và kiểm tra số dòng không đổi, để không âm thầm mất dữ liệu như inner join. Cùng câu hỏi viết bằng DataFrame API cho kết quả giống hệt trên toàn bộ 10.000 dòng.

Biểu đồ: cùng một phép GROUP BY, đọc Parquet nhanh hơn CSV, lần đo cuối là 4,2 so với 1,1 giây. Các lần đo dao động từ 3,9 đến 8,4 lần do bộ nhớ đệm, nên nhóm coi query plan là bằng chứng chắc chắn hơn con số thời gian.

*Chuyển lượt:* Tiếp theo, Kiệt trình bày phần machine learning và MongoDB.

---

## Phần 2 – Nguyễn Tấn Kiệt (slide 7–9)

### Slide 7 – Spark ML: chọn mô hình theo đúng thước đo (~1 phút 30 giây)

Cảm ơn Hảo. Nhóm dùng ALS trong Spark MLlib, thử hai cách học. ALS explicit học cách đoán số sao: RMSE 0,828, tốt hơn baseline 0,955. Nhưng người dùng không thấy số sao, họ thấy danh sách 10 cuốn. Đo bằng Precision@10 thì explicit chỉ đạt 0,008, thua cả việc gợi ý sách phổ biến cho mọi người, vì mô hình đẩy lên những cuốn ít người chấm mà toàn 5 sao, nên ai cũng nhận danh sách Calvin and Hobbes.

ALS implicit học xem người dùng có chọn đọc cuốn nào, số sao là độ tin cậy. Nhóm chọn tham số trên tập validation, test chỉ dùng một lần: rank 50, alpha 1, Precision@10 đạt 0,204, gấp 2,6 lần sách phổ biến, mà vẫn gợi ý hơn 1.200 đầu sách khác nhau. App dùng mô hình này. Đánh đổi là không còn đoán số sao và hơi nghiêng về sách nổi tiếng.

### Slide 8 – NoSQL: MongoDB phục vụ gợi ý (~1 phút)

App chỉ có một truy vấn: đưa user_id vào, lấy danh sách ra. Nên nhóm chọn mô hình document: mỗi người dùng một document chứa sẵn 30 gợi ý và 10 sách bù. Truy vấn theo _id dùng IDHACK, đọc đúng một document, dưới 1 mili giây.

Ba quyết định thiết kế: lưu 30 nhưng hiện 10, nên khi người dùng vừa đọc một cuốn, app lấy cuốn tiếp theo mà không phải huấn luyện lại; khi đọc gần hết thì bù bằng sách phổ biến mà chính người đó chưa đọc. Đổi phiên bản an toàn: ghi xong collection mới mới chuyển con trỏ meta.active. Và ghi phân tán bằng foreachPartition, upsert theo _id nên chạy lại không trùng.

### Slide 9 – Hiệu năng (~1 phút)

Nhóm đo khả năng song song bằng cách huấn luyện cùng một mô hình với 1, 2 và 4 core: 159, 106,5 và 82 giây, tức 4 core nhanh 1,95 lần, không phải 4 lần. Lý do: phần tuần tự ở driver và shuffle giữa các vòng lặp ALS không tăng tốc được, đúng định luật Amdahl, và 4 core còn chia cho Kafka, MongoDB. RAM đỉnh 4,6 trên 7,7 GiB. Kết luận: dữ liệu lớn gấp 38 lần sẽ không vừa một máy, đó là lý do cho thiết kế cluster ở slide 13.

*Chuyển lượt:* Tiếp theo, Tuấn trình bày phần streaming.

---

## Phần 3 – Đinh Trần Quốc Tuấn (slide 10–12)

### Slide 10 – Streaming: thiết kế (~1 phút 15 giây)

Cảm ơn Kiệt. Producer phát lại rating thành sự kiện, cố ý gửi kèm bản trùng, sự kiện lỗi và sự kiện trễ. Kafka giữ sự kiện trong 3 partition, key là user_id nên sự kiện của một người luôn đúng thứ tự. Spark Structured Streaming chạy bốn query, mỗi query một checkpoint riêng: valid dùng watermark 10 phút và bỏ trùng theo event_id để tính điểm trung bình trực tiếp; trending gom theo cửa sổ event time 10 phút, trượt 5 phút để ra "sách đang hot"; events_to_parquet lưu mọi sự kiện hợp lệ cho lần huấn luyện sau; rejected đếm sự kiện lỗi.

Thiết kế theo Lambda: lớp nhanh bỏ sự kiện trễ để giới hạn bộ nhớ trạng thái, còn kho lưu trữ giữ lại nên không mất rating thật.

### Slide 11 – Streaming: kiểm chứng với Kafka (~1 phút 15 giây)

Nhóm giết đột ngột job streaming bằng kill -9 giữa chừng rồi chạy lại từ checkpoint, sau đó đếm lại toàn bộ Kafka làm mốc. Bảng cho thấy cả bốn kiểm tra đều khớp: 3.762 sự kiện hợp lệ đúng bằng số Kafka đếm được trừ 43 sự kiện trễ; tổng trong rating_stats khớp; 42 sự kiện lỗi bị loại đủ; và tổng lượt trong các cửa sổ trượt đúng bằng 2 lần số sự kiện, vì mỗi sự kiện thuộc 2 cửa sổ.

Khi thử, nhóm phát hiện watermark là riêng cho từng query và chỉ cập nhật giữa các batch, nên đã tách kho lưu trữ khỏi watermark. Bên phải là Spark UI của query trending: tốc độ vào khoảng 20 sự kiện mỗi giây, khoảng cách watermark khoảng 600 giây, đúng 10 phút đã cấu hình.

### Slide 12 – Kiểm thử và kiểm chứng (~1 phút)

Nhóm có ba lớp kiểm thử. Unit test, 8 trên 8 đạt: mapper loại đúng dòng lỗi, combiner chạy 0, 1 hay nhiều lần đều cùng kết quả, gợi ý không bao giờ chứa sách đã đọc. Đối chiếu độc lập: MapReduce so với Spark SQL, SQL so với DataFrame, streaming so với Kafka. Và đọc lại MongoDB. Nhờ kiểm chứng, nhóm tìm ra năm lỗi thật ở dòng cuối, đều đã sửa.

*Chuyển lượt:* Hảo trình bày thiết kế triển khai và giới hạn.

---

## Phần 4 – Lương Huỳnh Ngọc Hảo (slide 13–14)

### Slide 13 – Thiết kế triển khai cluster/cloud (~1 phút)

Đây là thiết kế, chưa triển khai thật. Mỗi dòng ánh xạ một thành phần từ một máy lên production: Kafka 3 broker có replication; dữ liệu lên object storage như S3 hoặc MinIO để tách lưu trữ khỏi tính toán; Spark chạy trên Kubernetes hoặc YARN, mở rộng bằng số executor; MongoDB thành replica set 3 node; checkpoint lưu trên object storage nên job chết có thể đọc tiếp; metric như consumer lag và thời gian batch đưa lên Prometheus, Grafana; secret, TLS và phân quyền tối thiểu. Về chi phí: huấn luyện chỉ chạy theo lịch nên dùng cụm tạm thời, còn streaming và MongoDB chạy thường trực nhưng tải nhỏ.

### Slide 14 – Giới hạn và hướng phát triển (~1 phút)

Ba giới hạn chính. Dữ liệu: chưa chạy bộ UCSD đầy đủ, và Goodbooks không có thời gian thật nên streaming là mô phỏng, chia train/test là ngẫu nhiên. Mô hình: mới thử 4 bộ tham số, còn nghiêng về sách nổi tiếng. Streaming: nếu job chết đúng lúc đang ghi một batch vào MongoDB thì có thể cộng lặp một phần.

Hướng phát triển: dùng UCSD theo thể loại, vốn có thời gian thật, để chia theo thời gian và phát lại sự kiện đúng trình tự; chạy trên cluster theo thiết kế; thêm gợi ý theo nội dung cho sách mới.

---

### Slide 15 – Demo trực tiếp (8–10 phút)

Làm theo file `Present/BookPulse - Kich ban demo Session 10.md`: Hảo chạy MapReduce và trình bày Spark SQL, Kiệt trình bày app và MongoDB, Tuấn chạy streaming và kiểm chứng với Kafka.

### Slide 16 – Xin cảm ơn

Phần trình bày của nhóm đến đây là hết. Mã nguồn, số đo và báo cáo nằm trên GitHub của nhóm. Cảm ơn thầy cô và các bạn đã lắng nghe, nhóm rất mong nhận câu hỏi và góp ý.

*Kiệt điều phối hỏi đáp: ai trình bày phần nào trả lời phần đó (xem file Hỏi đáp Session 10).*
