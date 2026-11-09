# BookPulse – Script thuyết trình Session 10 (Final Assessment)

Final Assessment (40%): Final Project Report, Presentation, Demo & Q&A · CLO1–CLO5

Số liệu lấy từ `bookpulse/reports/` (lần chạy lại toàn bộ bằng `scripts/run_pipeline.sh` ngày 06/11/2026, dữ liệu Goodbooks-10k). Báo cáo Word dùng cùng bộ số liệu này. Riêng các phần sau lấy từ file khác: MapReduce từ `mapreduce_verify_20261105_170058.json` (05/11) và lần chạy thử 07/11 (`reports/superseded/rehearsal_20261107/`); lượt thử kill -9 và lượt demo streaming đo lại ngày 08/11 (`kafka_audit_20261107_220738.json` và `kafka_audit_20261107_221105.json`, tên file theo giờ UTC); lượt kill -9 lặp lại có kiểm tra kho Parquet và lượt thử tắt Kafka, MongoDB (`kafka_audit_20261108_040236.json`, `kafka_audit_20261108_041203.json`, thư mục `reports/streaming/outage_test_20261108_110750/`); kiểm tra gợi ý không trùng sách đã chấm từ `nosql_evidence_20261107_221150.json`; RAM đỉnh từ `reports/scaling/docker_stats_20261104_batch.csv`.

## Phân công và thời lượng

| Người trình bày | Slide | Nội dung | Thời lượng |
| --- | --- | --- | --- |
| Lương Huỳnh Ngọc Hảo | 1–6 | Mở đầu, kết quả chính, kiến trúc, dữ liệu, MapReduce, Spark SQL | ~6 phút |
| Nguyễn Tấn Kiệt | 7–9 | Spark ML, MongoDB, hiệu năng | ~4 phút 20 giây |
| Đinh Trần Quốc Tuấn | 10–12 | Streaming (thiết kế, kiểm chứng), kiểm thử | ~4 phút 55 giây |
| Lương Huỳnh Ngọc Hảo | 13–14 | Thiết kế cluster/cloud, giới hạn | ~3 phút 20 giây |
| Cả nhóm | 15 | Demo trực tiếp (theo file Kịch bản demo) | 8–10 phút |
| Hảo; Kiệt điều phối hỏi đáp | 16 | Cảm ơn, hỏi đáp | — |
| (dự phòng) | 17 | Kiến trúc chi tiết, mở khi giảng viên hỏi sâu | — |

Thông điệp xuyên suốt: **hệ thống chạy end-to-end, và mỗi thành phần đều được kiểm chứng bằng một cách tính độc lập.** Phần nói (slide 1–14) khoảng 17,5 phút, chưa tính demo. Nếu bị giới hạn thời gian, rút ngắn slide 4 và 12 trước, rồi đoạn thứ hai của slide 11 và phần "Hướng phát triển" của slide 14 (đã có trên slide).

---

## Phần 1 – Lương Huỳnh Ngọc Hảo (slide 1–6)

### Slide 1 – BookPulse (~20 giây)

Xin chào thầy cô và các bạn. Nhóm 06 gồm Lương Huỳnh Ngọc Hảo, Đinh Trần Quốc Tuấn và Nguyễn Tấn Kiệt. Hôm nay nhóm trình bày bài cuối kỳ: BookPulse, hệ thống gợi ý sách trên nền tảng Big Data, gồm đủ năm phần mà môn học yêu cầu: MapReduce, Spark SQL, Spark ML, NoSQL và streaming. Cuối bài nhóm sẽ demo trực tiếp.

### Slide 2 – Kết quả chính (~1 phút)

Bốn con số tóm tắt hệ thống. Một: gần 6 triệu rating được xử lý bằng MapReduce trên Hadoop, kết quả khớp 100% với Spark SQL. Hai: mô hình gợi ý đạt Precision@10 là 0,204, tức trung bình 10 gợi ý thì khoảng 2 cuốn đúng là sách người dùng thích, gấp 2,6 lần cách gợi ý sách phổ biến. Ba: app đọc gợi ý từ MongoDB với trung vị chưa tới 1 mili giây. Bốn: trong lượt thử giết đột ngột job streaming bằng kill -9 giữa chừng, không sự kiện nào bị mất hay đếm trùng, và 26 sự kiện trễ bị loại; con số 26 suy từ kafka_audit: 1.474 bằng 1.500 trừ 26. Điều kiện là sự kiện trễ đến sau khi job đã có watermark; slide 11 sẽ nói rõ điểm này.

Bảng bên dưới đối chiếu với năm yêu cầu của bài cuối kỳ trong syllabus; mỗi dòng sẽ có một slide riêng và một phần trong demo.

### Slide 3 – Kiến trúc hệ thống (~1 phút 30 giây)

Đây là kiến trúc đã chạy thật, tổ chức theo ý tưởng Lambda. Đọc từ trái sang phải.

Nguồn là Goodbooks-10k, 6 triệu rating. Dữ liệu đi theo hai đường. Đường trên là lớp batch, màu xanh: Spark làm sạch dữ liệu, phân tích bằng Spark SQL, rồi huấn luyện ALS để tính sẵn top-N gợi ý cho từng người. Lớp batch chạy theo đợt và đọc từ kho Parquet ở giữa.

Đường dưới là lớp tốc độ, màu cam: producer mô phỏng người dùng chấm sao, gửi sự kiện vào Kafka; Spark Structured Streaming đọc mỗi 5 giây, dùng watermark cho sự kiện trễ, cửa sổ thời gian cho "sách đang hot", và checkpoint để chạy lại không mất dữ liệu. Mọi sự kiện hợp lệ cũng được lưu vào kho Parquet, để lần batch sau học thêm.

Cả hai lớp ghi vào MongoDB, là lớp phục vụ: gợi ý từ batch, số liệu mới từ streaming. App Streamlit đọc MongoDB và gộp hai phần khi đọc, ví dụ ẩn ngay cuốn sách người dùng vừa chấm. Mũi tên cam dưới cùng: khi người dùng bấm chấm sao trong app, app chỉ gửi sự kiện vào Kafka, không ghi thẳng vào MongoDB; sự kiện đi qua lớp tốc độ như mọi sự kiện khác.

Hadoop MapReduce phía trên là nhánh riêng, dùng để kiểm chứng: đếm lại trên HDFS và YARN, kết quả khớp 100% với Spark SQL.

*Sơ đồ chi tiết với tên file, collection và từng bước nằm ở slide 17 (phụ lục); mở khi giảng viên hỏi sâu về một thành phần.*

### Slide 4 – Dữ liệu và lý do dùng Big Data (~30 giây)

Dữ liệu là Goodbooks-10k từ Goodreads: gần 6 triệu rating của hơn 53 nghìn người cho 10 nghìn cuốn sách, ma trận gần 99% ô trống, lệch về 4–5 sao. Nói thẳng: 6 triệu dòng vẫn chạy trên một máy, đây là bộ phát triển. Nhưng đã đo: Spark dùng đỉnh 4,6 trên 7,7 GiB RAM, 4 core chỉ nhanh gần 2 lần; bộ UCSD lớn gấp 38 lần sẽ không vừa. Khi lên cluster, logic Spark giữ nguyên nhưng lớp cấu hình phải sửa; slide 13 nói cụ thể.

### Slide 5 – MapReduce trên Hadoop (~1 phút 30 giây)

Thành phần MapReduce thống kê rating theo từng cuốn sách. File 72 MB được chia thành 3 block trên HDFS. Mapper kiểm tra từng dòng và phát ra cặp key là mã sách, value là một vector: số lượt, tổng điểm và phân bố 1–5 sao. Combiner cộng các vector ngay phía map. Điểm quan trọng: combiner không được cộng điểm trung bình, vì trung bình của trung bình là sai; nó chỉ cộng tổng và số lượt, và điểm trung bình chỉ được tính và làm tròn một lần ở reducer.

Kết quả: combiner giảm 99,2% số bản ghi phải shuffle, từ gần 6 triệu xuống 47.087, dữ liệu qua mạng chỉ còn 1,2 MB. Thời gian thì dao động theo lần chạy: lần 05/11 job mất 134 giây với 4 map task cho 3 split, vì ApplicationMaster của MapReduce chạy thêm bản dự phòng cho task chậm, gọi là speculative execution, bản thừa bị hủy; lần chạy thử 07/11 chỉ 52 giây với 3 map task. Counter và kết quả hai lần giống hệt. Để kiểm chứng, nhóm tính lại cùng thống kê bằng Spark SQL và so bằng exceptAll theo hai chiều: 0 dòng khác biệt trên 10.000 sách.

### Slide 6 – Spark DataFrame và Spark SQL (~1 phút 15 giây)

Sau khi làm sạch và lưu Parquet, nhóm đăng ký dữ liệu thành temporary view và trả lời sáu câu hỏi phân tích bằng Spark SQL, ví dụ truy vấn bên trái tìm sách được chấm nhiều nhất.

Bên phải là bốn điểm kiểm tra. Query plan cho thấy Spark chỉ đọc 2 cột cần dùng từ Parquet; bảng sách nhỏ, đọc từ file CSV, được gửi tới mọi executor bằng một BroadcastExchange, nên plan chỉ có một Exchange shuffle, là của phép GROUP BY. Join dùng LEFT JOIN và kiểm tra số dòng không đổi, để không âm thầm mất dữ liệu như inner join. Cùng câu hỏi viết bằng DataFrame API cho kết quả giống hệt trên toàn bộ 10.000 dòng.

Biểu đồ: cùng một phép GROUP BY, đọc Parquet nhanh hơn CSV, lần đo cuối là 4,22 so với 1,08 giây, tức 3,9 lần. Các lần đo dao động từ 3,9 đến 8,4 lần do bộ nhớ đệm, nên nhóm coi query plan là bằng chứng chắc chắn hơn con số thời gian.

*Chuyển lượt:* Tiếp theo, Kiệt trình bày phần machine learning và MongoDB.

---

## Phần 2 – Nguyễn Tấn Kiệt (slide 7–9)

### Slide 7 – Spark ML: chọn mô hình theo đúng thước đo (~1 phút 40 giây)

Cảm ơn Hảo. Nhóm dùng ALS trong Spark MLlib, thử hai cách học. ALS explicit học cách đoán số sao: RMSE 0,828, tốt hơn baseline 0,955. Nhưng người dùng không thấy số sao, họ thấy danh sách 10 cuốn. Đo bằng Precision@10 thì explicit chỉ đạt 0,008, thua cả việc gợi ý sách phổ biến cho mọi người, vì mô hình đẩy lên những cuốn ít người chấm mà toàn 5 sao, nên ai cũng nhận danh sách Calvin and Hobbes.

ALS implicit học xem người dùng có chọn đọc cuốn nào, số sao là độ tin cậy. Nhóm chọn tham số trên tập validation, test chỉ dùng một lần: rank 50, alpha 1, Precision@10 đạt 0,204, gấp 2,6 lần sách phổ biến, mà vẫn gợi ý hơn 1.200 đầu sách khác nhau. App dùng mô hình này. Đánh đổi là không còn đoán số sao và hơi nghiêng về sách nổi tiếng. Nhóm cũng nói rõ: mô hình app đang phục vụ chính là mô hình đã đánh giá, fit trên toàn bộ tập train, tức 80% dữ liệu; khoảng 1,2 triệu rating của tập test chưa được học. Khi đưa vào dùng thật, nhóm sẽ fit lại trên toàn bộ dữ liệu với cùng tham số rồi mới phục vụ.

### Slide 8 – NoSQL: MongoDB phục vụ gợi ý (~1 phút 35 giây)

Truy vấn chính của app là: đưa user_id vào, lấy danh sách gợi ý ra. Nên nhóm chọn mô hình document: mỗi người dùng một document chứa sẵn 30 gợi ý và 10 sách bù, app đọc bằng find_one theo _id. Truy vấn theo _id dùng IDHACK, đọc đúng một document; hai lần đo cho trung vị 0,51 đến 0,94 mili giây, p95 1,14 đến 2,81 mili giây. Đây là đọc tuần tự từ một client, dữ liệu nằm trong cache, chưa phải đo dưới tải. Mỗi trang còn vài truy vấn nhỏ theo khóa hoặc index: meta, user_seen, tên sách, điểm trung bình stream và sách hot.

Ba quyết định thiết kế: lưu 30 nhưng hiện 10, nên khi người dùng vừa chấm một cuốn đang được gợi ý, app ẩn cuốn đó và đưa cuốn tiếp theo lên mà không phải huấn luyện lại; phần demo sẽ cho thấy điều này bằng script rate_from_recs.py. Khi đọc gần hết thì bù bằng sách phổ biến mà chính người đó chưa đọc. Đổi phiên bản an toàn: ghi collection mới, kiểm tra đủ số người dùng rồi mới chuyển con trỏ meta.active, thiếu thì bỏ bản mới; tên sách được cập nhật trước khi chuyển, và luôn giữ bản trước để quay lại. Và phía batch, write_recs ghi phân tán từ executor bằng foreachPartition, upsert theo _id nên chạy lại không trùng; phía streaming thì vẫn ghi ở driver, slide 13 sẽ nói. Dòng cuối là một kiểm tra độc lập bằng nosql_evidence.py: trong 1.602.720 cặp gợi ý và 534.240 cặp sách bù, không cặp nào là sách người dùng đã chấm.

### Slide 9 – Hiệu năng (~1 phút 10 giây)

Nhóm đo khả năng song song bằng cách chạy cùng một bước huấn luyện ALS rank 10, gồm cả chia validation và fit lại, với 1, 2 và 4 core: 159,3, 106,5 và 81,9 giây, tức 4 core nhanh 1,95 lần, không phải 4 lần. Lý do: phần tuần tự ở driver và shuffle giữa các vòng lặp ALS không tăng tốc được, đúng định luật Amdahl, và 4 core còn chia cho Kafka, MongoDB. RAM đỉnh 4,6 trên 7,7 GiB, đo bằng docker stats mỗi 5 giây. Với streaming, batch đầu tiên sau khi khởi động mất 7 đến 12 giây vì JVM còn lạnh; các batch sau thường 3 đến 6 giây, sát trigger 5 giây. Kết luận: dữ liệu lớn gấp 38 lần sẽ không vừa một máy, đó là lý do cho thiết kế cluster ở slide 13.

*Chuyển lượt:* Tiếp theo, Tuấn trình bày phần streaming.

---

## Phần 3 – Đinh Trần Quốc Tuấn (slide 10–12)

### Slide 10 – Streaming: thiết kế (~1 phút 40 giây)

Cảm ơn Kiệt. Producer phát lại rating thành sự kiện, cố ý gửi kèm bản trùng, sự kiện lỗi và sự kiện trễ. Kafka giữ sự kiện trong 3 partition, key là user_id nên sự kiện của một người vào cùng một partition; Kafka giữ thứ tự trong partition, nhưng khi producer gửi lại thì thứ tự không còn bảo đảm, vì vậy job không dựa vào thứ tự đến, mà dựa vào event time. Spark Structured Streaming chạy bốn query, mỗi query một checkpoint riêng: valid dùng watermark 10 phút và bỏ trùng theo event_id để tính điểm trung bình từ luồng stream; trending gom theo cửa sổ event time 10 phút, trượt 5 phút để ra "sách đang hot"; events_to_parquet lưu mọi sự kiện hợp lệ cho lần huấn luyện sau; rejected đếm sự kiện lỗi.

Theo ý tưởng Lambda: lớp tốc độ bỏ sự kiện trễ để giới hạn bộ nhớ trạng thái, còn kho lưu trữ giữ lại để lần huấn luyện sau học được, cho đến khi chạy reset_streaming.sh. Lưu ý: rating_stats chỉ gồm các sự kiện stream từ lần reset gần nhất, không gộp với thống kê batch; cột "Điểm TB (stream)" trong app cũng vậy. Thống kê theo sách của MapReduce và Spark SQL dùng cho phân tích và đối chiếu. Về bảo đảm: Kafka cộng checkpoint cho at-least-once; kho Parquet là exactly-once nhờ file sink; trending ghi đè theo sách và cửa sổ nên chạy lại không sai; còn valid đánh dấu batch đã ghi để bỏ qua khi bị phát lại. "Không mất" là kết quả đo, không phải bảo đảm: job đặt failOnDataLoss bằng false, nên nếu job dừng lâu hơn thời gian Kafka giữ log thì Spark chỉ cảnh báo rồi bỏ qua phần đã mất.

### Slide 11 – Streaming: kiểm chứng với Kafka (~2 phút 15 giây)

Nhóm giết đột ngột job streaming bằng kill -9 giữa chừng rồi chạy lại từ checkpoint, sau đó dùng kafka_audit.py đếm lại toàn bộ Kafka làm mốc và đọc lại MongoDB để so. Toàn bộ lượt thử nằm trong script stream_fault_test.sh, chạy khoảng 3 phút và ai cũng chạy lại được. Lượt ngày 08/11: producer gửi 1.500 sự kiện hợp lệ, trong đó 26 sự kiện trễ, kèm 32 bản trùng và 15 sự kiện lỗi. Bảng cho thấy cả bốn kiểm tra đều khớp: MongoDB đếm 1.474 sự kiện hợp lệ, đúng bằng 1.500 trừ 26 sự kiện trễ; tổng trong rating_stats cũng 1.474; 15 sự kiện lỗi bị loại đủ; và tổng lượt trong các cửa sổ trượt là 2.948, đúng bằng 2 lần 1.474, vì mỗi sự kiện đúng giờ thuộc 2 cửa sổ. Lặp lại lần hai có thêm kiểm tra kho Parquet cũng khớp: 1.475 bằng 1.500 trừ 25, kho giữ đủ 1.537 tin hợp lệ. Nhóm còn thử tắt Kafka 40 giây rồi MongoDB 30 giây khi đang gửi 3.000 sự kiện: Spark tự nối lại Kafka, còn khi MongoDB tắt thì job dừng; script thử nghiệm chạy lại job từ cùng checkpoint sau 11 giây, ngoài thử nghiệm thì phải chạy lại tay vì chưa có restart policy; kết quả 3.000 bằng 3.000, không mất, không trùng. Lượt đầu của thử nghiệm này lộ ra lỗi producer làm mất 571 tin: tin chờ trong bộ đệm hết hạn sau 30 giây mặc định của kafka-python, trong khi Kafka tắt hơn 40 giây, và không ai kiểm tra kết quả gửi. Nhóm đã sửa: cho tin chờ tới 2 phút, gửi lại khi request lỗi, và đếm tin gửi lỗi.

Lượt demo cùng ngày cũng khớp: 1.002 sự kiện hợp lệ, 19 sự kiện trễ, MongoDB đếm 983. Có một điều kiện nhóm phát hiện khi thử: Spark 3.5 lọc sự kiện trễ theo watermark của batch trước, nên 1–2 batch đầu sau khi khởi động mới, khi checkpoint còn trống, chưa lọc được; chạy lại từ checkpoint thì watermark được khôi phục. Vì vậy producer chỉ gửi sự kiện trễ sau 30 giây. Bên phải là Spark UI của query trending: batch đầu khoảng 12 giây, các batch sau khoảng 3 đến 6 giây, tốc độ vào khoảng 20 sự kiện mỗi giây.

### Slide 12 – Kiểm thử và kiểm chứng (~1 phút)

Nhóm có ba lớp kiểm thử. Unit test, 11 trên 11 đạt: mapper loại đúng dòng lỗi, combiner chạy 0, 1 hay nhiều lần đều cùng kết quả, gợi ý không bao giờ chứa sách đã đọc, và job streaming dừng khi checkpoint lệch MongoDB thay vì bỏ qua batch. Đối chiếu độc lập: MapReduce so với Spark SQL, SQL so với DataFrame, Kafka so với MongoDB, và hai script thử sự cố, kill -9 và tắt Kafka, MongoDB, chạy lại được. Và đọc lại MongoDB, kể cả kiểm tra không gợi ý nào là sách đã chấm. Nhờ kiểm chứng, nhóm tìm ra các lỗi thật ở dòng cuối; lỗi producer mất 571 tin đã nói ở slide 11. Hai lỗi khác đáng chú ý là: watermark chưa lọc ở batch đầu, đã xử lý bằng cách gửi sự kiện trễ sau 30 giây; và sự kiện phát lại từ ratings.csv đã có trong dữ liệu huấn luyện nên không bao giờ trúng top-10, vì vậy app nay hiển thị "Lượt 4–5 sao trúng top-10" dạng số trúng trên số lượt thích, và nhóm dùng rate_from_recs.py để tạo lượt chấm cho sách đang được gợi ý.

*Chuyển lượt:* Hảo trình bày thiết kế triển khai và giới hạn.

---

## Phần 4 – Lương Huỳnh Ngọc Hảo (slide 13–14)

### Slide 13 – Thiết kế triển khai cluster/cloud (~1 phút 40 giây)

Đây là thiết kế, chưa triển khai thật. Mỗi dòng ánh xạ một thành phần từ một máy lên production: Kafka 3 broker có replication; dữ liệu lên object storage như S3 hoặc MinIO để tách lưu trữ khỏi tính toán; Spark chạy trên Kubernetes hoặc YARN, phần batch mở rộng bằng số executor. Riêng streaming, nhóm nói thẳng: hiện hai query valid và trending gom mỗi micro-batch về driver rồi mới ghi MongoDB. Với tốc độ sự kiện của đồ án thì đủ, và maxOffsetsPerTrigger 20.000 giới hạn kích thước một batch sau khi job dừng lâu; nhưng ở quy mô lớn, việc đầu tiên phải đổi là ghi từ executor bằng foreachPartition hoặc MongoDB Spark connector, sau khi đã gộp theo sách trong Spark. MongoDB thành replica set 3 node; checkpoint lưu trên object storage nên job chết có thể đọc tiếp. Theo dõi lag: Spark không commit offset vào consumer group của Kafka, nên lấy metric avgOffsetsBehindLatest của Spark đưa lên Prometheus, Grafana.

Khi lên cluster, logic xử lý giữ nguyên nhưng phải sửa lớp cấu hình và viết lại vài đoạn I/O: pathlib biến hdfs:// thành hdfs:/, nên đường dẫn phải thành chuỗi URI và clean.py phải dùng FileSystem API của Hadoop; master đặt qua biến SPARK_MASTER, vì cờ --master của spark-submit bị .master() trong code ghi đè; checkpoint của ALS và streaming phải lên HDFS hoặc S3; số shuffle partition và bộ nhớ driver đang đặt cho laptop. Chi phí: huấn luyện chạy theo lịch trên cụm tạm thời, streaming và MongoDB thường trực nhưng tải nhỏ. Bảo mật: hiện MongoDB không xác thực, Kafka không mã hóa, chỉ chấp nhận trên laptop; production dùng SCRAM, TLS, tài khoản app chỉ đọc và secret manager.

### Slide 14 – Giới hạn và hướng phát triển (~1 phút 40 giây)

Ba nhóm giới hạn. Dữ liệu và hạ tầng: chưa chạy bộ UCSD đầy đủ; Goodbooks không có thời gian thật nên streaming là mô phỏng, chia train/test ngẫu nhiên; mọi thứ chạy một máy, Kafka một broker, MongoDB một node, Spark local, HDFS một DataNode, nên thành phần nào cũng là điểm lỗi duy nhất, và chưa có restart policy: job dừng phải chạy lại tay; RAM không đủ cho hai driver nên lớp batch và lớp tốc độ chạy lần lượt, trong lúc batch thì sự kiện chờ trong Kafka. Mô hình: mới thử 4 bộ tham số, nghiêng về sách nổi tiếng, mô hình phục vụ fit trên tập train, tức 80% dữ liệu, chưa fit lại trên toàn bộ; và vì reset_streaming.sh xóa kho stream_events trước mỗi lượt demo, mô hình cuối chưa học sự kiện stream nào. Streaming: nếu job chết đúng lúc đang ghi MongoDB, trước khi ghi last_batch, batch phát lại có thể cộng lặp một phần; việc ghi MongoDB vẫn ở driver; sự kiện mô phỏng chưa bị lọc khi gộp vào dữ liệu huấn luyện.

Hướng phát triển: dùng UCSD theo thể loại, có thời gian thật, để chia theo thời gian; ghi stream idempotent theo batch_id hoặc dùng transaction trên replica set, ghi từ executor; fit lại trên toàn bộ dữ liệu; chạy trên cluster theo thiết kế. Sau rà soát, nhóm đã sửa: Kafka lưu log trên volume, giới hạn 20.000 offset mỗi batch, kafka_audit đọc lại MongoDB và kho Parquet, producer chờ Kafka tới 2 phút và đếm tin gửi lỗi, write_recs kiểm tra đủ người dùng trước khi chuyển và giữ bản đang dùng cùng bản trước, job streaming dừng khi checkpoint lệch MongoDB, và có script thử sự cố chạy lại được.

---

### Slide 15 – Demo trực tiếp (8–10 phút)

Làm theo file `Present/BookPulse - Kich ban demo Session 10.md`: Hảo chạy MapReduce và trình bày Spark SQL, Kiệt trình bày app và MongoDB, Tuấn chạy streaming, cho user 1 chấm 5 sao ngay trong app ở mục Chấm sao (dự phòng: rate_from_recs.py) (2 cuốn đầu danh sách bị ẩn, cuốn thứ 3 lên đầu, chỉ số "Lượt 4–5 sao trúng top-10" tăng lên) và kiểm chứng Kafka với MongoDB bằng kafka_audit.

### Slide 16 – Xin cảm ơn

Phần trình bày của nhóm đến đây là hết. Mã nguồn, số đo và báo cáo nằm trên GitHub của nhóm. Cảm ơn thầy cô và các bạn đã lắng nghe, nhóm rất mong nhận câu hỏi và góp ý.

*Kiệt điều phối hỏi đáp: ai trình bày phần nào trả lời phần đó (xem file Hỏi đáp Session 10).*

---

## Phụ lục (không trình bày, mở khi được hỏi)

### Slide 17 – Phụ lục: kiến trúc chi tiết

Đây là kiến trúc đã chạy thật, tổ chức theo ý tưởng Lambda; đọc từ trái sang phải theo lớp. Cột trái là nguồn và thu nhận: file CSV Goodbooks-10k, và producer.py mô phỏng người dùng, gửi sự kiện rating vào Kafka, topic rating-events 3 partition, key user_id.

Hàng xanh dương là lớp batch, chạy theo đợt bằng run_pipeline.sh. B1: clean.py làm sạch CSV ra Parquet. B2: Spark SQL phân tích. B3: huấn luyện ALS, write_recs loại sách đã chấm. B4: ghi MongoDB hai bước: ① ghi collection gợi ý phiên bản mới, ② xong mới chuyển meta.active. Spark dùng đĩa local, không dùng HDFS.

Hàng cam là lớp tốc độ, chạy liên tục, trigger 5 giây. S1: producer gửi vào Kafka. S2: stream_stats.py đọc bằng 4 query. S3: upsert số liệu vào speed view trong MongoDB. S4: lưu sự kiện hợp lệ ra kho stream_events. S5: clean.py gộp kho này ở lần batch sau.

Bên phải là lớp phục vụ: MongoDB chứa batch view và speed view. Rồi lớp trình bày: app Streamlit đọc MongoDB và gộp lúc đọc, còn nút Chấm sao chỉ gửi sự kiện vào Kafka (mũi tên cam dưới cùng): gợi ý từ batch view, ẩn sách vừa chấm trong user_seen, điểm trung bình và sách hot từ speed view. Nhóm chưa hợp nhất hai view thành một số liệu.

Khung tím trên cùng là cụm Hadoop riêng: MapReduce trên HDFS và YARN, nhánh kiểm chứng, app không dùng. Nét đứt xám là đọc hoặc đối chiếu.
