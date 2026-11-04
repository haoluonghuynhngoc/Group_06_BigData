# BookPulse – Script thuyết trình Session 5


## Phân công và thời lượng

| Người trình bày | Slide | Nội dung | Thời lượng |
| --- | --- | --- | --- |
| Lương Huỳnh Ngọc Hảo | 1–4 | Mở đầu, đề xuất đồ án, kiến trúc, công nghệ sử dụng | ~4 phút 15 giây |
| Nguyễn Tuấn Kiệt | 5–6 | Spark ML, ví dụ dự đoán; điều phối hỏi đáp | ~2 phút 30 giây + hỏi đáp |
| Đinh Trần Quốc Tuấn | 7–11 | Luồng streaming, đánh giá, rủi ro, NoSQL và MongoDB, kế hoạch thực hiện, cảm ơn | ~5 phút 30 giây |

Tổng thời gian nói khoảng 10–11 phút, phần còn lại dành cho hỏi đáp. Nếu lớp quy định ít hơn, rút gọn slide 2 và 3 trước vì hai slide này có nhiều lời giải thích nhất.

---

## Phần 1 – Lương Huỳnh Ngọc Hảo (slide 1–4)

### Slide 1 – BookPulse (~30 giây)

Xin chào thầy cô và các bạn. Nhóm em gồm ba thành viên: Lương Huỳnh Ngọc Hảo, Đinh Trần Quốc Tuấn và Nguyễn Tuấn Kiệt.

Hôm nay nhóm trình bày Team Presentation 1 của môn BDA501: đề xuất đồ án cuối kỳ BookPulse – một hệ thống gợi ý sách dựa trên Big Data, cùng phần nền tảng về NoSQL và Spark ML. Nội dung gắn với chuẩn đầu ra CLO1, CLO4 và CLO5.

Nhóm xin lưu ý trước: đây là bản đề xuất thiết kế, nên hiện chưa có phần nào được triển khai hay đo đạc.

### Slide 2 – Problem, dataset, Big Data justification (~1 phút 30 giây)

Chắc ai ở đây cũng từng gặp cảnh này: đọc xong một cuốn sách hay, muốn tìm cuốn tiếp theo cùng gu, nhưng lên mạng thì chỉ thấy danh sách bán chạy, ai vào cũng thấy giống nhau. Sách thì có cả triệu đầu, mà gợi ý thì không dành riêng cho mình.

BookPulse là hệ thống gợi ý sách. Ví dụ: bạn An chấm 5 sao cho Harry Potter và Chúa tể những chiếc nhẫn. Bạn Bình cũng chấm 5 sao cho đúng hai cuốn đó, tức là hai bạn có gu giống nhau. Bình còn chấm 5 sao cho The Hobbit, mà An chưa đọc. Vậy hệ thống sẽ gợi ý The Hobbit cho An.

BookPulse làm đúng việc đó, nhưng trên hàng triệu lượt chấm điểm cùng lúc, và trả về cho mỗi người 10 cuốn họ có khả năng thích nhất. Trong app, chỉ cần nhập mã người dùng là thấy 10 cuốn đó.

Để làm được việc đó, nhóm cần dữ liệu đúng như bảng vừa rồi: ai chấm cuốn nào mấy sao. Mỗi dòng dữ liệu chỉ có ba thông tin: mã người dùng, mã sách và số sao.

Nhóm dùng hai bộ dữ liệu, cùng lấy từ Goodreads, một nhỏ một lớn. Bộ nhỏ là Goodbooks-10k: khoảng 6 triệu lượt chấm điểm của hơn 50 nghìn người cho 10 nghìn cuốn sách, chỉ khoảng 69 MB. Bộ này chạy vài phút là xong, nên nhóm dùng để viết code và sửa lỗi.

Bộ lớn là UCSD Book Graph, do nhóm nghiên cứu của Đại học California, San Diego thu thập từ Goodreads: khoảng 229 triệu lượt tương tác, gấp gần 40 lần bộ nhỏ, riêng file CSV đã khoảng 4,1 GB. Đây mới là bộ để chứng minh hệ thống chạy được với dữ liệu lớn. Nhóm sẽ không chạy cả bộ ngay mà bắt đầu với một vài thể loại, ví dụ fantasy, chạy ổn rồi mới mở rộng.

Hai điểm nhóm nói rõ luôn. Một: dữ liệu chỉ dùng cho học thuật. Hai: theo tài liệu, file này không ghi lại thời điểm người dùng chấm điểm, nhóm sẽ kiểm tra lại khi tải về. Không có thời điểm thì không phát lại được dòng sự kiện thật, nên phần streaming lát nữa bạn Tuấn trình bày dùng sự kiện mô phỏng.

Vậy dự án này có thật sự cần Big Data không? Nói thật thì với Goodbooks-10k, một laptop là đủ. Nhưng lên bộ UCSD thì khác: ma trận người dùng nhân sách có cỡ gần 900 nghìn dòng nhân hơn 2 triệu cột, mà gần như toàn ô trống. Lúc đó một máy với Pandas không còn kham nổi, và đó là chỗ Spark phát huy tác dụng.

Nhóm cũng không muốn chỉ nói "lớn" suông. Khi chạy thật, nhóm sẽ đo và báo cáo: dữ liệu bao nhiêu, chạy mất bao lâu, tốn bao nhiêu RAM, và tăng số core thì nhanh lên được bao nhiêu.

### Slide 3 – Architecture (~1 phút 30 giây)

*Sơ đồ để nắm luồng trước khi nói (không đọc lên):*

```text
BATCH PATH (chạy theo đợt)
┌──────────────┐   ┌──────────────┐   ┌─────────┐   ┌──────────┐   ┌──────────────┐   ┌───────────────┐
│ Goodbooks /  │──▶│ Validate +   │──▶│ Parquet │──▶│   ALS    │──▶│ Top-10 per   │──▶│   MongoDB     │
│ UCSD CSV     │   │ clean (Spark)│   │         │   │ training │   │ user         │   │ (gợi ý)       │
└──────────────┘   └──────────────┘   └─────────┘   └──────────┘   └──────────────┘   └───────┬───────┘
                                                                                              │
                                                                                              ▼
                                                                                     ┌─────────────────┐
                                                                                     │ Streamlit demo  │
                                                                                     │ user ID → top-10│
                                                                                     └─────────────────┘
                                                                                              ▲
STREAMING PATH (sự kiện mô phỏng)                                                             │
┌──────────────────┐   ┌─────────────┐   ┌──────────────────────┐   ┌───────────────────┐     │
│ Event generator /│──▶│ Kafka topic │──▶│ Structured Streaming │──▶│ MongoDB           │─────┘
│ replay           │   │             │   │ (Spark)              │   │ (statistics)      │
└──────────────────┘   └─────────────┘   └──────────────────────┘   └───────────────────┘
```

Vậy hệ thống chạy thế nào? Đây là kiến trúc nhóm đề xuất, chưa xây dựng. Hệ thống có hai luồng: hàng trên là luồng batch, tạo ra gợi ý; hàng dưới là luồng streaming, cập nhật số liệu theo thời gian thực.

Hàng trên, đi từ trái sang phải. Đầu tiên là file CSV, chính là bảng ai chấm cuốn nào mấy sao. Spark đọc file này và làm sạch: bỏ dòng trùng, bỏ dòng thiếu dữ liệu, bỏ rating không hợp lệ. Dữ liệu sạch được lưu sang Parquet, một định dạng nén theo cột, nên những lần sau đọc nhanh hơn CSV nhiều. Tiếp theo, mô hình ALS học từ dữ liệu này, chính là việc tìm ra An và Bình có gu giống nhau như ví dụ lúc nãy. Từ đó tính ra 10 cuốn sách cho mỗi người và lưu vào MongoDB.

Luồng này chạy theo đợt, ví dụ mỗi đêm một lần, chứ không chạy mỗi khi có người bấm vào app. Giống như quán ăn nấu sẵn từ sáng: khách vào là có món ngay, không phải chờ nấu.

Hàng dưới là luồng streaming. Vì dữ liệu không có thời điểm chấm điểm, nhóm viết một chương trình tự tạo các lượt chấm điểm giả, giống như có người đang chấm sách liên tục. Các sự kiện này được đẩy vào Kafka, đóng vai trò như một hàng đợi. Spark Structured Streaming đọc từ Kafka, tính các số liệu như mỗi cuốn sách được chấm trung bình mấy sao, bao nhiêu lượt trong vài phút gần nhất, rồi cập nhật vào MongoDB.

Cuối cùng là app demo bằng Streamlit, đọc dữ liệu từ MongoDB. Nhập mã người dùng vào là thấy 10 cuốn được gợi ý, kèm số liệu cập nhật trực tiếp.

Có một điểm nhóm muốn nói rõ: luồng streaming chỉ cập nhật số liệu, không huấn luyện lại mô hình sau mỗi lượt chấm. Huấn luyện lại vẫn là việc của luồng batch. Như vậy hệ thống đơn giản hơn, và một trong hai luồng có lỗi thì luồng kia vẫn chạy được.

### Slide 4 – Công nghệ sử dụng (~45 giây)

Sơ đồ này giống kiến trúc vừa rồi, nhưng ghi rõ tên công nghệ cho từng khối. Màu vẫn giữ như cũ: xám là dữ liệu vào, xanh là Spark xử lý, cam là nơi lưu, xanh đậm là ứng dụng.

Hàng trên là luồng batch: dữ liệu CSV từ Goodbooks và UCSD, Apache Spark để làm sạch, Parquet để lưu, và Spark MLlib với thuật toán ALS để huấn luyện và tính top-10.

Hàng dưới là luồng streaming: một đoạn code Python gửi các lượt chấm điểm giả, Apache Kafka làm hàng đợi, và Spark Structured Streaming tính thống kê liên tục.

Cả hai luồng đổ vào MongoDB, và Streamlit là app demo đọc từ đó. Tất cả đều là công cụ mã nguồn mở hoặc có bản miễn phí; phiên bản cụ thể nhóm sẽ chốt khi cài đặt ở Buổi 6.

*Chuyển lượt:* Tiếp theo, Kiệt sẽ đi vào luồng batch: mô hình Spark ML và một ví dụ dự đoán.

---

## Phần 2 – Nguyễn Tuấn Kiệt (slide 5–6)

### Slide 5 – Spark ML và bước tiếp theo (~1 phút 30 giây)

Cảm ơn Hảo. Em xin đi vào luồng batch, cụ thể là phần Spark ML. Hàng trên là năm bước của pipeline, em đi từ trái sang phải.

Dữ liệu rating đi vào bước làm sạch. Bước này là một Transformer: nó chỉ biến đổi dữ liệu, không cần học gì. Bước tiếp theo là huấn luyện ALS. ALS là một Estimator: nó phải học từ dữ liệu bằng hàm fit(), và kết quả là một model. Model đó dùng để gợi ý top-10 cho mỗi người. Cuối cùng nhóm đánh giá bằng RMSE và Precision@10.

Khung bên trái tóm lại bốn khái niệm. Transformer biến đổi dữ liệu. Estimator học từ dữ liệu rồi cho ra model. Pipeline nối các bước thành một chuỗi, giống hàng trên. Còn feature vector là gom nhiều cột thành một vector bằng VectorAssembler; ALS không cần bước này, nhóm chỉ dùng khi làm baseline.

Khung bên phải là các bước tiếp theo đến Buổi 10: kiểm tra dữ liệu UCSD và điều khoản sử dụng, so sánh ALS với baseline, và có bản demo chạy được kèm số đo thật.

### Slide 6 – Ví dụ: dữ liệu đầu vào và dự đoán (~1 phút)

Để dễ hình dung, slide này là một ví dụ nhỏ, các con số chỉ để minh họa.

Bên trái là đầu vào: bảng chấm điểm của ba người. An và Bình đều chấm Harry Potter và Chúa Nhẫn 5 sao, tức là hai bạn có gu giống nhau. Những ô có dấu hỏi là sách người đó chưa đọc, đây chính là chỗ mô hình phải đoán.

ALS ở giữa học gu của từng người từ những ô đã có số sao. Bên phải là đầu ra: điểm dự đoán cho những cuốn An chưa đọc. The Hobbit được 4,8 sao vì Bình, người có gu giống An, rất thích cuốn này. Hệ thống xếp các cuốn theo điểm và lấy 10 cuốn cao nhất, đó là top-10 của An.

Một lưu ý: dữ liệu thật không lưu thành bảng đầy đủ như vậy, vì bảng thật gần như toàn ô trống. Mỗi dòng dữ liệu chỉ gồm mã người dùng, mã sách và số sao.

*Chuyển lượt:* Tiếp theo, Tuấn sẽ trình bày luồng streaming, cách đánh giá, phần lưu trữ và kế hoạch thực hiện.

---

## Phần 3 – Đinh Trần Quốc Tuấn (slide 7–11)

### Slide 7 – Luồng streaming: một lượt chấm điểm đi đâu? (~1 phút)

Cảm ơn Kiệt. Phần của em là luồng streaming. Để dễ hiểu, em theo chân một lượt chấm điểm đi qua bốn bước ở hàng trên.

Bước một, gửi: vì nhóm chưa có người dùng thật, một đoạn code giả làm người dùng, gửi đi lượt chấm "người dùng 15 chấm sách 42 được 5 sao", kèm mã sự kiện và thời điểm.

Bước hai, xếp hàng: lượt chấm này vào Kafka, trong topic tên rating-events. Kafka giống hộp thư: nếu Spark đang tắt thì sự kiện vẫn nằm chờ, bật lại là Spark đọc tiếp từ chỗ cũ, không mất lượt nào.

Bước ba, kiểm tra và tính: cứ vài giây Spark lấy các sự kiện mới ra, bỏ lượt trùng và lượt lỗi, rồi tính lại điểm trung bình. Ví dụ sách 42 đang 4,50 sao, thêm một lượt 5 sao thì thành 4,502.

Bước bốn, lưu và hiển thị: con số mới được ghi đè vào MongoDB, nên app hiện ngay điểm mới nhất.

Tóm lại: có người chấm điểm thì vài giây sau con số đã cập nhật. Luồng này chỉ cập nhật thống kê, không huấn luyện lại mô hình. Các con số trên slide chỉ để minh họa.

### Slide 8 – Streaming, đánh giá và rủi ro (~2 phút)

Slide này nói kỹ hơn ba chuyện: Spark phải làm gì để số liệu đúng, nhóm đánh giá hệ thống thế nào, và các rủi ro.

Hàng trên là đường đi của một sự kiện: bộ sinh tạo ra lượt chấm điểm giả, gửi vào Kafka; Spark Structured Streaming đọc ra, tính số liệu rồi ghi vào MongoDB.

Khung đầu tiên là ba việc Spark phải làm cho đúng. Một, loại trùng: mỗi sự kiện có một mã event_id, gặp mã đã thấy thì bỏ qua. Hai, watermark: sự kiện đến trễ vẫn được tính nếu trễ trong ngưỡng cho phép, quá ngưỡng thì bỏ. Ba, checkpoint và upsert: khi hệ thống tắt rồi bật lại, Spark đọc tiếp từ chỗ cũ; nếu có sự kiện bị đọc lại thì việc ghi đè theo _id cố định giúp số liệu không bị đếm hai lần.

Khung giữa là cách đánh giá. Tất cả sẽ được đo, hiện chưa có số liệu. RMSE cho biết mô hình đoán số sao lệch bao nhiêu, so với baseline. Precision@10 và Recall@10 cho biết 10 cuốn được gợi ý có trúng những cuốn người dùng thật sự thích không. Thời gian chạy và throughput để đo hiệu năng.

Khung cuối là rủi ro, mỗi rủi ro có một phương án dự phòng. Kafka khó tích hợp thì cho Spark đọc sự kiện từ thư mục file. Thiếu RAM thì chạy một thể loại trước. Người dùng mới chưa có lịch sử chấm điểm thì gợi ý sách phổ biến.

### Slide 9 – NoSQL, CAP và lựa chọn MongoDB (~1 phút 30 giây)

Tiếp theo là phần lưu trữ, với nền tảng NoSQL. Bảng này có bốn mô hình chính: mỗi dòng là một mô hình, kèm phần mềm ví dụ, cách truy cập, và chỗ có thể dùng trong BookPulse.

Key-value như Redis: lấy dữ liệu theo khóa, rất nhanh; nhóm chỉ dùng làm cache nếu cần. Document như MongoDB: mỗi bản ghi là một khối JSON, truy vấn theo trường; đây là lựa chọn của nhóm. Wide-column như Cassandra: mạnh khi ghi rất nhiều và truy vấn theo partition key; là phương án nếu sau này lượng rating ghi vào quá lớn. Graph như Neo4j: dành cho dữ liệu dạng quan hệ; nhóm không dùng.

Vì sao chọn MongoDB? App chỉ cần một truy vấn chính: đưa user_id vào, lấy ra 10 cuốn sách. Mỗi người dùng một document là đủ, cấu trúc linh hoạt, và dễ bắt đầu với nhóm ba người.

Cuối cùng là định lý CAP: khi mạng giữa các máy bị chia cắt, hệ thống phải chọn giữa nhất quán, tức là tạm từ chối yêu cầu để không trả dữ liệu sai, hoặc sẵn sàng, tức là vẫn trả lời nhưng có thể là dữ liệu cũ. Vì vậy nhóm chọn cơ sở dữ liệu theo cách ứng dụng đọc và ghi, chứ không theo cái nào "mạnh hơn".

### Slide 10 – Kế hoạch thực hiện (~1 phút)

*Phân công và mốc thời gian trên slide là dự kiến, nhóm cần xác nhận trước khi trình bày.*

Cuối cùng, nhóm sẽ làm trong bao lâu? Kế hoạch chia làm ba giai đoạn, từ Buổi 6 đến Buổi 10.

Buổi 6 và 7 là phần dữ liệu và luồng batch: tải hai bộ dữ liệu, kiểm tra lại các con số và chuyện có timestamp hay không, làm sạch rồi lưu Parquet. Sau đó chạy ALS và các baseline trên bộ nhỏ Goodbooks-10k trước.

Buổi 8 và 9 là luồng streaming và app: viết bộ sinh sự kiện, nối Kafka với Spark Structured Streaming, ghi số liệu vào MongoDB, dựng app Streamlit, và thử tắt rồi bật lại hệ thống xem số liệu có bị đếm trùng không.

Buổi 10 là đánh giá và báo cáo: chạy trên tập con UCSD để đo thời gian, bộ nhớ và mức tăng tốc, lập bảng RMSE và Precision@10 so với baseline, rồi demo và nộp báo cáo.

Về phân công: Hảo phụ trách dữ liệu và pipeline batch, em phụ trách streaming và đánh giá, Kiệt phụ trách MongoDB, Spark ML và app. Nhóm làm phần batch trước vì mọi phần sau đều dựa vào nó.

### Slide 11 – Xin cảm ơn (~15 giây, sau đó hỏi đáp)

*Tuấn nói lời cảm ơn, sau đó Kiệt điều phối hỏi đáp.*

Phần trình bày của nhóm đến đây là hết. Cảm ơn thầy cô và các bạn đã lắng nghe. Nhóm rất mong nhận được câu hỏi và góp ý.

*Hỏi đáp – Kiệt điều phối, ai trình bày phần nào thì trả lời phần đó:*

- Hảo: bài toán, dataset, lý do cần Big Data, kiến trúc tổng thể.
- Tuấn: streaming, đánh giá, rủi ro, NoSQL, CAP, lựa chọn MongoDB, kế hoạch thực hiện.
- Kiệt: Spark ML, ALS, ví dụ dự đoán, các bước tiếp theo đến Buổi 10.

---

## Dự đoán câu hỏi của giảng viên

Mỗi câu ghi người trả lời chính và gợi ý ý trả lời. Câu đánh dấu **(cần thống nhất)** là điểm deck chưa nói rõ: cả nhóm nên chốt câu trả lời trước buổi thuyết trình.

Nguyên tắc chung: đây là bản đề xuất, nên khi chưa có số liệu thì nói thẳng "nhóm chưa đo, sẽ đo ở giai đoạn triển khai", không đoán con số.

### Hảo – bài toán, dữ liệu, kiến trúc

**1. Goodbooks-10k chỉ khoảng 69 MB, vậy tại sao gọi là Big Data?**
Nhóm đồng ý Goodbooks-10k không phải Big Data; nó chỉ là tập phát triển để viết pipeline nhanh. Phần Big Data là UCSD Book Graph, khoảng 229 triệu tương tác và 4,1 GB CSV, có thể vượt bộ nhớ của một máy khi join và huấn luyện. Nhóm không khẳng định suông: sẽ đo thời gian chạy, bộ nhớ và mức tăng tốc khi tăng số core trên các tập con lớn dần.

**2. Lấy tập con theo thể loại có làm lệch kết quả không?**
Có, tập con có thể lệch về một số thể loại phổ biến. Nhóm dùng tập con để kiểm tra pipeline và bộ nhớ trước, sau đó tăng dần kích thước, và sẽ ghi rõ kết quả thuộc tập con nào. Kết luận chỉ áp dụng cho phạm vi dữ liệu đã chạy.

**3. Tại sao lưu sang Parquet thay vì đọc CSV trực tiếp?**
Parquet là định dạng cột, có nén và lưu sẵn schema. Spark chỉ đọc các cột cần dùng và lọc sớm được (predicate pushdown), nên các lần huấn luyện và đánh giá lặp lại sẽ nhanh hơn nhiều so với parse CSV mỗi lần.

**4. Nhóm đã chạy thử phần nào chưa? Lấy gì đảm bảo khả thi?**
Chưa, đây là bản đề xuất. Mốc đầu tiên là chạy toàn bộ pipeline batch trên Goodbooks-10k, vì tập này nhỏ và chạy được trên một máy. Các phần rủi ro như Kafka đều có phương án dự phòng (slide 8).

**5. Hệ thống sẽ chạy trên hạ tầng nào: máy cá nhân, cluster hay cloud?** **(cần thống nhất)**
Deck chưa ghi điểm này. Gợi ý một câu trả lời trung thực: phát triển bằng Spark local mode và Kafka, MongoDB chạy bằng Docker trên máy cá nhân; đo khả năng mở rộng bằng cách tăng số core. Nếu có cluster hoặc cloud của trường thì sẽ chạy thử ở đó. Nhóm cần chốt câu trả lời thật trước buổi thuyết trình.

**6. Dữ liệu có vấn đề bản quyền hay quyền riêng tư không?**
Dataset được công bố cho mục đích học thuật, và nhóm chỉ dùng cho đồ án. Người dùng được định danh bằng ID, nhóm không thu thập thêm thông tin cá nhân. Điều khoản sử dụng của bộ UCSD nằm trong danh sách cần kiểm chứng ở slide 5.

### Tuấn – streaming, đánh giá, rủi ro, NoSQL, MongoDB

**7. Dữ liệu streaming là mô phỏng, vậy dùng Kafka để làm gì?**
Để thể hiện đúng kiến trúc streaming của môn học: Kafka tách bên sinh sự kiện khỏi bên xử lý, lưu sự kiện để phát lại và quản lý offset. Nhóm ghi rõ sự kiện là mô phỏng và không dùng chúng để khẳng định hiệu năng thực tế.

**8. Hệ thống có đảm bảo exactly-once không?**
Spark đọc Kafka kèm checkpoint đảm bảo at-least-once, nghĩa là một sự kiện có thể được ghi lại khi restart. Nhóm xử lý bằng upsert vào MongoDB với _id cố định, nên ghi lại nhiều lần vẫn cho cùng một kết quả. Kết quả cuối vì vậy tương đương exactly-once. Nhóm sẽ kiểm chứng bằng một lần restart có kiểm soát.

**9. Watermark là gì và nhóm chọn ngưỡng bao lâu?** **(cần thống nhất con số)**
Watermark là ngưỡng thời gian Spark chờ sự kiện đến trễ. Quá ngưỡng thì sự kiện bị bỏ qua, và Spark được giải phóng trạng thái cũ để bộ nhớ không tăng mãi. Ngưỡng cụ thể chưa chốt; nhóm sẽ chọn dựa trên độ trễ mà bộ sinh sự kiện tạo ra, ví dụ vài phút.

**10. Người dùng mới hoặc sách mới chưa có rating thì gợi ý thế nào?**
Đây là bài toán cold start. Người dùng mới nhận danh sách sách phổ biến. Khi đánh giá, nhóm đặt coldStartStrategy = "drop" trong ALS của Spark để bỏ các dòng không dự đoán được, tránh giá trị NaN làm hỏng RMSE.

**11. Precision@10 và Recall@10 tính cụ thể như thế nào?**
Với mỗi người dùng, nhóm lấy 10 sách mô hình gợi ý, sau khi loại các sách người đó đã đánh giá trong tập train. "Liên quan" là các sách trong tập test mà người dùng chấm 4 hoặc 5 sao. Precision@10 là tỷ lệ sách liên quan trong 10 gợi ý; Recall@10 là tỷ lệ sách liên quan được tìm thấy. Kết quả lấy trung bình trên tất cả người dùng.

**12. Data leakage trong đồ án này là gì?**
Là khi thông tin của tập test lọt vào bước huấn luyện. Ví dụ: tính độ phổ biến hoặc rating trung bình trên toàn bộ dữ liệu, hoặc chọn tham số dựa trên tập test. Nhóm chỉ tính thống kê trên tập train, chọn tham số trên tập validation, và chỉ dùng tập test một lần ở cuối.

**13. Theo CAP, MongoDB và Cassandra thuộc loại nào?**
Không nên gắn nhãn cứng vì cả hai đều chỉnh được. MongoDB với cấu hình mặc định nghiêng về nhất quán: khi phân vùng, primary nằm ở phía thiểu số sẽ thôi làm primary và ngừng nhận ghi. Cassandra thường được cấu hình nghiêng về sẵn sàng, và mức nhất quán chỉnh được theo từng truy vấn. Demo một node của nhóm không kiểm chứng được điều này.

**14. Cassandra ghi nhanh hơn, sao nhóm không chọn Cassandra?**
Truy vấn chính của BookPulse là đọc top-10 của một người dùng. Dữ liệu ghi gồm một lần ghi hàng loạt sau mỗi lần huấn luyện và các lần cập nhật thống kê với tốc độ vừa phải. MongoDB đáp ứng đủ, linh hoạt hơn khi truy vấn và dễ vận hành với nhóm ba người. Cassandra chỉ hợp lý khi tốc độ ghi rating rất cao.

**15. Dữ liệu gợi ý trong MongoDB được lưu theo cấu trúc nào?**
Mỗi người dùng một document: _id là user_id, kèm mảng recs gồm book_id, score và rank, thêm phiên bản mô hình và thời điểm tạo. Ứng dụng đọc đúng một document theo _id, vốn đã có index sẵn, nên truy vấn rất nhanh. Thống kê streaming nằm ở collection riêng, rating_stats.

**16. Tại sao không dùng Redis làm cache?**
Với quy mô demo, đọc một document theo _id trong MongoDB đã đủ nhanh, nên thêm Redis chỉ tăng số thành phần phải vận hành. Redis được nêu ở slide 9 như một lựa chọn khi cần độ trễ thấp hơn nữa.

### Kiệt – Spark ML, kế hoạch

**17. ALS có những tham số nào và nhóm chọn ra sao?**
Các tham số chính là rank (số yếu tố ẩn), regParam (mức chống overfitting) và maxIter (số vòng lặp). Vì đây là rating 1–5 sao nên dùng chế độ explicit (implicitPrefs = false). Nhóm dùng ParamGridBuilder kèm TrainValidationSplit hoặc CrossValidator để thử một lưới tham số trên tập validation. Lưới cụ thể sẽ chốt ở giai đoạn triển khai.

**18. RMSE thấp có nghĩa là gợi ý tốt không?**
Không hẳn. RMSE đo độ chính xác khi dự đoán điểm số, còn người dùng chỉ thấy 10 sách đứng đầu. Vì vậy nhóm đo thêm Precision@10 và Recall@10, và so cả hai loại chỉ số với baseline.

**19. Tại sao chọn ALS mà không dùng deep learning hay content-based?**
ALS có sẵn trong Spark MLlib, chạy phân tán và là baseline mạnh cho dữ liệu rating dạng explicit. Đây cũng là đúng nội dung Spark ML của môn học. Gợi ý theo nội dung có thể thêm như phần tùy chọn bằng VectorAssembler; deep learning nằm ngoài phạm vi đồ án.

**20. Kế hoạch đến Session 10 có thực tế không?**
Nhóm tách rõ MVP, gồm mọi khối trong sơ đồ kiến trúc, với phần tùy chọn là replica set và lịch huấn luyện lại. Mỗi rủi ro lớn đều có phương án dự phòng. Nếu thiếu thời gian, nhóm ưu tiên pipeline batch, kết quả đánh giá và demo trước, streaming sau.

### Câu hỏi chung (ai cũng có thể bị hỏi)

**21. Phần khó nhất hoặc rủi ro lớn nhất của đồ án là gì?**
Tích hợp Kafka với Spark (tương thích phiên bản, checkpoint, restart) và bộ nhớ khi chạy ALS trên dữ liệu UCSD. Cả hai đều có phương án dự phòng ở slide 8.

**22. Công việc trong nhóm được chia như thế nào?** **(cần thống nhất)**
Phân công dự kiến đã ghi ở slide 10: Hảo phụ trách dữ liệu và pipeline batch, Tuấn phụ trách streaming và đánh giá, Kiệt phụ trách MongoDB, Spark ML và ứng dụng demo. Nhóm cần xác nhận đây là cách chia thật trước khi trình bày.

---

## Câu hỏi theo chủ đề: tại sao dùng, thuật toán gì, dữ liệu ra sao

Phần này gom các câu hỏi "tại sao" thường gặp nhất, trả lời ngắn để cả nhóm cùng thuộc. Một số câu trùng ý với phần trên, ghi số câu liên quan để đọc thêm.

### A. Tại sao dùng công nghệ này?

**A1. Tại sao dùng Spark mà không dùng Pandas?**
Bộ UCSD có khoảng 229 triệu dòng, file CSV khoảng 4,1 GB. Pandas phải nạp toàn bộ vào RAM của một máy, và khi xử lý thường tốn gấp vài lần dung lượng file. Spark chia dữ liệu thành nhiều partition, xử lý song song trên nhiều core hoặc nhiều máy, và có sẵn ALS chạy phân tán. Với bộ nhỏ Goodbooks-10k thì Pandas vẫn đủ, nhóm nói rõ điều này (câu 1).

**A2. Tại sao cần Kafka?**
Kafka là hàng đợi đứng giữa bên gửi sự kiện và Spark. Nếu Spark đang bận hoặc bị tắt, sự kiện vẫn nằm trong Kafka, không bị mất. Kafka cũng lưu vị trí đã đọc (offset), nên Spark có thể đọc tiếp hoặc phát lại từ một điểm (câu 7).

**A3. Tại sao dùng Structured Streaming thay vì cứ vài phút chạy batch một lần?**
Structured Streaming cập nhật số liệu sau vài giây, và có sẵn các công cụ cho dữ liệu chảy liên tục: watermark cho sự kiện trễ, loại trùng, checkpoint để khởi động lại đúng chỗ. Tự chạy batch lặp lại thì phải tự viết lại các phần này.

**A4. Tại sao dùng MongoDB?**
Truy vấn chính của app là đưa user_id vào và lấy ra 10 cuốn sách. Mỗi người dùng một document chứa sẵn danh sách đó, đọc theo _id rất nhanh. Document cũng linh hoạt khi cần thêm trường, và dễ bắt đầu với nhóm ba người (câu 14, 15).

**A5. Sao không dùng cơ sở dữ liệu SQL như MySQL hay PostgreSQL?**
Ở quy mô demo, SQL vẫn làm được. Nhóm chọn MongoDB vì dữ liệu đọc ra là một danh sách lồng trong từng người dùng, hợp với dạng document, và MongoDB mở rộng ngang bằng sharding. Ngoài ra môn học yêu cầu phân tích và áp dụng NoSQL.

**A6. Tại sao lưu Parquet?**
Parquet lưu theo cột, có nén và ghi sẵn kiểu dữ liệu. Spark chỉ đọc đúng các cột cần, nên các lần huấn luyện lặp lại nhanh hơn nhiều so với đọc CSV (câu 3).

**A7. Tại sao dùng Streamlit?**
Streamlit dựng giao diện web bằng Python trong vài chục dòng code, đủ để demo: nhập user ID, hiện top-10 và số liệu. App chỉ là phần trình diễn, không phải trọng tâm của đồ án.

### B. Dùng thuật toán gì?

**B1. Nhóm dùng thuật toán gì để gợi ý?**
ALS (Alternating Least Squares), một thuật toán lọc cộng tác (collaborative filtering) dạng phân rã ma trận, có sẵn trong Spark MLlib.

**B2. ALS hoạt động như thế nào?**
Hình dung một bảng lớn: mỗi dòng là một người dùng, mỗi cột là một cuốn sách, ô là số sao. Phần lớn ô bị trống. ALS tách bảng này thành hai bảng nhỏ: mỗi người dùng được mô tả bằng k con số (gu đọc), mỗi cuốn sách cũng bằng k con số (đặc điểm). Điểm dự đoán là tích của hai dãy số đó. ALS học bằng cách luân phiên: giữ cố định phần sách để tính phần người dùng, rồi đổi ngược lại, lặp nhiều vòng. Mỗi bước tính độc lập cho từng người hoặc từng sách nên chia ra chạy song song được.

**B3. Tại sao chọn ALS?**
Dữ liệu chỉ có ai chấm cuốn nào mấy sao, không cần nội dung sách. ALS có sẵn trong Spark, chạy phân tán tốt, và là baseline mạnh cho bài toán gợi ý từ rating. Nó cũng đúng phần Spark ML của môn học (câu 19).

**B4. Có thuật toán nào khác không, sao không dùng?**
- Gợi ý sách phổ biến: rất đơn giản, nhóm dùng làm baseline để so sánh.
- Item-based KNN (tìm sách giống nhau): phải so từng cặp sách, với hơn 2 triệu cuốn thì quá tốn kém.
- Content-based (dựa trên nội dung, thể loại, mô tả): cần thêm dữ liệu nội dung, có thể làm phần tùy chọn bằng VectorAssembler.
- Deep learning: nằm ngoài phạm vi và thời gian của đồ án.

**B5. Baseline là gì?**
Là cách làm đơn giản để so sánh, xem ALS có thật sự tốt hơn không. Với RMSE: đoán mọi rating bằng điểm trung bình chung, hoặc bằng điểm trung bình của từng cuốn. Với Precision@10: gợi ý cho ai cũng là 10 cuốn phổ biến nhất.

**B6. Các tham số chính của ALS?**
rank (số con số k mô tả mỗi người và mỗi sách), regParam (chống học thuộc dữ liệu), maxIter (số vòng lặp), và coldStartStrategy = "drop" khi đánh giá. Chọn bằng thử lưới tham số trên tập validation (câu 17).

**B7. Đánh giá thuật toán bằng gì?**
RMSE đo đoán số sao lệch bao nhiêu. Precision@10 và Recall@10 đo 10 cuốn gợi ý có trúng sách người dùng thích (chấm 4–5 sao) không. Cả hai đều so với baseline (câu 11, 18).

### C. Dữ liệu như thế nào?

**C1. Dữ liệu có những cột gì?**
- Goodbooks-10k, file ratings: user_id, book_id, rating (1–5 sao). Có thêm file thông tin sách (tên, tác giả, năm…).
- UCSD Book Graph, file tương tác CSV: user_id, book_id, is_read (đã đọc chưa), rating (0–5, trong đó 0 là chưa chấm), is_reviewed (có viết review không).

**C2. Dữ liệu lớn cỡ nào?**
Theo trang công bố, cần kiểm tra lại khi tải về:
- Goodbooks-10k: khoảng 6 triệu rating, khoảng 53 nghìn người dùng, 10 nghìn cuốn sách, khoảng 69 MB.
- UCSD: khoảng 229 triệu tương tác, khoảng 876 nghìn người dùng, khoảng 2,36 triệu cuốn sách, CSV khoảng 4,1 GB.

**C3. Dữ liệu thưa đến mức nào?**
Goodbooks-10k: khoảng 1,1% số ô trong bảng người dùng × sách có giá trị. UCSD: chỉ khoảng 0,01%. Bảng gần như toàn ô trống, nên phải dùng thuật toán như ALS, không thể lưu và tính trên cả bảng đầy đủ.

**C4. Làm sạch dữ liệu những gì?**
Bỏ dòng trùng cùng người dùng và cùng sách, bỏ dòng thiếu giá trị, bỏ rating ngoài khoảng 1–5 (với UCSD là bỏ rating = 0 vì đó là chưa chấm). Có thể lọc thêm người dùng hoặc sách có quá ít rating; ngưỡng cụ thể sẽ chốt khi xem dữ liệu thật.

**C5. Chia train và test thế nào?**
Vì không có thời điểm chấm điểm, nhóm chia ngẫu nhiên theo từng người dùng, ví dụ 80% train và 20% test, tách thêm một phần validation từ train để chọn tham số. Tập test chỉ dùng một lần ở cuối (câu 12).

**C6. "ID maps" trên slide là gì?**
Trong bộ UCSD, mã người dùng và mã sách ở file CSV là số đã được mã hóa lại, đi kèm file ánh xạ về mã gốc của Goodreads. ALS của Spark yêu cầu mã dạng số nguyên, nên nhóm cần kiểm tra file ánh xạ này để nối được với thông tin sách (tên, thể loại).

**C7. Dữ liệu streaming lấy từ đâu?**
Mô phỏng: chương trình lấy mẫu các rating có sẵn trong dataset, gán thêm mã event_id và thời điểm event_time mới, rồi gửi vào Kafka. Chương trình cố ý chèn thêm sự kiện trùng và sự kiện lỗi để kiểm tra hệ thống. Mọi sự kiện đều được ghi rõ là mô phỏng.
