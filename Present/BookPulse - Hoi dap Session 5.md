# BookPulse – Hỏi đáp Session 5

Tài liệu ôn hỏi đáp cho buổi thuyết trình BDA501 Session 5 (Team Presentation 1). Số slide trong tài liệu theo bản tiếng Việt 8 slide.

## Nguyên tắc khi trả lời

- **Ai trình bày phần nào thì trả lời phần đó.** Kiệt điều phối: nghe câu hỏi, chỉ định người trả lời, bổ sung nếu cần.
- **Trả lời ngắn trước, giải thích sau.** Nói ý chính trong một câu, giảng viên hỏi tiếp mới đi vào chi tiết.
- **Chưa có số liệu thì nói thẳng.** Đây là bản đề xuất: "Nhóm chưa đo, sẽ đo ở giai đoạn triển khai." Không đoán con số.
- **Không biết thì ghi nhận.** "Nhóm chưa tìm hiểu kỹ điểm này, nhóm sẽ bổ sung trong báo cáo." Tốt hơn trả lời sai.

## Phần 1 – Trả lời nhanh trong một câu

| Nếu bị hỏi | Trả lời một câu |
|---|---|
| BookPulse làm gì? | Gợi ý 10 cuốn sách cho mỗi người dựa trên cách những người có gu giống họ chấm điểm. |
| Vì sao cần Big Data? | Bộ UCSD có khoảng 229 triệu tương tác; bảng người dùng × sách cỡ 876 nghìn × 2,36 triệu và gần như trống, một máy không xử lý nổi. |
| Dùng thuật toán gì? | ALS (lọc cộng tác, phân rã ma trận) trong Spark MLlib. |
| Dữ liệu gồm gì? | Mỗi dòng: mã người dùng, mã sách, số sao; Goodbooks-10k để phát triển, UCSD Book Graph để chạy quy mô lớn. |
| Streaming để làm gì? | Cập nhật số liệu rating sau vài giây; không huấn luyện lại mô hình. |
| Lưu kết quả ở đâu? | MongoDB, mỗi người dùng một document chứa sẵn top-10. |
| Đánh giá thế nào? | RMSE, Precision@10 và Recall@10, so với baseline. |
| Đã có kết quả chưa? | Chưa, đây là bản đề xuất; mọi kết quả sẽ được đo khi triển khai. |

## Câu nhóm cần chốt trước buổi thuyết trình

- **Câu 5** – Chạy trên hạ tầng nào: máy cá nhân, cluster hay cloud.
- **Câu 9** – Ngưỡng watermark cụ thể.
- **Câu 22** – Phân công thật trong nhóm (slide 10 đang ghi là dự kiến).
- **Số liệu dataset** – Kiểm tra lại số dòng, số người dùng, số sách và việc có timestamp hay không khi tải dữ liệu về.

## Phần 2 – Câu hỏi dự đoán theo người trả lời

Câu đánh dấu **(cần thống nhất)** là điểm deck chưa nói rõ.

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

## Phần 3 – Câu hỏi theo chủ đề: tại sao dùng, thuật toán gì, dữ liệu ra sao

Các câu hỏi "tại sao" thường gặp nhất. Câu nào trùng ý với phần 2 có ghi số câu để đọc thêm.

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
