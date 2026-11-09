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
| Mô hình nào, tốt cỡ nào? | ALS implicit, Precision@10 0,204 (sách phổ biến 0,078; ALS explicit 0,008); mô hình phục vụ fit trên toàn bộ tập train (80% dữ liệu) |
| Vì sao MongoDB? | Truy vấn chính theo user_id → document mỗi người, đọc bằng `find_one({_id})`; p50 0,51–0,94 ms, p95 1,14–2,81 ms (hai lần đo, một client, dữ liệu nằm trong cache: chưa đo dưới tải) |
| Batch và streaming chạy cùng lúc? | Không, trên máy này chạy lần lượt: hai driver 4 GB không vừa 7,7 GiB; trong lúc batch, sự kiện chờ trong Kafka (K19) |
| Streaming có gì? | Kafka 3 partition; 4 query: watermark + bỏ trùng, cửa sổ trượt 10/5 phút, kho lưu trữ, đếm lỗi; lượt kill -9 và lượt tắt Kafka/MongoDB: 0 mất, 0 trùng (kết quả đo, không phải bảo đảm; còn khe hở khi chết giữa lúc ghi MongoDB) |
| Lambda hay Kappa? | Theo ý tưởng Lambda; chưa hợp nhất batch view và speed view, app chỉ gộp lúc đọc; không Kappa vì 6 triệu rating lịch sử chưa từng qua Kafka (K3) |
| App hiện "2 / 672" là sao? | Rating phát lại đã có trong dữ liệu huấn luyện nên không thể trúng; 2 lượt trúng do `rate_from_recs.py` |
| Có phải Big Data không? | Goodbooks là bộ phát triển; đã đo giới hạn một máy; bộ đầy đủ ×38 cần cluster (slide 13) |

---

## Kiến trúc – phần giảng viên chú trọng (Hảo trả lời, cả nhóm cần nắm)

Mở slide 3 (sơ đồ tổng quan) khi trả lời, chỉ tay theo đường đi của dữ liệu; hỏi sâu về một thành phần thì mở slide 17 (sơ đồ chi tiết, cùng hình trong `bookpulse/docs/kien-truc.html`). Các câu hay bị hỏi có phần **Nói ngắn** (khoảng 30–45 giây) để nói trước; phần sau để đọc thêm khi bị hỏi tiếp.

**K1. Trình bày kiến trúc trong 30 giây.**
**Nói ngắn:** "Trái sang phải: dữ liệu nguồn, rồi hai lớp: lớp batch Spark làm sạch, phân tích, huấn luyện ALS và tính sẵn gợi ý; lớp tốc độ đọc Kafka mỗi 5 giây để cập nhật số liệu. Cả hai ghi vào MongoDB, app đọc MongoDB và gộp lúc đọc, còn nút chấm sao của app chỉ gửi sự kiện vào Kafka; Hadoop MapReduce là nhánh kiểm chứng."

Slide 3 là sơ đồ tổng quan (Nguồn · Thu nhận · Xử lý · Phục vụ · Trình bày; kho Parquet ở giữa hai lớp), slide 17 là sơ đồ chi tiết (phụ lục: bước B1–B4, S1–S5, tên file và collection). Đọc theo slide 17, từ trái sang phải. **Nguồn · thu nhận**: file CSV Goodbooks-10k; `producer.py` mô phỏng người dùng gửi sự kiện vào Kafka (topic `rating-events`, 3 partition, key user_id, log trên volume riêng). **Lớp batch** (xanh dương, chạy theo đợt bằng `scripts/run_pipeline.sh`): B1 `clean.py` → Parquet, B2 Spark SQL, B3 ALS, B4 `write_recs.py` ghi collection gợi ý mới rồi mới chuyển `meta.active`. **Lớp tốc độ** (cam, chạy liên tục, trigger 5 giây): `stream_stats.py` với 4 query, upsert số liệu vào MongoDB và lưu sự kiện ra kho `stream_events`, kho này được `clean.py` gộp ở lần batch sau. **Lớp phục vụ**: MongoDB 7.0, batch view (gợi ý, `meta`, `books`) và speed view (`rating_stats`, `user_seen`, `trending`, `online_eval`, `stream_state`). **Lớp trình bày**: app Streamlit đọc MongoDB, gộp hai view lúc đọc; mục "Chấm sao" của app chỉ gửi sự kiện vào Kafka (mũi tên cam từ app về `rating-events`), không ghi MongoDB. Hadoop (tím) là cụm riêng, **nhánh kiểm chứng**: MapReduce trên HDFS + YARN, kết quả so với Spark SQL, app không dùng; HDFS chỉ là chỗ lưu cho MapReduce, Spark đọc đĩa local.

**K2. Vì sao chọn từng công nghệ?**
Kafka: log bền, đọc lại được theo offset và chia partition, nên Spark tắt rồi chạy lại vẫn đọc tiếp được, miễn là trong thời gian Kafka còn giữ log (mặc định 7 ngày). Spark: một engine cho batch, SQL, ML và streaming, chạy trong bộ nhớ. Parquet: lưu theo cột, có nén, chỉ đọc cột cần dùng. MongoDB: truy vấn chính là "gợi ý của một người" nên mỗi người một document, đọc theo khóa chính; các truy vấn phụ của app (meta, `user_seen`, tên sách, thống kê stream) cũng theo khóa hoặc index. HDFS + YARN: yêu cầu môn học, chỉ phục vụ MapReduce, đồng thời là cách tính độc lập để kiểm chứng Spark SQL.

**K3. Đây là Lambda hay Kappa?**
**Nói ngắn:** "Theo ý tưởng Lambda: batch huấn luyện lại, streaming cập nhật số liệu trong vài giây, MongoDB phục vụ; chưa hợp nhất hai view. Không chọn Kappa vì 6 triệu rating lịch sử chưa từng qua Kafka và Kafka chỉ giữ 7 ngày, nên log không phải nguồn dữ liệu gốc."

Theo ý tưởng Lambda: lớp batch (huấn luyện lại ALS theo đợt), lớp tốc độ (streaming, vài giây), lớp phục vụ (MongoDB). Chưa đầy đủ: chưa hợp nhất batch view và speed view thành một số liệu (`rating_stats` chỉ chứa rating đến qua streaming); app chỉ gộp lúc đọc, ví dụ ẩn sách có trong `user_seen`. Không chọn Kappa **không phải** vì ALS không cập nhật từng sự kiện (Kappa cũng huấn luyện lại theo đợt bằng cách đọc lại log). Lý do thật: 6 triệu rating lịch sử chưa từng đi qua Kafka, và Kafka chỉ giữ 7 ngày, nên log không phải nguồn dữ liệu gốc. Muốn Kappa phải nạp lịch sử vào topic, giữ log lâu dài (tiered storage) rồi huấn luyện lại bằng cách đọc lại log. Cái giá của Lambda: hai đường xử lý riêng, và hiện chúng **không** cho cùng kết quả: `clean.py` giữ một rating mới nhất cho mỗi cặp (người dùng, sách), còn lớp tốc độ đếm mọi `event_id` khác nhau, nên chấm lại bị đếm hai lần trong `rating_stats`; sự kiện trễ hơn watermark bị lớp tốc độ bỏ nhưng batch giữ. Vì vậy nhóm không cộng hai view (K20).

**K4. Một thành phần chết thì sao?**
**Nói ngắn:** "Job streaming chết thì chạy lại từ checkpoint: kill -9 hai lần, 0 mất 0 trùng. Kafka tắt 40 giây: producer giữ trong bộ đệm, Spark tự nối lại; MongoDB tắt: job dừng, chạy lại từ checkpoint vẫn khớp 3.000 = 3.000. Một máy nên mọi thành phần là điểm lỗi đơn, chưa có restart policy."

| Chết | Hệ quả | Phục hồi |
|---|---|---|
| Job streaming | Sự kiện vẫn nằm trong Kafka | Chạy lại từ checkpoint; đã thử kill -9: 1.474 = 1.500 − 26, không mất, không trùng; lặp lại lần hai có kiểm tra kho Parquet: 1.475 = 1.500 − 25, kho 1.537 = 1.537 (`kafka_audit_20261108_040236.json`) |
| Kafka (đã thử tắt 40 giây) | Producer giữ tin trong bộ đệm, mỗi tin chờ được tới 120 giây (`request_timeout_ms`), failed = 0; job Spark vẫn chạy và tự nối lại, không cần khởi động lại | Topic nằm trên volume `kafka-data`. Lượt thử đầu, producer để mặc định của kafka-python 2.0.2: tin chờ trong bộ đệm hết hạn sau 30 giây, Kafka ngừng hơn 40 giây, không callback nào kiểm tra: mất 571 tin (gửi 3.078, Kafka có 2.507); phía Spark không mất gì so với Kafka. Đã sửa: `request_timeout_ms` 120 giây, `retries=10`, callback đếm tin gửi lỗi (K16) |
| MongoDB (đã thử tắt 30 giây) | App không đọc được; job Spark **dừng** (pymongo `ServerSelectionTimeoutError` sau 30 giây trong `foreachBatch`) | Script thử nghiệm chạy lại job từ cùng checkpoint sau 11 giây (`events.log` 11:11:17 → 11:11:28); chưa có restart policy, ngoài thử nghiệm phải chạy lại tay. Batch chưa ghi `last_batch` được phát lại; kết quả 3.000 = 3.000, trending 6.000 = 2 × 3.000, kho 3.059 = 3.059, 0 mất, 0 trùng (`kafka_audit_20261108_041203.json`). Một node, chưa có dự phòng; production dùng replica set |
| Job batch giữa chừng | App không bị ảnh hưởng, vẫn phục vụ bản cũ | `write_recs` ghi collection mới, đếm document đã ghi: đủ bằng số người dùng cần ghi mới chuyển `meta.active`; thiếu thì xóa collection mới, job báo lỗi, `meta.active` giữ bản cũ. Tên sách (`books`) được upsert theo `_id` trước khi chuyển nên không bị trống. Nhánh "thiếu" mới kiểm bằng code, chưa chạy thử |
| Checkpoint bị xóa hoặc chuyển chỗ, MongoDB vẫn giữ | Batch id bắt đầu lại từ 0, nhỏ hơn `stream_state.last_batch` | Job **dừng và báo lỗi** thay vì âm thầm bỏ qua mọi batch tới `last_batch` (sửa 08/11, 3 unit test `AlreadyAppliedTest`); xử lý: chạy `reset_streaming.sh` để xóa cả hai cho khớp |
| Cụm Hadoop | Không ảnh hưởng luồng chính | MapReduce chỉ để thống kê và đối chiếu |

Nói thẳng: chạy một máy nên mọi thành phần là điểm lỗi duy nhất; không có restart policy: job dừng phải chạy lại tay (trong lượt thử là script chạy lại); production dùng restart policy hoặc orchestrator kèm cảnh báo. Thiết kế cluster ở slide 13. Bằng chứng: `bash scripts/stream_outage_test.sh` (khoảng 5 phút), dòng thời gian ở `reports/streaming/outage_test_20261108_110750/events.log`.

**K5. Tải tăng 100 lần thì nghẽn ở đâu trước?**
**Nói ngắn:** "Chưa thử tải. Nghẽn đầu tiên là phần ghi MongoDB của streaming đang ở driver; tiếp theo Kafka 1 broker và Spark local. Sửa: ghi từ executor sau khi gộp theo sách, thêm broker và partition, Spark trên Kubernetes hoặc YARN."

Đây là suy luận từ thiết kế, chưa có phép thử tải (K23). (1) Phần ghi MongoDB của streaming đang chạy ở driver (cả `valid` và `trending` đều `collect()` micro-batch về driver): chuyển sang executor (foreachPartition hoặc MongoDB Spark connector), gom theo sách trước khi ghi. (2) Kafka 1 broker, 3 partition: mặc định Spark đọc mỗi partition bằng một task (tùy chọn `minPartitions` chia nhỏ hơn được), phần có trạng thái sau shuffle chạy trên 8 partition (`spark.sql.shuffle.partitions`); thêm broker, replication 3, tăng partition. (3) Spark local: chạy trên YARN/Kubernetes với nhiều executor. (4) Huấn luyện ALS đã dùng 4,6/7,7 GiB RAM: cần cluster. (5) Kho sự kiện sinh nhiều file nhỏ (trigger 5 giây, chia theo ngày, không gộp file): kéo dài trigger của query lưu trữ hoặc gộp file định kỳ, hoặc dùng Delta Lake/Iceberg. Đã có `maxOffsetsPerTrigger` (20.000) để một batch không kéo quá nhiều sau khi job ngừng lâu.

**K6. Dữ liệu lưu ở đâu, giữ bao lâu?**
Kafka: mặc định 7 ngày (168 giờ; docker-compose không đặt lại), trên volume. Kho sự kiện Parquet: giữ sự kiện cho lần huấn luyện sau cho đến khi chạy `reset_streaming.sh` (script này xóa kho và topic trước mỗi lượt demo/thử); production: kho sự kiện là dữ liệu gốc bất biến, không reset. Checkpoint: trên volume `spark-checkpoints`, mỗi query một thư mục (`offsets/`, `commits/`, `state/`); checkpoint phải đi cùng `stream_state` trong MongoDB, lệch nhau thì job dừng (K4). MongoDB: chỉ collection gợi ý có phiên bản, giữ đúng bản đang dùng và bản trước đó (`previous_collection` trong `meta`), collection sót lại của lần chạy lỗi bị xóa; mô hình `models/als_implicit` và Parquet `ratings` bị ghi đè tại chỗ; `trending` tự xóa sau 24 giờ (TTL); `rating_stats` và `user_seen` tăng dần cho đến khi reset bằng tay (K20).

**K7. Vì sao app không gọi Spark mà đọc MongoDB?**
Gợi ý được tính trước theo đợt; truy vấn chính của app là một `find_one` theo `_id` đọc cả document người dùng (30 gợi ý + 10 sách bù), trung vị 0,51–0,94 ms qua hai lần đo (`find_one({_id}, {recs: {$slice: 10}})` trong `nosql_evidence.py`, 1.000 lần đọc tuần tự, một client, dữ liệu nằm trong cache: độ trễ đọc RAM, chưa đo dưới tải). Mỗi trang còn đọc `meta`, `user_seen` (index user_id), `books` và `rating_stats` theo `$in _id`, `stream_state`, `online_eval`, `trending`. App không phụ thuộc Spark đang chạy, và đổi mô hình chỉ là chuyển `meta.active` (nguyên tử). Đánh đổi xem K13.

**K8. Batch và stream có mâu thuẫn nhau không?**
**Nói ngắn:** "Về gợi ý thì không mâu thuẫn: gợi ý đã loại sách đã chấm, app ẩn thêm sách vừa chấm, kiểm tra 0 trên 1,6 triệu cặp. Về thống kê thì hai lớp có thể lệch (chấm lại, sự kiện rất trễ), nên nhóm chưa cộng hai view."

Không về gợi ý: gợi ý đã loại sách người dùng từng chấm; app ẩn thêm sách vừa chấm qua streaming (`user_seen`); `clean.py` gộp kho sự kiện vào dữ liệu huấn luyện, bỏ trùng theo `event_id`, cùng một cặp (người dùng, sách) thì giữ rating mới hơn theo `event_time`. Kiểm tra độc lập: 0 trong 1.602.720 cặp gợi ý trùng sách đã chấm. Nói thẳng hai điểm: (1) vòng stream → huấn luyện lại đã chạy thật ngày 06/11 (`clean_20261106_122043.json` gộp 4.901 dòng), nhưng lần làm sạch cho mô hình cuối (`clean_20261106_123904.json`) có `stream_events` = null vì `reset_streaming.sh` đã xóa kho trước đó, nên mô hình cuối (P@10 0,204) chưa học sự kiện stream nào; (2) sự kiện mô phỏng có `simulated=true` nhưng `clean.py` chưa lọc, nên một lượt 5 sao demo từ `rate_from_recs.py` sẽ vào lần huấn luyện sau; production phải lọc. Về thống kê: `rating_stats` (stream) và thống kê batch không cộng được với nhau, xem K20.

**K9. Vì sao Spark không đọc HDFS, không chạy trên YARN?**
HDFS + YARN trong đồ án chỉ phục vụ MapReduce. Spark chạy `local[*]`: một JVM trên một máy, nên đọc HDFS có 1 DataNode, replication 1 không thêm chịu lỗi hay song song, chỉ thêm độ trễ RPC. Hai cụm tách thành hai file compose để giữ nguyên image Hadoop của bài lab và bật/tắt cụm Hadoop độc lập, vì cả hai cùng chạy trên một máy ảo Docker và chia chung 7,7 GiB RAM (tắt Hadoop sau phần MapReduce là để trả RAM); chúng chỉ nối với nhau qua thư mục chung `bookpulse/`. Trên cluster thật, Spark đọc HDFS/S3 và chạy trên YARN/Kubernetes (K18).

**K10. CAP và nhân bản thì sao?**
Hiện chưa có nhân bản: MongoDB standalone, Kafka 1 broker RF 1; một node chết là thành phần đó ngừng, nên chưa có lựa chọn CAP thật sự. Production: MongoDB replica set 3 node, ghi `w: majority` để không mất dữ liệu khi node chính đổi; đọc gợi ý bằng `secondaryPreferred` vì gợi ý cũ vài giây vẫn chấp nhận được (ưu tiên sẵn sàng), nhưng phải xử lý trường hợp secondary chưa có collection mới (K26). Kafka 3 broker, replication factor 3, `min.insync.replicas` 2, producer `acks=all`, để một broker chết không mất tin đã xác nhận. Producer hiện đã đặt `acks="all"`, nhưng với RF 1 thì tương đương `acks=1`; chỉ có ý nghĩa khi RF 3 và `min.insync.replicas` 2.

**K11. Schema thay đổi hoặc sự kiện lỗi thì sao?**
Sự kiện là JSON, không có schema registry; `from_json` dùng schema 6 trường. Thêm trường thì bị bỏ qua; thiếu, đổi tên hay sai kiểu thì thành null và đi vào `rejected`. Sự kiện lỗi chỉ được đếm, không lưu, nên hết thời gian giữ của Kafka là mất hẳn. Production: thêm topic dead-letter cho sự kiện lỗi, dùng Avro/Protobuf với Schema Registry để kiểm soát thay đổi schema.

**K12. Vì sao 4 query riêng cùng đọc Kafka, không gộp một?**
Vì ngữ nghĩa khác nhau: `valid` và `trending` cần watermark và trạng thái (bỏ trùng, cửa sổ), `trending` ghi theo update mode; kho lưu trữ (`events_to_parquet`) không dùng watermark để giữ cả sự kiện trễ, ghi bằng file sink exactly-once; `rejected` chỉ đếm. Mỗi query một checkpoint riêng nên chạy lại độc lập. Cái giá: Kafka bị đọc 4 lần, và trạng thái bỏ trùng bị lặp lại ở các query có trạng thái. Với tải của đồ án thì chấp nhận được.

**K13. Vì sao tính gợi ý trước thay vì tính online? Người dùng mới, sách mới?**
Tính trước: app đọc một document khoảng 1 ms và không phụ thuộc Spark. Cái giá: gợi ý cũ cho đến lần batch sau (chỉ có ẩn sách vừa chấm là tức thì); người dùng mới nhận danh sách sách phổ biến từ `meta`; sách mới không bao giờ được gợi ý trước lần huấn luyện lại. Phương án online: lưu vector người dùng/sách của ALS, tính điểm khi có truy vấn, và fold-in vector cho người dùng mới từ vài rating đầu; nhóm chưa làm.

**K14. Bảo mật thế nào?**
Hiện cổng MongoDB (27017) và Kafka (9094) mở ra máy host, MongoDB không xác thực, Kafka PLAINTEXT. Chỉ chấp nhận được trên laptop. Production: mạng nội bộ, không mở cổng ra ngoài; SCRAM + TLS cho Kafka và MongoDB; người dùng MongoDB theo vai trò (app chỉ đọc); mật khẩu trong secret manager.

**K15. Theo dõi lag của streaming thế nào?**
Spark không commit offset vào consumer group của Kafka, nên công cụ lag exporter của Kafka không thấy lag của job. Dùng metric nguồn của Spark `avgOffsetsBehindLatest` (đã có trong `reports/stream_progress_*.json`) hoặc `StreamingQueryListener`, đưa lên Prometheus/Grafana và cảnh báo khi lag tăng. Spark UI chỉ mở cổng 4040; không chạy job batch khi job streaming đang chạy (UI thứ hai sang 4041, và hai driver 4 GB vượt 7,7 GiB RAM của máy ảo Docker; xem K19).

**K16. Kafka tắt thì có mất sự kiện không?**
**Nói ngắn:** "Phía Spark không mất: Kafka tắt 40 giây, job tự nối lại. Phía producer, lượt đầu mất 571 tin vì kafka-python hủy tin chờ quá 30 giây và không ai kiểm tra; nay chờ tới 2 phút, gửi lại và đếm tin lỗi, lượt sau Kafka có đủ 3.078 tin."

Phía Spark thì không: đã thử tắt Kafka 40 giây, job vẫn chạy và tự nối lại, đọc tiếp từ offset trong checkpoint. Phía producer thì lượt thử đầu có mất 571 tin (gửi 3.078, Kafka có 2.507; `reports/superseded/outage_test_20261108_110251_producer_no_retry/`). Nguyên nhân: kafka-python 2.0.2 cho tin nằm chờ trong bộ đệm hết hạn sau `request_timeout_ms` (mặc định 30 giây), Kafka ngừng hơn 40 giây, và không có callback nào kiểm tra kết quả gửi. Sửa quan trọng nhất là `request_timeout_ms` 120 giây (tin chờ được tới 2 phút); thêm `retries=10` (chờ 1 giây giữa các lần) cho request lỗi giữa chừng, và callback đếm tin gửi lỗi ("failed"). Lượt sau: failed = 0, Kafka có đủ 3.078 tin, MongoDB 3.000 = 3.000. Còn lại: Kafka ngừng lâu hơn khoảng 2 phút thì vẫn mất tin, nhưng nay được đếm chứ không mất âm thầm. Gửi lại có thể ghi một tin hai lần (bỏ trùng theo `event_id` xử lý được); kafka-python 2.0.2 không có producer idempotent nên sau khi gửi lại, thứ tự trong partition không bảo đảm; job dùng `event_time` nên chấp nhận được.

**K17. Phiên bản mô hình và quay lại bản cũ?**
**Nói ngắn:** "Gợi ý mới ghi vào collection mới, đủ số người dùng mới chuyển meta.active, giữ bản trước để quay lại bằng tay. Đã thử trên database riêng; chưa so chất lượng trước khi chuyển, mô hình và Parquet vẫn bị ghi đè."

`model_version` là thời điểm `write_recs` chạy, không phải mã lần huấn luyện. Chỉ collection gợi ý có phiên bản: giữ đúng bản đang dùng và bản đang dùng trước đó, `meta` ghi `previous_collection`; quay lại = đặt `recs_collection` về `previous_collection` bằng tay. Collection sót lại của lần chạy lỗi bị xóa. Trước khi chuyển có kiểm tra **đủ**: số document ghi được phải bằng số người dùng cần ghi, thiếu thì xóa collection mới và job báo lỗi; `books` được upsert theo `_id` trước khi chuyển. Đã thử trên database riêng `bookpulse_test` (53.424/53.424 người dùng, chuyển sang bản mới, giữ bản trước, xóa bản sót; `reports/superseded/write_recs_gate_test_20261108/`); nhánh thiếu chưa chạy thử. Kiểm tra này chỉ về độ đủ, **không** về chất lượng: chưa so P@10 trước khi chuyển. `models/als_implicit` và Parquet `ratings` vẫn bị ghi đè. Production: mỗi lần chạy một thư mục mô hình theo run_id, ghi đường dẫn mô hình và report huấn luyện vào `meta`, chỉ chuyển khi P@10 trên validation ≥ bản đang chạy, lập lịch bằng Airflow/cron. Lưu ý: database đang demo do code trước khi sửa ghi (phiên bản 20261106_125827), nên `meta` chưa có trường `previous_collection`; bản trước `recommendations_20261105_173716` vẫn còn. Trường này chỉ có từ lần chạy `write_recs.py` sau 08/11 (đã thử trên `bookpulse_test`).

**K18. Lên cluster thật cần sửa gì?**
**Nói ngắn:** "Logic xử lý giữ nguyên; phải sửa lớp cấu hình (đường dẫn URI thay pathlib, SPARK_MASTER, checkpoint lên HDFS/S3, số shuffle partition, gói Kafka) và viết lại phần ghi MongoDB của streaming sang executor."

Logic xử lý (DataFrame, SQL, ALS) giữ nguyên; phải sửa lớp cấu hình và viết lại vài đoạn I/O (`has_parquet()` trong `clean.py`, phần ghi MongoDB của `valid`/`trending`): (1) `common/settings.py` dựng mọi đường dẫn bằng `pathlib`, mà `pathlib` biến `hdfs://nn/x` thành `hdfs:/nn/x` (s3a cũng vậy), nên phải dùng chuỗi URI; `has_parquet()` trong `clean.py` phải dùng FileSystem API của Hadoop. (2) Master lấy từ biến `SPARK_MASTER` (mặc định `local[*]`): đặt thành `yarn` hoặc `k8s://…`; chỉ dùng cờ `--master` của spark-submit thì bị `.master()` trong code ghi đè. (3) Thư mục checkpoint của ALS (`train_als.py`) và checkpoint streaming phải nằm trên HDFS/S3 vì executor ở máy khác. (4) Gửi `common/` kèm `--py-files`; executor cần `MONGO_URI` (`write_recs` ghi từ executor bằng `foreachPartition`). (5) Chuyển phần ghi MongoDB của `valid` và `trending` từ driver sang executor. (6) `settings.py` đang đặt mặc định cho laptop: `spark.sql.shuffle.partitions` = 8, driver 4 GB; đổi qua biến `SPARK_SHUFFLE_PARTITIONS`, `SPARK_DRIVER_MEMORY` hoặc bật AQE. Gói `spark-sql-kafka` đang tải từ Maven lúc chạy: phải đóng sẵn vào image hoặc dùng mirror nội bộ.

**K19. Lớp batch và lớp tốc độ có chạy cùng lúc không?**
Trên máy này thì không: hai driver 4 GB không vừa 7,7 GiB RAM của máy ảo Docker, nên hai lớp chạy lần lượt. Trong lúc `run_pipeline.sh` chạy (khoảng 25–35 phút), job streaming dừng; sự kiện chờ trong Kafka (giữ 7 ngày), app vẫn phục vụ gợi ý cũ và speed view cũ. Chạy lại job thì nó đọc tiếp từ checkpoint, tối đa 20.000 offset mỗi batch cho đến khi bắt kịp. Trên cluster: batch chạy trên cụm riêng (tạm thời), streaming chạy thường trực, nên hai lớp chạy song song như Lambda yêu cầu.

**K20. Batch view và speed view có cho cùng con số không? Gộp thế nào?**
Không. `clean.py` giữ một rating mới nhất cho mỗi cặp (người dùng, sách) theo `event_time`; lớp tốc độ đếm mọi `event_id` khác nhau, nên một lượt chấm lại bị đếm hai lần trong `rating_stats`. Sự kiện trễ hơn watermark 10 phút bị lớp tốc độ bỏ, batch vẫn giữ (qua kho Parquet). Vì vậy nhóm không cộng hai view. Muốn gộp cần: (1) lớp tốc độ theo khóa (người dùng, sách) để trừ rating cũ khi chấm lại; (2) một mốc cắt: ghi vào `meta` `event_time`/offset lớn nhất mà batch đã học, speed view lưu theo từng khoảng thời gian, chỉ cộng các khoảng sau mốc, xóa các khoảng batch đã học. Chưa làm: hiện `rating_stats` và `user_seen` tăng dần cho đến khi reset bằng tay.

**K21. Rating đến rất trễ có bị ẩn khỏi gợi ý không?**
Chưa: `user_seen` được ghi trong query `valid`, tức sau watermark. Một rating thật đến trễ hơn 10 phút (ví dụ máy khách offline) bị bỏ ở đó, nên app chưa ẩn sách cho đến lần batch sau, dù kho Parquet vẫn giữ sự kiện. `user_seen` là upsert idempotent, nên có thể ghi từ một nhánh không trạng thái, không watermark, giống query lưu trữ. Nhóm chưa sửa.

**K22. Lượt kill -9 có chứng minh khe hở cộng lặp đã đóng không?**
Không. Khe hở là vài mili giây giữa `bulk_write` và `mark_applied`; kill ở giây thứ 45 gần như chắc chắn không rơi đúng vào đó. "0 trùng" chứng minh đường phục hồi bình thường, không chứng minh khe hở đã đóng. Muốn thử phải cài lỗi có chủ đích (thoát ngay sau `bulk_write`). Cách sửa (chưa làm): chặn theo từng document, lọc `{_id: sách, last_batch: {$lt: batch_id}}` rồi `$inc` cùng `$set last_batch`; MongoDB nguyên tử trên một document nên không cần transaction.

**K23. Đã thử tải chưa? Chịu được bao nhiêu sự kiện mỗi giây?**
Chưa thử tải, chưa biết thông lượng tối đa. Ở 20 sự kiện/giây, batch mất 3–6 giây so với trigger 5 giây, phần lớn là chi phí cố định (4 query chia 4 core). `maxOffsetsPerTrigger` 20.000 chưa được hiệu chỉnh. Cách thử: tăng dần tốc độ producer cho đến khi `avgOffsetsBehindLatest` tăng mãi, đó là trần của cấu hình hiện tại.

**K24. Vì sao watermark 10 phút, trigger 5 giây, cửa sổ 10 phút trượt 5 phút?**
Chọn theo kịch bản, không đo (dữ liệu mô phỏng nên không có độ trễ thật). Production: đo (processing time − event time) rồi đặt watermark gần p99. Kho lưu trữ giữ sự kiện trễ cho batch, nên watermark ngắn hơn chỉ làm số liệu tức thời kém đủ hơn, không mất dữ liệu huấn luyện.

**K25. Kho sự kiện do job streaming ghi và bị reset xóa: có ổn không?**
Không ổn cho production: dữ liệu gốc để huấn luyện lại phụ thuộc vào job tính speed view, và `reset_streaming.sh` xóa luôn kho. Production: tách việc lưu ra một thành phần riêng (ví dụ Kafka Connect S3 sink) ghi topic vào bộ lưu trữ bất biến; job streaming chỉ tính speed view.

**K26. Đọc từ secondary (K10) có vấn đề gì với việc chuyển `meta.active`?**
Có: secondary có thể đã thấy `meta` mới nhưng chưa có collection gợi ý mới, `find_one` trả về None và người dùng bị coi là người mới. Sửa: ghi gợi ý và `meta` bằng `w: majority`, đọc `meta` và gợi ý bằng read concern majority trong một causally consistent session; hoặc không thấy thì đọc `previous_collection`.

**K27. App đọc thẳng MongoDB có ổn không?**
Ổn cho demo. Production đặt một API chỉ đọc ở giữa: xác thực, cache, giới hạn số request, gộp batch view và speed view ở một chỗ, và giấu `meta.active` khỏi client.

**K28. Lên bộ UCSD đầy đủ (876 nghìn người dùng, 2,36 triệu sách) thì nghẽn ở đâu?**
Phía batch: nghẽn là `recommendForAllUsers` (người dùng × sách ≈ 2 × 10^12 phép nhân vô hướng ở rank 50), không phải bước fit; bản thân các vector nhỏ (khoảng (0,88 + 2,36) triệu × 50 × 4 byte ≈ 650 MB). Giảm bằng tập ứng viên, hoặc lưu vector rồi tìm láng giềng gần đúng (ANN) lúc truy vấn. MongoDB: ước tính 1,5–2 KB mỗi document người dùng (cần xác nhận bằng `collStats`), tức khoảng 1,5–2 GB mỗi phiên bản, vừa một replica set; chỉ cần sharding theo hashed user_id khi lên hàng chục triệu người dùng. Streaming: ở 2.000 sự kiện/giây, trạng thái bỏ trùng 10 phút khoảng 1,2 triệu khóa; state store mặc định (HDFSBackedStateStore) giữ trong heap, nên dùng RocksDB state store (có trong Spark 3.5) và checkpoint trên S3.

**K29. Chi phí chạy trên cloud?**
Chưa tính chi tiết. Chi phí cố định: Kafka (3 broker), MongoDB (3 node) và job streaming chạy thường trực. Chi phí thay đổi: cụm Spark tạm thời cho huấn luyện theo lịch (dùng spot instance được, vì job chạy lại được). Vài GB Parquet trên S3 là không đáng kể.

---

## Hảo – dữ liệu, kiến trúc, MapReduce, Spark SQL, cluster

**1. 6 triệu dòng thì có phải Big Data không?**
**Nói ngắn:** "Không, đây là bộ phát triển; nhóm đã đo giới hạn một máy: RAM đỉnh 4,6/7,7 GiB, 4 core chỉ nhanh 1,95 lần, bộ UCSD gấp 38 lần sẽ không vừa. Lên cluster thì logic xử lý giữ nguyên; phải sửa lớp cấu hình (đường dẫn URI thay pathlib, SPARK_MASTER, checkpoint lên HDFS/S3, số shuffle partition, gói Kafka) và viết lại phần ghi MongoDB của streaming sang executor."

Không, và nhóm nói rõ đây là bộ phát triển. Nhóm đã đo giới hạn của một máy: RAM đỉnh 4,6/7,7 GiB (`reports/scaling/docker_stats_20261104_batch.csv`), 4 core chỉ nhanh 1,95 lần. Bộ UCSD đầy đủ có 229 triệu tương tác (gấp 38 lần) sẽ không vừa. Khi lên cluster, logic xử lý (DataFrame, SQL, ALS) giữ nguyên nhưng phải sửa lớp cấu hình và viết lại vài đoạn I/O: đường dẫn đang dựng bằng `pathlib` (biến `hdfs://` thành `hdfs:/`) phải thành chuỗi URI, `clean.py` dùng FileSystem API của Hadoop, master đặt qua biến `SPARK_MASTER` (cờ `--master` của spark-submit bị `.master()` trong code ghi đè), checkpoint lên HDFS/S3; chi tiết ở K18, thiết kế triển khai ở slide 13.

**2. Vì sao vẫn cần MapReduce khi đã có Spark?**
Môn học yêu cầu, và nó cho thấy mô hình map → shuffle/sort → reduce mà Spark kế thừa. Nhóm dùng nó như một cách tính độc lập để kiểm chứng Spark SQL: hai hệ thống, hai cách viết, cùng kết quả. Trên thực tế Spark nhanh hơn vì giữ dữ liệu trong bộ nhớ giữa các bước.

**3. Combiner có luôn dùng được không?**
Chỉ với phép toán có tính kết hợp và giao hoán (tổng, đếm). Trung bình thì không: trung bình của trung bình là sai. Nhóm mang theo (tổng, số lượt) và chia ở reducer; unit test kiểm tra kết quả không đổi khi combiner chạy 0, 1 hay nhiều lần.

**4. Vì sao có 4 map task khi chỉ có 3 block?**
Số map task thay đổi theo lần chạy: 05/11 có 4 (134 giây), lần tập 07/11 có 3 (job 52 giây), kết quả giống hệt nhau. Task thứ tư là bản speculative execution: ApplicationMaster của job MapReduce (không phải YARN) chạy thêm bản dự phòng cho task chậm, bản nào xong trước thì giữ, bản kia bị hủy; đây là cơ chế chịu lỗi của MapReduce. Log `job_20261105_165718.log`: Launched map tasks = 4, Killed map tasks = 2, Killed reduce tasks = 1; mỗi split trong 3 split hoàn tất đúng một lần.

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
Chưa chạy thật, đây là thiết kế: Kafka 3 broker, object storage (S3/MinIO) cho dữ liệu và checkpoint, Spark trên Kubernetes/YARN mở rộng theo executor, MongoDB replica set, Prometheus/Grafana (lag lấy từ metric `avgOffsetsBehindLatest` của Spark, K15), secret manager và TLS. Hiện mọi thành phần chạy một bản trên một máy (Kafka 1 broker, Mongo standalone, Spark `local[*]`, 1 DataNode), nên mỗi thành phần là một điểm lỗi đơn.

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
80%: app dùng chính mô hình đã đánh giá trên tập test, fit lại trên toàn bộ tập train (80% dữ liệu, 4.780.968 rating) với tham số đã chọn; chỉ bước tìm tham số mới fit trên 80% của tập train và chấm trên phần validation còn lại. Khoảng 1,2 triệu rating của tập test không được học. Khi triển khai thật nên fit lại trên 100% dữ liệu với tham số đã chọn (rank 50, alpha 1, regParam 0,1) rồi mới ghi gợi ý; nhóm chưa làm bước này.

**18. Dữ liệu trong MongoDB tổ chức thế nào? Vì sao không Cassandra?**
Mỗi người dùng một document (`_id` = user_id, mảng `recs` 30 phần tử, `fallback` 10 sách). App đọc bằng `find_one({"_id": user_id})`, dùng index có sẵn (IDHACK); độ trễ đo bằng `find_one({_id}, {recs: {$slice: 10}})` trong `nosql_evidence.py`, hai lần đo cùng phiên bản mô hình: trung vị 0,94 ms, p95 2,81 ms (`nosql_evidence_20261106_130058.json`) và 0,51 ms, 1,14 ms (`nosql_evidence_20261107_221150.json`); cả hai là 1.000 lần đọc tuần tự, một client, cùng máy, dữ liệu nằm trong cache WiredTiger, tức độ trễ đọc RAM, chưa đo dưới tải. Đó là truy vấn chính, không phải duy nhất: mỗi trang còn đọc `meta`, `user_seen`, `books`, `rating_stats`, `stream_state`, `online_eval`, `trending`, đều theo khóa hoặc index (K7). Cassandra hợp khi ghi rất nhiều; tải ghi ở đây nhỏ. MongoDB hợp hơn ở: index phụ trên trường không phải khóa (user_id cho `user_seen`), truy vấn linh hoạt, cập nhật bằng aggregation pipeline, và collection có phiên bản kèm `meta` để đổi bản gợi ý. (TTL không phải lý do: Cassandra cũng có TTL.)

**19. Đổi mô hình mới thì app có bị gián đoạn không?**
Không: gợi ý mới ghi vào collection mới; ghi đủ số người dùng mới chuyển `meta.active` (cập nhật một document là nguyên tử), thiếu thì bỏ bản mới và giữ bản cũ; giữ bản đang dùng và bản trước đó để quay lại. Tên sách (`books`) được upsert theo `_id` trước khi chuyển, nên không có lúc trống. Giới hạn: chỉ gợi ý có phiên bản, mô hình và Parquet bị ghi đè, kiểm tra trước khi chuyển chỉ về độ đủ, chưa so chất lượng (P@10), quay lại phải sửa `meta` bằng tay (K17).

**20. Vì sao 4 core chỉ nhanh 1,95 lần?**
Định luật Amdahl: phần tuần tự ở driver và shuffle giữa các vòng lặp ALS không tăng tốc được; 4 core còn chia cho Kafka và MongoDB.

## Tuấn – streaming, kiểm thử

**21. Event time khác processing time thế nào? Watermark để làm gì?**
Event time là lúc rating xảy ra, processing time là lúc Spark xử lý. Thống kê dựa trên event time. Watermark (10 phút) là ngưỡng chờ sự kiện trễ: cũ hơn thì bị loại khỏi phép tính có trạng thái, và trạng thái cũ được giải phóng nên bộ nhớ không tăng mãi. Checkpoint mỗi query có ba phần: `offsets/N` là khoảng offset của batch N, ghi **trước** khi chạy batch (write-ahead log); `commits/N` chỉ là dấu "batch N đã xong" (có `nextBatchWatermarkMs`, không chứa offset); `state/` là state store (bỏ trùng, cửa sổ). Batch có trong `offsets/` mà chưa có trong `commits/` được chạy lại với đúng batch id và offset cũ.

**22. Vì sao output mode `update` cho cửa sổ?**
`append` chỉ ghi khi cửa sổ đã đóng (sau watermark), tức chậm khoảng 20 phút; `update` ghi ngay giá trị mới của cửa sổ đang mở, hợp với "sách đang hot".

**23. Có exactly-once không?**
**Nói ngắn:** "Không hoàn toàn: Kafka + checkpoint là at-least-once; kho Parquet exactly-once, trending ghi đè nên idempotent, rating_stats bỏ batch đã ghi bằng last_batch. Còn khe hở nếu chết đúng lúc ghi MongoDB, và 0 mất là kết quả đo."

Không hoàn toàn. Kafka + checkpoint là at-least-once; kho Parquet exactly-once nhờ file sink; `trending` ghi đè document (ReplaceOne) nên phát lại vẫn đúng. `rating_stats` bỏ qua batch đã ghi bằng `last_batch`, nhưng còn một khe hở: nếu job chết đúng lúc đang ghi MongoDB (`rating_stats` và `online_eval` cộng dồn trước khi ghi `last_batch`), batch phát lại có thể cộng lặp một phần; lượt kill -9 gần như chắc chắn không rơi vào khe hở này (K22). Sửa triệt để bằng transaction (replica set) hoặc chặn theo từng document (`last_batch` trong mỗi document sách, K22). Nếu checkpoint bị xóa mà MongoDB còn giữ, job nay dừng và báo lỗi thay vì bỏ qua batch âm thầm (K4). "0 mất" là kết quả đo, không phải bảo đảm: job đặt `failOnDataLoss=false`, nên nếu job dừng lâu hơn thời gian Kafka giữ log (mặc định 168 giờ) hoặc topic bị tạo lại, Spark chỉ ghi cảnh báo rồi bỏ qua offset đã mất. Production: `failOnDataLoss=true` và cảnh báo theo lag.

**24. Đã thử sự cố thế nào?**
**Nói ngắn:** "stream_fault_test.sh kill -9 job giữa chừng, chạy lại từ checkpoint, kafka_audit đếm lại Kafka: 1.474 = 1.500 trừ 26 trễ, 0 mất 0 trùng. Lặp lại lần hai và thử tắt Kafka, MongoDB cũng khớp."

`bash scripts/stream_fault_test.sh` (khoảng 3 phút, lặp lại được): 1.500 sự kiện, `kill -9` cả Python lẫn JVM sau 45 giây, chạy lại từ checkpoint. `kafka_audit.py` đối chiếu Kafka với MongoDB: 1.474 = 1.500 − 26 sự kiện trễ (số 26 suy từ kafka_audit, xem #25), tổng cửa sổ 2.948 = 2 × 1.474, 15 sự kiện lỗi; lượt này 0 mất, 0 trùng (`reports/kafka_audit_20261107_220738.json`). Lặp lại lần hai có thêm kiểm tra kho Parquet: 1.475 = 1.500 − 25, kho 1.537 dòng = 1.537 tin hợp lệ, 1.500 `event_id` khác nhau, 0 file chưa commit (`kafka_audit_20261108_040236.json`). Thêm `bash scripts/stream_outage_test.sh` (khoảng 5 phút): tắt Kafka 40 giây rồi MongoDB 30 giây khi đang gửi 3.000 sự kiện; 3.000 = 3.000, 0 mất, 0 trùng (`kafka_audit_20261108_041203.json`, xem K4, K16). Giới hạn: các lượt này kiểm đường phục hồi bình thường, chưa cài lỗi đúng giữa lúc ghi MongoDB (K22).

**25. Sự kiện trễ thì sao?**
Lớp tốc độ bỏ: 26 sự kiện trễ ở lượt kill -9 (suy từ kafka_audit: 1.474 = 1.500 − 26) và 19 ở lượt demo (983 = 1.002 − 19). Số 26 không do Spark đếm: file tiến độ chỉ ghi 0 trước khi kill + 23 sau khi chạy lại, vì batch commit ngay trước `kill -9` nằm sau lần ghi file tiến độ cuối (mỗi 10 giây), nên không có trong file nào; lượt lặp lại cũng vậy (file ghi 0 + 23, kafka_audit 25 trễ). Tức bộ đếm tiến độ có thể **thiếu** batch commit ngay trước khi bị kill. Điều kiện: sự kiện trễ đến sau khi job đã có watermark; Spark 3.5 lọc bằng watermark của batch trước, nên 1–2 batch đầu sau khi khởi động **mới** (checkpoint trống) chưa lọc; chạy lại từ checkpoint thì watermark được khôi phục (job thứ hai của lượt kill -9 vẫn loại 23). Vì vậy producer chỉ gửi sự kiện trễ sau 30 giây: `--late 0.05` là 5% số sự kiện gửi sau giây thứ 30 (lượt demo: 19/1.000). Kho lưu trữ giữ lại, nên lần huấn luyện sau vẫn học được rating đó.

**26. Vì sao key Kafka là user_id?**
Mọi sự kiện của một người vào cùng một partition (Kafka giữ thứ tự trong một partition), và tải chia đều giữa các partition. Job **không** dựa vào thứ tự đến: thống kê dùng `event_time`, `clean.py` giữ rating mới nhất theo `event_time`. Mặc định nguồn Kafka của Spark tạo một task đọc cho mỗi partition (tùy chọn `minPartitions` chia nhỏ hơn được); phần có trạng thái sau shuffle chạy trên 8 partition (`spark.sql.shuffle.partitions`). Đánh đổi: người dùng hoạt động nhiều có thể làm lệch tải giữa các partition.

**27. Theo dõi hệ thống thế nào?**
Mỗi query có tên trên Spark UI (tab Structured Streaming); file `reports/stream_progress_*.json` ghi tốc độ vào/xử lý, thời gian batch, sự kiện trễ, bản trùng, kích thước trạng thái, watermark; `kafka_audit.py` lưu phép đối chiếu Kafka – MongoDB – kho Parquet. Lag: Spark không commit offset vào consumer group của Kafka nên lag exporter của Kafka không thấy; dùng `avgOffsetsBehindLatest` (đã có trong `stream_progress_*.json`) hoặc `StreamingQueryListener` (K15). Batch đầu sau khi khởi động mất 7–12 giây (JVM còn lạnh), các batch sau thường 3–6 giây, sát trigger 5 giây (lượt kill -9 và lượt demo); trong lượt tắt Kafka/MongoDB có batch kéo dài gần 1 phút (57,7 giây, `stream_progress_20261108_040808.json`; lượt đầu với producer chưa sửa: 61 giây).

**28. Tỷ lệ trúng online trên app bằng 0? Vì sao hiện "2 / 672"?**
`producer.py` phát lại rating đã có trong dữ liệu huấn luyện, mà gợi ý đã loại mọi sách người dùng đã chấm, nên 670 lượt 4–5 sao từ producer không thể trúng. 2 lượt trúng là 2 cuốn user 1 chấm từ chính top-10 của mình bằng `scripts/rate_from_recs.py`; đánh giá chất lượng mô hình vẫn dựa trên tập test offline. Định nghĩa "trúng": sách nằm trong 10 gợi ý đầu **đã lưu** của phiên bản đang dùng (không bỏ qua sách đã ẩn, không tính sách bù); bộ đếm cộng dồn qua các phiên bản, `model_version` là bản mới nhất.

**29. Ghi MongoDB của streaming có phân tán không?**
Không: hai query `valid` và `trending` gom mỗi micro-batch về driver (`collect()`) rồi ghi bằng một client MongoDB (`rejected` chỉ đếm), đủ cho tốc độ sự kiện của đồ án; `maxOffsetsPerTrigger` 20.000 giới hạn một batch sau khi job tắt lâu. Khi tải lớn, chuyển việc ghi sang executor (`foreachPartition` hoặc MongoDB Spark connector) và gộp theo sách trong Spark trước khi ghi.

**30. Tắt Docker thì Kafka có mất dữ liệu không?**
`docker compose down` không mất: Kafka lưu log trên volume `kafka-data` (trước đây topic nằm trong container và bị xóa, nhóm đã sửa). `down -v` xóa mọi volume, khi đó phải chạy `bash scripts/reset_streaming.sh --yes` để xóa luôn checkpoint, `stream_state` và kho `stream_events` cho khớp nhau.

**31. Kiểm thử gồm những gì?**
11 unit test, đều qua (MapReduce, hàm gợi ý, file tiến độ, và 3 test `AlreadyAppliedTest`: batch mới được ghi, batch phát lại bị bỏ qua, checkpoint lệch MongoDB thì dừng) và các phép đối chiếu độc lập: MapReduce so với Spark SQL, SQL so với DataFrame, Kafka so với MongoDB (`kafka_audit.py`, khối `mongo_check`), gợi ý không chứa sách đã chấm (`nosql_evidence.py`), thử `kill -9` bằng `stream_fault_test.sh` (nay kiểm tra cả kho Parquet), và thử tắt Kafka, MongoDB bằng `stream_outage_test.sh`.

## Câu chung

**32. Dữ liệu thay đổi thì hệ thống xử lý thế nào?** (câu giảng viên đã hỏi ở Session 8)
Hai tốc độ: thay đổi nhanh do streaming xử lý trong vài giây: ẩn cuốn vừa chấm, lấy tiếp từ 30 gợi ý, bù bằng sách chưa đọc, cập nhật điểm trung bình (chỉ từ stream). Hiệu ứng ẩn sách chỉ thấy khi người dùng chấm sách đang nằm trong top-10 (demo bằng `rate_from_recs.py`); rating phát lại bằng producer đã có trong dữ liệu huấn luyện nên không bao giờ nằm trong danh sách. Gu đọc, người dùng mới, sách mới chờ lần huấn luyện sau; rating từ streaming được lưu ra Parquet nên lần huấn luyện sau học được, miễn là chưa chạy `reset_streaming.sh` (mô hình cuối chưa học sự kiện stream nào, xem K8). Chi tiết: `Present/BookPulse - Cau hoi giang vien Session 8.md`.

**33. Điều khó nhất / bài học lớn nhất?**
Kiểm chứng: nhiều vấn đề chỉ lộ ra khi chạy thật và đọc ngược kết quả. Ví dụ: mô hình RMSE tốt nhưng gợi ý kém; sự kiện trễ mất khỏi dữ liệu huấn luyện; watermark chưa lọc ở batch đầu; topic Kafka mất khi `docker compose down`; sự kiện phát lại không thể làm đổi gợi ý; image MongoDB và pyarrow lỗi do không ghim phiên bản; producer để timeout mặc định 30 giây và không kiểm tra kết quả gửi nên mất 571 tin khi Kafka tắt hơn 40 giây.

**34. Ai làm gì?** *(cần thống nhất)*
Hảo: dữ liệu, batch, Spark SQL, MapReduce, thiết kế triển khai. Tuấn: streaming và đánh giá. Kiệt: Spark ML, MongoDB, app. Nhóm xác nhận lại trước khi nộp.

**35. Chạy lại được không?**
Có: `docker compose up -d`, rồi `bash scripts/run_pipeline.sh` (khoảng 30 phút; dừng job streaming trước, sự kiện chờ trong Kafka, K19); MapReduce theo README mục 8 (trong Git Bash cần `export MSYS_NO_PATHCONV=1`); thử sự cố streaming bằng `bash scripts/stream_fault_test.sh` (khoảng 3 phút) và `bash scripts/stream_outage_test.sh` (khoảng 5 phút); mọi số đo tự ghi vào `reports/`.
