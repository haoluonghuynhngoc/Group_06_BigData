# BookPulse – Kịch bản demo Session 10

Demo trực tiếp cho Final Assessment: chạy lần lượt MapReduce → Spark SQL → Spark ML + MongoDB → Streaming, mỗi phần có một kết quả nhìn thấy được và một phép kiểm chứng. Thời lượng dự kiến 8–10 phút (đo lần tập 07/11: chuẩn bị 80 giây, Hadoop sẵn sàng sau khoảng 37 giây; job MapReduce 72 giây tính cả khởi động (job 52 giây) + kiểm chứng 30 giây; Spark SQL tức thì; mongosh khoảng 3 giây; streaming khoảng 3,5 phút kể cả `rate_from_recs.py`). Mọi lệnh chạy trong Git Bash, từ thư mục `bookpulse/` trừ khi ghi khác.

**Mỗi terminal Git Bash mở ra đều chạy trước:** `export MSYS_NO_PATHCONV=1`. Thiếu dòng này, Git Bash đổi đường dẫn trong container `/bookpulse/...` thành `C:/Program Files/Git/bookpulse/...` và lệnh MapReduce báo "No such file".

## 1. Chuẩn bị trước buổi (ít nhất 30 phút trước)

```bash
cd "D:\Big Data\Group_06_BigData\bookpulse"
export MSYS_NO_PATHCONV=1
docker compose up -d                                  # spark, kafka, mongo, app
docker compose ps                                     # 4 service "running"
cd mapreduce/hadoop && docker compose up -d && cd ../..    # chỉ `stop`/`start` cụm Hadoop, KHÔNG `down`: down xoá container, lần up sau phải build lại image (nhiều dòng "pull access denied" rồi mới Building), tạo volume mới và HDFS trống cho tới khi run_hadoop.sh upload lại
docker compose -f mapreduce/hadoop/docker-compose.yml logs client | grep "BOOKPULSE HADOOP READY"   # phải in ra dòng này; chưa có thì chờ rồi chạy lại
docker compose -f mapreduce/hadoop/docker-compose.yml exec -T client hdfs dfsadmin -safemode wait   # chờ đến "Safe mode is OFF"
bash scripts/reset_streaming.sh --yes                 # streaming bắt đầu từ số 0
docker compose restart app                            # app nạp code mới nhất
```

Tùy chọn, làm trước buổi (khoảng 3 phút): `bash scripts/stream_fault_test.sh` để có bằng chứng `kill -9` mới (mục 5), rồi chạy lại `bash scripts/reset_streaming.sh --yes`.

Tùy chọn, **không** làm trong demo trực tiếp (khoảng 5 phút): `bash scripts/stream_outage_test.sh` tắt Kafka 40 giây rồi MongoDB 30 giây trong lúc producer gửi 3.000 sự kiện và job streaming đang chạy, rồi chạy `kafka_audit.py`. Script tự reset streaming lúc bắt đầu; chạy xong thì chạy lại `bash scripts/reset_streaming.sh --yes` để demo bắt đầu từ số 0. Kết quả ghi vào `reports/streaming/outage_test_<thời điểm>/` (`events.log` là dòng thời gian).

Kiểm tra nhanh:
- http://localhost:8501: app (nền sáng) hiện "ALS implicit · phiên bản mô hình <bản mới nhất, xem `db.meta.findOne()`; 15/11: 20261114_190600>" và top-10 của user 1: #1 The Catcher in the Rye, #2 The Joy Luck Club, #3 The Guernsey Literary and Potato Peel Pie Society (sau mỗi lần bấm Train lại, danh sách có thể đổi nhẹ; kiểm tra lại dòng này trước buổi).
- http://localhost:8088: YARN có 1 NodeManager.
- Mở sẵn 3 terminal Git Bash ở thư mục `bookpulse/` (đã `export MSYS_NO_PATHCONV=1`) và các tab trình duyệt: app (8501), YARN (8088), HDFS (9870).
- Máy cần đủ RAM: tắt bớt ứng dụng khác. Nếu máy chậm, chạy MapReduce trước giờ demo và chỉ trình bày kết quả (mục 6).
- **Không chạy batch khi job streaming đang chạy.** Nếu phải chạy lại `bash scripts/run_pipeline.sh` (khoảng 25–35 phút), dừng job streaming trước (Ctrl+C): hai driver 4 GB không vừa 7,7 GiB RAM. Trong lúc đó sự kiện chờ trong Kafka (giữ 7 ngày), app vẫn phục vụ gợi ý và số liệu cũ; chạy lại job thì nó đọc tiếp từ checkpoint, tối đa 20.000 offset mỗi batch.

## 2. MapReduce (Hảo, ~2 phút)

Bắt đầu job **ngay đầu phần demo** vì job mất khoảng 1–2 phút (lần tập 07/11: job 52 giây, 72 giây tính cả khởi động; ngày 05/11: 134 giây); trong lúc chờ thì chuyển sang phần 3.

```bash
# Terminal 1
docker compose -f mapreduce/hadoop/docker-compose.yml exec client bash /bookpulse/mapreduce/run_hadoop.sh
```

Khi job xong:
- Chỉ trên màn hình: `3 block(s)`, `map 100% reduce 100%`, counter `Combine input records` 5.976.479 → `Combine output records` 47.087 (giảm 99,2%).
- Số map task có thể là 3 hoặc 4 (bản speculative), thời gian cũng khác nhau giữa các lần chạy; kết quả thì giống hệt.
- YARN UI (8088): application `BookPulse-book-rating-stats` ở trạng thái FINISHED.
- Kiểm chứng (khoảng 15 giây; lần tập 15/11: 14 giây):

```bash
docker compose exec spark python mapreduce/verify_with_spark.py | grep -E 'rows_only|"match"|combiner_reduction'
```

Nói: "`rows_only_in_mapreduce` và `rows_only_in_spark` đều bằng 0, `match: true`: MapReduce và Spark SQL cho cùng kết quả trên 10.000 sách; combiner giảm 99,2% dữ liệu shuffle."

## 3. Spark SQL (Hảo, ~1,5 phút, chạy song song với job MapReduce)

Không chạy lại (mất khoảng 2 phút); mở kết quả đã có:

```bash
# Tên file cố định: mọi report có cùng mtime sau khi checkout git, nên `ls -t | head -1` trả về file 04/11 (sai: không có join_check, plan Inner join)
grep -A4 -E '"join_check"|"sql_vs_dataframe"|"csv_vs_parquet_seconds"' reports/analytics_sql_20261106_123956.json
head -22 reports/sql_plan_20261106_123938.txt
```

Chỉ vào:
- `join_check`: `joined_rows` = `ratings_rows` = 5.976.479, `rows_preserved: true`.
- `sql_vs_dataframe`: `only_in_sql` 0, `only_in_dataframe` 0 trên 10.000 dòng.
- `csv_vs_parquet_seconds`: csv 4.22, parquet 1.08 (3,9 lần; qua các lần đo 3,9–8,4 lần, dao động theo bộ nhớ đệm).
- Trong plan: `BroadcastHashJoin LeftOuter`, **một `Exchange` shuffle cộng một `BroadcastExchange`** (gửi bảng sách nhỏ tới mọi task); `Scan parquet` có `Output [2]: [book_id, rating]`, tức chỉ đọc 2 cột; phía sách là `Scan csv` (đọc `books.csv`).

## 4. Spark ML + MongoDB (Kiệt, ~2 phút)

- App: nhập user **1**, rồi **2**, rồi **22**: mỗi người một danh sách khác nhau (bản 20261114_190600, kiểm tra lại trước buổi vì mỗi lần bấm Train lại danh sách đổi nhẹ). User 1: The Catcher in the Rye, The Joy Luck Club, Guernsey, The Diary of a Young Girl, The Da Vinci Code…; user 2 nghiêng về kinh điển học ở trường và phi hư cấu (Freakonomics, To Kill a Mockingbird, Animal Farm, Huckleberry Finn, Lord of the Flies, 1984); user 22 gần như toàn Stephen King (The Shining, Misery, Carrie, Pet Sematary, rồi A Game of Thrones). Không nói là ba thể loại tách bạch: Animal Farm và Lord of the Flies có trong danh sách của cả user 1 và 2 (mô hình hơi nghiêng về sách nổi tiếng). Dòng chú thích ghi "ALS implicit · phiên bản mô hình …". (User 3 không dùng nữa vì hai cuốn đầu trùng user 1.)
- Nói lý do chọn implicit: Precision@10 0,204 so với explicit 0,008 và sách phổ biến 0,078 (slide 10). Nếu bị hỏi: mô hình phục vụ fit trên toàn bộ tập train (80% dữ liệu).
- MongoDB (khoảng 3 giây):

```bash
docker compose exec mongo mongosh bookpulse --quiet --eval 'db.meta.findOne({}, {popular: 0})'
docker compose exec mongo mongosh bookpulse --quiet --eval '
  const c = db.meta.findOne().recs_collection;
  printjson(db[c].find({_id: 1}, {recs: {$slice: 3}, fallback: 1}).next());
  print(db[c].find({_id: 1}).explain("executionStats").queryPlanner.winningPlan.stage)'
```

Nói: "Mỗi người dùng một document; truy vấn theo `_id` dùng IDHACK, đọc đúng một document." Ba gợi ý đầu của user 1 (bản 20261114_190600): 8 (0,80), 118 (0,77), 195 (0,75).

*Nếu được hỏi về `previous_collection`: `meta` ghi bản đang dùng và bản dùng trước đó (15/11: `recommendations_20261114_190600`, trước là `_20261114_190214`); quay lại bằng cách đặt `recs_collection` về bản trước. Cổng kiểm tra đủ 53.424 user trước khi chuyển đã chạy thật ở mỗi lần bấm Train lại (`reports/retrain_*.json`, `write_recs_*.json`).*

## 5. Streaming (Tuấn, ~3,5 phút)

Chỉ bắt đầu khi `verify_with_spark.py` ở mục 2 đã xong: không chạy job Spark batch cùng lúc với job streaming (Spark UI thứ hai sang cổng 4041, không được mở ra host; hai driver 4 GB vượt 7,7 GiB RAM của máy ảo Docker).

```bash
# Terminal 2: job streaming (để chạy)
docker compose exec spark python streaming/stream_stats.py
# Terminal 3: khi Terminal 2 in "streaming from …"; khoảng 55 giây
# Tạo dữ liệu giả một lần (khoảng 15 giây; có rồi thì bỏ qua): 5.000 cặp user–sách chưa có trong ratings.csv
docker compose exec spark python scripts/make_fake_stream.py
# --late 0.05: 5% số sự kiện gửi sau giây thứ 30 là sự kiện trễ (lần 08/11: 19/1.000)
docker compose exec spark python streaming/producer.py --file data/fake/stream_ratings.csv --limit 1000 --late 0.05
```

Cách không dùng dòng lệnh: thanh bên trái của app, khung **Giả lập sự kiện** → chọn số sự kiện → **Gửi vào Kafka** (tạo file dữ liệu giả nếu chưa có). Khung này không gửi sự kiện trễ; muốn thấy watermark loại sự kiện trễ thì dùng lệnh producer ở trên với `--late 0.05`.

Nói: "Bộ Goodbooks không có người dùng đang hoạt động, nên nhóm sinh rating giả cho các cặp user–sách **chưa có** trong dữ liệu, chọn theo độ phổ biến và tỷ lệ số sao thật. Producer gửi chúng như lượt chấm vừa xảy ra, kèm cố ý 2% bản trùng, 1% sự kiện lỗi và 5% sự kiện trễ để thấy hệ thống loại."

Số trùng/lỗi/trễ đổi theo lần chạy (lần tập 15/11: `sent 1000, duplicates 23, invalid 10, late 11`; lần 10/11: 20 / 8 / 22), nên KHÔNG đọc số cố định mà đọc công thức: hợp lệ trên app = 1.000 − số `late` producer in; sự kiện lỗi = số `invalid`; "Lượt 4–5 sao trúng top-10" khác 0 ngay trong lúc producer chạy vì dữ liệu giả là cặp chưa có trong train (15/11: 6 / 656 trước khi chấm sao; 10/11: 11 / 651). Bằng chứng có file: `reports/kafka_audit_20261114_19*.json`, `nosql_evidence_20261114_19*.json` (lượt 15/11).

Trong lúc producer chạy:
- Mở http://localhost:4040 → tab **Structured Streaming**: 4 query có tên (`valid`, `rejected`, `events_to_parquet`, `trending`); bấm vào `trending` để thấy tốc độ vào (khoảng 20 sự kiện/giây), thời gian batch (lần tập 15/11 với cache Ivy ấm: job sẵn sàng sau 9 giây, batch dài nhất 3 giây; máy lạnh: batch đầu 7–12 giây), watermark.
- App → bấm **Làm mới**: số sự kiện tăng, cột "Điểm TB (stream)", mục **"Sách đang hot"** xuất hiện (chỉ sách có lượt 4–5 sao, xếp theo số lượt 4–5 sao; lượt 1–3 sao không đẩy sách lên bảng, K37).
- Chỉ số "Lượt 4–5 sao trúng top-10" đã khác 0 (dữ liệu giả có thể trúng). **Đọc to con số này trước khi chấm sao** để lát nữa chỉ ra "+2". (Chỉ khi phát lại `ratings.csv` chỉ số mới luôn 0/N, vì sách đó đã bị loại khỏi gợi ý; ảnh `app_trending.png` 08/11 với 2 / 672 là lượt như vậy.)

**Sách vừa chấm biến khỏi top-10 (chấm ngay trong app).** Khi producer báo `done`, nhập user **1** trong app, chỉ vào #1 (15/11: The Catcher in the Rye). Kéo xuống mục **Chấm sao**: chọn sách "#1 · …", số sao ★★★★★, bấm **Gửi lượt chấm**. App báo "Đã gửi: user 1 chấm 5 sao cho #1 · The Catcher in the Rye." (app chỉ gửi sự kiện vào Kafka). Muốn chấm thêm #2 (The Joy Luck Club) thì chọn "#2 · …" rồi bấm gửi lần nữa (chưa cần làm mới).

Chờ 5–10 giây (một trigger), bấm **Làm mới**. Chỉ vào:
- Cuốn vừa chấm đã biến mất, các cuốn sau dồn lên, cuốn mới vào cuối danh sách, lấy từ 30 gợi ý đã lưu. Nếu chấm cả #1 và #2 (bản 20261114_190600): Guernsey lên #1, hai cuốn #11–12 của danh sách đã lưu vào #9–10 (xem trước bằng `db[c].findOne({_id: 1}).recs.slice(10, 12)` trong mongosh).
- Dòng chú thích trên bảng: "ALS implicit · phiên bản mô hình … · đã ẩn 2 cuốn vừa chấm".
- Chỉ số "Lượt 4–5 sao trúng top-10" tăng đúng bằng số lượt vừa chấm (15/11: 6 / 656 → 8 / 658).

Nói: "Nút này không ghi MongoDB. App chỉ gửi một sự kiện chấm sao vào Kafka, giống producer; job Spark Structured Streaming kiểm tra, bỏ trùng rồi ghi `user_seen` trong MongoDB; app đọc lại thì ẩn cuốn đó và đẩy cuốn tiếp theo lên, không cần huấn luyện lại."

Dự phòng nếu trình duyệt có vấn đề: cùng việc đó bằng dòng lệnh (2 cuốn đầu một lượt):

```bash
docker compose exec spark python scripts/rate_from_recs.py --user 1 --n 2 --rating 5
```

**Kiểm chứng** (chờ khoảng 15 giây sau lượt chấm), một lệnh:

```bash
docker compose exec spark python scripts/kafka_audit.py | grep -E 'distinct_valid|invalid_messages|counted|expected|_match|no_loss'
```

Nói: "Script đếm lại toàn bộ topic Kafka rồi đọc MongoDB: `valid_events_counted` = `expected_valid_minus_late` = `sum_n_ratings_in_rating_stats`, tức hợp lệ trừ trễ, không mất, không trùng; số lỗi khớp; tổng các cửa sổ bằng 2 lần vì mỗi sự kiện nằm trong 2 cửa sổ trượt. Ba dòng `_match` / `no_loss…` đều `true`, và kết quả được lưu vào `reports/kafka_audit_*.json`." (Lần chạy 08/11: 1.002 hợp lệ, 19 trễ, 983 đếm được, 1.966 = 2 × 983, 10 lỗi.) Lệnh cũng in vài dòng `expected_…` của khối `archive_check` (kho Parquet); chỉ giải thích nếu được hỏi.

**Khôi phục sau sự cố.** Không làm `kill -9` trực tiếp trên sân khấu; trình bày bằng chứng của `bash scripts/stream_fault_test.sh`: thư mục `reports/streaming/fault_test_<thời điểm>/` (`kill.log`, `audit.log`) và `reports/kafka_audit_20261107_220738.json`: 1.474 = 1.500 − 26 trễ, 2.948 = 2 × 1.474, 15 lỗi, ba phép so `true`. Nói rõ: "Lượt thử này 0 mất, 0 trùng; đó là kết quả đo, không phải bảo đảm, và vẫn còn khe hở nếu job chết đúng lúc đang ghi MongoDB." Nếu được hỏi thêm: lượt lặp lại có kiểm tra kho Parquet (`reports/kafka_audit_20261108_040236.json`: 1.475 = 1.500 − 25, kho 1.537 = 1.537) và lượt tắt Kafka 40 giây, MongoDB 30 giây (`reports/kafka_audit_20261108_041203.json`, `reports/streaming/outage_test_20261108_110750/events.log`: Spark tự nối lại Kafka; MongoDB tắt thì job dừng, script thử nghiệm chạy lại job từ cùng checkpoint sau 11 giây, chưa có restart policy nên ngoài thử nghiệm phải chạy lại tay; 3.000 = 3.000, 0 mất, 0 trùng). Nếu còn thời gian, khôi phục nhẹ: Ctrl+C ở Terminal 2, chờ dấu nhắc, chạy lại cùng lệnh (đọc tiếp từ checkpoint), gửi thêm `producer.py --limit 200`, rồi chạy lại lệnh kiểm chứng: vẫn `true`.

Kết thúc: Ctrl+C ở Terminal 2.

**Tuỳ chọn, nếu còn thời gian hoặc bị hỏi "khi nào mô hình học rating mới": nút Train lại.** Trang /setting, mục
**6 · Train lại với rating mới** → bấm **Train lại** (user so sánh: 1). Script `scripts/retrain.py` tự dừng job streaming,
chạy `clean.py` (gộp kho sự kiện, bỏ trùng), `train_als.py --implicit` một bộ tham số, `write_recs.py` (phiên bản mới,
chuyển `meta.active`), rồi bật lại job; đo 15/11: khoảng 3 phút (164–179 giây), log hiện ngay trên trang. Bấm rồi nói
tiếp phần kiểm chứng, quay lại khi xong: app hiện "phiên bản mô hình" mới, danh sách user 1 đổi (cuốn vừa chấm không còn trong 30 gợi ý vì đã
thành rating), report `reports/retrain_*.json` ghi top-10 trước/sau và thời gian từng bước. Nói: "Streaming không train
theo từng sự kiện; mô hình học lại theo đợt, bằng nút này hoặc theo lịch, khoảng 3 phút trên máy này."

## 6. Phương án dự phòng

| Sự cố | Cách xử lý |
|---|---|
| Lệnh MapReduce báo "C:/Program Files/Git/bookpulse/...: No such file" | Chưa `export MSYS_NO_PATHCONV=1` trong terminal đó; chạy rồi chạy lại lệnh |
| `grep "BOOKPULSE HADOOP READY"` không in gì | Cụm chưa sẵn sàng (khoảng 40 giây sau `up -d`); chờ rồi chạy lại |
| `run_hadoop.sh` báo "Name node is in safe mode" | NameNode vừa khởi động (gặp khi chạy thử 09/11 dù đã có dòng READY); chạy `hdfs dfsadmin -safemode wait` trong container client rồi chạy lại |
| Job MapReduce chậm hoặc lỗi | Trình bày kết quả đã chạy: `reports/mapreduce/job_*.log`, `reports/mapreduce_verify_*.json`, ảnh `reports/mapreduce/yarn_applications.png` |
| Spark UI không mở được | Ảnh `reports/streaming/spark_ui_*.png`; số liệu trong `reports/stream_progress_*.json` |
| App không hiện thay đổi code | `docker compose restart app` |
| App báo "Không gửi được vào Kafka" khi bấm Gửi lượt chấm | Kafka chưa chạy hoặc container app cũ chưa có biến `KAFKA_BOOTSTRAP`: `docker compose up -d kafka app`, rồi bấm lại; hoặc dùng `rate_from_recs.py` |
| Sau khi chấm sao (nút trong app hoặc `rate_from_recs.py`) app chưa đổi | Job streaming phải đang chạy; chờ thêm một trigger rồi bấm Làm mới. Nếu vẫn không: ảnh `docs/img/app_after_rate_from_recs.png` (trước đó: `docs/img/app_implicit_user1.png`) |
| App không hiện streaming | Ảnh `reports/streaming/app_trending.png` (cả trang: 983 / 10 / 2 / 672 và "Sách đang hot"; lượt 08/11 phát lại `ratings.csv`, nên chỉ 2 lượt trúng) |
| Một phép so của `kafka_audit.py` ra `false` | Thường do sự kiện trễ đến khi job chưa có watermark (1–2 batch đầu sau khi khởi động mới, checkpoint trống). Nói thẳng điều đó, rồi trình bày `reports/kafka_audit_20261107_221105.json` và `_220738.json` (đều `true`) |
| Job streaming báo lỗi topic | Job tự tạo topic; nếu vẫn lỗi: `bash scripts/reset_streaming.sh --yes` rồi chạy lại |
| Job streaming dừng với lỗi "checkpoint and stream_state do not match" | Checkpoint bị xóa hoặc chuyển chỗ mà MongoDB vẫn giữ `stream_state`; job cố ý dừng thay vì bỏ qua batch. Chạy `bash scripts/reset_streaming.sh --yes` rồi chạy lại |
| Máy hết RAM | Tắt cụm Hadoop sau phần MapReduce: `docker compose -f mapreduce/hadoop/docker-compose.yml stop` |
| Mọi thứ đều hỏng | Trình bày bằng slide + ảnh trong `reports/` và `docs/` (mọi số đo đều có file); bằng chứng `kill -9` ở `reports/streaming/fault_test_20261108_050413/` |

## 7. Sau buổi tập dượt

```bash
export MSYS_NO_PATHCONV=1
# 1. Dừng job streaming: Ctrl+C ở Terminal 2, chờ dấu nhắc
bash scripts/reset_streaming.sh --yes                          # 2. buổi thật bắt đầu lại từ số 0
docker compose -f mapreduce/hadoop/docker-compose.yml stop     # 3. dừng cụm Hadoop (giữ dữ liệu HDFS)
docker compose ps                                              # 4. stack chính vẫn "running"
git status --short reports/                                    # 5. tìm file report mới của lần tập
```

**Không bao giờ di chuyển hay xoá** `clean_*`, `train_als_*`, `train_als_implicit_*`, `write_recs_*`, `retrain_*` của lần chạy sinh ra mô hình đang phục vụ (report của lần train 10/11 đã mất vì dọn nhầm). Mỗi lần tập sinh thêm file trong `reports/` (`kafka_audit_*`, `stream_progress_*`, `mapreduce_verify_*`, `reports/mapreduce/*_<thời điểm>*`, có thể cả `nosql_evidence_*`). Chuyển chúng vào `reports/superseded/rehearsal_<ngày>/` để `reports/` chỉ giữ các lần chạy chính thức:

```bash
mkdir -p reports/superseded/rehearsal_<ngày>
mv reports/kafka_audit_<ts>.json reports/stream_progress_<ts>.json reports/mapreduce_verify_<ts>.json \
   reports/mapreduce/*_<ts>.* reports/superseded/rehearsal_<ngày>/
```

Tên file theo giờ UTC (chậm 7 giờ so với giờ Việt Nam). **Không chuyển** các file chính thức: lần chạy cuối 06/11 (`*_20261106_*`), MapReduce 05/11 (`*_20261105_*`) và bằng chứng streaming 08/11: `kafka_audit_20261107_220738.json`, `kafka_audit_20261107_221105.json`, `stream_progress_20261107_220431.json`, `_220545.json`, `_220918.json`, `nosql_evidence_20261107_221150.json`, thư mục `reports/streaming/fault_test_20261108_050413/`; lượt lặp lại và lượt tắt Kafka/MongoDB: `kafka_audit_20261108_040236.json`, `kafka_audit_20261108_041203.json`, `stream_progress_20261108_035851.json`, `_040005.json` (kill -9 lần 2), `_040808.json`, `_041127.json` (tắt Kafka/MongoDB), thư mục `reports/streaming/fault_test_20261108_105833/` và `reports/streaming/outage_test_20261108_110750/`; lượt tắt Kafka với producer chưa sửa (kể cả `stream_progress_20261108_040312.json`, `_040633.json`) đã nằm ở `reports/superseded/outage_test_20261108_110251_producer_no_retry/`. File của lần tập 07/11 đã nằm ở `reports/superseded/rehearsal_20261107/`.
