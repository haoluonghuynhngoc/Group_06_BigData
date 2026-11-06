# BookPulse – Kịch bản demo Session 10

Demo trực tiếp cho Final Assessment: chạy lần lượt MapReduce → Spark SQL → Spark ML + MongoDB → Streaming, mỗi phần có một kết quả nhìn thấy được và một phép kiểm chứng. Thời lượng dự kiến 8–10 phút. Mọi lệnh chạy trong Git Bash, từ thư mục `bookpulse/` trừ khi ghi khác.

## 1. Chuẩn bị trước buổi (ít nhất 30 phút trước)

```bash
cd "D:\Big Data\Group_06_BigData\bookpulse"
docker compose up -d                                  # spark, kafka, mongo, app
docker compose ps                                     # 4 service "running"
cd mapreduce/hadoop && docker compose up -d && cd ../..
docker compose -f mapreduce/hadoop/docker-compose.yml logs client | tail -3   # phải có "BOOKPULSE HADOOP READY"
bash scripts/reset_streaming.sh --yes                 # streaming bắt đầu từ số 0
docker compose restart app                            # app nạp code mới nhất
```

Kiểm tra nhanh:
- http://localhost:8501: app hiện "ALS implicit" và top-10 của user 1.
- http://localhost:8088: YARN có 1 NodeManager.
- Mở sẵn 3 terminal Git Bash ở thư mục `bookpulse/` và các tab trình duyệt: app (8501), YARN (8088), HDFS (9870).
- Máy cần đủ RAM: tắt bớt ứng dụng khác. Nếu máy chậm, chạy MapReduce trước giờ demo và chỉ trình bày kết quả (mục 6).

## 2. MapReduce (Hảo, ~2 phút)

Bắt đầu job **ngay đầu phần demo** vì job mất khoảng 2 phút; trong lúc chờ thì chuyển sang phần 3.

```bash
# Terminal 1
docker compose -f mapreduce/hadoop/docker-compose.yml exec client bash /bookpulse/mapreduce/run_hadoop.sh
```

Khi job xong:
- Chỉ trên màn hình: `3 block(s)`, `map 100% reduce 100%`, counter `Combine input records` → `Combine output records` (giảm khoảng 99%).
- YARN UI (8088): application `BookPulse-book-rating-stats` ở trạng thái FINISHED.
- Kiểm chứng:

```bash
docker compose exec spark python mapreduce/verify_with_spark.py
```

Nói: "`rows_only_in_mapreduce` và `rows_only_in_spark` đều bằng 0: MapReduce và Spark SQL cho cùng kết quả trên 10.000 sách."

## 3. Spark SQL (Hảo, ~1,5 phút, chạy song song với job MapReduce)

Không chạy lại (mất khoảng 2 phút); mở kết quả đã có:

```bash
ls -t reports/analytics_sql_*.json | head -1 | xargs cat | head -60
ls -t reports/sql_plan_*.txt | head -1 | xargs head -16
```

Chỉ vào: `join_check.rows_preserved: true`, `sql_vs_dataframe` = 0/0, `csv_vs_parquet_seconds`, và trong plan: `Scan parquet` chỉ đọc 2 cột, `BroadcastHashJoin`, một `Exchange`.

## 4. Spark ML + MongoDB (Kiệt, ~2 phút)

- App: nhập user **1**, rồi **2**, rồi **3**: mỗi người một danh sách khác nhau (văn học / phi hư cấu / cổ điển); dòng chú thích ghi "ALS implicit · phiên bản …".
- Nói lý do chọn implicit: Precision@10 so với explicit và sách phổ biến (slide 7).
- MongoDB:

```bash
docker compose exec mongo mongosh bookpulse --quiet --eval 'db.meta.findOne({}, {popular: 0})'
docker compose exec mongo mongosh bookpulse --quiet --eval '
  const c = db.meta.findOne().recs_collection;
  printjson(db[c].find({_id: 1}, {recs: {$slice: 3}, fallback: 1}).next());
  print(db[c].find({_id: 1}).explain("executionStats").queryPlanner.winningPlan.stage)'
```

Nói: "Mỗi người dùng một document; truy vấn theo `_id` dùng IDHACK, đọc đúng một document."

## 5. Streaming (Tuấn, ~3 phút)

```bash
# Terminal 2: job streaming (để chạy)
docker compose exec spark python streaming/stream_stats.py
# Terminal 3: khi Terminal 2 in "streaming from …"
docker compose exec spark python streaming/producer.py --limit 600 --late 0.03
```

Trong lúc producer chạy (khoảng 30 giây):
- Mở http://localhost:4040 → tab **Structured Streaming**: 4 query có tên (`valid`, `rejected`, `events_to_parquet`, `trending`); bấm vào `trending` để thấy tốc độ vào, thời gian batch, watermark.
- App → bấm **Làm mới**: số sự kiện tăng, mục **"Sách đang hot"** xuất hiện.

Kiểm chứng (khi producer báo `done`, chờ khoảng 15 giây):

```bash
docker compose exec spark python scripts/kafka_audit.py | grep -E 'distinct_valid|invalid_messages'
docker compose exec mongo mongosh bookpulse --quiet --eval 'printjson(db.stream_state.find().toArray())'
```

Nói: "Sự kiện hợp lệ MongoDB đếm được = `distinct_valid_events` − `distinct_valid_late_events` của Kafka: sự kiện trùng bị bỏ, sự kiện trễ bị watermark loại, không mất sự kiện nào."

Nếu còn thời gian, thử khôi phục: Ctrl+C ở Terminal 2, chạy lại cùng lệnh, gửi thêm `--limit 200`; số đếm vẫn khớp Kafka.

Kết thúc: Ctrl+C ở Terminal 2.

## 6. Phương án dự phòng

| Sự cố | Cách xử lý |
|---|---|
| Job MapReduce chậm hoặc lỗi | Trình bày kết quả đã chạy: `reports/mapreduce/job_*.log`, `reports/mapreduce_verify_*.json`, ảnh `reports/mapreduce/yarn_applications.png` |
| Spark UI không mở được | Ảnh `reports/streaming/spark_ui_*.png`; số liệu trong `reports/stream_progress_*.json` |
| App không hiện thay đổi code | `docker compose restart app` |
| Job streaming báo lỗi topic | Job tự tạo topic; nếu vẫn lỗi: `bash scripts/reset_streaming.sh --yes` rồi chạy lại |
| Máy hết RAM | Tắt cụm Hadoop sau phần MapReduce: `docker compose -f mapreduce/hadoop/docker-compose.yml stop` |
| Mọi thứ đều hỏng | Trình bày bằng slide + ảnh trong `reports/` và `docs/` (mọi số đo đều có file) |

## 7. Sau buổi tập dượt

`bash scripts/reset_streaming.sh --yes` để buổi thật bắt đầu lại từ số 0.
