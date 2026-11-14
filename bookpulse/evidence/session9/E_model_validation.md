# E. ML / large-scale analytics validation (Session 9, mục 4.2)

Trạng thái: **measured**. Số liệu mới ngày 13/11/2026 từ `scripts/session9/ranking_eval.py` → `E_model_validation.json` (G2) và `G1_implicit_semantics.json` (G1); số liệu cũ từ `reports/train_als_implicit_20261106_125816.json`, `reports/train_als_20261106_124521.json`.

## Kết quả chính (mô hình đang phục vụ `models/als_implicit`, bản ghi MongoDB 20261110_212851)

| Mô hình | Precision@10 | Recall@10 | **NDCG@10** | User có ≥ 1 hit / 4.995 | Số đầu sách khác nhau trong top-10 |
|---|---|---|---|---|---|
| Baseline sách phổ biến (200 phổ biến nhất trong train, bỏ sách đã đọc) | 0,0777 | 0,0511 | 0,0881 | 2.179 | 38 |
| ALS explicit (`models/als` train 10/11, rank 20; RMSE 0,828 là report 06/11) | 0,0078 | 0,0063 | 0,0074 | 332 | 1.296 |
| **ALS implicit đang phục vụ** (rank 50, alpha 1, reg 0,1) | **0,2008** | **0,1330** | **0,2348** | **3.938** | 1.208 |
| ALS implicit, scheme liked-only (G1, chỉ rating ≥ 4; không lưu) | 0,1995 | 0,1320 | 0,2356 | 3.945 | 1.086 |

NDCG@10 của ALS implicit gấp **2,67×** baseline; explicit thua baseline 12×. Lần đo 06/11 (bản 20261106_125827) cho P@10 0,2038 / R@10 0,1352 trên 4.993 user; lần này 0,2008 / 0,1330 trên 4.995 user: cùng kết luận.

## 4.2 Bảng câu hỏi

| Question | Your answer / evidence |
|---|---|
| What is the task? | **Recommendation** (top-10 ranking cho mỗi người dùng trong 10.000 sách), không phải đoán số sao. Bài toán phụ đã bỏ: regression số sao (ALS explicit, RMSE), giữ để so sánh. |
| What are the features, and which are available BEFORE the target occurs? | ALS không dùng feature nội dung: đầu vào chỉ là ma trận (user_id, book_id, rating) của tập **train**. Với implicit: p = 1 nếu đã chấm, c = 1 + rating. Mọi tín hiệu dùng để xếp hạng user u đến từ rating của u và của người khác **trong train**; rating test của u không được dùng ở bất kỳ đâu (không fold-in, không popularity từ test: `popular` tính trên train). Không có timestamp nên "trước/sau" chỉ bảo đảm theo split, không theo thời gian thật. |
| What is the split/validation method? Why appropriate? | `randomSplit([0.8, 0.2], seed 42)` trên rating CSV → 4.780.731 train / 1.195.748 test (13/11, Parquet có cột `from_stream`; lần 06/11 với code cũ: 4.780.968 / 1.195.511); rating từ streaming luôn vào train. Chọn tham số (rank 20/50 × alpha 1/10) trên validation 80/20 tách **từ train**; test dùng **một lần**. Hợp lý vì Goodbooks không có thời gian; **không** hợp lý nếu muốn mô phỏng tương lai (xem rủi ro). Split 13/11 được dựng lại bằng đúng hàm `split_train_test` và seed trên đúng Parquet mà `train_als.py` đã đọc ngày 10/11; report train của lần 10/11 không còn trong `reports/` nên không đối chiếu được số dòng, nhưng P@10 0,2008 xấp xỉ 0,2038 (06/11) và 0,2089 (05/11). Kiểm chứng 13/11: hai phiên Spark riêng cho cùng split và cùng hash tập test; Parquet ghi 21:20 UTC trước model 21:28 UTC, không clean lại sau đó; MongoDB user 1 book 5 score 0,7943 = model trên đĩa; nếu test lọt vào train, implicit ALS sẽ xếp các sách đó lên đầu và P@10 nhảy xa trên 0,2. Lần train 10/11 fit một bộ tham số (220 s giữa hai model, không đủ cho lưới 4 bộ). |
| What metric(s) are used? Why do they match the task? | Precision@10, Recall@10, **NDCG@10** (binary relevance = 4–5 sao trong test, IDCG trên min(n_rel, 10)); tỷ lệ user có ít nhất 1 hit; số đầu sách khác nhau (độ phủ). Người dùng thấy đúng 10 cuốn nên thước đo phải là chất lượng danh sách 10; NDCG thêm trọng số vị trí (#1 quan trọng hơn #10). RMSE **không** dùng để chọn mô hình: explicit RMSE 0,828 nhưng NDCG 0,0074. |
| What is the baseline or comparison? | (1) Sách phổ biến nhất trong train, bỏ sách đã đọc: NDCG 0,0881. (2) ALS explicit: 0,0074. (3) Scheme implicit liked-only (G1): 0,2356. (4) Gợi ý ngẫu nhiên: ~15 sách liên quan / 10.000 → P@10 ≈ 0,0015 (tính tay). Baseline RMSE: global mean 0,992, item mean 0,955. |
| What result would indicate the model is not useful? | NDCG@10 hoặc P@10 **≤ baseline phổ biến** (0,088 / 0,078): tức cá nhân hóa không hơn một danh sách chung (đó chính là trường hợp của explicit, 0,008, nên nhóm đã bỏ). Hoặc độ phủ ≈ 38 đầu sách như baseline (ai cũng nhận cùng danh sách). Online: tỷ lệ 4–5 sao trúng top-10 trên sự kiện giả ≈ 0 (hiện 11/651 ≈ 1,7%, với dữ liệu giả theo độ phổ biến nên chỉ là sanity check). |
| What is the largest leakage / bias / evaluation risk? | **(a) Không có thời gian**: split ngẫu nhiên cho phép mô hình học rating "tương lai" của người khác về cùng cuốn sách (temporal leakage ở mức tập thể), nên 0,20 có thể cao hơn thực tế triển khai. **(b) Popularity bias**: implicit nghiêng về sách nổi tiếng (Animal Farm trong 3/3 danh sách demo; 1.208 đầu sách so với 1.296 của explicit). **(c) 1 sao là tín hiệu dương** (G1): đo được là không đổi chất lượng (0,2008 so với 0,1995) nhưng đổi 5/10 cuốn mỗi danh sách. **(d) Mô hình phục vụ fit trên 80%**; 1,2 triệu rating test không được học. **(e) Chỉ 4 bộ tham số**; eval trên 5.000 user mẫu (≈ 9% user). Không có leakage kiểu target-encoding vì không có feature. |

## G1 tóm tắt (chi tiết ở `H_project_card.md`)

| Stars | p | c = 1 + α·r (α = 1) | Tỷ lệ dòng train |
|---|---|---|---|
| 1 | 1 | 2 | 2,08 % |
| 2 | 1 | 3 | 6,00 % |
| 3 | 1 | 4 | 22,94 % |
| 4 | 1 | 5 | 35,80 % |
| 5 | 1 | 6 | 33,18 % |

Scheme liked-only (3.297.772 dòng, fit 47,6 s so với ~138 s) cho NDCG 0,2356 so với 0,2348: **không khác có ý nghĩa**, nhưng top-10 chỉ trùng trung bình 5,0/10 cuốn (28/5.000 user trùng hết). Kết luận: cách mã hóa hiện tại không làm giảm chất lượng đo được, vì 1–2 sao chỉ chiếm 8,1 % và trọng số 2–3 so với 5–6; nhưng đây là lựa chọn phải nói rõ, không phải điều hiển nhiên.

## Lưu ý khi đọc danh sách user 1

Top-10 của user 1 trong G1 (Life of Pi, The Red Tent, The Great Gatsby, …) khác top-10 lưu trong MongoDB (The Great Gatsby, The Catcher in the Rye, …) vì `write_recs.py` loại **mọi** sách user đã chấm trong toàn bộ Parquet (train + test), còn đánh giá chỉ loại sách trong train (phải thế, vì sách trong test là đáp án). Life of Pi, The Red Tent, Pride and Prejudice là sách user 1 đã chấm trong phần test.
