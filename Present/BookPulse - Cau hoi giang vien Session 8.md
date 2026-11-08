# BookPulse – Câu hỏi của giảng viên ở Session 8

Ghi lại câu hỏi giảng viên đặt ra khi nhóm thuyết trình Session 8, câu trả lời chính và các câu hỏi tiếp theo có thể gặp ở Buổi 10.

## Câu hỏi

> Dữ liệu đã lưu sẵn 10 cuốn sách gợi ý cho mỗi người, nhưng thực tế dữ liệu sẽ thay đổi. Vậy hệ thống xử lý những thay đổi đó như thế nào?

## Câu trả lời chính (khoảng 30 giây)

> Dạ, 10 cuốn trong MongoDB là kết quả tính sẵn, nhưng hệ thống không đứng yên khi dữ liệu thay đổi. Nhóm chia thay đổi thành hai tốc độ.
>
> Thay đổi nhanh, như người dùng vừa đọc hay vừa chấm một cuốn, đi qua luồng streaming và được xử lý trong vài giây. Cuốn vừa đọc bị ẩn đi, và vì nhóm lưu sẵn 30 gợi ý nên app lấy cuốn tiếp theo. Nếu người dùng đọc gần hết, app bù bằng sách phổ biến mà chính người đó chưa đọc, nên luôn đủ 10 cuốn.
>
> Thay đổi về gu đọc thì cần huấn luyện lại mô hình, và việc này chạy theo đợt. Mọi rating từ streaming đều được lưu lại, nên lần huấn luyện sau học được cả dữ liệu mới. Mô hình mới được ghi sang chỗ mới rồi mới chuyển sang, nên app không bị gián đoạn.

Nếu trả lời ở Buổi 10, nói thêm: "Sau câu hỏi của thầy/cô ở Buổi 8, nhóm phát hiện rating từ streaming chưa được đưa vào lần huấn luyện sau, nên đã bổ sung và kiểm thử."

> **Ghi chú trung thực (cập nhật 08/11).** Hiệu ứng "cuốn vừa đọc bị ẩn, lấy cuốn tiếp theo" chỉ xảy ra khi người dùng chấm một cuốn **đang nằm trong** danh sách gợi ý. `producer.py` phát lại cặp (user, sách) đã có trong dữ liệu huấn luyện, mà gợi ý đã loại mọi sách người dùng đã chấm, nên sự kiện phát lại không bao giờ nằm trong danh sách và không làm đổi top-10. Ở Buổi 10, hiệu ứng được demo bằng `scripts/rate_from_recs.py` (user 1 chấm 2 cuốn đầu trong top-10 của mình → hai cuốn bị ẩn, cuốn tiếp theo lên). Kế hoạch nêu ở Buổi 8 là phát lại một tập test giữ riêng để đo đánh giá online **chưa được làm**; app hiện chỉ số "Lượt 4–5 sao trúng top-10" dạng trúng / tổng (ví dụ 2 / 672) kèm giải thích vì sao sự kiện phát lại không thể trúng.

## Ý chính cần nhớ

| Loại thay đổi | Xử lý | Bao lâu |
|---|---|---|
| Vừa đọc hoặc chấm một cuốn trong top-10 | Streaming ghi vào `user_seen`; app ẩn cuốn đó, lấy cuốn tiếp theo trong 30 cuốn đã lưu (demo bằng `rate_from_recs.py`; sự kiện phát lại của producer không nằm trong top-10) | Vài giây |
| Đọc gần hết danh sách | App bù từ `fallback`: 10 sách phổ biến người đó chưa chấm; cột "Nguồn" ghi "Phổ biến (bù)" | Ngay |
| Điểm trung bình của sách thay đổi | Cộng dồn tổng điểm và số lượt trong `rating_stats`, rồi mới chia | Vài giây |
| Gu đọc thay đổi, người dùng mới, sách mới | Sự kiện được lưu ra Parquet → `clean.py` gộp (lấy rating mới nhất) → huấn luyện lại ALS | Theo lịch batch |
| Thay mô hình mới | Ghi vào collection mới, xong mới chuyển `meta.active`; giữ 2 phiên bản để quay lại | Chuyển tức thì |

Vì sao không huấn luyện lại sau mỗi sự kiện: một lần huấn luyện ALS trên 4,8 triệu dòng mất khoảng 4 phút (đã đo). Cập nhật số liệu thì rẻ nên làm ngay; học lại mô hình thì đắt nên làm theo đợt. Đây là cách tổ chức của kiến trúc Lambda.

## Bằng chứng đã kiểm thử

- Gửi 5 rating mới qua Kafka: dữ liệu huấn luyện tăng đúng 5 dòng so với 5.976.479 dòng gốc (lần chạy 05/11), và 5 cuốn đó biến khỏi gợi ý của người dùng.
- Kafka, Parquet và MongoDB cùng đếm đúng 2.300 sự kiện hợp lệ: không mất, không trùng.
- 534.240 cặp trong danh sách bù: 0 cuốn người dùng đã đọc.
- Giả lập người dùng đã đọc 25 trên 30 gợi ý: app vẫn hiện đủ 10 cuốn (5 cuốn ALS, 5 cuốn bù).

File report: `bookpulse/reports/clean_20261105_155646.json`, `bookpulse/reports/write_recs_20261105_155958.json`, `bookpulse/reports/kafka_audit_20261105_155146.json`.

## Câu hỏi tiếp theo có thể gặp

**1. Bao lâu huấn luyện lại một lần?**
Thiết kế là mỗi đêm. Hiện nhóm chạy bằng tay; phần chạy tự động theo lịch nằm trong kế hoạch Buổi 10.

**2. Job streaming chết thì sao?**
Sự kiện vẫn nằm trong Kafka; bật lại thì Spark đọc tiếp từ checkpoint. Nhóm đã thử restart: không mất, không trùng sự kiện nào. Riêng phần ghi ra Parquet dùng file sink của Spark nên đảm bảo exactly-once. (Cập nhật Buổi 10: lượt `kill -9` ngày 08/11 cũng 0 mất, 0 trùng, nhưng ghi MongoDB chưa exactly-once hoàn toàn: nếu job chết đúng lúc đang ghi, batch phát lại có thể cộng lặp một phần.)

**3. Muốn gợi ý cập nhật nhanh hơn, không chờ đến đêm?**
Vector của sách cố định giữa các lần huấn luyện, nên có thể tính lại vector của một người dùng từ các rating mới của họ bằng một phép giải bình phương tối thiểu nhỏ (fold-in), mất vài giây. Đây là hướng nâng cao, nhóm chưa làm.

**4. Sách mới chưa ai chấm thì sao?**
ALS chưa gợi ý được cho đến khi sách có rating và mô hình được huấn luyện lại. Muốn gợi ý ngay thì cần thêm gợi ý theo nội dung sách (thể loại, tác giả); phần này nằm ngoài phạm vi đồ án.

**5. Người dùng chấm lại một cuốn đã chấm thì lấy rating nào?**
Lấy rating mới nhất. `clean.py` ưu tiên rating từ streaming (có thời điểm) hơn rating trong CSV, và trong streaming thì lấy sự kiện có `event_time` muộn nhất.

**6. Người dùng mới hoàn toàn thì sao?**
Chưa có document gợi ý, nên app hiện sách phổ biến (cold start). Sau lần huấn luyện tiếp theo, người dùng có gợi ý riêng.

**7. Lưu sự kiện ra Parquet có sinh nhiều file nhỏ không?**
Có, mỗi micro-batch có thể sinh vài file nhỏ. Hiện chưa ảnh hưởng vì `clean.py` đọc tất cả rồi ghi gộp lại thành bảng `ratings`. Với dữ liệu lớn, cần thêm bước gộp file định kỳ (compaction).
