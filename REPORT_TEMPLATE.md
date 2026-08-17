# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Tiến  **Mã SV:** 2A202601655  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details open>
<summary>Output ba lần chạy của make verify</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 16.7s
  run 2/3 … 16.7s
  run 3/3 … 16.6s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt** (100% Pass mọi bài kiểm tra chính và bài mở rộng)

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Khi chạy lại pipeline (hoặc khi clear task trên Airflow), số hàng trong bảng `gold_training_set` tăng lên sau mỗi lượt chạy (từ 12,480 lên 25,615 rồi 38,750 hàng), `make verify` báo `FAIL` cột ỔN ĐỊNH và xuất hiện nhiều ticket bị lặp lại. |
| **Nguyên nhân** | Model `gold_training_set` được khai báo `materialized='incremental'` nhưng thiếu `unique_key` và `incremental_strategy`. Khi không có `unique_key`, dbt tự động sinh ra câu lệnh `INSERT INTO` (append-only) thay vì `MERGE` / `DELETE+INSERT`. Do đó, khi chạy lại cùng một ngày hoặc khi một ticket có nhiều bản ghi cập nhật CDC (`op='u'`) rơi vào các ngày khác nhau, các bản ghi cũ không được thay thế mà bị chèn thêm dòng mới vào bảng đích. Ngoài ra, Airflow DAG thiết lập `catchup=True` và không giới hạn `max_active_runs` khiến nhiều luồng chạy bù cùng lúc, nhân bản dữ liệu hàng loạt. |
| **Cách khắc phục** | - `dbt/models/gold/gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` trong khối `config()`.<br>- `dags/ai_training_pipeline.py`: Cấu hình `catchup=False` và `max_active_runs=1` để ngăn chặn việc chạy bù tự động ngoài kiểm soát và hạn chế chạy đồng thời nhiều pipeline vào cùng một kho dữ liệu. |
| **Bằng chứng** | trước: 38,750 hàng (12,480 ticket bị lặp) · sau: 12,480 hàng (1 hàng / 1 ticket, không lặp) · checksum 3 lượt: `8dd7c98653` (ổn định tuyệt đối). |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` bị thiếu khoảng 5% số dòng (chỉ có 8,645 hàng thay vì 9,100 hàng kỳ vọng). Sự thiếu hụt chỉ xảy ra ở các ngày trong quá khứ, trong khi ngày mới nhất thì đầy đủ. |
| **P99 độ trễ đo được** | **2.73 ngày** (chính xác: 2.7258 ngày, tương đương ~65.4 giờ) |
| **Lookback đã chọn** | **3 ngày** — vì phân bố độ trễ đến kho (`_ingested_at - event_time`) có 2 cụm rõ rệt: cụm bình thường (0-6 giờ) và cụm đến muộn (43-71 giờ). P99 đo được là 2.73 ngày và độ trễ tối đa (max) là 2.94 ngày. Cửa sổ lookback 3 ngày bao phủ 100% các sự kiện đến muộn trong toàn bộ hệ thống. |
| **Nguyên nhân** | Model `gold_feature_daily` sử dụng điều kiện lọc tăng dần `where event_date > (select max(event_date) from {{ this }})`. Khi một sự kiện xảy ra ngày D1 (ví dụ 08-12) nhưng bị trễ và chỉ tới kho vào ngày D2 (ví dụ 08-15), tại lượt chạy ngày 08-15 thì `max(event_date)` trong bảng đích đã là 08-14, khiến sự kiện ngày 08-12 không thỏa mãn điều kiện `event_date > max(event_date)` và bị bỏ qua vĩnh viễn. |
| **Cách khắc phục** | - `dbt/models/gold/gold_feature_daily.sql`: Mở rộng cửa sổ tính lại (lookback window) trong khối `is_incremental()` thành `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`.<br>- Bổ sung vào `config()` khóa chính tổng hợp `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'delete+insert'` để đảm bảo tính idempotent khi tính lại các ngày cũ mà không bị nhân bản dòng. |
| **Bằng chứng** | trước: 8,645 hàng (thiếu 455 hàng) · sau: 9,100 hàng (đạt 100% kỳ vọng: 14 ngày × 650 khách hàng = 9,100 hàng) · checksum 3 lượt: `3db448685c` (ổn định). |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Trong thiết kế Data Pipeline thực tế, độ trễ tối đa (`max`) thường bị chi phối bởi các trường hợp ngoại lệ cực đoan (outliers - ví dụ: node IoT mất kết nối mạng nhiều tuần, server ngắt nguồn lâu ngày). Nếu chọn `max` làm lookback window cố định, toàn bộ pipeline hàng ngày sẽ phải quét lại và tính toán lại một lượng dữ liệu lịch sử khổng lồ (kéo dài thời gian chạy, tốn chi phí I/O, compute và làm trễ SLA hàng ngày cho 99.9% trường hợp thông thường chỉ để đón vài bản ghi hiếm hoi). Lựa chọn **P99** đảm bảo cân bằng tối ưu giữa **tính toàn vẹn dữ liệu** (phục hồi >99% dữ liệu đến trễ) và **chi phí tài nguyên**. Phần nhỏ dữ liệu trễ vượt P99 (<1%) sẽ được xử lý bằng batch đối soát định kỳ (weekly/monthly reconciliation batch) thay vì gánh nặng hàng ngày.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Từ ngày 08-10, backend thay đổi format cột `priority` từ số sang chuỗi. Pipeline chạy không báo lỗi nhưng model AI phân loại dự đoán sai lệch do cột `priority` trong `silver_tickets` chứa nhiều giá trị `NULL` hoặc số ngoài phạm vi 1..4 (6,606 hàng sai), trong khi bảng `quarantine_tickets` rỗng (0 / 312). |
| **Nguyên nhân** | 1. Sử dụng `try_cast(priority_raw as integer)` đơn thuần khiến toàn bộ nhãn chuỗi hợp lệ ('urgent', 'high', 'medium', 'low') bị ép kiểu thành `NULL` (mất dữ liệu nghiệp vụ), đồng thời lại chấp nhận các giá trị số không hợp lệ như `0, 5, -1`.<br>2. Bảng `silver_tickets` chưa bật `contract: enforced: true` và chưa có bài test kiểm tra miền giá trị 1..4.<br>3. Thứ tự xử lý trong Silver nếu lọc sau `row_number()` sẽ loại bỏ toàn bộ ticket có bản ghi mới nhất bị lỗi thay vì chỉ loại bỏ bản ghi CDC lỗi đó. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **1. Số hợp lệ (`'1'`, `'2'`, `'3'`, `'4'` - 6,846 bản ghi):** Đúng contract ban đầu $\rightarrow$ Ép kiểu giữ nguyên giá trị integer 1..4.<br>**2. Nhãn chuỗi (`'urgent'`, `'high'`, `'medium'`, `'low'` - 7,142 bản ghi):** Schema evolution từ backend, ý nghĩa không đổi $\rightarrow$ Ánh xạ (Map) về số nguyên tương ứng: `urgent -> 1`, `high -> 2`, `medium -> 3`, `low -> 4`.<br>**3. Dữ liệu lỗi (`'0'`, `'5'`, `'-1'`, `'P1'`, `'P2'`, `'unknown'`, `''`, `NULL` - 312 bản ghi):** Dữ liệu hỏng thật sự $\rightarrow$ Trả về `NULL` trong macro chuẩn hóa để cách ly vào `quarantine_tickets`. |
| **Cách khắc phục** | - `dbt/macros/normalize_priority.sql`: Dùng khối `CASE` xử lý trọn vẹn 3 nhóm trên, trả về `NULL` cho nhóm lỗi; macro `priority_reject_reason` phân loại rõ nguyên nhân cách ly.<br>- `dbt/models/silver/silver_tickets.sql`: Lọc bỏ bản ghi lỗi (`priority_clean is not null`) **trước khi** đánh số `row_number()`, giúp giữ lại trạng thái hợp lệ trước đó của ticket.<br>- `dbt/models/silver/quarantine_tickets.sql`: Đặt `where {{ normalize_priority('priority_raw') }} is null` để tiếp nhận các bản ghi CDC vi phạm.<br>- `dbt/models/silver/schema.yml`: Bật `contract: enforced: true` và bổ sung test `not_null`, `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng (khớp chính xác 312/312) · `silver_tickets` giữ đủ 12,480 ticket · `silver_tickets.priority ∈ 1..4, không NULL` sạch 100% · `dbt test` 11/11 pass (thêm 2 test mới). |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> 1. **Nên chặn và kiểm tra ở tầng Silver, không chặn ở Bronze**: Tầng Bronze là hồ chứa dữ liệu thô (raw data lake / immutable log) với nhiệm vụ lưu trữ nguyên trạng mọi tín hiệu từ các nguồn gửi về. Nếu từ chối bản ghi lỗi ngay tại Bronze, dữ liệu gốc sẽ bị mất vĩnh viễn, vô hiệu hóa khả năng điều tra sự cố (Root Cause Analysis), audit, và không thể reprocess khi backend cung cấp bản sửa lỗi. Tầng Silver mới là nơi thực thi Data Contract, chuẩn hóa kiểu dữ liệu và làm sạch nghiệp vụ.
> 2. **Vì sao không để pipeline dừng (fail fast)**: Trong môi trường thực tế, số lượng bản ghi lỗi (312 bản ghi) chỉ chiếm tỉ lệ rất nhỏ so với tổng khối lượng dữ liệu khổng lồ (hơn 14,300 bản ghi CDC và hơn 130,000 sự kiện). Nếu để pipeline dừng hoàn toàn, hàng trăm nghìn sự kiện và bản ghi hoàn toàn hợp lệ sẽ bị tắc nghẽn, làm sập downstream services (RAG search, CSKH dashboard, Routing agent), gây thiệt hại nghiêm trọng về SLA. Áp dụng mô hình **Quarantine (Dead-Letter Queue)** giúp cách ly dữ liệu lỗi sang bảng riêng cho kỹ sư xử lý sau, trong khi luồng dữ liệu hợp lệ vẫn được xử lý thông suốt.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | Cả hai bài: **Bài A** (Dashboard Compaction & Partitioning) và **Bài B** (Consumer Crash Recovery) |
| **Nguyên nhân** | - **Bài A**: 5,000 file Parquet nhỏ (small-file problem) và không phân vùng khiến engine phải mở toàn bộ file; mệnh đề `WHERE strftime(event_time, '%Y-%m-%d') = '2026-08-09'` bọc hàm khiến engine không thể pruning theo partition hay row group stats.<br>- **Bài B**: Thứ tự commit offset trước khi ghi dữ liệu dẫn đến mất message khi tiến trình bị kill giữa batch (At-most-once); câu lệnh `INSERT` thuần không thể tự bảo vệ khi message được replay (thiếu tính Idempotent). |
| **Cách khắc phục** | - **Bài A**: Viết `tools/compact.py` gom 5,000 file thành 14 file theo `PARTITION_BY (event_date)`, sắp xếp `ORDER BY event_date, customer_name, event_time`, đặt `ROW_GROUP_SIZE 1000`; viết lại `queries/dashboard.sql` dùng `hive_partitioning=1` và điều kiện lọc sargable `event_date = '2026-08-09'`.<br>- **Bài B**: Trong `ingest/consumer.py`, đảo thứ tự thành ghi dữ liệu trước (`write_batch`) rồi mới commit offset (`consumer.commit`); thêm khóa chính `event_id PRIMARY KEY` và thực hiện câu lệnh idempotent `INSERT INTO ... ON CONFLICT (event_id) DO UPDATE SET ...`. |
| **Bằng chứng** | - **Bài A**: `rows scanned` giảm từ 5,000,000 xuống **9,324** (giảm **536.3×**, yêu cầu $\ge 10\times$); số files giảm từ 5,000 xuống **14**; `result hash` giữ nguyên: `4379e4c5d9f3`.<br>- **Bài B**: `make crash-test` vượt qua hoàn hảo: 0 mất hàng, 0 trùng hàng, $C == A = 20,000$ message $\rightarrow$ **BÀI MỞ RỘNG B: ĐẠT ✓**. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra tính **Idempotent** của các model incremental (đã khai báo `unique_key` và `incremental_strategy` phù hợp chưa), và kiểm tra cấu hình scheduler DAG (`catchup=False`, `max_active_runs=1`) để tránh nhân bản dữ liệu khi chạy lại. |
| 2 | Đo lường phân bố độ trễ dữ liệu (`_ingested_at - event_time`) qua các mốc **Percentile (P50, P95, P99)** để thiết lập **Lookback Window** chính xác cho late-arriving data, kết hợp khóa chính hợp lý để cập nhật lại dữ liệu cũ an toàn. |
| 3 | Kiểm tra các ràng buộc **Data Contract** (`enforced: true`), bộ **Data Quality Tests**, và thiết lập cơ chế **Quarantine Table / Dead-Letter Queue** để cách ly bản ghi vi phạm schema mà không làm gián đoạn pipeline chính. |
