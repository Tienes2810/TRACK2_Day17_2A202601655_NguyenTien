# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Tiến  **Mã SV:** 2A202601655  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details open>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 17.5s
  run 2/3 … 17.2s
  run 3/3 … 17.3s

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

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Job lỗi mạng, người trực Clear Task trên Airflow rồi chạy lại: `gold_training_set` phình từ 12.480 → 25.615 → 38.750 hàng. Không có exception. `make verify` báo ỔN ĐỊNH ✗ và 12.480 ticket bị lặp. |
| **Nguyên nhân** | Nguồn CDC có bản ghi `op='u'` (1.310 update). Grain bảng training là **entity** (1 hàng / 1 `ticket_id`), không phải event theo ngày. Model lại `materialized='incremental'` nhưng **không khai `unique_key`**, nên dbt mặc định sinh `INSERT` append-only. Chạy lại cùng partition — hoặc một ticket được tạo ngày D1 rồi update ngày D2, đi qua `WHERE run_date` hai lần trong một lượt — **ghi thêm hàng mới thay vì ghi đè** hàng cũ. `catchup=True` và `max_active_runs` không giới hạn chỉ làm Clear Task kích hoạt lỗi dày hơn; chúng không phải cơ chế nhân bản. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key='ticket_id'` và `incremental_strategy='merge'` — chọn `merge` vì khoá tự nhiên là entity, không phải ngày; `delete+insert` theo partition ngày sẽ bỏ sót update xuyên ngày, `append` thì nhân bản. `dags/ai_training_pipeline.py`: `catchup=False`, `max_active_runs=1` để giảm tần suất kích hoạt khi Clear Task. |
| **Bằng chứng** | trước: 38.750 hàng, 12.480 ticket lặp, checksum khác nhau 3 lượt · sau: **12.480 hàng**, 0 lặp, checksum `8dd7c98653` × 3 · DAG `True/None` → `False/1` · đối chứng `gold_doc_chunks` vẫn 31.200 / `92d8e50131` × 3. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` thiếu ~5% so với đối chiếu thủ công: 8.645 / 9.100 hàng. Ngày mới đủ; các ngày đã chạy xong từ lâu thì thiếu. ỔN ĐỊNH vẫn ✓ — bảng ổn định nhưng sai. |
| **P99 độ trễ đo được** | **2,7258 ngày** (~65,4 giờ). P50 = 0,1281 ngày; P95 = 1,8137 ngày; max = 2,9447 ngày; 5,05% event tới kho muộn hơn 1 ngày. |
| **Lookback đã chọn** | **3 ngày** — vì P99 = 2,73 và max trên dataset này = 2,94, cửa sổ 3 ngày phủ toàn bộ late data đo được. |
| **Nguyên nhân** | Điều kiện incremental là `where event_date > (select max(event_date) from {{ this }})`: watermark gắn vào **ngày sự kiện đã có trong Gold**, không gắn vào thời điểm dữ liệu **tới kho**. Event xảy ra 08-12 nhưng `_ingested_at` = 08-15: lúc chạy 08-15, `max(event_date)` đã là 08-14, nên hàng 08-12 không thoả `>` và **bị bỏ vĩnh viễn**. Đổi `>` thành `>=` chỉ nới thêm đúng 1 ngày, không đủ với P99 ≈ 2,7 ngày. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: `where event_date >= max(event_date) - interval 3 day`. Grain là aggregate `(event_date, customer_id)` nên thêm `unique_key=['event_date','customer_id']` và `incremental_strategy='delete+insert'` — tính lại một ngày phải **thay cả hàng aggregate**; nếu chỉ nới window mà giữ append thì tái tạo đúng lỗi nhiệm vụ 1. Không dùng `merge` theo entity vì đây không phải bảng 1-ticket-1-hàng. |
| **Bằng chứng** | trước: 8.645 hàng (thiếu 455 ≈ 5,05% late) · sau: **9.100** = 14 ngày × 650 khách · checksum `3db448685c` × 3 · `gold_training_set` vẫn 12.480 / `8dd7c98653`. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> `max` bị outlier kéo (node mất mạng nhiều tuần) → lookback vô hạn, **mọi** run hàng ngày phải quét lại lịch sử, trả I/O/compute cho 99% traffic vốn đã đúng hạn. P99 phủ >99% late data với cửa sổ cố định; phần <1% còn lại để reconciliation định kỳ. Trong lab này max chỉ 2,94 ngày nên lookback 3 ngày vừa khớp P99 vừa phủ max — trên production hai mốc thường **không** trùng.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Từ 08-10 backend đổi `priority` số → chuỗi (có Slack). Pipeline **không dừng**. Classifier kém. `silver_tickets` có 6.606 hàng sai (NULL / ngoài 1..4). `quarantine_tickets` = 0 / 312. |
| **Nguyên nhân** | `try_cast(priority_raw as integer)` sai **hai hướng**: nhãn schema-evolution hợp lệ (`urgent/high/medium/low`) thành NULL nên mất tín hiệu nghiệp vụ; đồng thời `'0'/'5'/'-1'` vẫn vào Silver vì chúng đúng là số, dù contract chỉ cho 1..4. Contract `enforced: false` nên dbt không chặn kiểu. Chưa có test miền giá trị. Nếu lọc bản ghi hỏng **sau** `row_number()`, ticket nào bản CDC mới nhất bị lỗi sẽ **mất cả entity** khỏi Silver (12.480 → 12.168), dù lần update trước vẫn hợp lệ. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | (1) `'1'..'4'` — đúng contract cũ → giữ integer. (2) `'urgent'→1, 'high'→2, 'medium'→3, 'low'→4` — cùng ý nghĩa, khác biểu diễn → **map**, không quarantine. (3) `'0','5','-1','P1','unknown','',NULL` (312 bản ghi CDC) — dữ liệu hỏng → macro trả NULL → `quarantine_tickets`. |
| **Cách khắc phục** | `dbt/macros/normalize_priority.sql`: `CASE` xử lý 3 nhóm, NULL cho nhóm 3 — macro dùng chung Silver và quarantine để hai model không lệch. `silver_tickets.sql`: lọc `priority_clean is not null` **trước** `row_number()` (loại bản ghi, không loại ticket). `quarantine_tickets.sql`: `where normalize_priority(...) is null`. `schema.yml`: `contract.enforced: true` (ràng **kiểu** integer) + test `not_null` và `accepted_values: [1,2,3,4]` (ràng **miền**; contract một mình vẫn cho `priority=99` đi qua). |
| **Bằng chứng** | trước: quarantine 0, 6.606 hàng priority sai, 9/9 test · sau: **quarantine 312/312** (grain 1 hàng / 1 bản ghi CDC), `silver_tickets` đủ **12.480** ticket, phân bố `1:3134 · 2:3029 · 3:3115 · 4:3202`, 0 NULL, `dbt test` **11/11**. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> 1. **Chặn ở Silver, không chặn ở Bronze.** Bronze là log thô bất biến. Từ chối ở Bronze = mất bản gốc → không audit, không reprocess khi có mapper mới. Silver mới là nơi enforce contract.
> 2. **Không fail-fast cả DAG.** 312 hàng lỗi / 14.300 CDC (~2,2%) không có quyền chặn 31.200 chunk RAG và ~130.000 event hợp lệ. Quarantine (DLQ) cách ly hàng bẩn; luồng sạch chạy tiếp.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A và B |
| **Nguyên nhân** | **A:** 5.000 file Parquet nhỏ, không partition; `strftime(event_time,...)` bọc cột trong hàm nên engine không pruning theo path hay min/max row-group — Parquet không có B-tree index, chỉ layout đĩa mới giảm `rows scanned`. **B:** `commit()` offset **trước** `write_batch()` → crash = at-most-once (mất hàng). `INSERT` thuần khi replay = trùng hàng. Exactly-once không tồn tại ở tầng transport. |
| **Cách khắc phục** | **A:** `tools/compact.py` — `PARTITION_BY (event_date)` (query lọc 1 ngày, 14 giá trị; không partition 650 `customer_name` kẻo tái tạo small-file), `ORDER BY event_date, customer_name, event_time`, `ROW_GROUP_SIZE 1000` (mặc định 122.880 nhét cả ngày vào 1 group, pruning chết). `queries/dashboard.sql`: `hive_partitioning=1` và `event_date = '2026-08-09'` (sargable). **B:** `ingest/consumer.py` — ghi trước, commit sau (at-least-once); `event_id PRIMARY KEY` + `ON CONFLICT DO UPDATE` (không `DO NOTHING`: replay payload đã đổi thì phải lấy bản mới, không giữ bản cũ). |
| **Bằng chứng** | **A:** rows scanned 5.000.000 → **9.324** (536,3× ≥ 10×); files 5.000 → **14**; hash không đổi `4379e4c5d9f3`. **B:** `make crash-test` kill lô 7: offset còn **3.000** (không phải 3.500) → lô 7 đã ghi chưa commit; restart `C == A = 20.000`, 0 mất, 0 trùng → **ĐẠT**. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Incremental đã khai `unique_key` chưa, và strategy có khớp grain không (entity CDC → `merge`; aggregate theo ngày → `delete+insert`)? Chạy lại N lần ra cùng checksum không? |
| 2 | Đo P50/P95/**P99** của `_ingested_at - event_time` trước khi đặt lookback. Watermark theo `event_date` bỏ late data. Nới window phải kèm unique key. |
| 3 | Contract ràng kiểu + test ràng miền + quarantine/DLQ. Macro dùng chung Silver và hàng lỗi. Lọc bản ghi hỏng **trước** khi lấy bản mới nhất của entity. |
