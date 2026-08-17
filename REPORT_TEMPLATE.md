# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Tiến  **Mã SV:** 2A202601655  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## Tech stack và vì sao chọn từng thành phần

Lab mô phỏng đường ống **Bronze → Silver → Gold** của nền tảng AI hỗ trợ khách hàng. Hạ tầng được rút gọn (không Docker, không cloud) nhưng giữ nguyên khái niệm vận hành: CDC, offset, contract, incremental transform, late data, idempotent write.

| Tầng | Công nghệ | Vai trò trong lab | Vì sao dùng cái này, không dùng cái khác |
|---|---|---|---|
| Kho dữ liệu | **DuckDB** | Warehouse local: Bronze/Silver/Gold, đọc/ghi Parquet, `MERGE`, `INSERT ON CONFLICT` | DuckDB là OLAP in-process, đủ để tái hiện hành vi của Snowflake/BigQuery (incremental merge, predicate pushdown trên Parquet) mà không cần tài khoản cloud. OLTP (Postgres) không phù hợp cho aggregate Gold và quét Parquet. |
| Transform | **dbt-duckdb** | Mỗi file `.sql` = một model; `ref()` tạo lineage; `config()` quyết định cách ghi | dbt tách *logic nghiệp vụ* khỏi *cách materialize*. Lỗi lab nằm ở `incremental` / `unique_key` / `contract` — đúng bề mặt mà data engineer dùng trên production. Script SQL thuần không có contract, test runner, hay `is_incremental()`. |
| Điều phối | **Airflow DAG** (chỉ đọc bằng AST, không chạy) | `catchup`, `max_active_runs` — hệ quả của Clear Task | Phiếu #1041 là sự cố scheduler. Sửa model mới hết nhân bản; sửa DAG chỉ giảm tần suất kích hoạt lỗi khi retry/backfill. |
| Nguồn Bronze | JSONL giả lập **Postgres CDC / S3 / Kafka** | `tickets_cdc.jsonl` (`op = c/u/d`), `transcripts.jsonl`, `events.jsonl` + commit log trên đĩa | Giữ đúng semantics (CDC update, object storage, offset tách khỏi ghi) mà không dựng Kafka/Postgres. |
| Bài A | **Parquet + Hive partitioning** | Layout đĩa cho dashboard | Parquet không có B-tree index. Thứ duy nhất engine dùng để bỏ file/row-group là *đường dẫn partition* và *min/max stats*. Index SQL không giải quyết small-file problem. |
| Bài B | **Commit log + offset file** (`ingest/log_client.py`) | Kafka-lite: `poll()` / `commit()` | Exactly-once không tồn tại ở tầng vận chuyển. Lab buộc chọn at-least-once + ghi idempotent — đúng trade-off của consumer thật. |

**Hai chiến lược incremental dùng trong lab — không thể hoán đổi:**

| Model | Grain | Strategy | Vì sao |
|---|---|---|---|
| `gold_training_set` | 1 hàng / 1 **entity** `ticket_id` | **`merge`** theo `ticket_id` | CDC có `op='u'`: ticket tạo ngày D1, sửa ngày D2 đi qua *hai partition ngày* trong cùng một lượt. `delete+insert` theo ngày chỉ xoá partition đang chạy, không cập nhật bản ghi entity đã nằm ở ngày khác. `append` thì nhân bản. `merge` upsert đúng khoá tự nhiên. |
| `gold_feature_daily` | 1 hàng / cặp **`(event_date, customer_id)`** | **`delete+insert`** theo 2 cột | Đây là bảng *aggregate theo ngày*. Khi lookback tính lại ngày cũ, cả hàng aggregate phải được thay nguyên. Xoá các key trong cửa sổ rồi insert lại rẻ và đúng hơn merge từng cột. |

---

## 0 · Kết quả `make verify`

<details open>
<summary>Output ba lần chạy của make verify (chạy lại 2026-08-17)</summary>

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

Tổng kết: **4 / 4 tiêu chí đạt**. Extra B (`make crash-test`) chạy riêng: **ĐẠT** (20.000 / 20.000, không mất, không trùng).

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Clear Task / chạy lại pipeline: `gold_training_set` phình ra (12.480 → 25.615 → 38.750), cột ỔN ĐỊNH ✗, nhiều `ticket_id` lặp. |
| **Nguyên nhân** | Model `incremental` **không khai `unique_key`**. dbt khi đó sinh `INSERT` append-only. Retry cùng partition = ghi *thêm* hàng, không ghi *đè*. CDC `op='u'` (1.310 bản ghi) làm cùng một ticket đi qua `WHERE run_date` nhiều lần → nhân bản ngay trong một lượt. `catchup=True` + `max_active_runs` không giới hạn chỉ *làm lỗi xảy ra dày hơn* khi Clear Task; chúng không phải root cause. |
| **Cách khắc phục** | `gold_training_set.sql`: `unique_key='ticket_id'`, `incremental_strategy='merge'`. DAG: `catchup=False`, `max_active_runs=1`. |
| **Vì sao dùng `merge` + `ticket_id`** | Grain là **entity** (1 ticket = 1 hàng training). Khoá tự nhiên là `ticket_id`, không phải ngày. `append` nhân bản; `delete+insert` theo ngày bỏ sót update xuyên partition. `merge` upsert đúng bản ghi mới nhất. DAG chỉ là lớp bảo vệ scheduler, không thay được ghi idempotent ở model. |

**Evidence**

| Chỉ số | Trước sửa | Sau sửa | Nguồn |
|---|---|---|---|
| Số hàng `gold_training_set` | 38.750 (thừa 26.270) | **12.480** = expected | `make verify`, `expected/gold_training_set.count` |
| Ticket bị lặp | 12.480 | **0** (1 hàng / 1 ticket) | dòng kiểm tra `gold_training_set: 1 hàng / 1 ticket` |
| Checksum 3 lượt | khác nhau (ỔN ĐỊNH ✗) | `8dd7c98653` × 3 | bảng CHECKSUM |
| DAG `catchup` / `max_active_runs` | `True` / `None` | **`False` / `1`** | `tools/check_dag.py` đọc AST |
| `gold_doc_chunks` (đối chứng) | 31.200, ổn định | 31.200, `92d8e50131` × 3 | chứng minh sửa merge không phá model khác |

CDC nguồn đo được: 14.300 bản ghi (`c=12.735`, `u=1.310`, `d=255`) — đúng loại dữ liệu buộc phải `merge` theo entity, không append.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` thiếu ~5% (8.645 / 9.100). Thiếu ở ngày cũ; ngày mới đủ. |
| **P99 độ trễ đo được** | **2.7258 ngày** (~65,4 giờ) |
| **Lookback đã chọn** | **3 ngày** — P99 = 2,73 và max = 2,94 nên `interval 3 day` phủ 100% late data của dataset này. |
| **Nguyên nhân** | `where event_date > max(event_date)` chỉ nhận *ngày mới hơn watermark*. Event xảy ra 08-12 nhưng tới kho 08-15: lúc chạy 08-15 watermark đã là 08-14 → hàng 08-12 bị bỏ *vĩnh viễn*. Đổi `>` thành `>=` chỉ nới thêm 1 ngày, không đủ với P99 ≈ 2,7 ngày. |
| **Cách khắc phục** | Lookback `event_date >= max(event_date) - interval 3 day`. `unique_key=['event_date','customer_id']` + `delete+insert` để tính lại không nhân bản. |
| **Vì sao lookback 3 ngày + `delete+insert`** | Cửa sổ phải theo **P99 đo được**, không theo cảm tính. `delete+insert` vì grain là aggregate `(ngày, khách)`: tính lại một ngày phải *thay cả hàng*, không merge từng metric. Nếu chỉ nới window mà giữ `append` thì tái tạo đúng lỗi nhiệm vụ 1 trên bảng khác. |

**Evidence — phân bố độ trễ** (`bronze_events`, `_ingested_at - event_time`)

| Mốc | Giá trị đo |
|---|---|
| P50 | 0,1281 ngày (~3,1 giờ) — cụm đúng hạn |
| P95 | 1,8137 ngày |
| **P99** | **2,7258 ngày** |
| max | 2,9447 ngày |
| Tỷ lệ tới muộn > 1 ngày | 5,05% ≈ đúng phần hàng bị thiếu (455 / 9.100) |

Hai cụm tách: 0–6 giờ (đúng hạn) và 43–71 giờ (late). Lookback 3 ngày phủ max 2,94 ngày trên *dataset này*.

**Evidence — số hàng**

| Chỉ số | Trước | Sau |
|---|---|---|
| `gold_feature_daily` | 8.645 (thiếu 455) | **9.100** = 14 ngày × 650 khách |
| Checksum 3 lượt | ổn định nhưng *sai* | `3db448685c` × 3 và đúng số hàng |
| `gold_training_set` (không được phá) | 12.480 | vẫn 12.480, `8dd7c98653` |

Vì sao căn P99 chứ không neo cứng `max` trên production? `max` bị outlier (mất mạng nhiều tuần) kéo lookback vô hạn → mỗi run ngày nào cũng quét lại lịch sử, trả phí I/O/compute cho 99% traffic vốn đã đúng hạn. P99 cân bằng coverage và chi phí; phần <1% còn lại để reconciliation định kỳ. *Trong lab này* max chỉ 2,94 ngày nên 3 ngày vừa khớp P99 vừa phủ max — không phải lúc nào cũng trùng như vậy.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Từ 08-10 backend đổi `priority` số → chuỗi. Pipeline không dừng. Classifier kém. `silver_tickets` có 6.606 hàng sai (NULL / ngoài 1..4). `quarantine_tickets` = 0 / 312. |
| **Nguyên nhân** | `try_cast` sai hai hướng: nhãn hợp lệ `urgent/high/medium/low` thành NULL, trong khi `'0'/'5'/'-1'` vẫn vào vì chúng là số. Contract `enforced: false`. Chưa test miền 1..4. Lọc *sau* `row_number()` sẽ đánh rơi cả ticket khi bản ghi mới nhất hỏng. |
| **Ba nhóm `priority`** | (1) `'1'..'4'` → giữ integer. (2) `'urgent'→1, 'high'→2, 'medium'→3, 'low'→4` — schema evolution, *map không quarantine*. (3) `'0','5','-1','P1','unknown','',NULL` (312) → NULL → quarantine. |
| **Cách khắc phục** | Macro `CASE` dùng chung Silver + quarantine. Lọc `priority_clean is not null` **trước** `row_number()`. `where normalize_priority(...) is null` ở quarantine. `contract.enforced: true` + test `not_null` + `accepted_values: [1,2,3,4]`. |
| **Vì sao stack này (macro + contract + test + quarantine)** | **Macro** = một nguồn sự thật: Silver loại đúng những gì quarantine nhận, không lệch. **Contract** ràng *kiểu* (integer); một mình contract vẫn cho `priority=99` đi qua. **Test** ràng *miền* 1..4. **Quarantine / DLQ** tách 312 hàng lỗi (~2,2% CDC) để 12.480 ticket hợp lệ và toàn bộ event/chunk vẫn xuống Gold — pipeline không fail-fast. |

**Evidence**

| Chỉ số | Trước | Sau | Ý nghĩa |
|---|---|---|---|
| `quarantine_tickets` | 0 | **312 / 312**, grain 1 hàng / 1 bản ghi CDC (0 key trùng) | Đúng nhóm 3, không nhầm nhóm 2 |
| `silver_tickets` | priority bẩn, 6.606 hàng sai | **12.480** ticket; phân bố `1: 3.134 · 2: 3.029 · 3: 3.115 · 4: 3.202`; 0 NULL, 0 ngoài 1..4 | Loại *bản ghi* hỏng, không loại *ticket* |
| `dbt test` | 9/9 (bản gốc) | **11/11** (thêm `not_null` + `accepted_values` trên `priority`) | Contract + miền giá trị |
| `gold_training_set` | phụ thuộc Silver bẩn | vẫn **12.480**, không lặp | Downstream không mất entity |

Nếu quarantine nhóm 2 như nhóm 3: bảng lỗi sẽ lên hàng nghìn hàng (rubric trừ điểm khi > 1.000) và mất dữ liệu hợp lệ chỉ vì backend đổi format.

Câu hỏi thiết kế: nên chặn ở Bronze hay Silver? Vì sao không dừng pipeline?

> 1. **Chặn ở Silver, không chặn ở Bronze.** Bronze là log thô bất biến. Từ chối ở Bronze là mất evidence, không audit được, không reprocess khi có mapper mới. Silver mới là nơi enforce contract.
> 2. **Không fail-fast cả DAG.** 312 hàng lỗi / 14.300 CDC (~2,2%) không có quyền chặn 31.200 chunk RAG, ~130.000 event và dashboard CSKH. Quarantine = DLQ: luồng sạch chạy tiếp, người trực xử lý hàng bẩn sau.

---

## 4 · *(mở rộng)* EXTRA.md — bài A và bài B

| | |
|---|---|
| **Bài đã làm** | Cả **A** (dashboard Parquet) và **B** (consumer crash) |

### Bài A — small-file + partition pruning

**Nguyên nhân.** 5.000 file Parquet nhỏ, không partition → engine mở hết file. `strftime(event_time, '%Y-%m-%d') = '...'` bọc cột trong hàm → không sargable, không dùng được Hive partition hay min/max row group.

**Vì sao layout này, không phải index.** Parquet trên đĩa không có index. Ba đòn bẩy duy nhất: (1) **partition `event_date`** — query lọc một ngày; 14 giá trị = 14 thư mục. Partition theo `customer_name` (650 giá trị) sẽ tạo hàng trăm folder nhỏ, tái tạo small-file problem. (2) **`ORDER BY event_date, customer_name, event_time`** — hàng cùng khách nằm kề, min/max row group có ích khi lọc `customer_name='ACME'`. (3) **`ROW_GROUP_SIZE 1000`** — mặc định ~122.880 sẽ nhét cả ngày vào một row group, min/max `customer_name` phủ mọi khách, pruning chết. Query viết lại `event_date = '2026-08-09'` (cột đứng một mình) + `hive_partitioning=1`.

**Evidence A** (`make explain` / dòng dashboard trong `make verify`)

| Chỉ số | Baseline | Sau compact | Tiêu chí |
|---|---|---|---|
| rows scanned | 5.000.000 | **9.324** | giảm **536,3×** (≥ 10×) |
| số file | 5.000 | **14** | hàng chục, đúng 14 ngày |
| result hash | `4379e4c5d9f3` | `4379e4c5d9f3` | ngữ nghĩa query không đổi |

### Bài B — at-least-once + idempotent write

**Nguyên nhân.** Cũ: `commit()` → crash → `write_batch()`. Offset đã tiến, dữ liệu chưa ghi → **at-most-once = mất hàng**. `INSERT` thuần khi replay → trùng hàng.

**Vì sao at-least-once + `ON CONFLICT DO UPDATE`, không phải exactly-once.** Exactly-once không có ở tầng transport. Chọn **ghi trước, commit offset sau** (at-least-once): crash tại `maybe_crash()` thì batch đã vào DuckDB, offset chưa dịch, restart đọc lại batch đó. Phải có ghi idempotent nếu không `INSERT` sẽ nhân bản.

`event_id PRIMARY KEY` + `INSERT ... ON CONFLICT (event_id) DO UPDATE`: DuckDB chỉ cho `ON CONFLICT` khi có PK/UNIQUE.

**`DO UPDATE` khác `DO NOTHING` khi message được replay với nội dung đã đổi:** `DO NOTHING` giữ bản cũ → consumer bỏ qua payload mới hơn (sai nếu producer retry bản sửa). `DO UPDATE` lấy `excluded.*` → hàng luôn là phiên bản mới nhất. Lab chọn `DO UPDATE` vì event/CDC có thể được gửi lại với field đã sửa (`latency_ms`, `event_type`, …).

**Evidence B** (`make crash-test`, giết ở lô 7, batch 500, 20.000 message)

| Lượt | Quan sát | Kết luận |
|---|---|---|
| A — chạy một mạch | 20.000 hàng / 20.000 `event_id` | baseline |
| B — kill lô 7 | exit 137; **offset đã commit = 3.000** (không phải 3.500) | 6 lô đầu đã commit; lô 7 đã ghi nhưng *chưa* commit → đúng thứ tự ghi-trước |
| C — restart | ghi thêm 17.000 (replay 500 + 16.500 còn lại); cuối **20.000 / 20.000** | replay không mất, không trùng |
| Đối chiếu | `C == A`, lost = 0, dup = 0 | **BÀI MỞ RỘNG B: ĐẠT ✓** |

Nếu vẫn commit trước khi ghi, offset sau crash sẽ là 3.500 và C thiếu 500 hàng.

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận hệ thống chưa quen, kiểm tra điều này trước |
|---|---|
| 1 | Incremental đã có `unique_key` + strategy khớp *grain* chưa (entity → `merge`, partition aggregate → `delete+insert`)? Scheduler có `catchup=False`, `max_active_runs=1` không? Chạy lại N lần ra cùng checksum không? |
| 2 | Đo P50/P95/**P99** của `_ingested_at - event_time` trước khi đặt lookback. Watermark `event_date > max(...)` bỏ late data. Nới window phải kèm unique key kẻo nhân bản. |
| 3 | Contract *kiểu* + test *miền giá trị* + quarantine/DLQ. Macro dùng chung Silver và hàng lỗi. Lọc bản ghi hỏng trước khi lấy bản mới nhất của entity. |
