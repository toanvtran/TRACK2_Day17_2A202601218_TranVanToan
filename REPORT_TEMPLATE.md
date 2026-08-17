# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Trần Văn Toàn  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 28.3s
  run 2/3 … 27.4s
  run 3/3 … 27.8s

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
| **Triệu chứng** | Chạy pipeline hai lần liên tiếp, `gold_training_set` nhảy từ 12.480 lên gần gấp đôi (mỗi lượt cộng thêm ~12.480 hàng). Cùng một `ticket_id` xuất hiện nhiều lần dù nguồn `silver_tickets` chỉ giữ 1 hàng / 1 ticket. |
| **Nguyên nhân** | `gold_training_set` là incremental model nhưng **không khai báo `unique_key` và `incremental_strategy`**. Thiếu key, dbt sinh ra câu lệnh `INSERT` thuần; chạy lại cùng một partition ngày sẽ **ghi thêm** (append) thay vì ghi đè, nên hàng cũ tồn tại song song với hàng mới. Việc người trực bấm *Clear Task* trên Airflow (phiếu #1041), kết hợp `catchup=True` và không giới hạn `max_active_runs`, khiến cùng một ngày bị nạp lại nhiều lần → khuếch đại lỗi. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'delete+insert'` → mỗi lượt xoá partition theo key rồi ghi lại, thành idempotent. `dags/ai_training_pipeline.py`: đặt `catchup=False` và `max_active_runs=1` để giảm tần suất kích hoạt (không phải root cause). |
| **Bằng chứng** | trước: 26.270 hàng (sau 2 lượt) · sau: **12.480** hàng ổn định qua mọi lượt · checksum 3 lượt: `8dd7c98653` / `8dd7c98653` / `8dd7c98653` (không đổi) |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` chỉ có 8.645 hàng thay vì 9.100. Các cặp `(event_date, customer_id)` bị thiếu tập trung ở những **ngày cũ**, tương ứng các event tới kho muộn (`_ingested_at` lớn hơn `event_time` nhiều ngày). |
| **P99 độ trễ đo được** | **2.73 ngày** *(bắt buộc)* — (P50 = 0.13, P95 = 1.81, max = 2.94; ~5.05% event tới muộn hơn 1 ngày) |
| **Lookback đã chọn** | **3 ngày** — vì phải phủ trọn P99 (2.73 ngày) và cả max (2.94 ngày), làm tròn lên số nguyên an toàn là 3. |
| **Nguyên nhân** | Điều kiện lọc incremental cũ là `where event_date > (select max(event_date) from {{ this }})`. Một event có `event_date = 08-12` nhưng `_ingested_at = 08-15` sẽ không bao giờ lọt qua: hôm 08-15 `max(event_date)` trong đích đã là 08-14/08-15, nên `08-12 > max` là sai, và ngày sau `max` còn lớn hơn nữa → hàng đó bị bỏ vĩnh viễn. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: đổi điều kiện thành `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day` (lookback window bắt event về muộn), đồng thời thêm `unique_key = ['event_date','customer_id']` + `incremental_strategy = 'delete+insert'` để mỗi lần tính lại **thay thế** cặp cũ thay vì cộng dồn. |
| **Bằng chứng** | trước: 8.645 hàng · sau: **9.100** hàng · checksum ổn định `3db448685c` qua 3 lượt |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> P99 (2.73 ngày) bao phủ 99% dữ liệu về muộn mà không phải trả giá cho vài ngoại lệ cực đoan. Nếu căn theo `max`, gặp một event lạc tới muộn 30 ngày thì window phải mở 30 ngày, và **mọi lượt chạy về sau** đều phải tính lại 30 ngày dữ liệu — chi phí cố định vĩnh viễn để cứu một tỷ lệ rất nhỏ hàng. Ở đây P99 và max gần bằng nhau (2.73 vs 2.94) nên chọn 3 ngày phủ được cả hai; nhưng nguyên tắc chung là mỗi ngày lookback thêm là một khối dữ liệu phải quét + ghi lại ở TẤT CẢ các lượt sau, nên window nên bám P99 chứ không bám đuôi phân bố.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `silver_tickets.priority` xuất hiện rất nhiều `NULL`, kèm các giá trị ngoài miền hợp lệ như `0`, `5`, `-1` — trong khi contract quy định `priority ∈ 1..4`. Mốc bắt đầu lệch rơi vào 2026-08-10. |
| **Nguyên nhân** | Từ 08-10 team backend đổi cách biểu diễn: gửi **nhãn chữ** (`urgent/high/medium/low`) thay cho số (schema evolution). Macro cũ dùng `try_cast(... as integer)` sai theo hai hướng: (1) biến nhãn chữ hợp lệ thành `NULL`, (2) lại chấp nhận `0/5/-1` vì chúng đúng là số dù ngoài miền 1..4. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **Nhóm 1** — `'1'..'4'`: đã đúng contract → **giữ nguyên**. **Nhóm 2** — `urgent/high/medium/low`: chỉ đổi biểu diễn, ý nghĩa không đổi → **map về số** (urgent=1, high=2, medium=3, low=4). **Nhóm 3** — `P1/unknown/0/5/-1/''/NULL`: dữ liệu hỏng thật → **trả về NULL** (tín hiệu để quarantine nhặt ra). |
| **Cách khắc phục** | `dbt/macros/normalize_priority.sql`: thay `try_cast` bằng khối `CASE` xử lý đủ ba nhóm. `dbt/models/silver/silver_tickets.sql`: **lọc bỏ bản ghi CDC hỏng TRƯỚC khi `row_number()`** (lọc trước, xếp hạng sau) để không đánh rớt cả ticket khi bản ghi mới nhất bị hỏng — ticket vẫn giữ trạng thái hợp lệ trước đó. `dbt/models/silver/quarantine_tickets.sql`: `where normalize_priority(priority_raw) is null` (dùng chung macro nên không lệch nhau). `dbt/models/silver/schema.yml`: bật `contract.enforced: true` và thêm test `not_null` + `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = **312** hàng (đúng kỳ vọng) · `dbt test` **11/11** pass · `silver_tickets` giữ đủ 12.480 ticket, priority ∈ 1..4 không NULL |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> Nên chuẩn hoá/loại lỗi ở tầng **Silver**, không ở Bronze. Bronze phải giữ nguyên payload gốc để còn điều tra sự cố về sau: nếu Bronze từ chối luôn bản ghi lỗi thì khi cần truy vết "backend đã gửi gì, từ lúc nào" ta không còn bằng chứng — mất khả năng phân biệt nhóm 2 (đổi format, cần cứu) với nhóm 3 (hỏng thật). Không để `dbt test` fail và dừng cả DAG vì đây là bài toán quy mô: chỉ 312 bản ghi hỏng không có quyền chặn 130.000+ event và 31.200 chunk hoàn toàn bình thường đến tay người dùng. Bản ghi lỗi được tách vào `quarantine_tickets` như một hàng đợi cho người trực xử lý, còn pipeline vẫn chạy tiếp phục vụ phần dữ liệu tốt.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A — Query dashboard chậm |
| **Triệu chứng** | Query dashboard chạy 38 giây (trước đây 2 giây), dù không ai sửa dòng SQL nào. |
| **Nguyên nhân** | Hai vấn đề cộng lại: (1) **small-file problem** — `data/gold_events/` có 5.000 file tí hon, không partition; DuckDB làm tròn công quét lên theo từng file nên `rows scanned` = 5.000.000 cho tập chỉ ~130.000 hàng. (2) **Predicate không sargable** — điều kiện `strftime(event_time,'%Y-%m-%d') = '2026-08-09'` bọc cột trong function, engine không so được với tên thư mục partition lẫn min/max của row group nên buộc phải quét toàn bộ. |
| **Cách khắc phục** | `tools/compact.py`: `COPY ... TO 'data/gold_events_v2'` với `partition_by (event_date)` (14 giá trị → 14 thư mục, KHÔNG partition theo `customer_name` vì 650 giá trị sẽ tái lập small-file problem), `order by event_date, customer_name` (gom hàng cùng khách hàng để min/max row group hẹp lại), `row_group_size 2048` (một ngày ~9.300 hàng, mặc định 122.880 gói cả ngày vào 1 row group làm min/max vô dụng). `queries/dashboard.sql`: trỏ vào dataset mới với `hive_partitioning = true`, viết lại filter thành `event_date = DATE '2026-08-09'` (sargable). |
| **Bằng chứng** | `rows scanned`: 5.000.000 → **9.324** (giảm **536.3×**, cần ≥ 10×) · `files`: 5.000 → **14** · `result hash`: `4379e4c5d9f3` **không đổi** (ngữ nghĩa query được bảo toàn) |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Với mọi incremental model: đã khai báo `unique_key` + `incremental_strategy` chưa? Chạy lại cùng một partition hai lần có ra cùng kết quả (idempotent) không? |
| 2 | Đo phân bố độ trễ `(_ingested_at - event_time)` của nguồn; điều kiện lọc incremental có lookback window đủ phủ P99 để không bỏ sót dữ liệu về muộn không? |
| 3 | Nguồn có bị schema evolution (đổi kiểu/format giữa chừng) không? Contract ràng buộc kiểu + test ràng buộc miền giá trị đã bật đủ chưa, và dữ liệu lỗi có được quarantine thay vì làm dừng cả pipeline không? |
