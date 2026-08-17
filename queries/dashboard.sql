-- Dashboard "Sức khoẻ hội thoại theo khách hàng" của đội CSKH.
-- Người dùng chọn MỘT khách hàng và MỘT ngày, rồi bấm Load.
--
-- Ba tháng trước truy vấn này chạy 2 giây. Bây giờ 38 giây.
-- Không ai sửa dòng nào trong file này.
--
-- Bạn ĐƯỢC PHÉP viết lại truy vấn, miễn là kết quả trả về không đổi
-- (tools/explain.py kiểm tra điều đó bằng hash của kết quả).

select
    customer_name,
    count(*)                                        as n_events,
    count(distinct ticket_id)                       as n_tickets,
    round(avg(latency_ms), 1)                       as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int            as p95_latency_ms,
    sum(case when is_escalated then 1 else 0 end)   as n_escalated,
    sum(tokens_in + tokens_out)                     as tokens_total
-- Trỏ vào dataset đã nén (tools/compact.py), bật hive_partitioning để
-- DuckDB đọc event_date từ tên thư mục partition.
from read_parquet('data/gold_events_v2/**/*.parquet', hive_partitioning = true)
where customer_name = 'ACME'
  -- Predicate sargable: cột đứng một mình, so trực tiếp với giá trị DATE nên
  -- engine dùng được partition pruning + min/max của row group (khác với
  -- strftime(event_time, ...) bọc cột trong function làm hỏng cả hai).
  and event_date = DATE '2026-08-09'
group by 1
