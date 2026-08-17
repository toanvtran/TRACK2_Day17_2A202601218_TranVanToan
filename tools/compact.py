#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — NHIỆM VỤ 4.  CHƯA CÓ LOGIC.

Hiện trạng: `data/gold_events/` gồm 5.000 file, mỗi file vài chục KB, không
partition, thứ tự hàng ngẫu nhiên.

Yêu cầu: đọc toàn bộ dataset cũ, ghi ra dataset mới có layout hợp lý hơn, sau đó cập
nhật `queries/dashboard.sql` để trỏ vào dataset mới.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

KHUNG THỰC HIỆN

    COPY (
        SELECT *
        FROM   read_parquet('data/gold_events/*.parquet')
        ORDER  BY <cột A>, <cột B>
    ) TO 'data/gold_events_v2' (
        FORMAT          parquet,
        PARTITION_BY    (<cột partition>),
        OVERWRITE_OR_IGNORE,
        ROW_GROUP_SIZE  <?>
    )

Ba quyết định, mỗi quyết định cần một lý do viết được ra giấy:

  <cột partition>   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi
                    mở file. Thông tin đó đến từ đường dẫn. Vậy cột nào của
                    truy vấn dashboard nên xuất hiện trong tên thư mục? Cột đó
                    có bao nhiêu giá trị phân biệt — tức bao nhiêu thư mục?
                    Partition theo cột có 650 giá trị thì hệ quả là gì?

  <cột A>, <cột B>  Thứ tự hàng trong file quyết định thống kê min/max của mỗi
                    row group có ích hay vô dụng. Sắp thế nào để các hàng cùng
                    một khách hàng nằm liền nhau?

  ROW_GROUP_SIZE    Mặc định 122.880 hàng. Một ngày có khoảng bao nhiêu hàng?
                    Nếu cả ngày gói gọn trong MỘT row group thì min/max của
                    row group đó phủ những gì, và còn tác dụng lọc không?

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


def main() -> int:
    con = duckdb.connect()

    n_src = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src:,} file)")

    src_glob = str(SRC / "*.parquet").replace("\\", "/")
    dst_path = str(DST).replace("\\", "/")

    n_before = con.execute(
        f"select count(*) from read_parquet('{src_glob}')"
    ).fetchone()[0]

    # ------------------------------------------------------------------
    # Ba quyết định layout (xem docstring):
    #
    #   partition_by = (event_date)
    #       Dashboard lọc theo event_date. Cột này chỉ có 14 giá trị -> 14 thư
    #       mục, engine bỏ qua 13/14 dữ liệu TRƯỚC khi mở file chỉ nhờ đường
    #       dẫn. KHÔNG partition theo customer_name (650 giá trị) vì sẽ đẻ ra
    #       650 thư mục × 14 ngày = hàng nghìn file tí hon — tái lập đúng
    #       small-file problem đang cần xoá.
    #
    #   order by customer_name
    #       Điều kiện lọc thứ hai là customer_name. Sắp các hàng cùng khách
    #       hàng nằm liền nhau để thống kê min/max của mỗi row group hẹp lại,
    #       nhờ đó engine bỏ qua được các row group không chứa 'ACME'.
    #
    #   row_group_size = 2048
    #       Một ngày ~9.300 hàng. Mặc định 122.880 gói cả ngày vào MỘT row
    #       group -> min/max của customer_name trải khắp A..Z, vô dụng để lọc.
    #       Chia nhỏ để mỗi row group chỉ phủ một dải customer_name hẹp.
    # ------------------------------------------------------------------
    con.execute(f"""
        copy (
            select * from read_parquet('{src_glob}')
            order by event_date, customer_name
        ) to '{dst_path}' (
            format parquet,
            partition_by (event_date),
            overwrite_or_ignore,
            row_group_size 2048
        )
    """)

    n_after = con.execute(
        f"select count(*) from read_parquet('{dst_path}/**/*.parquet')"
    ).fetchone()[0]
    n_files = len(list(DST.glob("**/*.parquet")))

    assert n_before == n_after, f"mất hàng: {n_before:,} -> {n_after:,}"

    print(f"  đích  : {DST}  ({n_files:,} file, {n_after:,} hàng)")
    print(f"  kiểm tra: {n_before:,} == {n_after:,}  ✓ không mất hàng")
    print("\n  Xong. Nhớ trỏ queries/dashboard.sql vào dataset mới rồi chạy")
    print("  `make explain` để so với baseline.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
