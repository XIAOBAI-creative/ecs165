"""
A data strucutre holding indices for various columns of a table.
Key column should be indexed by default, other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from typing import List


class Index:
    def __init__(self, table):
        self.table = table
        # 注：M1中我们只支持单列索引
        # 因此这里的indices列表长度为table.num_columns
        # 但实际上只有一个元素会被使用
        # （即：索引只会建立在主键列上）
        self.indices = [None] * table.num_columns

    # 通过column和value定位记录，使用dict快速定位单个主键列的记录
    def locate(self, column: int, value: int) -> List[int]:
        if int(column) != self.table.key:
            return []
        # key2rid是一种使用hash表实现的索引结构，
        # 能够在O(1)内根据主键值找到对应的记录ID（RID）
        rid = self.table.key2rid.get(int(value))
        return [] if rid is None else [rid]

    # key2rid的hash方式不适用于范围查询，
    # 因此我们需要通过sorted_keys来获取范围内的记录ID
    def locate_range(self, begin: int, end: int, column: int) -> List[int]:
        keys = self.table.sorted_keys
        # 二分法的方式找begin和end更快速
        lo = bisect_left(keys, int(begin))
        hi = bisect_right(keys, int(end))
        rids = self.table.sorted_rids
        return [rids[i] for i in range(lo, hi)]

    # optional: Create index on specific column (no-op for M1)
    def create_index(self, column_number: int):
        return

    # optional: Drop index of specific column (no-op for M1)
    def drop_index(self, column_number: int):
        return

    # -----------------------------
    # Wrappers expected by Query
    # -----------------------------

    # 将列的某个值（key）映射到它对应的数据记录位置（RID）
    def insert_entry(self, column: int, value: int, rid: int) -> None:
        if int(column) != self.table.key:
            return
        key = int(value)
        rid = int(rid)
        self.table.key2rid[key] = rid
        self.table.register_key(key, rid)


    # 将列的某个值（key）取消映射到对应的数据记录（RID）
    def delete_entry(self, column: int, value: int, rid: int) -> None:
        if int(column) != self.table.key:
            return
        key = int(value)
        self.table.key2rid.pop(key, None)
        self.table.unregister_key(key)
