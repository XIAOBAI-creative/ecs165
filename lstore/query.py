
from __future__ import annotations
from bisect import bisect_left, bisect_right
from typing import List, Optional
from lstore.table import Record, Table
class Query:
    """Query interface for Milestone 1 (all-in optimized).

    Key ideas:
      - latest data is stored column-wise (array('q'))
      - update touches only updated columns (no full-row copy)
      - sum uses per-column Fenwick tree over key-order
    """

    def __init__(self, table: Table):#当前table
        self.table = table
        self._num_cols = table.num_columns
        self._key_col = table.key #主键

    def delete(self, primary_key: int):
        try:
            k = int(primary_key)
            rid = self.table.key2rid.get(k)#主键找rid
            if rid is None:
                return False

            # remove key index entries
            self.table.index.delete_entry(self._key_col, k, rid)

            # mark row as deleted
            if 0 < rid < len(self.table.alive):
                self.table.alive[rid] = False
            return True
        except Exception:
            return False

    def insert(self, *columns):
        try:#匹配列数
            if len(columns) != self._num_cols:
                return False
            rid = self.table.alloc_base_rid()
            cols = self.table.cols
            # Write directly into columnar storage
            vals = [0] * self._num_cols
            for i in range(self._num_cols):
                v = int(columns[i])
                cols[i][rid] = v
                vals[i] = v
            # Version 0 snapshot
            self.table.versions[rid] = [vals]
            pk = int(columns[self._key_col])
            self.table.index.insert_entry(self._key_col, pk, rid)
            return True
        except Exception:
            return False

    def select(self, key: int, column: int, query_columns: List[int]) -> List[Record]:
        # "column" exists for API; only primary key lookups are expected for M1 tester.
        try:
            if int(column) != self._key_col:
                return []
            k = int(key)
            rid = self.table.key2rid.get(k)
            if rid is None or rid >= len(self.table.alive) or not self.table.alive[rid]:
                return []

            cols = self.table.cols
            # Match starter-code semantics: projected list has length = num_columns,
            # with None in columns that are not requested.
            projected = [None] * self._num_cols
            for i, flag in enumerate(query_columns):
                if flag:
                    projected[i] = int(cols[i][rid])

            return [Record(rid, k, projected)]
        except Exception:
            return []

    def update(self, key: int, *columns) -> bool:
        try:
            k = int(key)
            rid = self.table.key2rid.get(k)
            if rid is None or rid >= len(self.table.alive) or not self.table.alive[rid]:
                return False

            # columns may contain None for "no update"
            cols: List[Optional[int]] = list(columns)
            if len(cols) != self._num_cols:
                # Some starter tests pass full length; keep strict
                return False

            self.table.apply_update(rid, cols)
            return True
        except Exception:
            return False

    def sum(self, start_range: int, end_range: int, aggregate_column_index: int) -> int:
        try:
            return self.table.sum_range(int(start_range), int(end_range), int(aggregate_column_index))
        except Exception:
            return 0

    # Versioning APIs (Milestone 2/3); provide safe defaults
    def select_version(self, key: int, column: int, query_columns: List[int], relative_version: int) -> List[Record]:
            """Select a record at a specific *relative* version.

            Tester (m1_tester_new.py) semantics:
              - version 0  : latest (current) version
              - version -1 : previous version (1 step back). If it doesn't exist, return oldest.
              - version -2 : 2 steps back. If it doesn't exist, return oldest.
            """
            try:
                if int(column) != self._key_col:
                    return []
                k = int(key)
                rid = self.table.key2rid.get(k)
                if rid is None or rid >= len(self.table.alive) or not self.table.alive[rid]:
                    return []

                versions = self.table.versions[rid]
                if not versions:
                    return []

                rv = int(relative_version)

                # versions are stored oldest -> newest; latest is versions[-1]
                if rv == 0:
                    row = versions[-1]
                else:
                    # treat negative (and positive >0) as "steps back from latest"
                    steps_back = abs(rv) if rv != 0 else 0
                    # -1 means previous => steps_back=1 => idx = -2
                    idx = -1 - steps_back
                    if abs(idx) > len(versions):
                        row = versions[0]  # clamp to oldest
                    else:
                        row = versions[idx]

                out_cols: List[Optional[int]] = [None] * self._num_cols
                for i, take in enumerate(query_columns):
                    if take:
                        out_cols[i] = int(row[i])

                return [Record(rid, k, out_cols)]
            except Exception:
                return []
    def sum_version(self, start_range: int, end_range: int, aggregate_column_index: int, relative_version: int) -> int:
            """Aggregate (sum) over a key range at a specific relative version.
              0：latest (current) version
             -1：previous (1 step back); clamp to oldest if missing
             -2:2 steps back; clamp to oldest if missing
            """
            try:
                start_k = int(start_range)
                end_k = int(end_range)
                if start_k > end_k:
                    start_k, end_k = end_k, start_k
                c = int(aggregate_column_index)
                rv = int(relative_version)

                # Fast path: latest version uses BIT to be O(log n)
                if rv == 0:
                    return int(self.table.sum_range(start_k, end_k, c))#newest

                keys = self.table.sorted_keys
                rids = self.table.sorted_rids
                lo = bisect_left(keys, start_k)
                hi = bisect_right(keys, end_k)

                total = 0
                versions_all = self.table.versions

                steps_back = abs(rv)  # -1 => 1 step back; -2 => 2 steps back
                idx = -1 - steps_back  # from latest (-1)

                alive = self.table.alive
                for i in range(lo, hi):
                    rid = rids[i]
                    if rid >= len(alive) or not alive[rid]:
                        continue
                    vers = versions_all[rid]
                    if not vers:
                        continue
                    if abs(idx) > len(vers):
                        row = vers[0]  # clamp to oldest
                    else:
                        row = vers[idx]
                    total += int(row[c])
                return int(total)
            except Exception:
                return 0

    def increment(self, key: int, column: int) -> bool:
        # Provided for compatibility; treated as update(col += 1)
        try:
            k = int(key)
            rid = self.table.key2rid.get(k)
            if rid is None or rid >= len(self.table.alive) or not self.table.alive[rid]:
                return False
            col = int(column)
            val = int(self.table.cols[col][rid]) + 1
            cols = [None] * self._num_cols
            cols[col] = val
            self.table.apply_update(rid, cols)
            return True
        except Exception:
            return False
