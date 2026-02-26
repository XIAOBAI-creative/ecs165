from __future__ import annotations
from typing import List, Optional
from lstore.table import Record, Table


class Query:
    """Query interface for Milestone 2.

    Key change for Milestone 2 extended grading:
      - Do NOT call apply_all_merges_if_ready() on every query.
      - Merge is background; foreground should only do minimal work.
      - We optionally apply at most ONE ready merge on write paths.
    """

    def __init__(self, table: Table):
        self.table = table
        self._num_cols = table.num_columns
        self._key_col = table.key

    # -------------------------
    # Merge handling (lightweight)
    # -------------------------
    def _maybe_apply_one_merge(self) -> None:
        """
        Apply at most one merge result if ready.
        This avoids turning every query into a "front-ground merge".
        """
        try:
            with self.table._merge_lock:
                ready_ids = list(self.table._merge_results.keys())
            if not ready_ids:
                return
            self.table.apply_merge_if_ready(int(ready_ids[0]))
        except Exception:
            return

    # -------------------------
    # DELETE
    # -------------------------
    def delete(self, primary_key: int) -> bool:
        self._maybe_apply_one_merge()
        try:
            pk = int(primary_key)
            base_rid = self.table.key2rid.get(pk)
            if base_rid is None or self.table.is_deleted_rid(int(base_rid)):
                return False
            base_rid = int(base_rid)

            old_row = self.table.read_latest_user_columns(base_rid)

            self.table.mark_deleted(base_rid)

            for c in range(self._num_cols):
                if self.table.index.is_indexed(c):
                    self.table.index.delete_entry(c, int(old_row[c]), int(base_rid))

            return True
        except Exception:
            return False

    # -------------------------
    # INSERT
    # -------------------------
    def insert(self, *columns) -> bool:
        self._maybe_apply_one_merge()
        try:
            if len(columns) != self._num_cols:
                return False
            row = [int(x) for x in columns]

            base_rid = self.table.alloc_base_rid()
            self.table.write_base_record(base_rid, row)

            for c in range(self._num_cols):
                if self.table.index.is_indexed(c):
                    self.table.index.insert_entry(c, int(row[c]), int(base_rid))

            return True
        except Exception:
            return False

    # -------------------------
    # SELECT (latest)
    # -------------------------
    def select(self, key: int, column: int, query_columns: List[int]) -> List[Record]:
        # IMPORTANT: do NOT force-apply merges on read path
        try:
            search_col = int(column)
            search_val = int(key)

            # ======= NEW: Fast path for primary-key search =======
            if search_col == self._key_col:
                base_rid = self.table.key2rid.get(search_val)
                if base_rid is None:
                    return []
                base_rid = int(base_rid)
                if self.table.is_deleted_rid(base_rid):
                    return []

                latest = self.table.read_latest_user_columns(base_rid)

                projected: List[Optional[int]] = [None] * self._num_cols
                for i, take in enumerate(query_columns):
                    if take:
                        projected[i] = int(latest[i])

                pk = int(latest[self._key_col])
                return [Record(int(base_rid), pk, projected)]
            # =====================================================

            # get candidate base rids
            if self.table.index.is_indexed(search_col):
                rids = self.table.index.locate(search_col, search_val)  # List[int]
            else:
                rids = []
                for rid in self.table.all_base_rids():
                    if self.table.is_deleted_rid(rid):
                        continue
                    v = self.table.read_latest_user_value(rid, search_col)
                    if int(v) == search_val:
                        rids.append(int(rid))

            out: List[Record] = []
            for rid in rids:
                if self.table.is_deleted_rid(rid):
                    continue
                latest = self.table.read_latest_user_columns(int(rid))

                projected: List[Optional[int]] = [None] * self._num_cols
                for i, take in enumerate(query_columns):
                    if take:
                        projected[i] = int(latest[i])

                pk = int(latest[self._key_col])
                out.append(Record(int(rid), pk, projected))
            return out
        except Exception:
            return []

    # -------------------------
    # UPDATE
    # -------------------------
    def update(self, key: int, *columns) -> bool:
        self._maybe_apply_one_merge()
        try:
            if len(columns) <= self._key_col:
                return False
            if columns[self._key_col] is not None:
                return False

            pk = int(key)
            base_rid = self.table.key2rid.get(pk)
            if base_rid is None:
                return False
            base_rid = int(base_rid)
            if self.table.is_deleted_rid(base_rid):
                return False

            cols: List[Optional[int]] = list(columns)
            if len(cols) != self._num_cols:
                return False

            old_row = self.table.read_latest_user_columns(base_rid)

            self.table.apply_update(base_rid, cols)

            new_row = self.table.read_latest_user_columns(base_rid)

            self.table.index.update_entry(base_rid, old_row, new_row)

            self._maybe_apply_one_merge()

            return True
        except Exception:
            return False

    # -------------------------
    # SUM (latest)
    # -------------------------
    def sum(self, start_range: int, end_range: int, aggregate_column_index: int):
        try:
            start_k = int(start_range)
            end_k = int(end_range)
            if start_k > end_k:
                start_k, end_k = end_k, start_k
            c = int(aggregate_column_index)

            record_found = False
            total = 0
            for i in range(start_k, end_k + 1):
                record = self.select(i, self._key_col, [1] * self._num_cols)
                if not record:
                    continue
                record_found = True
                total += int(record[0].columns[c])
            if not record_found:
                return False
            return int(total)
        except Exception:
            return 0

    # -------------------------
    # SELECT VERSION
    # -------------------------
    def select_version(self, key: int, column: int, query_columns: List[int], relative_version: int) -> List[Record]:
        """
        Tester semantics:
          - version 0  : latest
          - version -1 : previous; clamp to oldest if missing
          - version -2 : 2 steps back; clamp to oldest if missing
        """
        try:
            search_col = int(column)
            search_val = int(key)
            steps_back = abs(int(relative_version))

            # ======= NEW: Fast path for primary-key versioned search =======
            if search_col == self._key_col:
                base_rid = self.table.key2rid.get(search_val)
                if base_rid is None:
                    return []
                base_rid = int(base_rid)
                if self.table.is_deleted_rid(base_rid):
                    return []

                cols = self.table.read_relative_user_columns(base_rid, steps_back)

                projected: List[Optional[int]] = [None] * self._num_cols
                for i, take in enumerate(query_columns):
                    if take:
                        projected[i] = int(cols[i])

                pk = int(cols[self._key_col])
                return [Record(int(base_rid), pk, projected)]
            # ===============================================================

            if self.table.index.is_indexed(search_col):
                rids = self.table.index.locate(search_col, search_val)
            else:
                rids = []
                for rid in self.table.all_base_rids():
                    if self.table.is_deleted_rid(rid):
                        continue
                    v = self.table.read_relative_user_value(rid, search_col, steps_back)
                    if int(v) == search_val:
                        rids.append(int(rid))

            out: List[Record] = []
            for rid in rids:
                if self.table.is_deleted_rid(rid):
                    continue

                cols = self.table.read_relative_user_columns(int(rid), steps_back)

                projected: List[Optional[int]] = [None] * self._num_cols
                for i, take in enumerate(query_columns):
                    if take:
                        projected[i] = int(cols[i])

                pk = int(cols[self._key_col])
                out.append(Record(int(rid), pk, projected))
            return out
        except Exception:
            return []

    # -------------------------
    # SUM VERSION
    # -------------------------
    def sum_version(self, start_range: int, end_range: int, aggregate_column_index: int, relative_version: int) -> int:
        try:
            start_k = int(start_range)
            end_k = int(end_range)
            if start_k > end_k:
                start_k, end_k = end_k, start_k
            c = int(aggregate_column_index)

            record_found = False
            total = 0
            for i in range(start_k, end_k + 1):
                record = self.select_version(i, self._key_col, [1] * self._num_cols, int(relative_version))
                if not record:
                    continue
                record_found = True
                total += int(record[0].columns[c])
            if not record_found:
                return False
            return int(total)
        except Exception:
            return 0

    # -------------------------
    # INCREMENT
    # -------------------------
    def increment(self, key: int, column: int) -> bool:
        self._maybe_apply_one_merge()
        try:
            k = int(key)
            base_rid = self.table.key2rid.get(k)
            if base_rid is None:
                return False
            base_rid = int(base_rid)
            if self.table.is_deleted_rid(base_rid):
                return False

            col = int(column)
            if col < 0 or col >= self._num_cols:
                return False

            old_row = self.table.read_latest_user_columns(base_rid)

            cur = int(self.table.read_latest_user_value(base_rid, col))
            cols = [None] * self._num_cols
            cols[col] = cur + 1

            self.table.apply_update(base_rid, cols)

            new_row = self.table.read_latest_user_columns(base_rid)
            self.table.index.update_entry(base_rid, old_row, new_row)

            self._maybe_apply_one_merge()

            return True
        except Exception:
            return False
