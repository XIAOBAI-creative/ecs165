from __future__ import annotations
from typing import List, Optional
from lstore.table import Record, Table
from lstore.lock_manager import LockConflict


class Query:
    """
    Entry point for all database operations -- insert, delete, update, select, etc.
    In Milestone 2, merge runs in the background. We don't trigger a full merge on
    every query; write ops just try to apply one ready merge result along the way.
    """

    def __init__(self, table: Table):
        self.table = table
        self._num_cols = table.num_columns
        self._key_col = table.key

    def _acquire_shared_if_needed(self, txn, rid: int) -> bool:
        """Try to grab a shared lock for reads. If we can't get it, there's a conflict."""
        if txn is None:
            return True
        try:
            lm = getattr(txn, "lm", None)
            txn_id = getattr(txn, "txn_id", None)
            if lm is None or txn_id is None:
                return True
            lm.acquire_S(int(txn_id), int(rid))
            return True
        except LockConflict:
            return False
        except Exception:
            return False

    # ---- DELETE ----

    def delete(self, primary_key: int) -> bool:
        """Delete a record by primary key. Also cleans up index entries."""
        try:
            pk = int(primary_key)
            base_rid = self.table.key2rid.get(pk)
            if base_rid is None:
                return False
            base_rid = int(base_rid)
            if self.table.is_deleted_rid(base_rid):
                return False
            
            # grab old values first -- we need them to remove index entries
            old_row = self.table.read_latest_user_columns(base_rid)

            # mark it as deleted
            self.table.mark_deleted(base_rid)

            # clean up index entries for every indexed column
            for c in range(self._num_cols):
                if self.table.index.is_indexed(c):
                    self.table.index.delete_entry(c, int(old_row[c]), base_rid)
            if self.table.key2rid.get(pk) == base_rid:
                del self.table.key2rid[pk]

            return True
        except Exception:
            return False

    # ---- INSERT ----

    def insert(self, *columns) -> bool:
        """Insert a new record. Column count must match, and no None values allowed."""
        try:
            if len(columns) != self._num_cols:
                return False
    
            # NULLs not allowed on insert
            if any(v is None for v in columns):
                return False
    
            row = [int(x) for x in columns]
    
            pk = int(row[self._key_col])
            existing = self.table.key2rid.get(pk)
            if existing is not None and not self.table.is_deleted_rid(int(existing)):
                return False
    
            base_rid = self.table.alloc_base_rid()
            self.table.write_base_record(base_rid, row)
    
            # register in every index that exists
            for c in range(self._num_cols):
                if self.table.index.is_indexed(c):
                    self.table.index.insert_entry(c, int(row[c]), int(base_rid))
    
            return True
        except Exception:
            return False

    # ---- SELECT (latest version) ----

    def select(self, key: int, column: int, query_columns: List[int], txn=None):
        """
        Find records where the given column equals key, return latest version.
        query_columns is a bitmask for which columns to include in the result.
        """
        try:
            search_col = int(column)
            search_val = int(key)

            # find matching base rids
            if self.table.index.is_indexed(search_col):
                # index exists, nice and fast
                rids = self.table.index.locate(search_col, search_val)
            else:
                # no index, gotta do a full scan
                rids = []
                for rid in self.table.all_base_rids():
                    rid = int(rid)
                    if self.table.is_deleted_rid(rid):
                        continue
                    # need to lock before reading during scan
                    if not self._acquire_shared_if_needed(txn, rid):
                        return False
                    v = self.table.read_latest_user_value(rid, search_col)
                    if int(v) == search_val:
                        rids.append(rid)

            # now build the result set from the matching rids
            out: List[Record] = []
            for rid in rids:
                rid = int(rid)
                if self.table.is_deleted_rid(rid):
                    continue
                # lock before reading
                if not self._acquire_shared_if_needed(txn, rid):
                    return False
                latest = self.table.read_latest_user_columns(rid)

                # project -- only keep the columns the caller asked for
                projected: List[Optional[int]] = [None] * self._num_cols
                for i, take in enumerate(query_columns):
                    if take:
                        projected[i] = int(latest[i])

                pk = int(latest[self._key_col])
                out.append(Record(rid, pk, projected))
            return out
        except Exception:
            if txn is not None:
                return False
            return []

    # ---- UPDATE ----

    def update(self, key: int, *columns) -> bool:
        """Update a record. None means 'don't change this column'. Can't change the primary key."""
        try:
            if len(columns) != self._num_cols:
                return False

            # can't change the primary key
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

            # save old values -- need them to update index later
            old_row = self.table.read_latest_user_columns(base_rid)

            # write a tail record and update base metadata pointers
            self.table.apply_update(base_rid, cols, prev_latest=old_row)

            # read back the new values and move index entries accordingly
            new_row = list(old_row)
            for i, v in enumerate(cols):
                if v is not None:
                    new_row[i] = int(v)
            self.table.index.update_entry(base_rid, old_row, new_row)

            return True
        except Exception:
            return False

    # ---- SUM (latest version) ----

    def sum(self, start_range: int, end_range: int, aggregate_column_index: int, txn=None):
        """Sum up a column for all records whose primary key falls in [start, end]."""
        try:
            start_k = int(start_range)
            end_k = int(end_range)
            if start_k > end_k:
                start_k, end_k = end_k, start_k
            c = int(aggregate_column_index)

            total = 0
            record_found = False

            # if there's an index on the key column, use range lookup -- way faster
            if self.table.index.is_indexed(self._key_col):
                rids = self.table.index.locate_range(start_k, end_k, self._key_col)
                for rid in rids:
                    rid = int(rid)
                    if self.table.is_deleted_rid(rid):
                        continue
                    if not self._acquire_shared_if_needed(txn, rid):
                        return False
                    total += int(self.table.read_latest_user_value(rid, c))
                    record_found = True
                return int(total) if record_found else False

            # no index, fall back to scanning everything
            for rid in self.table.all_base_rids():
                rid = int(rid)
                if self.table.is_deleted_rid(rid):
                    continue
                if not self._acquire_shared_if_needed(txn, rid):
                    return False
                pk = int(self.table.read_latest_user_value(rid, self._key_col))
                if start_k <= pk <= end_k:
                    total += int(self.table.read_latest_user_value(rid, c))
                    record_found = True

            return int(total) if record_found else False
        except Exception:
            if txn is not None:
                return False
            return 0

    # ---- SELECT VERSION (read a past version) ----

    def select_version(self, key: int, column: int, query_columns: List[int], relative_version: int):
        """Same idea as select, but goes back in history. relative_version says how many steps back."""
        try:
            search_col = int(column)
            search_val = int(key)
            steps_back = abs(int(relative_version))

            if self.table.index.is_indexed(search_col):
                rids = self.table.index.locate(search_col, search_val)
            else:
                # no index -- scan and match against the historical value
                rids = []
                for rid in self.table.all_base_rids():
                    rid = int(rid)
                    if self.table.is_deleted_rid(rid):
                        continue
                    v = self.table.read_relative_user_value(rid, search_col, steps_back)
                    if int(v) == search_val:
                        rids.append(rid)

            out: List[Record] = []
            for rid in rids:
                rid = int(rid)
                if self.table.is_deleted_rid(rid):
                    continue

                # read all columns at that version
                cols = self.table.read_relative_user_columns(rid, steps_back)

                projected: List[Optional[int]] = [None] * self._num_cols
                for i, take in enumerate(query_columns):
                    if take:
                        projected[i] = int(cols[i])

                pk = int(cols[self._key_col])
                out.append(Record(rid, pk, projected))
            return out
        except Exception:
            return []

    # ---- SUM VERSION (sum over a past version) ----

    def sum_version(self, start_range: int, end_range: int, aggregate_column_index: int, relative_version: int) -> int:
        """Like sum but uses historical values instead of the latest."""
        try:
            start_k = int(start_range)
            end_k = int(end_range)
            if start_k > end_k:
                start_k, end_k = end_k, start_k
            c = int(aggregate_column_index)

            record_found = False
            total = 0
            # go through each key in the range and look up its historical version
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

    # ---- INCREMENT ----

    def increment(self, key: int, column: int) -> bool:
        """Bump a column's value by 1. Basically just a shortcut for update."""
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

            # read current value, add 1, write it back as a normal update
            cur = int(old_row[col])
            cols = [None] * self._num_cols
            cols[col] = cur + 1

            self.table.apply_update(base_rid, cols, prev_latest=old_row)

            new_row = list(old_row)
            new_row[col] = int(cur + 1)
            self.table.index.update_entry(base_rid, old_row, new_row)

            return True
        except Exception:
            return False
