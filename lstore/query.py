from __future__ import annotations
from typing import List, Optional
from lstore.table import Record, Table
from lstore.lock_manager import LockConflict


class Query:
    """
    Entry point for all database operations -- insert, delete, update, select, etc.
    In Milestone 3:
      - write locks are managed by Transaction
      - read locks are acquired here on actual accessed RID(s)
      - strict 2PL / no-wait conflicts must raise LockConflict in txn path
    """

    def __init__(self, table: Table):
        self.table = table
        self._num_cols = table.num_columns
        self._key_col = table.key

    def _acquire_shared_if_needed(self, txn, rid: int) -> bool:
        if txn is None:
            return True

        txn_id = getattr(txn, "txn_id", None)
        if txn_id is None:
            return True

        lm = getattr(self.table, "lock_manager", None)
        if lm is None:
            return True

        lm.acquire_S(int(txn_id), ("RID", self.table.name, int(rid)))
        return True

    def _acquire_insert_rid_x_if_needed(self, txn, rid: int) -> bool:
        if txn is None:
            return True

        txn_id = getattr(txn, "txn_id", None)
        if txn_id is None:
            return True

        lm = getattr(self.table, "lock_manager", None)
        if lm is None:
            return True

        lm.acquire_X(int(txn_id), ("RID", self.table.name, int(rid)))
        return True

    def _rollback_insert_local(
        self,
        base_rid: int,
        row: List[int],
        old_existing: Optional[int] = None,
    ) -> None:
        pk = int(row[self._key_col])

        with self.table._meta_lock:
            self.table._deleted.pop(int(base_rid), None)
            self.table._latest_cache.pop(int(base_rid), None)

            if self.table.key2rid.get(pk) == int(base_rid):
                if old_existing is None:
                    self.table.key2rid.pop(pk, None)
                else:
                    self.table.key2rid[pk] = int(old_existing)

            self.table.page_directory.pop(int(base_rid), None)

            try:
                self.table._base_rid_list.remove(int(base_rid))
            except ValueError:
                pass

        for c in range(self._num_cols):
            try:
                if self.table.index.is_indexed(c):
                    self.table.index.delete_entry(c, int(row[c]), int(base_rid))
            except Exception:
                pass

    def _rollback_delete_local(self, base_rid: int, old_row: List[int], old_deleted: bool) -> None:
        pk = int(old_row[self._key_col])

        with self.table._meta_lock:
            self.table._deleted[int(base_rid)] = bool(old_deleted)
            if old_deleted:
                self.table._latest_cache.pop(int(base_rid), None)
            else:
                self.table.key2rid[pk] = int(base_rid)
                self.table._latest_cache[int(base_rid)] = [int(v) for v in old_row]

        if not old_deleted:
            for c in range(self._num_cols):
                try:
                    if self.table.index.is_indexed(c):
                        self.table.index.insert_entry(c, int(old_row[c]), int(base_rid))
                except Exception:
                    pass

    def _rollback_update_local(
        self,
        base_rid: int,
        old_row: List[int],
        old_indirection: int,
        old_schema: int,
        new_row: Optional[List[int]] = None,
    ) -> None:
        try:
            new_tail = int(self.table._base_latest_tail_rid(base_rid))
        except Exception:
            new_tail = 0

        try:
            self.table.overwrite_base_indirection(base_rid, int(old_indirection))
        except Exception:
            pass

        try:
            self.table.overwrite_base_schema(base_rid, int(old_schema))
        except Exception:
            pass

        if new_tail != 0 and new_tail != int(old_indirection):
            with self.table._meta_lock:
                self.table._deleted.pop(int(new_tail), None)
                self.table.page_directory.pop(int(new_tail), None)

        with self.table._meta_lock:
            if not bool(self.table._deleted.get(int(base_rid), False)):
                self.table._latest_cache[int(base_rid)] = [int(v) for v in old_row]

        if new_row is not None:
            for c in range(self._num_cols):
                try:
                    if self.table.index.is_indexed(c):
                        old_v = int(old_row[c])
                        new_v = int(new_row[c])
                        if old_v != new_v:
                            self.table.index.delete_entry(c, new_v, int(base_rid))
                            self.table.index.insert_entry(c, old_v, int(base_rid))
                except Exception:
                    pass

    def delete(self, primary_key: int, txn=None) -> bool:
        try:
            pk = int(primary_key)
            base_rid = self.table.get_base_rid_by_key(pk)
            if base_rid is None:
                return False

            base_rid = int(base_rid)
            old_row = self.table.read_latest_user_columns(base_rid)
            with self.table._meta_lock:
                old_deleted = bool(self.table._deleted.get(base_rid, False))

            self.table.mark_deleted(base_rid)

            for c in range(self._num_cols):
                if self.table.index.is_indexed(c):
                    self.table.index.delete_entry(c, int(old_row[c]), base_rid)

            with self.table._meta_lock:
                if self.table.key2rid.get(pk) == base_rid:
                    del self.table.key2rid[pk]

            return True

        except LockConflict:
            raise
        except Exception:
            try:
                if "base_rid" in locals() and "old_row" in locals():
                    self._rollback_delete_local(int(base_rid), list(old_row), bool(old_deleted))
            except Exception:
                pass
            return False

    def insert(self, *columns, txn=None):
        try:
            if len(columns) != self._num_cols:
                return False
            if any(v is None for v in columns):
                return False

            row = [int(x) for x in columns]
            pk = int(row[self._key_col])

            existing = self.table.key2rid.get(pk)
            old_existing = None if existing is None else int(existing)

            if existing is not None and not self.table.is_deleted_rid(int(existing)):
                return False

            base_rid = self.table.alloc_base_rid()
            self._acquire_insert_rid_x_if_needed(txn, int(base_rid))
            self.table.write_base_record(base_rid, row)

            for c in range(self._num_cols):
                if self.table.index.is_indexed(c):
                    self.table.index.insert_entry(c, int(row[c]), int(base_rid))

            if txn is not None:
                return (True, int(base_rid))
            return True

        except LockConflict:
            raise
        except Exception:
            try:
                if "base_rid" in locals() and "row" in locals():
                    self._rollback_insert_local(
                        int(base_rid),
                        list(row),
                        old_existing if "old_existing" in locals() else None,
                    )
            except Exception:
                pass
            return False

    def select(self, key: int, column: int, query_columns: List[int], txn=None):
        try:
            search_col = int(column)
            search_val = int(key)
            use_index = self.table.index.is_indexed(search_col)

            if use_index:
                rids = self.table.index.locate(search_col, search_val)
            else:
                rids = self.table.all_base_rids()

            out: List[Record] = []
            for rid in rids:
                rid = int(rid)
                self._acquire_shared_if_needed(txn, rid)

                if self.table.is_deleted_rid(rid):
                    continue

                latest = self.table.read_latest_user_columns(rid)
                if int(latest[search_col]) != search_val:
                    continue

                projected: List[Optional[int]] = [None] * self._num_cols
                for i, take in enumerate(query_columns):
                    if take:
                        projected[i] = int(latest[i])

                pk = int(latest[self._key_col])
                out.append(Record(rid, pk, projected))

            return out

        except LockConflict:
            raise
        except Exception:
            if txn is not None:
                return False
            return []

    def update(self, key: int, *columns, txn=None) -> bool:
        try:
            if len(columns) != self._num_cols:
                return False
            if columns[self._key_col] is not None:
                return False

            pk = int(key)
            base_rid = self.table.get_base_rid_by_key(pk)
            if base_rid is None:
                return False

            base_rid = int(base_rid)
            cols: List[Optional[int]] = list(columns)
            old_row = self.table.read_latest_user_columns(base_rid)

            result = self.table.apply_update(base_rid, cols, prev_latest=old_row)
            # apply_update 返回 (tail_rid, old_indirection, old_schema, old_row)
            _, old_indirection, old_schema, _ = result

            new_row = list(old_row)
            for i, v in enumerate(cols):
                if v is not None:
                    new_row[i] = int(v)

            self.table.index.update_entry(base_rid, old_row, new_row)
            # 带事务时返回完整 tuple 供 transaction 构建 undo log
            if txn is not None:
                return result
            return True

        except LockConflict:
            raise
        except Exception:
            try:
                if "base_rid" in locals() and "old_row" in locals():
                    self._rollback_update_local(
                        int(base_rid),
                        [int(v) for v in old_row],
                        int(old_indirection) if "old_indirection" in locals() else 0,
                        int(old_schema) if "old_schema" in locals() else 0,
                        new_row if "new_row" in locals() else None,
                    )
            except Exception:
                pass
            return False

    def sum(self, start_range: int, end_range: int, aggregate_column_index: int, txn=None):
        try:
            start_k = int(start_range)
            end_k = int(end_range)
            if start_k > end_k:
                start_k, end_k = end_k, start_k

            c = int(aggregate_column_index)
            total = 0
            record_found = False
            use_index = self.table.index.is_indexed(self._key_col)

            if use_index:
                rids = self.table.index.locate_range(start_k, end_k, self._key_col)
            else:
                rids = self.table.all_base_rids()

            for rid in rids:
                rid = int(rid)
                self._acquire_shared_if_needed(txn, rid)

                if self.table.is_deleted_rid(rid):
                    continue

                pk = int(self.table.read_latest_user_value(rid, self._key_col))
                if start_k <= pk <= end_k:
                    total += int(self.table.read_latest_user_value(rid, c))
                    record_found = True

            return int(total) if record_found else False

        except LockConflict:
            raise
        except Exception:
            if txn is not None:
                return False
            return 0

    def select_version(self, key: int, column: int, query_columns: List[int], relative_version: int):
        try:
            search_col = int(column)
            search_val = int(key)
            steps_back = abs(int(relative_version))

            if self.table.index.is_indexed(search_col):
                rids = self.table.index.locate(search_col, search_val)
            else:
                rids = []
                for rid in self.table.all_base_rids():
                    rid = int(rid)
                    if self.table.is_deleted_rid(rid):
                        continue
                    v = self.table.read_latest_user_value(rid, search_col)
                    if int(v) == search_val:
                        rids.append(rid)

            out: List[Record] = []
            for rid in rids:
                rid = int(rid)
                if self.table.is_deleted_rid(rid):
                    continue
                versioned = self.table.read_relative_user_columns(rid, steps_back)

                projected: List[Optional[int]] = [None] * self._num_cols
                for i, take in enumerate(query_columns):
                    if take:
                        projected[i] = int(versioned[i])

                pk = int(versioned[self._key_col])
                out.append(Record(rid, pk, projected))

            return out

        except Exception:
            return []

    def sum_version(self, start_range: int, end_range: int, aggregate_column_index: int, relative_version: int):
        try:
            start_k = int(start_range)
            end_k = int(end_range)
            if start_k > end_k:
                start_k, end_k = end_k, start_k
            c = int(aggregate_column_index)
            steps_back = abs(int(relative_version))

            total = 0
            found = False

            if self.table.index.is_indexed(self._key_col):
                rids = self.table.index.locate_range(start_k, end_k, self._key_col)
            else:
                rids = self.table.all_base_rids()

            for rid in rids:
                rid = int(rid)
                if self.table.is_deleted_rid(rid):
                    continue

                pk = int(self.table.read_latest_user_value(rid, self._key_col))
                if start_k <= pk <= end_k:
                    total += int(self.table.read_relative_user_value(rid, c, steps_back))
                    found = True

            return int(total) if found else False

        except Exception:
            return 0

    def increment(self, key: int, column: int, txn=None) -> bool:
        try:
            recs = self.select(int(key), self._key_col, [1] * self._num_cols, txn=txn)
            if not recs:
                return False

            curr = recs[0].columns
            updated = [None] * self._num_cols
            updated[int(column)] = int(curr[int(column)]) + 1

            return bool(self.update(int(key), *updated, txn=txn))

        except LockConflict:
            raise
        except Exception:
            return False
