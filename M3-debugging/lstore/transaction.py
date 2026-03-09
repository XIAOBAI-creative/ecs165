from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple, Dict, Hashable
from contextlib import nullcontext
import threading
import time

from lstore.table import Table
from lstore.lock_manager import LockManager, LockConflict


@dataclass
class UndoEntry:
    typ: str  # "INSERT" | "UPDATE" | "DELETE"
    base_rid: int
    payload: Dict[str, Any]


_TXN_ID_LOCK = threading.Lock()
_TXN_ID = 1
def _next_txn_id() -> int:
    global _TXN_ID
    with _TXN_ID_LOCK:
        tid = _TXN_ID
        _TXN_ID += 1
        return tid


class Transaction:
    def __init__(self):
        self.queries: List[Tuple[Callable[..., Any], Tuple[Any, ...]]] = []
        self.txn_id: int = _next_txn_id()
        self.table: Optional[Table] = None
        self.lm: Optional[LockManager] = None
        self._undo: List[UndoEntry] = []

    def add_query(self, query: Callable[..., Any], table: Table, *args) -> None:
        if self.table is None:
            self.table = table
            if not hasattr(table, "lock_manager") or getattr(table, "lock_manager") is None:
                setattr(table, "lock_manager", LockManager())
            self.lm = getattr(table, "lock_manager")
        self.queries.append((query, args))

    def _is_write_op(self, op: Callable[..., Any]) -> bool:
        name = getattr(op, "__name__", "")
        return name in ("insert", "update", "delete", "increment")

    def _meta_guard(self):
        if self.table is None:
            return nullcontext()
        lock = getattr(self.table, "_meta_lock", None)
        if lock is None:
            return nullcontext()
        return lock

    # ------------------------------------------------------------------
    # Lock planning
    # ------------------------------------------------------------------
    def _plan_locks(self, op: Callable[..., Any], args: Tuple[Any, ...]) -> Tuple[List[Hashable], List[Hashable]]:
        assert self.table is not None
        name = getattr(op, "__name__", "")

        if name == "insert":
            if len(args) <= self.table.key:
                return ([], [])
            pk = int(args[self.table.key])
            return ([], [("PK", pk)])

        if name in ("update", "delete", "increment"):
            if len(args) < 1:
                return ([], [])
            pk = int(args[0])
            base_rid = self.table.key2rid.get(pk)
            if base_rid is None:
                return ([], [])
            return ([], [int(base_rid)])

        if name == "select":
            if len(args) < 2:
                return ([], [])
            search_key = int(args[0])
            search_col = int(args[1])
            if search_col == self.table.key:
                base_rid = self.table.key2rid.get(search_key)
                if base_rid is None:
                    return ([], [])
                return ([int(base_rid)], [])
            return ([], [])

        return ([], [])

    # ------------------------------------------------------------------
    # Undo capture (before each write op)
    # ------------------------------------------------------------------
    def _capture_before_write(self, op: Callable[..., Any], args: Tuple[Any, ...]) -> Optional[UndoEntry]:
        assert self.table is not None
        name = getattr(op, "__name__", "")

        if name == "insert":
            if len(args) <= self.table.key:
                return None
            pk = int(args[self.table.key])
            with self._meta_guard():
                predicted_rid = int(getattr(self.table, "_next_base_rid", 0))
            row = [int(x) for x in args]
            indexed = [(c, int(row[c])) for c in range(self.table.num_columns) if self.table.index.is_indexed(c)]
            return UndoEntry(
                typ="INSERT",
                base_rid=predicted_rid,
                payload={"pk": pk, "indexed": indexed},
            )

        if name in ("update", "increment"):
            pk = int(args[0])
            base_rid = self.table.key2rid.get(pk)
            if base_rid is None:
                return None
            base_rid = int(base_rid)
            old_ind = int(self.table._base_latest_tail_rid(base_rid))
            old_schema = int(self.table._base_schema(base_rid))
            # Also capture old user column values for reliable undo
            old_row = self.table.read_latest_user_columns(base_rid)
            return UndoEntry(typ="UPDATE", base_rid=base_rid,
                            payload={
                                "old_indirection": old_ind,
                                "old_schema": old_schema,
                                "old_row": [int(v) for v in old_row],
                            })

        if name == "delete":
            if len(args) < 1:
                return None
            pk = int(args[0])
            base_rid = self.table.key2rid.get(pk)
            if base_rid is None:
                return None
            base_rid = int(base_rid)
            old_row = self.table.read_latest_user_columns(base_rid)
            with self._meta_guard():
                old_deleted = bool(self.table._deleted.get(base_rid, False))
            return UndoEntry(
                typ="DELETE",
                base_rid=base_rid,
                payload={"old_deleted": old_deleted, "old_row": old_row},
            )

        return None

    def _finalize_after_write(self, op: Callable[..., Any], undo: Optional[UndoEntry]) -> None:
        if undo is None:
            return

    # ------------------------------------------------------------------
    # Undo application
    # ------------------------------------------------------------------
    def _apply_undo(self, undo: UndoEntry) -> None:
        assert self.table is not None
        t = self.table

        if undo.typ == "INSERT":
            base_rid = int(undo.base_rid)
            pk = int(undo.payload["pk"])
            indexed = list(undo.payload.get("indexed", []))

            try:
                with self._meta_guard():
                    t._deleted[base_rid] = True
                    t._latest_cache.pop(base_rid, None)
                    if t.key2rid.get(pk) == base_rid:
                        t.key2rid.pop(pk, None)
            except Exception:
                pass
            for (c, v) in indexed:
                try:
                    t.index.delete_entry(int(c), int(v), int(base_rid))
                except Exception:
                    pass
            return

        if undo.typ == "DELETE":
            base_rid = int(undo.base_rid)
            old_deleted = bool(undo.payload.get("old_deleted", False))
            old_row = undo.payload.get("old_row", None)

            try:
                with self._meta_guard():
                    t._deleted[base_rid] = old_deleted
                    if old_deleted:
                        t._latest_cache.pop(base_rid, None)
                    elif old_row is not None:
                        t._latest_cache[base_rid] = [int(v) for v in old_row]
                        pk = int(old_row[t.key])
                        t.key2rid[pk] = int(base_rid)
            except Exception:
                pass

            if old_row is not None and old_deleted is False:
                for c in range(t.num_columns):
                    if t.index.is_indexed(c):
                        try:
                            t.index.insert_entry(int(c), int(old_row[c]), int(base_rid))
                        except Exception:
                            pass
            return

        if undo.typ == "UPDATE":
            base_rid = int(undo.base_rid)
            old_ind = int(undo.payload["old_indirection"])
            old_schema = int(undo.payload["old_schema"])
            # old_row was captured before the write - this is the ground truth
            old_row = undo.payload.get("old_row")

            # Read current (new) values for index rollback
            try:
                new_row = t.read_latest_user_columns(base_rid)
            except Exception:
                new_row = None

            # Get the tail rid that our write created
            try:
                new_tail = int(t._base_latest_tail_rid(base_rid))
            except Exception:
                new_tail = 0

            # Restore base record metadata to pre-write state
            try:
                t.overwrite_base_indirection(base_rid, old_ind)
            except Exception:
                pass
            try:
                t.overwrite_base_schema(base_rid, old_schema)
            except Exception:
                pass

            # Mark the tail record we created as deleted
            if new_tail != 0:
                try:
                    with self._meta_guard():
                        t._deleted[int(new_tail)] = True
                except Exception:
                    pass

            # Restore cache directly from captured old_row (don't re-read!)
            # This avoids the stale-cache bug entirely: we KNOW what the
            # old values were because we captured them before the write.
            if old_row is not None:
                with self._meta_guard():
                    t._latest_cache[base_rid] = [int(v) for v in old_row]
            else:
                # Fallback: invalidate cache and re-read
                with self._meta_guard():
                    t._latest_cache.pop(base_rid, None)
                try:
                    restored = t.read_latest_user_columns(base_rid)
                    with self._meta_guard():
                        t._latest_cache[base_rid] = [int(v) for v in restored]
                except Exception:
                    pass

            # Rollback index entries
            if old_row is not None and new_row is not None:
                for c in range(t.num_columns):
                    if t.index.is_indexed(c):
                        try:
                            old_v = int(old_row[c])
                            new_v = int(new_row[c])
                            if old_v != new_v:
                                t.index.delete_entry(int(c), new_v, int(base_rid))
                                t.index.insert_entry(int(c), old_v, int(base_rid))
                        except Exception:
                            pass
            return

    # ------------------------------------------------------------------
    # Transaction execution
    # ------------------------------------------------------------------
    def run(self) -> bool:
        if self.table is None:
            return True
        self._undo.clear()
        try:
            # Phase 1: Collect ALL lock requirements from ALL queries,
            # then acquire in one pass. If a resource needs both S and X,
            # promote to X only. This prevents the S->X upgrade deadlock
            # where two txns both hold S and neither can upgrade.
            all_read: set = set()
            all_write: set = set()
            for (op, args) in self.queries:
                read_res, write_res = self._plan_locks(op, args)
                for r in read_res:
                    all_read.add(r)
                for r in write_res:
                    all_write.add(r)

            # Promote overlapping S+X to X only
            all_read -= all_write

            # Acquire X first, then S
            for r in all_write:
                self.lm.acquire_X(self.txn_id, r)
            for r in all_read:
                self.lm.acquire_S(self.txn_id, r)

            # Phase 2: Execute all queries
            for (op, args) in self.queries:
                undo = None
                if self._is_write_op(op):
                    undo = self._capture_before_write(op, args)

                op_name = getattr(op, "__name__", "")
                if op_name in ("select", "sum"):
                    result = op(*args, txn=self)
                else:
                    result = op(*args)

                ok = (result is not False)
                if not ok:
                    return self.abort()

                # After a successful insert, lock the real base_rid
                if undo is not None and undo.typ == "INSERT":
                    try:
                        pk = int(undo.payload["pk"])
                        real_rid = self.table.key2rid.get(pk)
                        if real_rid is not None:
                            undo.base_rid = int(real_rid)
                            self.lm.acquire_X(self.txn_id, int(real_rid))
                    except Exception:
                        pass

                if undo is not None:
                    self._finalize_after_write(op, undo)
                    self._undo.append(undo)

            return self.commit()
        except LockConflict:
            return self.abort()
        except Exception:
            return self.abort()

    def abort(self) -> bool:
        try:
            for i in range(len(self._undo) - 1, -1, -1):
                self._apply_undo(self._undo[i])
        finally:
            if self.lm is not None:
                self.lm.release_all(self.txn_id)
        return False

    def commit(self) -> bool:
        if self.lm is not None:
            self.lm.release_all(self.txn_id)
        return True
