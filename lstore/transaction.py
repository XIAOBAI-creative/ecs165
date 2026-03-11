from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple, Dict
from contextlib import nullcontext
import threading

from lstore.table import Table
from lstore.lock_manager import LockManager, LockConflict


@dataclass
class UndoEntry:
    typ: str  # INSERT | UPDATE | DELETE
    table: Table
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
        self.queries: List[Tuple[Callable[..., Any], Table, Tuple[Any, ...]]] = []
        self.txn_id: int = _next_txn_id()
        self.table: Optional[Table] = None
        self.lm: Optional[LockManager] = None
        self._undo: List[UndoEntry] = []
        self._last_abort_reason: Optional[str] = None

    def add_query(self, query: Callable[..., Any], table: Table, *args) -> None:
        if not hasattr(table, "lock_manager") or getattr(table, "lock_manager") is None:
            setattr(table, "lock_manager", LockManager())

        if self.table is None:
            self.table = table
            self.lm = getattr(table, "lock_manager")

        self.queries.append((query, table, args))

    def _is_write_op(self, op: Callable[..., Any]) -> bool:
        return getattr(op, "__name__", "") in ("insert", "update", "delete", "increment")

    def _meta_guard(self, table: Table):
        lock = getattr(table, "_meta_lock", None)
        if lock is None:
            return nullcontext()
        return lock

    def _capture_before_write(
        self,
        table: Table,
        op: Callable[..., Any],
        args: Tuple[Any, ...],
    ) -> Optional[UndoEntry]:
        name = getattr(op, "__name__", "")

        if name == "insert":
            if len(args) != table.num_columns:
                return None

            row = [int(x) for x in args]
            pk = int(row[table.key])
            old_existing = table.key2rid.get(pk)

            return UndoEntry(
                typ="INSERT",
                table=table,
                base_rid=-1,
                payload={
                    "pk": pk,
                    "row": row,
                    "old_existing": None if old_existing is None else int(old_existing),
                },
            )

        if name in ("update", "increment"):
            if len(args) < 1:
                return None

            pk = int(args[0])
            base_rid = table.get_base_rid_by_key(pk)
            if base_rid is None:
                return None

            base_rid = int(base_rid)
            old_row = table.read_latest_user_columns(base_rid)
            # old_indirection/old_schema 将在操作执行后从 apply_update 返回值里填充
            # 这里先用占位符 -1，_run_once 会在操作后更新
            return UndoEntry(
                typ="UPDATE",
                table=table,
                base_rid=base_rid,
                payload={
                    "old_row": [int(v) for v in old_row],
                    "old_indirection": -1,  # 将在 _run_once 中填充
                    "old_schema": -1,       # 将在 _run_once 中填充
                },
            )

        if name == "delete":
            if len(args) < 1:
                return None

            pk = int(args[0])
            base_rid = table.get_base_rid_by_key(pk)
            if base_rid is None:
                return None

            base_rid = int(base_rid)
            old_row = table.read_latest_user_columns(base_rid)

            with self._meta_guard(table):
                old_deleted = bool(table._deleted.get(base_rid, False))

            return UndoEntry(
                typ="DELETE",
                table=table,
                base_rid=base_rid,
                payload={
                    "old_row": [int(v) for v in old_row],
                    "old_deleted": old_deleted,
                },
            )

        return None

    def _apply_undo(self, undo: UndoEntry) -> None:
        t = undo.table

        if undo.typ == "INSERT":
            base_rid = int(undo.base_rid)
            if base_rid < 0:
                return

            row = [int(v) for v in undo.payload.get("row", [])]
            if not row:
                return

            pk = int(undo.payload["pk"])
            old_existing = undo.payload.get("old_existing", None)

            with self._meta_guard(t):
                t._deleted.pop(base_rid, None)
                t._latest_cache.pop(base_rid, None)

                if t.key2rid.get(pk) == base_rid:
                    if old_existing is None:
                        t.key2rid.pop(pk, None)
                    else:
                        t.key2rid[pk] = int(old_existing)

                t.page_directory.pop(base_rid, None)

                try:
                    t._base_rid_list.remove(base_rid)
                except ValueError:
                    pass

            for c in range(t.num_columns):
                try:
                    if t.index.is_indexed(c):
                        t.index.delete_entry(c, int(row[c]), int(base_rid))
                except Exception:
                    pass
            return

        if undo.typ == "DELETE":
            base_rid = int(undo.base_rid)
            old_deleted = bool(undo.payload.get("old_deleted", False))
            old_row = [int(v) for v in undo.payload.get("old_row", [])]
            if not old_row:
                return

            pk = int(old_row[t.key])

            with self._meta_guard(t):
                t._deleted[base_rid] = old_deleted
                if old_deleted:
                    t._latest_cache.pop(base_rid, None)
                else:
                    t.key2rid[pk] = base_rid
                    t._latest_cache[base_rid] = list(old_row)

            if not old_deleted:
                for c in range(t.num_columns):
                    try:
                        if t.index.is_indexed(c):
                            t.index.insert_entry(c, int(old_row[c]), int(base_rid))
                    except Exception:
                        pass
            return

        if undo.typ == "UPDATE":
            base_rid = int(undo.base_rid)
            old_row = [int(v) for v in undo.payload.get("old_row", [])]
            old_indirection = int(undo.payload.get("old_indirection", 0))
            old_schema = int(undo.payload.get("old_schema", 0))
            if not old_row:
                return

            try:
                new_row = t.read_latest_user_columns(base_rid)
            except Exception:
                new_row = None

            try:
                new_tail = int(t._base_latest_tail_rid(base_rid))
            except Exception:
                new_tail = 0

            try:
                t.overwrite_base_indirection(base_rid, old_indirection)
            except Exception:
                pass

            try:
                t.overwrite_base_schema(base_rid, old_schema)
            except Exception:
                pass

            if new_tail != 0 and new_tail != old_indirection:
                with self._meta_guard(t):
                    t._deleted.pop(int(new_tail), None)
                    t.page_directory.pop(int(new_tail), None)

            with self._meta_guard(t):
                if not bool(t._deleted.get(base_rid, False)):
                    t._latest_cache[base_rid] = list(old_row)

            if new_row is not None:
                for c in range(t.num_columns):
                    try:
                        if t.index.is_indexed(c):
                            old_v = int(old_row[c])
                            new_v = int(new_row[c])
                            if old_v != new_v:
                                t.index.delete_entry(c, new_v, int(base_rid))
                                t.index.insert_entry(c, old_v, int(base_rid))
                    except Exception:
                        pass
            return

    def _acquire_read_locks_for_op(
        self,
        table: Table,
        op: Callable[..., Any],
        args: Tuple[Any, ...],
    ) -> None:
        """为 select 等读操作获取 S 锁（strict 2PL）"""
        lm = getattr(table, "lock_manager", None)
        if lm is None:
            setattr(table, "lock_manager", LockManager())
            lm = getattr(table, "lock_manager")

        name = getattr(op, "__name__", "")
        if name == "select":
            if len(args) < 1:
                return
            pk = int(args[0])
            lm.acquire_S(self.txn_id, ("PK", table.name, pk))
            base_rid = table.get_base_rid_by_key(pk)
            if base_rid is not None:
                lm.acquire_S(self.txn_id, ("RID", table.name, int(base_rid)))

    def _acquire_write_locks_for_op(
        self,
        table: Table,
        op: Callable[..., Any],
        args: Tuple[Any, ...],
    ) -> None:
        lm = getattr(table, "lock_manager", None)
        if lm is None:
            setattr(table, "lock_manager", LockManager())
            lm = getattr(table, "lock_manager")

        name = getattr(op, "__name__", "")

        if name == "insert":
            if len(args) <= table.key:
                return
            pk = int(args[table.key])
            lm.acquire_X(self.txn_id, ("PK", table.name, pk))
            return

        if name in ("update", "delete", "increment"):
            if len(args) < 1:
                return
            pk = int(args[0])

            lm.acquire_X(self.txn_id, ("PK", table.name, pk))

            base_rid = table.get_base_rid_by_key(pk)
            if base_rid is not None:
                lm.acquire_X(self.txn_id, ("RID", table.name, int(base_rid)))
            return

    def _run_once(self) -> bool:
        self._undo.clear()
        self._last_abort_reason = None

        for (op, table, args) in self.queries:
            op_name = getattr(op, "__name__", "")

            if self._is_write_op(op):
                self._acquire_write_locks_for_op(table, op, args)
            elif op_name == "select":
                self._acquire_read_locks_for_op(table, op, args)

            undo = None
            if self._is_write_op(op):
                undo = self._capture_before_write(table, op, args)
                if undo is not None:
                    self._undo.append(undo)

            op_name = getattr(op, "__name__", "")
            if op_name in ("insert", "update", "delete", "select", "sum", "increment"):
                result = op(*args, txn=self)
            else:
                result = op(*args)

            if result is False:
                # increment가 실패하는 경우는 락 경쟁일 가능성이 높으므로 LOCK으로 처리
                if op_name == "increment":
                    self._last_abort_reason = "LOCK"
                else:
                    self._last_abort_reason = "QUERY_FAIL"
                return self.abort()

            # 对 update/increment：从 apply_update 返回值填充 undo 里的 old_indirection/old_schema
            if undo is not None and undo.typ == "UPDATE":
                if isinstance(result, tuple) and len(result) == 4:
                    # apply_update 返回 (tail_rid, old_indirection, old_schema, old_row)
                    _, old_ind, old_sch, _ = result
                    undo.payload["old_indirection"] = int(old_ind)
                    undo.payload["old_schema"] = int(old_sch)

            if undo is not None and undo.typ == "INSERT":
                if isinstance(result, tuple):
                    ok, new_rid = result
                    if not ok:
                        self._last_abort_reason = "QUERY_FAIL"
                        return self.abort()
                    undo.base_rid = int(new_rid)
                else:
                    pk = int(undo.payload["pk"])
                    real_rid = table.get_base_rid_by_key(pk)
                    if real_rid is None:
                        self._last_abort_reason = "QUERY_FAIL"
                        return self.abort()
                    undo.base_rid = int(real_rid)

        return self.commit()

    def run(self) -> bool:
        if not self.queries:
            return True

        try:
            return bool(self._run_once())
        except LockConflict:
            self._last_abort_reason = "LOCK"
            self.abort()
            return False
        except Exception:
            self._last_abort_reason = "EXCEPTION"
            self.abort()
            return False

    def abort(self) -> bool:
        try:
            for i in range(len(self._undo) - 1, -1, -1):
                try:
                    self._apply_undo(self._undo[i])
                except Exception:
                    pass
        finally:
            released = set()
            for (_, table, _) in self.queries:
                lm = getattr(table, "lock_manager", None)
                if lm is not None and id(lm) not in released:
                    released.add(id(lm))
                    lm.release_all(self.txn_id)
        return False

    def commit(self) -> bool:
        released = set()
        for (_, table, _) in self.queries:
            lm = getattr(table, "lock_manager", None)
            if lm is not None and id(lm) not in released:
                released.add(id(lm))
                lm.release_all(self.txn_id)
        return True
