from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple, Dict, Hashable
import threading
import time

from lstore.table import Table
from lstore.lock_manager import LockManager, LockConflict

# Undo 日志条目
@dataclass
class UndoEntry:
    typ: str  # "INSERT" | "UPDATE" | "DELETE"
    base_rid: int
    payload: Dict[str, Any]

# 事务 id 生成器

_TXN_ID_LOCK = threading.Lock()
_TXN_ID = 1
def _next_txn_id() -> int:
    global _TXN_ID
    with _TXN_ID_LOCK:
        tid = _TXN_ID
        _TXN_ID += 1
        return tid
class Transaction:
    """
    add_query(query_callable, table, *args)给tester， table 用来挂 lock_manager和undo，这里只存 (callable, args)
    run()：严格 2PL和no-wait，任意 query 失败或任意锁拿不到则abort，abort 会rollback/undo（基于当前 Table API）

    根据已有的东西这里简单挂一下：
          insert(*columns) -> bool
          update(key, *columns) -> bool
          delete(primary_key) -> bool
          increment(key, column) -> bool   （写）
          select(key, column, query_columns) -> List[Record] （读）
          sum(...) （读）
      总而言之这里只加锁和undo
    """

    def __init__(self):
        self.queries: List[Tuple[Callable[..., Any], Tuple[Any, ...]]] = []
        self.txn_id: int = _next_txn_id()
        self.table: Optional[Table] = None
        self.lm: Optional[LockManager] = None
        self._undo: List[UndoEntry] = []

    def add_query(self, query: Callable[..., Any], table: Table, *args) -> None:
        # 只记录第一张表：用于加锁与 undo
        if self.table is None:
            self.table = table
            # 表上没有 lock_manager 就挂一个
            if not hasattr(table, "lock_manager") or getattr(table, "lock_manager") is None:
                setattr(table, "lock_manager", LockManager())
            self.lm = getattr(table, "lock_manager")
        self.queries.append((query, args))


    # 锁规划辅助函数

    def _is_write_op(self, op: Callable[..., Any]) -> bool:
        name = getattr(op, "__name__", "")
        return name in ("insert", "update", "delete", "increment")

    def _plan_locks(self, op: Callable[..., Any], args: Tuple[Any, ...]) -> Tuple[List[Hashable], List[Hashable]]:

        #返回 (读资源列表, 写资源列表)。资源：已存在记录用 base_rid（int），insert 用 ("PK", pk) 占坑
        assert self.table is not None
        name = getattr(op, "__name__", "")

        # 写操作
        if name == "insert":
            # insert(*columns)：pk 在 columns[key_col]
            if len(args) <= self.table.key:
                return ([], [])
            pk = int(args[self.table.key])
            return ([], [("PK", pk)])
        if name in ("update", "delete", "increment"):
            # update(key, *cols), delete(pk), increment(key, col)
            if len(args) < 1:
                return ([], [])
            pk = int(args[0])
            base_rid = self.table.key2rid.get(pk)
            if base_rid is None:
                # 本来就会失败就不加锁
                return ([], [])
            return ([], [int(base_rid)])

        # 读操作
        if name == "select":
            # select(key, column, query_columns)
            if len(args) < 2:
                return ([], [])
            search_key = int(args[0])
            search_col = int(args[1])

            # 按主键查，锁住对应 base rid
            if search_col == self.table.key:
                base_rid = self.table.key2rid.get(search_key)
                if base_rid is None:
                    return ([], [])
                return ([int(base_rid)], [])
            # 非主键：如果有索引，预定位 rid 后加锁
            try:
                if self.table.index.is_indexed(search_col):
                    rids = self.table.index.locate(search_col, search_key)
                    return ([int(r) for r in rids], [])
            except Exception:
                pass
            # 兜底：扫描类查询不预先锁（避免锁集合问题）
            return ([], [])

        # sum 等：不做逐记录锁，还是避免锁集合问题
        return ([], [])

    def _acquire_locks_no_wait(self, read_res: List[Hashable], write_res: List[Hashable]) -> None:
        assert self.lm is not None
        # 先共享锁
        for r in read_res:
            self.lm.acquire_S(self.txn_id, r)
        # 再排他锁
        for r in write_res:
            self.lm.acquire_X(self.txn_id, r)


    # Undo 采集与执行

    def _capture_before_write(self, op: Callable[..., Any], args: Tuple[Any, ...]) -> Optional[UndoEntry]:
        """
        写操作执行前看旧状态，用于回滚，只对写操作生成 UndoEntry。
        """
        assert self.table is not None
        name = getattr(op, "__name__", "")

        if name == "insert":
            # 预测下一条 base rid（用于 abort 时删索引/删 key2rid）
            # 注意：多线程下 predicted rid 不可靠，所以这里只先占位，真正 base_rid 在 insert 成功后从 key2rid 里拿
            if len(args) <= self.table.key:
                return None
            pk = int(args[self.table.key])
            predicted_rid = int(getattr(self.table, "_next_base_rid", 0))  # 仍保留，作为占位
            # 保存整行（用于 abort 时回滚索引）
            row = [int(x) for x in args]
            indexed = [(c, int(row[c])) for c in range(self.table.num_columns) if self.table.index.is_indexed(c)]
            return UndoEntry(
                typ="INSERT",
                base_rid=predicted_rid,   # 先占位；insert 成功后会被 run() 改成真实 base_rid
                payload={"pk": pk, "indexed": indexed},
            )

        if name == "update":
            if len(args) < 1:
                return None
            pk = int(args[0])
            base_rid = self.table.key2rid.get(pk)
            if base_rid is None:
                return None
            base_rid = int(base_rid)

            # 保存 base 元数据（indirection/schema）和旧用户列（用于索引回滚）
            base_full = self.table.read_physical_record(base_rid)
            old_ind = int(base_full[0])  # table.py 里 INDIRECTION_COLUMN = 0
            old_schema = int(base_full[3])  # table.py 里 SCHEMA_ENCODING_COLUMN = 3
            old_row = self.table.read_latest_user_columns(base_rid)
            return UndoEntry(
                typ="UPDATE",
                base_rid=base_rid,
                payload={
                    "old_indirection": old_ind,
                    "old_schema": old_schema,
                    "old_row": old_row,
                    # 执行后再补
                    "new_tail_rid": None,
                    "new_row": None,
                },
            )

        if name == "delete":
            if len(args) < 1:
                return None
            pk = int(args[0])
            base_rid = self.table.key2rid.get(pk)
            if base_rid is None:
                return None
            base_rid = int(base_rid)
            old_row = self.table.read_latest_user_columns(base_rid)
            old_deleted = bool(getattr(self.table, "_deleted", {}).get(base_rid, False))
            return UndoEntry(
                typ="DELETE",
                base_rid=base_rid,
                payload={"old_deleted": old_deleted, "old_row": old_row},
            )

        if name == "increment":
            # increment 本质也是写：按 UPDATE 处理
            if len(args) < 1:
                return None
            pk = int(args[0])
            base_rid = self.table.key2rid.get(pk)
            if base_rid is None:
                return None
            base_rid = int(base_rid)

            base_full = self.table.read_physical_record(base_rid)
            old_ind = int(base_full[0])
            old_schema = int(base_full[3])
            old_row = self.table.read_latest_user_columns(base_rid)
            return UndoEntry(
                typ="UPDATE",
                base_rid=base_rid,
                payload={
                    "old_indirection": old_ind,
                    "old_schema": old_schema,
                    "old_row": old_row,
                    "new_tail_rid": None,
                    "new_row": None,
                },
            )

        return None

    def _finalize_after_write(self, op: Callable[..., Any], undo: Optional[UndoEntry]) -> None:
        """
        写操作执行后补“新状态”（比如新 tail rid、新行值）。
        """
        if undo is None:
            return
        assert self.table is not None

        if undo.typ == "UPDATE":
            # update 后 base.indirection 指向最新 tail rid
            base_full_after = self.table.read_physical_record(undo.base_rid)
            new_tail = int(base_full_after[0])
            new_row = self.table.read_latest_user_columns(undo.base_rid)
            undo.payload["new_tail_rid"] = new_tail
            undo.payload["new_row"] = new_row

        # INSERT/DELETE 不需要补更多信息

    def _apply_undo(self, undo: UndoEntry) -> None:
        assert self.table is not None
        t = self.table

        if undo.typ == "INSERT":
            base_rid = int(undo.base_rid)
            pk = int(undo.payload["pk"])
            indexed = list(undo.payload.get("indexed", []))

            # 把插入的记录打墓碑，如果 insert 根本没成功，这些操作应当是安全的 no-op
            try:
                t._deleted[base_rid] = True
            except Exception:
                pass
            #key2rid 指向这条时才删
            try:
                if t.key2rid.get(pk) == base_rid:
                    t.key2rid.pop(pk, None)
            except Exception:
                pass
            # 删除索引条目
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

            # 恢复 deleted 标记
            try:
                t._deleted[base_rid] = old_deleted
            except Exception:
                pass

            # 如果之前没删过就把索引补回去
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
            old_row = undo.payload.get("old_row", None)
            new_row = undo.payload.get("new_row", None)
            new_tail = undo.payload.get("new_tail_rid", None)
            #恢复 base 元数据
            try:
                t.overwrite_base_indirection(base_rid, old_ind)
            except Exception:
                pass
            try:
                t.overwrite_base_schema(base_rid, old_schema)
            except Exception:
                pass
            #把新 tail 打墓碑
            if new_tail is not None:
                try:
                    t._deleted[int(new_tail)] = True
                except Exception:
                    pass

            #回滚索引：删新值、加回旧值
            if old_row is not None and new_row is not None:
                for c in range(t.num_columns):
                    if t.index.is_indexed(c):
                        try:
                            if int(old_row[c]) != int(new_row[c]):
                                t.index.delete_entry(int(c), int(new_row[c]), int(base_rid))
                                t.index.insert_entry(int(c), int(old_row[c]), int(base_rid))
                        except Exception:
                            pass
            return

    
    # 对外接口
    def run(self) -> bool:
        """
        成功提交返回 True，回滚返回 False， worker 会对 abort 的事务重试直到成功。
        """
        if self.table is None:
            # 没有任务则成功
            return True
        self._undo.clear()
        try:
            for (op, args) in self.queries:
                # 1。规划锁集合
                read_res, write_res = self._plan_locks(op, args)

                # 2.no-wait拿锁
                self._acquire_locks_no_wait(read_res, write_res)

                # 3.写前抓 undo
                undo = None
                if self._is_write_op(op):
                    undo = self._capture_before_write(op, args)

                # 4.执行操作
                # 只对 select/sum 传 txn（避免 insert/update/delete/increment 因为不支持关键字参数而 TypeError）
                op_name = getattr(op, "__name__", "")
                if op_name in ("select", "sum"):
                    result = op(*args, txn=self)
                else:
                    result = op(*args)

                # select 返回 list 也算成功,只有 False 才算失败
                ok = (result is not False)
                if not ok:
                    return self.abort()

                # insert 成功后：把 undo 里的 base_rid 改成真实 base_rid（必须从 key2rid 拿）
                if undo is not None and undo.typ == "INSERT":
                    try:
                        pk = int(undo.payload["pk"])
                        real_rid = self.table.key2rid.get(pk)
                        if real_rid is not None:
                            undo.base_rid = int(real_rid)
                            self.lm.acquire_X(self.txn_id, int(real_rid)) #修复：锁真实 base_rid，避免并发 update/delete
                    except Exception:
                        pass

                # 5.写后补 undo
                if undo is not None:
                    self._finalize_after_write(op, undo)
                    self._undo.append(undo)
            return self.commit()
        except LockConflict:
            # no-wait：拿不到锁就 abort
            return self.abort()
        except Exception:
            # 任何异常都 abort
            return self.abort()
    def abort(self) -> bool:
        # 逆序rollback
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
