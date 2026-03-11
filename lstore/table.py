from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import threading

from lstore.page import Page
from lstore.bufferpool import BufferPool

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
HIDDEN_COLS = 4


class Record:
    def __init__(self, rid: int, key: int, columns: List[Optional[int]]):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __getitem__(self, idx: int):
        return self.columns[idx]


@dataclass
class RecordLocator:
    page_range_id: int
    is_tail: bool
    page_id: int
    offset: int


@dataclass
class MergeResult:
    page_range_id: int
    merged_values: Dict[int, List[int]]
    new_tps: int
    new_version: int


class PageRange:
    def __init__(
        self,
        num_columns_total: int,
        table_name: str,
        page_range_id: int,
        buffer_pool: Optional[BufferPool] = None,
        bp_lock: Optional[threading.RLock] = None,
    ):
        self.num_columns_total = int(num_columns_total)
        self.table_name = str(table_name)
        self.page_range_id = int(page_range_id)

        self.base_pages: List[List[Page]] = [[] for _ in range(self.num_columns_total)]
        self.tail_pages: List[List[Page]] = [[] for _ in range(self.num_columns_total)]

        self.tps: int = 0
        self._next_base_page_id: int = 0
        self._next_base_offset: int = 0
        self._next_tail_page_id: int = 0
        self._next_tail_offset: int = 0

        self.buffer_pool = buffer_pool
        self.bp_lock = bp_lock

        # base pages version pointer (published version)
        self.base_version: int = 0
        self._lock = threading.RLock()

    def _ensure_base_page(self, col: int, page_id: int) -> Page:
        pages = self.base_pages[col]
        while len(pages) <= page_id:
            pages.append(Page(col, is_tail=False))
        return pages[page_id]

    def _ensure_tail_page(self, col: int, page_id: int) -> Page:
        pages = self.tail_pages[col]
        while len(pages) <= page_id:
            pages.append(Page(col, is_tail=True))
        return pages[page_id]

    def _pid(self, is_tail: bool, col: int, page_id: int):
        if bool(is_tail):
            return (self.table_name, self.page_range_id, True, int(col), int(page_id))
        return (self.table_name, self.page_range_id, False, int(self.base_version), int(col), int(page_id))

    def get_page(self, is_tail: bool, col: int, page_id: int) -> Page:
        with self._lock:
            if is_tail:
                self._ensure_tail_page(col, page_id)
            else:
                self._ensure_base_page(col, page_id)

            if self.buffer_pool is None:
                return self.tail_pages[col][page_id] if is_tail else self.base_pages[col][page_id]

            if self.bp_lock is None:
                return self.buffer_pool.fetch_page(self._pid(is_tail, col, page_id))

            with self.bp_lock:
                return self.buffer_pool.fetch_page(self._pid(is_tail, col, page_id))

    def release_page(self, is_tail: bool, col: int, page_id: int, dirty: bool):
        with self._lock:
            if self.buffer_pool is None:
                return

            if self.bp_lock is None:
                self.buffer_pool.unpin_page(self._pid(is_tail, col, page_id), is_dirty=bool(dirty))
                return

            with self.bp_lock:
                self.buffer_pool.unpin_page(self._pid(is_tail, col, page_id), is_dirty=bool(dirty))

    def get_pages_batch(self, is_tail: bool, page_id: int, num_cols: int) -> list:
        with self._lock:
            for col in range(num_cols):
                if is_tail:
                    self._ensure_tail_page(col, page_id)
                else:
                    self._ensure_base_page(col, page_id)

            if self.buffer_pool is None:
                if is_tail:
                    return [self.tail_pages[col][page_id] for col in range(num_cols)]
                else:
                    return [self.base_pages[col][page_id] for col in range(num_cols)]

            pids = [self._pid(is_tail, col, page_id) for col in range(num_cols)]
            return self.buffer_pool.fetch_many(pids)

    def release_pages_batch(self, is_tail: bool, page_id: int, num_cols: int, dirty: bool):
        with self._lock:
            if self.buffer_pool is None:
                return
            pids_dirty = [(self._pid(is_tail, col, page_id), bool(dirty)) for col in range(num_cols)]
            self.buffer_pool.unpin_many(pids_dirty)

    def alloc_base_slot(self) -> Tuple[int, int]:
        with self._lock:
            if self._next_base_offset >= Page.CAPACITY:
                self._next_base_page_id += 1
                self._next_base_offset = 0

            page_id = self._next_base_page_id
            offset = self._next_base_offset
            self._ensure_base_page(0, page_id)

            self._next_base_offset += 1
            return (page_id, offset)

    def alloc_tail_slot(self) -> Tuple[int, int]:
        with self._lock:
            if self._next_tail_offset >= Page.CAPACITY:
                self._next_tail_page_id += 1
                self._next_tail_offset = 0

            page_id = self._next_tail_page_id
            offset = self._next_tail_offset
            self._ensure_tail_page(0, page_id)

            self._next_tail_offset += 1
            return (page_id, offset)


class Table:
    MERGE_TRIGGER_EVERY_N_TAIL_PAGES = 10

    def __init__(self, name: str, num_columns: int, key: int, buffer_pool: Optional[BufferPool] = None):
        self._base_rid_list: List[int] = []
        self.name = str(name)
        self.num_columns = int(num_columns)
        self.key = int(key)
        self.num_columns_total = HIDDEN_COLS + self.num_columns

        self.page_ranges: List[PageRange] = []
        self.page_directory: Dict[int, RecordLocator] = {}

        self._next_base_rid: int = 1
        self._next_tail_rid: int = (1 << 64) - 1

        self.key2rid: Dict[int, int] = {}
        self._deleted: Dict[int, bool] = {}
        self._latest_cache: Dict[int, List[int]] = {}

        self._meta_lock = threading.RLock()

        self.buffer_pool = buffer_pool
        self._bp_lock: Optional[threading.RLock] = threading.RLock() if self.buffer_pool is not None else None

        from lstore.index import Index
        self.index = Index(self)

        # merge state
        self._merge_lock = threading.Lock()
        self._merge_threads: Dict[int, threading.Thread] = {}
        self._merge_results: Dict[int, MergeResult] = {}

        self._tail_update_count: Dict[int, int] = {}
        self._last_merge_tail_pages: Dict[int, int] = {}

    # -----------------
    # RID helpers
    # -----------------
    def alloc_base_rid(self) -> int:
        with self._meta_lock:
            rid = self._next_base_rid
            self._next_base_rid += 1
            return int(rid)

    def alloc_tail_rid(self) -> int:
        with self._meta_lock:
            rid = self._next_tail_rid
            self._next_tail_rid -= 1
            return int(rid)

    # -------------------------
    # PageRange mgmt
    # -------------------------
    def _ensure_page_range(self, page_range_id: int) -> PageRange:
        page_range_id = int(page_range_id)
        with self._meta_lock:
            while len(self.page_ranges) <= page_range_id:
                new_id = len(self.page_ranges)
                self.page_ranges.append(
                    PageRange(self.num_columns_total, self.name, new_id, self.buffer_pool, self._bp_lock)
                )
            return self.page_ranges[page_range_id]

    def _choose_page_range_for_insert(self) -> int:
        with self._meta_lock:
            if not self.page_ranges:
                self.page_ranges.append(PageRange(self.num_columns_total, self.name, 0, self.buffer_pool, self._bp_lock))
            return len(self.page_ranges) - 1

    # -------------------------
    # Delete helpers
    # -------------------------
    def is_deleted_rid(self, base_rid: int) -> bool:
        with self._meta_lock:
            return bool(self._deleted.get(int(base_rid), False))

    def mark_deleted(self, base_rid: int) -> None:
        with self._meta_lock:
            br = int(base_rid)
            self._deleted[br] = True
            self._latest_cache.pop(br, None)

    # -------------------------
    # Physical write
    # -------------------------
    def write_base_record(self, base_rid: int, user_columns: List[int]) -> RecordLocator:
        if len(user_columns) != self.num_columns:
            raise ValueError("wrong number of user columns")

        with self._meta_lock:
            pr_id = self._choose_page_range_for_insert()
            pr = self._ensure_page_range(pr_id)

        with pr._lock:
            page_id, offset = pr.alloc_base_slot()

            full = [0] * self.num_columns_total
            full[INDIRECTION_COLUMN] = 0
            full[RID_COLUMN] = int(base_rid)
            full[TIMESTAMP_COLUMN] = 0
            full[SCHEMA_ENCODING_COLUMN] = 0
            for i, v in enumerate(user_columns):
                full[HIDDEN_COLS + i] = int(v)

            pages = pr.get_pages_batch(False, page_id, self.num_columns_total)
            for col in range(self.num_columns_total):
                off = pages[col].write(full[col])
                if off != offset:
                    pr.release_pages_batch(False, page_id, self.num_columns_total, dirty=True)
                    raise RuntimeError("Base offset misalignment")
            pr.release_pages_batch(False, page_id, self.num_columns_total, dirty=True)

        loc = RecordLocator(pr_id, False, page_id, offset)
        with self._meta_lock:
            self.page_directory[int(base_rid)] = loc
            pk = int(user_columns[self.key])
            self.key2rid[pk] = int(base_rid)
            self._deleted[int(base_rid)] = False
            self._latest_cache[int(base_rid)] = [int(v) for v in user_columns]
            self._base_rid_list.append(int(base_rid))
        return loc

    def write_tail_record(
        self,
        tail_rid: int,
        base_rid: int,
        prev_tail_rid: int,
        schema_encoding: int,
        user_columns: List[Optional[int]],
    ) -> RecordLocator:
        if len(user_columns) != self.num_columns:
            raise ValueError("wrong number of user columns")

        with self._meta_lock:
            loc_base = self.page_directory.get(int(base_rid))
            if loc_base is None or loc_base.is_tail:
                raise KeyError("base rid not found")
            pr = self.page_ranges[loc_base.page_range_id]

        with pr._lock:
            page_id, offset = pr.alloc_tail_slot()

            full = [0] * self.num_columns_total
            full[INDIRECTION_COLUMN] = int(prev_tail_rid)
            full[RID_COLUMN] = int(tail_rid)
            full[TIMESTAMP_COLUMN] = 0
            full[SCHEMA_ENCODING_COLUMN] = int(schema_encoding)
            for i, v in enumerate(user_columns):
                full[HIDDEN_COLS + i] = 0 if v is None else int(v)

            pages = pr.get_pages_batch(True, page_id, self.num_columns_total)
            for col in range(self.num_columns_total):
                off = pages[col].write(full[col])
                if off != offset:
                    pr.release_pages_batch(True, page_id, self.num_columns_total, dirty=True)
                    raise RuntimeError("Tail offset misalignment")
            pr.release_pages_batch(True, page_id, self.num_columns_total, dirty=True)

        loc = RecordLocator(loc_base.page_range_id, True, page_id, offset)
        with self._meta_lock:
            self.page_directory[int(tail_rid)] = loc
        return loc

    # -------------------------
    # Physical read (front-end path)
    # -------------------------
    def read_physical_record(self, rid: int) -> List[int]:
        with self._meta_lock:
            loc = self.page_directory.get(int(rid))
            if loc is None:
                raise KeyError("RID not found")
            pr = self.page_ranges[loc.page_range_id]

        # 批量获取所有列的页，只需一次锁+bufferpool操作
        pages = pr.get_pages_batch(loc.is_tail, loc.page_id, self.num_columns_total)
        out = [int(pages[col].read(loc.offset)) for col in range(self.num_columns_total)]
        pr.release_pages_batch(loc.is_tail, loc.page_id, self.num_columns_total, dirty=False)
        return out

    # -------------------------
    # Base metadata overwrite
    # -------------------------
    def overwrite_base_indirection(self, base_rid: int, new_tail_rid: int) -> None:
        with self._meta_lock:
            loc = self.page_directory[int(base_rid)]
            if loc.is_tail:
                raise ValueError("base_rid points to tail")
            pr = self.page_ranges[loc.page_range_id]
        with pr._lock:
            page = pr.get_page(False, INDIRECTION_COLUMN, loc.page_id)
            page.overwrite(loc.offset, int(new_tail_rid))
            pr.release_page(False, INDIRECTION_COLUMN, loc.page_id, dirty=True)

    def overwrite_base_schema(self, base_rid: int, new_schema: int) -> None:
        with self._meta_lock:
            loc = self.page_directory[int(base_rid)]
            if loc.is_tail:
                raise ValueError("base_rid points to tail")
            pr = self.page_ranges[loc.page_range_id]
        with pr._lock:
            page = pr.get_page(False, SCHEMA_ENCODING_COLUMN, loc.page_id)
            page.overwrite(loc.offset, int(new_schema))
            pr.release_page(False, SCHEMA_ENCODING_COLUMN, loc.page_id, dirty=True)

    def overwrite_base_indirection_and_schema(self, base_rid: int, new_tail_rid: int, new_schema: int) -> None:
        """一次拿锁同时写 INDIRECTION 和 SCHEMA，减少锁竞争"""
        with self._meta_lock:
            loc = self.page_directory[int(base_rid)]
            if loc.is_tail:
                raise ValueError("base_rid points to tail")
            pr = self.page_ranges[loc.page_range_id]
        with pr._lock:
            p_ind = pr.get_page(False, INDIRECTION_COLUMN, loc.page_id)
            p_ind.overwrite(loc.offset, int(new_tail_rid))
            pr.release_page(False, INDIRECTION_COLUMN, loc.page_id, dirty=True)
            p_sch = pr.get_page(False, SCHEMA_ENCODING_COLUMN, loc.page_id)
            p_sch.overwrite(loc.offset, int(new_schema))
            pr.release_page(False, SCHEMA_ENCODING_COLUMN, loc.page_id, dirty=True)

    # -------------------------
    # TPS
    # -------------------------
    def get_tps(self, page_range_id: int) -> int:
        with self._meta_lock:
            return int(self.page_ranges[int(page_range_id)].tps)

    def set_tps(self, page_range_id: int, tps: int) -> None:
        with self._meta_lock:
            self.page_ranges[int(page_range_id)].tps = int(tps)

    # -------------------------
    # Helpers
    # -------------------------
    def all_base_rids(self) -> List[int]:
        with self._meta_lock:
            return list(self._base_rid_list)

    def get_base_rid_by_key(self, pk: int) -> Optional[int]:
        with self._meta_lock:
            base_rid = self.key2rid.get(int(pk))
            if base_rid is None:
                return None
            base_rid = int(base_rid)
            if bool(self._deleted.get(base_rid, False)):
                return None
            return base_rid

    def _read_physical_column(self, rid: int, col: int) -> int:
        with self._meta_lock:
            loc = self.page_directory.get(int(rid))
            if loc is None:
                raise KeyError("RID not found")
            pr = self.page_ranges[loc.page_range_id]
        with pr._lock:
            page = pr.get_page(loc.is_tail, int(col), loc.page_id)
            out = int(page.read(loc.offset))
            pr.release_page(loc.is_tail, int(col), loc.page_id, dirty=False)
            return out

    def _base_latest_tail_rid(self, base_rid: int) -> int:
        return int(self._read_physical_column(base_rid, INDIRECTION_COLUMN))

    def _base_schema(self, base_rid: int) -> int:
        return int(self._read_physical_column(base_rid, SCHEMA_ENCODING_COLUMN))

    def _base_indirection_and_schema(self, base_rid: int):
        """一次读 INDIRECTION 和 SCHEMA 两列，减少锁和 bufferpool 开销"""
        with self._meta_lock:
            loc = self.page_directory.get(int(base_rid))
            if loc is None:
                raise KeyError("RID not found")
            pr = self.page_ranges[loc.page_range_id]
        with pr._lock:
            p_ind = pr.get_page(loc.is_tail, INDIRECTION_COLUMN, loc.page_id)
            ind = int(p_ind.read(loc.offset))
            pr.release_page(loc.is_tail, INDIRECTION_COLUMN, loc.page_id, dirty=False)
            p_sch = pr.get_page(loc.is_tail, SCHEMA_ENCODING_COLUMN, loc.page_id)
            sch = int(p_sch.read(loc.offset))
            pr.release_page(loc.is_tail, SCHEMA_ENCODING_COLUMN, loc.page_id, dirty=False)
        return ind, sch

    def read_base_user_value(self, base_rid: int, user_col: int) -> int:
        return int(self._read_physical_column(int(base_rid), HIDDEN_COLS + int(user_col)))

    def _base_page_range_id(self, base_rid: int) -> int:
        with self._meta_lock:
            loc = self.page_directory[int(base_rid)]
            return int(loc.page_range_id)

    # -------------------------
    # Read-latest
    # -------------------------
    def read_latest_user_columns(self, base_rid: int) -> List[int]:
        br = int(base_rid)

        with self._meta_lock:
            if bool(self._deleted.get(br, False)):
                raise KeyError("record deleted")

            cached = self._latest_cache.get(br)
            if cached is not None:
                return list(cached)

        base_full = self.read_physical_record(br)
        pr_id = self._base_page_range_id(br)
        tps = self.get_tps(pr_id)
        ind = int(base_full[INDIRECTION_COLUMN])

        latest = [int(base_full[HIDDEN_COLS + i]) for i in range(self.num_columns)]
        if ind == 0:
            with self._meta_lock:
                if not bool(self._deleted.get(br, False)):
                    self._latest_cache[br] = list(latest)
            return latest

        if tps != 0 and ind >= tps:
            with self._meta_lock:
                if not bool(self._deleted.get(br, False)):
                    self._latest_cache[br] = list(latest)
            return latest

        cur = ind
        seen_mask = 0
        all_mask = (1 << self.num_columns) - 1

        while cur != 0 and seen_mask != all_mask:
            tail_full = self.read_physical_record(cur)
            schema = int(tail_full[SCHEMA_ENCODING_COLUMN])
            with self._meta_lock:
                tail_deleted = bool(self._deleted.get(int(cur), False))
            if not tail_deleted:
                for i in range(self.num_columns):
                    bit = 1 << i
                    if (schema & bit) and not (seen_mask & bit):
                        latest[i] = int(tail_full[HIDDEN_COLS + i])
                        seen_mask |= bit
            cur = int(tail_full[INDIRECTION_COLUMN])

        with self._meta_lock:
            if not bool(self._deleted.get(br, False)):
                self._latest_cache[br] = list(latest)
        return latest

    def read_latest_user_value(self, base_rid: int, user_col: int) -> int:
        return int(self.read_latest_user_columns(base_rid)[int(user_col)])

    # -------------------------
    # Read-relative
    # -------------------------
    def read_relative_user_columns(self, base_rid: int, relative_version: int) -> List[int]:
        if self.is_deleted_rid(base_rid):
            raise KeyError("record deleted")

        base_full = self.read_physical_record(base_rid)
        ind = int(base_full[INDIRECTION_COLUMN])

        latest = [int(base_full[HIDDEN_COLS + i]) for i in range(self.num_columns)]
        cur = ind

        seen_mask = 0
        all_mask = (1 << self.num_columns) - 1
        version = 0
        steps_back = abs(int(relative_version))

        while cur != 0 and seen_mask != all_mask:
            tail_full = self.read_physical_record(cur)
            schema = int(tail_full[SCHEMA_ENCODING_COLUMN])
            with self._meta_lock:
                tail_deleted = bool(self._deleted.get(int(cur), False))
            if version >= steps_back and (not tail_deleted):
                for i in range(self.num_columns):
                    bit = 1 << i
                    if (schema & bit) and not (seen_mask & bit):
                        latest[i] = int(tail_full[HIDDEN_COLS + i])
                        seen_mask |= bit
            cur = int(tail_full[INDIRECTION_COLUMN])
            version += 1

        return latest

    def read_relative_user_value(self, base_rid: int, user_col: int, relative_version: int) -> int:
        return int(self.read_relative_user_columns(base_rid, relative_version)[int(user_col)])
        # -------------------------
    # Update apply
    # -------------------------
    def apply_update(self, base_rid: int, new_user_cols: List[Optional[int]], prev_latest: Optional[List[int]] = None) -> int:
        br = int(base_rid)

        # 1. 先在 _meta_lock 内一次性取出所有需要的元数据
        with self._meta_lock:
            if bool(self._deleted.get(br, False)):
                raise KeyError("record deleted")
            if len(new_user_cols) != self.num_columns:
                raise ValueError("wrong number of columns")
            cached = self._latest_cache.get(br) if prev_latest is None else None
            loc_base = self.page_directory.get(br)
            if loc_base is None or loc_base.is_tail:
                raise KeyError("base rid not found in page directory")
            pr = self.page_ranges[loc_base.page_range_id]
            pr_id = loc_base.page_range_id
            next_tail_rid = self._next_tail_rid
            self._next_tail_rid -= 1

        if prev_latest is None:
            prev_latest = list(cached) if cached is not None else self.read_latest_user_columns(br)

        schema = 0
        for i, v in enumerate(new_user_cols):
            if v is not None:
                schema |= (1 << i)
        if schema == 0:
            return 0

        tail_rid = next_tail_rid

        # 2. 在 pr._lock 内一次性完成：读 indirection/schema + 写 tail record + 更新 base indirection/schema
        with pr._lock:
            # 预计算 base 页的 pid（避免 4 次 _pid 调用）
            base_pid_ind = pr._pid(False, INDIRECTION_COLUMN, loc_base.page_id)
            base_pid_sch = pr._pid(False, SCHEMA_ENCODING_COLUMN, loc_base.page_id)

            # 批量读 base 的 indirection 和 schema
            base_meta_pages = pr.buffer_pool.fetch_many(
                [base_pid_ind, base_pid_sch]
            ) if pr.buffer_pool is not None else [
                pr.base_pages[INDIRECTION_COLUMN][loc_base.page_id],
                pr.base_pages[SCHEMA_ENCODING_COLUMN][loc_base.page_id],
            ]
            prev_tail = int(base_meta_pages[0].read(loc_base.offset))
            base_schema = int(base_meta_pages[1].read(loc_base.offset))
            if pr.buffer_pool is not None:
                pr.buffer_pool.unpin_many([
                    (base_pid_ind, False),
                    (base_pid_sch, False),
                ])

            # 写 tail record
            page_id, offset = pr.alloc_tail_slot()
            full = [0] * self.num_columns_total
            full[INDIRECTION_COLUMN] = int(prev_tail)
            full[RID_COLUMN] = int(tail_rid)
            full[TIMESTAMP_COLUMN] = 0
            full[SCHEMA_ENCODING_COLUMN] = int(schema)
            for i, v in enumerate(new_user_cols):
                full[HIDDEN_COLS + i] = 0 if v is None else int(v)
            pages = pr.get_pages_batch(True, page_id, self.num_columns_total)
            for col in range(self.num_columns_total):
                off = pages[col].write(full[col])
                if off != offset:
                    pr.release_pages_batch(True, page_id, self.num_columns_total, dirty=True)
                    raise RuntimeError("Tail offset misalignment")
            pr.release_pages_batch(True, page_id, self.num_columns_total, dirty=True)

            # 批量写 base 的 indirection 和 schema
            base_write_pages = pr.buffer_pool.fetch_many(
                [base_pid_ind, base_pid_sch]
            ) if pr.buffer_pool is not None else [
                pr.base_pages[INDIRECTION_COLUMN][loc_base.page_id],
                pr.base_pages[SCHEMA_ENCODING_COLUMN][loc_base.page_id],
            ]
            base_write_pages[0].overwrite(loc_base.offset, int(tail_rid))
            base_write_pages[1].overwrite(loc_base.offset, int(base_schema | schema))
            if pr.buffer_pool is not None:
                pr.buffer_pool.unpin_many([
                    (base_pid_ind, True),
                    (base_pid_sch, True),
                ])

            tail_pages_now = len(pr.tail_pages[0])

        tail_loc = RecordLocator(pr_id, True, page_id, offset)

        # 3. 在 _meta_lock 内一次性更新所有元数据
        with self._meta_lock:
            self.page_directory[int(tail_rid)] = tail_loc
            if not bool(self._deleted.get(br, False)):
                new_latest = list(prev_latest)
                for i, v in enumerate(new_user_cols):
                    if v is not None:
                        new_latest[i] = int(v)
                self._latest_cache[br] = new_latest

            last = self._last_merge_tail_pages.get(pr_id, 0)
            should_trigger_merge = (
                self.buffer_pool is not None
                and (tail_pages_now - last) >= self.MERGE_TRIGGER_EVERY_N_TAIL_PAGES
            )
            if should_trigger_merge:
                self._last_merge_tail_pages[pr_id] = tail_pages_now

        if should_trigger_merge:
            self.request_merge(pr_id)

        # 返回 (tail_rid, prev_tail, base_schema, prev_latest) 供调用方构建 undo log，避免重复读
        return int(tail_rid), int(prev_tail), int(base_schema), list(prev_latest)

    # =========================================================
    # Snapshot helpers（后台 merge 专用：直接读磁盘，不 pin / unpin）
    # =========================================================
    def _snapshot_get_page(self, pid, snap_cache: Dict) -> Page:
        p = snap_cache.get(pid)
        if p is not None:
            return p

        if self.buffer_pool is None:
            raise RuntimeError("snapshot disk read requires buffer_pool")

        raw = self.buffer_pool.read_page_bytes(pid)

        if isinstance(pid, tuple) and len(pid) >= 5:
            is_tail = bool(pid[2])
            col = int(pid[-2])
        else:
            is_tail = False
            col = 0

        page = Page(col, is_tail=is_tail)
        page.from_bytes(raw)

        snap_cache[pid] = page
        return page

    def _snapshot_read_record(self, rid: int, base_ver: int, snap_cache: Dict) -> List[int]:
        with self._meta_lock:
            loc = self.page_directory.get(int(rid))
            if loc is None:
                raise KeyError("RID not found")
            pr_id = int(loc.page_range_id)

        out = [0] * self.num_columns_total
        for col in range(self.num_columns_total):
            if loc.is_tail:
                pid = (self.name, pr_id, True, int(col), int(loc.page_id))
            else:
                pid = (self.name, pr_id, False, int(base_ver), int(col), int(loc.page_id))
            page = self._snapshot_get_page(pid, snap_cache)
            out[col] = page.read(int(loc.offset))

        return out

    # -------------------------
    # Merge helpers (snapshot path)
    # -------------------------
    def _read_latest_user_columns_from_snapshot(
        self,
        base_rid: int,
        snapshot_indirection: int,
        base_ver_snapshot: int,
        snap_cache: Dict,
    ) -> List[int]:
        if self.is_deleted_rid(base_rid):
            raise KeyError("record deleted")

        base_full = self._snapshot_read_record(int(base_rid), int(base_ver_snapshot), snap_cache)
        latest = [int(base_full[HIDDEN_COLS + i]) for i in range(self.num_columns)]

        cur = int(snapshot_indirection)
        if cur == 0:
            return latest

        seen_mask = 0
        all_mask = (1 << self.num_columns) - 1

        while cur != 0 and seen_mask != all_mask:
            tail_full = self._snapshot_read_record(int(cur), int(base_ver_snapshot), snap_cache)
            schema = int(tail_full[SCHEMA_ENCODING_COLUMN])
            with self._meta_lock:
                tail_deleted = bool(self._deleted.get(int(cur), False))
            if not tail_deleted:
                for i in range(self.num_columns):
                    bit = 1 << i
                    if (schema & bit) and not (seen_mask & bit):
                        latest[i] = int(tail_full[HIDDEN_COLS + i])
                        seen_mask |= bit
            cur = int(tail_full[INDIRECTION_COLUMN])

        return latest

    def _merge_blocked_by_writer(self, base_rid: int) -> bool:
        lm = getattr(self, "lock_manager", None)
        if lm is None:
            return False

        checker = getattr(lm, "has_x_lock", None)
        if checker is None:
            return False

        rid_res = ("RID", self.name, int(base_rid))
        pk_res = None

        try:
            latest = self.read_latest_user_columns(int(base_rid))
            pk_res = ("PK", self.name, int(latest[self.key]))
        except Exception:
            pk_res = None

        try:
            if checker(rid_res):
                return True
            if pk_res is not None and checker(pk_res):
                return True
            return False
        except Exception:
            return False

    def _merge_worker(
        self,
        page_range_id: int,
        snapshot_indirections: Dict[int, int],
        base_ver_snapshot: int,
        tps_snapshot: int,
    ) -> None:
        page_range_id = int(page_range_id)
        base_ver_snapshot = int(base_ver_snapshot)
        tps_snapshot = int(tps_snapshot)

        snap_cache: Dict = {}
        merged_values: Dict[int, List[int]] = {}
        new_tps = 0

        for br, ind in snapshot_indirections.items():
            ind = int(ind)
            if ind != 0:
                new_tps = ind if new_tps == 0 else min(new_tps, ind)

        for br, ind in snapshot_indirections.items():
            br = int(br)
            if self.is_deleted_rid(br):
                continue
            try:
                merged_values[br] = self._read_latest_user_columns_from_snapshot(
                    br, int(ind), base_ver_snapshot, snap_cache
                )
            except KeyError:
                continue

        with self._meta_lock:
            pr = self.page_ranges[page_range_id]
            next_version = int(pr.base_version) + 1

        with self._merge_lock:
            self._merge_results[page_range_id] = MergeResult(
                page_range_id=page_range_id,
                merged_values=merged_values,
                new_tps=int(new_tps),
                new_version=int(next_version),
            )
            self._merge_threads.pop(page_range_id, None)

    def request_merge(self, page_range_id: int) -> None:
        page_range_id = int(page_range_id)

        if self.buffer_pool is None:
            return

        with self._merge_lock:
            if page_range_id in self._merge_threads:
                return
            if page_range_id in self._merge_results:
                return

        with self._meta_lock:
            if page_range_id >= len(self.page_ranges):
                return
            pr = self.page_ranges[page_range_id]
            base_ver_snapshot = int(pr.base_version)
            tps_snapshot = int(pr.tps)
            base_rids = [
                int(rid)
                for rid, loc in self.page_directory.items()
                if (not loc.is_tail)
                and int(loc.page_range_id) == page_range_id
                and (not bool(self._deleted.get(int(rid), False)))
            ]

        snapshot: Dict[int, int] = {}
        blocked = False
        for rid in base_rids:
            if self._merge_blocked_by_writer(int(rid)):
                blocked = True
                break
            base_full = self.read_physical_record(int(rid))
            if self._merge_blocked_by_writer(int(rid)):
                blocked = True
                break
            ind = int(base_full[INDIRECTION_COLUMN])
            with self._meta_lock:
                loc = self.page_directory.get(int(rid))
                if loc is None or loc.is_tail:
                    continue
                if int(loc.page_range_id) != page_range_id:
                    continue
                if bool(self._deleted.get(int(rid), False)):
                    continue
            snapshot[int(rid)] = ind

        if blocked:
            return
        if not snapshot:
            return
        if all(int(v) == 0 for v in snapshot.values()):
            return

        th = threading.Thread(
            target=self._merge_worker,
            args=(page_range_id, snapshot, base_ver_snapshot, tps_snapshot),
            daemon=True,
        )
        with self._merge_lock:
            self._merge_threads[page_range_id] = th
        th.start()

    # -------------------------
    # Apply merge
    # -------------------------
    def apply_merge_if_ready(self, page_range_id: int) -> None:
        page_range_id = int(page_range_id)
        with self._merge_lock:
            res = self._merge_results.get(page_range_id)
        if res is None:
            return

        lock = self._bp_lock
        if lock is None:
            return self._apply_merge_without_bp(page_range_id, res)

        with self._meta_lock:
            if page_range_id >= len(self.page_ranges):
                with self._merge_lock:
                    self._merge_results.pop(page_range_id, None)
                return

            pr = self.page_ranges[page_range_id]
            old_ver = int(pr.base_version)
            new_ver = int(res.new_version)

            num_pages_per_col = [len(pr.base_pages[col]) for col in range(pr.num_columns_total)]
            with pr._lock:
                for col in range(pr.num_columns_total):
                    for page_id in range(num_pages_per_col[col]):
                        pr.base_version = old_ver
                        old_page = pr.get_page(False, col, page_id)

                        pr.base_version = new_ver
                        new_page = pr.get_page(False, col, page_id)

                        n = int(getattr(old_page, "num_records", 0))
                        for i in range(n):
                            new_page.write(old_page.read(i))

                        pr.base_version = old_ver
                        pr.release_page(False, col, page_id, dirty=False)

                        pr.base_version = new_ver
                        pr.release_page(False, col, page_id, dirty=True)

            writes: Dict[Tuple[int, int], List[Tuple[int, int]]] = {}
            for br, vals in res.merged_values.items():
                loc = self.page_directory.get(int(br))
                if loc is None or loc.is_tail or int(loc.page_range_id) != page_range_id:
                    continue
                pid = int(loc.page_id)
                off = int(loc.offset)
                for i, v in enumerate(vals):
                    col_idx = HIDDEN_COLS + i
                    writes.setdefault((int(col_idx), int(pid)), []).append((off, int(v)))

            with pr._lock:
                for (col_idx, pid), pairs in writes.items():
                    pr.base_version = new_ver
                    page = pr.get_page(False, int(col_idx), int(pid))
                    for off, v in pairs:
                        page.overwrite(int(off), int(v))
                    pr.release_page(False, int(col_idx), int(pid), dirty=True)

            pr.base_version = new_ver
            pr.tps = int(res.new_tps)

        with self._merge_lock:
            self._merge_results.pop(page_range_id, None)

    def _apply_merge_without_bp(self, page_range_id: int, res: MergeResult) -> None:
        with self._meta_lock:
            if page_range_id >= len(self.page_ranges):
                with self._merge_lock:
                    self._merge_results.pop(page_range_id, None)
                return
            pr = self.page_ranges[page_range_id]
            pr.base_version = int(res.new_version)
            pr.tps = int(res.new_tps)
        with self._merge_lock:
            self._merge_results.pop(page_range_id, None)

    def apply_all_merges_if_ready(self) -> None:
        with self._merge_lock:
            ready_ids = list(self._merge_results.keys())
        for pr_id in ready_ids:
            self.apply_merge_if_ready(int(pr_id))
        # -------------------------
    # Persistence
    # -------------------------
    def to_metadata(self) -> dict:
        with self._meta_lock:
            page_dir = {
                str(int(rid)): [int(loc.page_range_id), bool(loc.is_tail), int(loc.page_id), int(loc.offset)]
                for rid, loc in self.page_directory.items()
            }

            prs = []
            for pr in self.page_ranges:
                prs.append(
                    {
                        "tps": int(pr.tps),
                        "next_base_page_id": int(pr._next_base_page_id),
                        "next_base_offset": int(pr._next_base_offset),
                        "next_tail_page_id": int(pr._next_tail_page_id),
                        "next_tail_offset": int(pr._next_tail_offset),
                        "base_version": int(pr.base_version),
                    }
                )

            return {
                "name": self.name,
                "num_columns": int(self.num_columns),
                "key": int(self.key),
                "next_base_rid": int(self._next_base_rid),
                "next_tail_rid": int(self._next_tail_rid),
                "key2rid": {str(int(k)): int(v) for k, v in self.key2rid.items()},
                "deleted": {str(int(rid)): bool(v) for rid, v in self._deleted.items()},
                "page_directory": page_dir,
                "page_ranges": prs,
            }

    @classmethod
    def from_metadata(cls, meta: dict, buffer_pool: Optional[BufferPool] = None) -> "Table":
        t = cls(
            name=meta["name"],
            num_columns=int(meta["num_columns"]),
            key=int(meta["key"]),
            buffer_pool=buffer_pool,
        )

        t._next_base_rid = int(meta.get("next_base_rid", 1))
        t._next_tail_rid = int(meta.get("next_tail_rid", (1 << 64) - 1))
        t.key2rid = {int(k): int(v) for k, v in meta.get("key2rid", {}).items()}
        t._deleted = {int(k): bool(v) for k, v in meta.get("deleted", {}).items()}

        t.page_ranges = []
        for idx, prm in enumerate(meta.get("page_ranges", [])):
            pr = PageRange(t.num_columns_total, t.name, idx, buffer_pool, t._bp_lock)
            pr.tps = int(prm.get("tps", 0))
            pr._next_base_page_id = int(prm.get("next_base_page_id", 0))
            pr._next_base_offset = int(prm.get("next_base_offset", 0))
            pr._next_tail_page_id = int(prm.get("next_tail_page_id", 0))
            pr._next_tail_offset = int(prm.get("next_tail_offset", 0))
            pr.base_version = int(prm.get("base_version", 0))
            t.page_ranges.append(pr)

        t.page_directory = {}
        for rid_s, arr in meta.get("page_directory", {}).items():
            rid = int(rid_s)
            pr_id, is_tail, page_id, offset = arr
            t.page_directory[rid] = RecordLocator(
                page_range_id=int(pr_id),
                is_tail=bool(is_tail),
                page_id=int(page_id),
                offset=int(offset),
            )

        t._base_rid_list = [
            int(rid) for rid, loc in t.page_directory.items() if not loc.is_tail
        ]

        from lstore.index import Index
        t.index = Index(t)

        t._tail_update_count = {}
        t._last_merge_tail_pages = {}

        return t
