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
        # tail pid: (table, pr, True, col, pid)
        if bool(is_tail):
            return (self.table_name, self.page_range_id, True, int(col), int(page_id))
        # base pid: (table, pr, False, base_ver, col, pid)
        return (self.table_name, self.page_range_id, False, int(self.base_version), int(col), int(page_id))

    def get_page(self, is_tail: bool, col: int, page_id: int) -> Page:
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
        if self.buffer_pool is None:
            return

        if self.bp_lock is None:
            self.buffer_pool.unpin_page(self._pid(is_tail, col, page_id), is_dirty=bool(dirty))
            return

        with self.bp_lock:
            self.buffer_pool.unpin_page(self._pid(is_tail, col, page_id), is_dirty=bool(dirty))

    def alloc_base_slot(self) -> Tuple[int, int]:
        # 关键：不要用 Page.num_records / has_capacity 判断满页（reopen 时会是 0）
        if self._next_base_offset >= Page.CAPACITY:
            self._next_base_page_id += 1
            self._next_base_offset = 0

        page_id = self._next_base_page_id
        offset = self._next_base_offset

        # 确保结构存在（不参与 capacity 判断）
        self._ensure_base_page(0, page_id)

        self._next_base_offset += 1
        return (page_id, offset)

    def alloc_tail_slot(self) -> Tuple[int, int]:
        if self._next_tail_offset >= Page.CAPACITY:
            self._next_tail_page_id += 1
            self._next_tail_offset = 0

        page_id = self._next_tail_page_id
        offset = self._next_tail_offset

        self._ensure_tail_page(0, page_id)

        self._next_tail_offset += 1
        return (page_id, offset)


class Table:
    """
    目标：contention-free merge（改 Table，不改 PageRange）
      - 前台：仍走 bufferpool(fetch/unpin)，正常读写
      - 后台 merge worker：只读磁盘 raw bytes -> Page.from_bytes，不 pin/unpin，不抢 _bp_lock
      - apply_merge_if_ready：页级短锁 + 发布短锁（发布 new base_version + TPS）
    """

    MERGE_TRIGGER_EVERY_N_TAIL_PAGES = 10  # grading config

    def __init__(self, name: str, num_columns: int, key: int, buffer_pool: Optional[BufferPool] = None):
        self.name = str(name)
        self.num_columns = int(num_columns)
        self.key = int(key)
        self.num_columns_total = HIDDEN_COLS + self.num_columns

        self.page_ranges: List[PageRange] = []
        self.page_directory: Dict[int, RecordLocator] = {}

        self._next_base_rid: int = 1
        self._next_tail_rid: int = (1 << 64) - 1  # decreasing tail rid

        self.key2rid: Dict[int, int] = {}
        self._deleted: Dict[int, bool] = {}

        from lstore.index import Index
        self.index = Index(self)

        self.buffer_pool = buffer_pool

        # BufferPool is not thread-safe
        self._bp_lock: Optional[threading.RLock] = threading.RLock() if self.buffer_pool is not None else None

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
        rid = self._next_base_rid
        self._next_base_rid += 1
        return int(rid)

    def alloc_tail_rid(self) -> int:
        rid = self._next_tail_rid
        self._next_tail_rid -= 1
        return int(rid)

    # -------------------------
    # PageRange mgmt
    # -------------------------
    def _ensure_page_range(self, page_range_id: int) -> PageRange:
        page_range_id = int(page_range_id)
        while len(self.page_ranges) <= page_range_id:
            new_id = len(self.page_ranges)
            self.page_ranges.append(
                PageRange(self.num_columns_total, self.name, new_id, self.buffer_pool, self._bp_lock)
            )
        return self.page_ranges[page_range_id]

    def _choose_page_range_for_insert(self) -> int:
        if not self.page_ranges:
            self.page_ranges.append(PageRange(self.num_columns_total, self.name, 0, self.buffer_pool, self._bp_lock))
        return len(self.page_ranges) - 1

    # -------------------------
    # Delete helpers
    # -------------------------
    def is_deleted_rid(self, base_rid: int) -> bool:
        return bool(self._deleted.get(int(base_rid), False))

    def mark_deleted(self, base_rid: int) -> None:
        self._deleted[int(base_rid)] = True

    # -------------------------
    # Physical write
    # -------------------------
    def write_base_record(self, base_rid: int, user_columns: List[int]) -> RecordLocator:
        if len(user_columns) != self.num_columns:
            raise ValueError("wrong number of user columns")

        pr_id = self._choose_page_range_for_insert()
        pr = self._ensure_page_range(pr_id)
        page_id, offset = pr.alloc_base_slot()

        full = [0] * self.num_columns_total
        full[INDIRECTION_COLUMN] = 0
        full[RID_COLUMN] = int(base_rid)
        full[TIMESTAMP_COLUMN] = 0
        full[SCHEMA_ENCODING_COLUMN] = 0
        for i, v in enumerate(user_columns):
            full[HIDDEN_COLS + i] = int(v)

        for col in range(self.num_columns_total):
            page = pr.get_page(False, col, page_id)
            off = page.write(full[col])
            if off != offset:
                pr.release_page(False, col, page_id, dirty=True)
                raise RuntimeError("Base offset misalignment")
            pr.release_page(False, col, page_id, dirty=True)

        loc = RecordLocator(pr_id, False, page_id, offset)
        self.page_directory[int(base_rid)] = loc

        pk = int(user_columns[self.key])
        self.key2rid[pk] = int(base_rid)
        self._deleted[int(base_rid)] = False
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

        base_loc = self.page_directory.get(int(base_rid))
        if base_loc is None or base_loc.is_tail:
            raise KeyError("base rid not found")

        pr = self.page_ranges[base_loc.page_range_id]
        page_id, offset = pr.alloc_tail_slot()

        full = [0] * self.num_columns_total
        full[INDIRECTION_COLUMN] = int(prev_tail_rid)  # tail chain -> older tail
        full[RID_COLUMN] = int(tail_rid)
        full[TIMESTAMP_COLUMN] = 0
        full[SCHEMA_ENCODING_COLUMN] = int(schema_encoding)
        for i, v in enumerate(user_columns):
            full[HIDDEN_COLS + i] = 0 if v is None else int(v)

        for col in range(self.num_columns_total):
            page = pr.get_page(True, col, page_id)
            off = page.write(full[col])
            if off != offset:
                pr.release_page(True, col, page_id, dirty=True)
                raise RuntimeError("Tail offset misalignment")
            pr.release_page(True, col, page_id, dirty=True)

        loc = RecordLocator(base_loc.page_range_id, True, page_id, offset)
        self.page_directory[int(tail_rid)] = loc
        return loc

    # -------------------------
    # Physical read (front-end path)
    # -------------------------
    def read_physical_record(self, rid: int) -> List[int]:
        loc = self.page_directory.get(int(rid))
        if loc is None:
            raise KeyError("RID not found")

        pr = self.page_ranges[loc.page_range_id]
        out = [0] * self.num_columns_total

        for col in range(self.num_columns_total):
            page = pr.get_page(loc.is_tail, col, loc.page_id)
            out[col] = page.read(loc.offset)
            pr.release_page(loc.is_tail, col, loc.page_id, dirty=False)

        return out

    # -------------------------
    # Base metadata overwrite
    # -------------------------
    def overwrite_base_indirection(self, base_rid: int, new_tail_rid: int) -> None:
        loc = self.page_directory[int(base_rid)]
        if loc.is_tail:
            raise ValueError("base_rid points to tail")

        pr = self.page_ranges[loc.page_range_id]
        page = pr.get_page(False, INDIRECTION_COLUMN, loc.page_id)
        page.overwrite(loc.offset, int(new_tail_rid))
        pr.release_page(False, INDIRECTION_COLUMN, loc.page_id, dirty=True)

    def overwrite_base_schema(self, base_rid: int, new_schema: int) -> None:
        loc = self.page_directory[int(base_rid)]
        if loc.is_tail:
            raise ValueError("base_rid points to tail")

        pr = self.page_ranges[loc.page_range_id]
        page = pr.get_page(False, SCHEMA_ENCODING_COLUMN, loc.page_id)
        page.overwrite(loc.offset, int(new_schema))
        pr.release_page(False, SCHEMA_ENCODING_COLUMN, loc.page_id, dirty=True)

    # -------------------------
    # TPS
    # -------------------------
    def get_tps(self, page_range_id: int) -> int:
        return int(self.page_ranges[int(page_range_id)].tps)

    def set_tps(self, page_range_id: int, tps: int) -> None:
        self.page_ranges[int(page_range_id)].tps = int(tps)

    # -------------------------
    # Helpers
    # -------------------------
    def all_base_rids(self) -> List[int]:
        rids = []
        for rid, loc in self.page_directory.items():
            if not loc.is_tail:
                rids.append(int(rid))
        rids.sort()
        return rids

    def _base_latest_tail_rid(self, base_rid: int) -> int:
        base = self.read_physical_record(base_rid)
        return int(base[INDIRECTION_COLUMN])

    def _base_schema(self, base_rid: int) -> int:
        base = self.read_physical_record(base_rid)
        return int(base[SCHEMA_ENCODING_COLUMN])

    def _base_page_range_id(self, base_rid: int) -> int:
        loc = self.page_directory[int(base_rid)]
        return int(loc.page_range_id)

    # -------------------------
    # Read-latest
    # -------------------------
    def read_latest_user_columns(self, base_rid: int) -> List[int]:
        if self.is_deleted_rid(base_rid):
            raise KeyError("record deleted")

        base_full = self.read_physical_record(base_rid)
        pr_id = self._base_page_range_id(base_rid)
        tps = self.get_tps(pr_id)
        ind = int(base_full[INDIRECTION_COLUMN])

        latest = [int(base_full[HIDDEN_COLS + i]) for i in range(self.num_columns)]
        if ind == 0:
            return latest

        # decreasing tail rids:
        # if ind >= TPS, then latest tail rid at/older-than TPS is already merged onto base
        if tps != 0 and ind >= tps:
            return latest

        cur = ind
        seen_mask = 0
        all_mask = (1 << self.num_columns) - 1

        while cur != 0 and seen_mask != all_mask:
            tail_full = self.read_physical_record(cur)
            schema = int(tail_full[SCHEMA_ENCODING_COLUMN])

            for i in range(self.num_columns):
                bit = 1 << i
                if (schema & bit) and not (seen_mask & bit):
                    latest[i] = int(tail_full[HIDDEN_COLS + i])
                    seen_mask |= bit

            cur = int(tail_full[INDIRECTION_COLUMN])  # older tail

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

            if version >= steps_back:
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
    # Update apply (trigger background merge)
    # -------------------------
    def apply_update(self, base_rid: int, new_user_cols: List[Optional[int]]) -> int:
        if self.is_deleted_rid(base_rid):
            raise KeyError("record deleted")
        if len(new_user_cols) != self.num_columns:
            raise ValueError("wrong number of columns")

        schema = 0
        for i, v in enumerate(new_user_cols):
            if v is not None:
                schema |= (1 << i)
        if schema == 0:
            return 0

        prev_tail = self._base_latest_tail_rid(base_rid)
        tail_rid = self.alloc_tail_rid()

        self.write_tail_record(
            tail_rid=tail_rid,
            base_rid=base_rid,
            prev_tail_rid=prev_tail,
            schema_encoding=schema,
            user_columns=new_user_cols,
        )

        self.overwrite_base_indirection(base_rid, tail_rid)
        base_schema = self._base_schema(base_rid)
        self.overwrite_base_schema(base_rid, base_schema | schema)

        # --------- trigger background merge (non-blocking) ----------
        pr_id = self._base_page_range_id(base_rid)
        pr = self.page_ranges[pr_id]

        # tail page count (col0 as reference)
        tail_pages_now = len(pr.tail_pages[0])
        last = self._last_merge_tail_pages.get(pr_id, 0)

        if self.buffer_pool is not None and (tail_pages_now - last) >= self.MERGE_TRIGGER_EVERY_N_TAIL_PAGES:
            self._last_merge_tail_pages[pr_id] = tail_pages_now
            self.request_merge(pr_id)

        return int(tail_rid)

    # =========================================================
    # Snapshot helpers (后台 merge 专用：直读磁盘，不 pin/unpin)
    # =========================================================
    def _snapshot_get_page(self, pid, snap_cache: Dict) -> Page:
        p = snap_cache.get(pid)
        if p is not None:
            return p

        if self.buffer_pool is None:
            raise RuntimeError("snapshot disk read requires buffer_pool")

        # ✅ 直读磁盘 bytes，不走 page_table，不 pin
        raw = self.buffer_pool.read_page_bytes(pid)

        # pid: base=(name, pr, False, ver, col, page_id)  tail=(name, pr, True, col, page_id)
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

            for i in range(self.num_columns):
                bit = 1 << i
                if (schema & bit) and not (seen_mask & bit):
                    latest[i] = int(tail_full[HIDDEN_COLS + i])
                    seen_mask |= bit

            cur = int(tail_full[INDIRECTION_COLUMN])

        return latest

    def _merge_worker(
        self,
        page_range_id: int,
        snapshot_indirections: Dict[int, int],
        base_ver_snapshot: int,
        tps_snapshot: int,
    ) -> None:
        page_range_id = int(page_range_id)
        base_ver_snapshot = int(base_ver_snapshot)
        tps_snapshot = int(tps_snapshot)  # 目前不强依赖，但保留接口以便你做裁剪

        snap_cache: Dict = {}  # pid -> Page (private snapshot cache)

        merged_values: Dict[int, List[int]] = {}
        new_tps = 0

        # decreasing tail rids: TPS boundary = min(snapshot_indirection)
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

        # If DB was not opened(), buffer_pool is None (pure in-memory mode).
        # In that mode, background merge that reads from disk is not applicable.
        if self.buffer_pool is None:
            return

        with self._merge_lock:
            if page_range_id in self._merge_threads:
                return
            if page_range_id in self._merge_results:
                return

        pr = self.page_ranges[page_range_id]
        base_ver_snapshot = int(pr.base_version)
        tps_snapshot = int(pr.tps)

        snapshot: Dict[int, int] = {}
        for rid, loc in self.page_directory.items():
            if loc.is_tail:
                continue
            if int(loc.page_range_id) != page_range_id:
                continue
            if self.is_deleted_rid(int(rid)):
                continue
            base_full = self.read_physical_record(int(rid))  # 前台快照阶段允许走 bufferpool
            snapshot[int(rid)] = int(base_full[INDIRECTION_COLUMN])

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
    # Apply merge (页级短锁 + 发布短锁)
    # -------------------------
    def apply_merge_if_ready(self, page_range_id: int) -> None:
        page_range_id = int(page_range_id)
        with self._merge_lock:
            res = self._merge_results.get(page_range_id)
        if res is None:
            return

        pr = self.page_ranges[page_range_id]
        old_ver = int(pr.base_version)
        new_ver = int(res.new_version)

        lock = self._bp_lock
        if lock is None:
            return self._apply_merge_without_bp(page_range_id, res)

        # 1) clone old base pages -> new version pages (每页短锁)
        num_pages_per_col = [len(pr.base_pages[col]) for col in range(pr.num_columns_total)]
        for col in range(pr.num_columns_total):
            for page_id in range(num_pages_per_col[col]):
                with lock:
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

        # 2) batch writes (纯内存，不加锁)
        writes: Dict[Tuple[int, int], List[Tuple[int, int]]] = {}
        for br, vals in res.merged_values.items():
            loc = self.page_directory.get(int(br))
            if loc is None or loc.is_tail or int(loc.page_range_id) != page_range_id:
                continue
            pid = int(loc.page_id)
            off = int(loc.offset)
            for i, v in enumerate(vals):
                col = HIDDEN_COLS + i
                writes.setdefault((int(col), int(pid)), []).append((off, int(v)))

        # 3) apply merged values on new version (每页短锁)
        for (col, pid), pairs in writes.items():
            with lock:
                pr.base_version = new_ver
                page = pr.get_page(False, int(col), int(pid))
                for off, v in pairs:
                    page.overwrite(int(off), int(v))
                pr.release_page(False, int(col), int(pid), dirty=True)

        # 4) publish swap + TPS（短锁）
        with lock:
            pr.base_version = new_ver
            self.set_tps(page_range_id, int(res.new_tps))

        with self._merge_lock:
            self._merge_results.pop(page_range_id, None)

    def _apply_merge_without_bp(self, page_range_id: int, res: MergeResult) -> None:
        pr = self.page_ranges[page_range_id]
        pr.base_version = int(res.new_version)
        self.set_tps(page_range_id, int(res.new_tps))
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

        from lstore.index import Index
        t.index = Index(t)

        # restore safe defaults
        t._tail_update_count = {}
        t._last_merge_tail_pages = {}

        return t
