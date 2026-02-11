from __future__ import annotations
from dataclasses import dataclass
from bisect import bisect_left, bisect_right
from array import array
from typing import Dict, List, Optional
from lstore.index import Index

# ---------------------------------------------------------------------------
# Compatibility constants (starter-code expects these symbols).
# 这些常量主要是为了兼容 starter code（不然有的地方 import 会炸）
# 实际这套优化实现里基本不靠它们干活
# ---------------------------------------------------------------------------
# used to avoid import errors
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid # record id（这条记录的 rid）
        self.key = key # primary key（主键值）
        self.columns = columns # List of column values（这一行的列数据）

    def __getitem__(self, idx):
        # 方便外面用 record[i] 这种写法取第 i 列
        return self.columns[idx]


@dataclass
class RecordLocator:
    """Compatibility shim for older designs (unused in optimized path).
    兼容老版本设计用的，现在这个优化路径基本不用它
    """
    is_tail: bool
    page_range_id: int
    offset: int


class _Fenwick:
    """BIT supporting point-update + prefix-sum.
    1-indexed internal tree. 这个就是 BIT（Fenwick Tree），主要给 sum_range 加速用的,内部用 1-index，所以别用 0 去访问
    """
    __slots__ = ("n", "tree")
    def __init__(self, n: int = 0):
        self.n = int(n)
        # tree[0] 空着，从 1 开始用（BIT 的老规矩）
        self.tree: List[int] = [0] * (self.n + 1)
    def build_from(self, values: List[int]) -> None:
        # O(n) build（直接线性建树，比一个个 add 更快）
        n = len(values)
        self.n = n
        tree = [0] * (n + 1)
        for i, v in enumerate(values, start=1):
            tree[i] += int(v)
            j = i + (i & -i)
            if j <= n:
                tree[j] += tree[i]
        self.tree = tree

    def ensure_size(self, n: int) -> None:
        # 保证 BIT 至少能装下 n 个元素，不够就扩容
        n = int(n)
        if n <= self.n:
            return
        # Extend tree; content for new indices is 0, then caller will add.
        # 扩出来的部分先全是 0，后面由调用者来 add
        self.tree.extend([0] * (n - self.n))
        self.n = n

    def add(self, idx: int, delta: int) -> None:
        # add delta to index（对单点做增量更新）
        n = self.n
        tree = self.tree
        i = int(idx)
        d = int(delta)
        while i <= n:
            tree[i] += d
            i += i & -i  # lowbit 往上跳

    def prefix_sum(self, idx: int) -> int:
        # return sum of elements（1..idx 的前缀和）
        s = 0
        tree = self.tree
        i = int(idx)
        while i > 0:
            s += tree[i]
            i -= i & -i
        return s

    def range_sum(self, left: int, right: int) -> int:
        # inclusive left..right, 1-indexed（区间和，左右都包含）
        if right < left:
            return 0
        return self.prefix_sum(right) - self.prefix_sum(left - 1)


class Table:
    """
    最新数据按列存 + 主键 dict + range 用有序 key + sum 用 BIT 加速，基本差不多了，再有什么想法的话大伙自己加
    """

    def __init__(self, name: str, num_columns: int, key: int):
        self.name = name
        self.num_columns = int(num_columns)
        self.key = int(key)

        # Columnar latest storage. Index 0 unused so rid can be used directly.
        # 列式存储：每列一个 array('q')，并且 0 号位留空，让 rid 直接当索引用
        self.cols: List[array] = [array('q', [0]) for _ in range(self.num_columns)]
        self.alive: List[bool] = [False]  # rid -> alive（逻辑删除用，False 就当不存在）

        # Version history (Milestone 2/3).
        # versions[rid] is a list of full-row snapshots (each snapshot is List[int]).
        # The latest version is versions[rid][-1].
        # 版本历史：M1 不一定用得上，但接口先留着（后面版本查询要用）
        self.versions: List[List[List[int]]] = [[]]  # index 0 unused

        # Primary index & key order
        # key2rid 是主键索引；sorted_keys / sorted_rids 是为了 range 扫描
        self.key2rid: Dict[int, int] = {}
        self.sorted_keys: List[int] = []
        self.sorted_rids: List[int] = []
        self.rid2pos: Dict[int, int] = {}  # rid -> position (1-indexed) in sorted lists
        self._needs_compact: bool = False  # 有删除后先不立刻清理，等需要时再压缩
        self._last_key: Optional[int] = None  # 记录最后插入的 key，方便走 append 快路径

        # Range-sum acceleration over key-order positions
        # 每列一个 BIT：按“key 排序后的顺序”建树，sum_range 才能 logn
        self._bits: List[_Fenwick] = [_Fenwick(0) for _ in range(self.num_columns)]
        self._bit_valid: bool = True  # when False, rebuild before using sum/update-delta（失效就重建）

        # RID allocation
        self._next_rid = 1  # 下一条新记录的 rid，从 1 开始

        # Index wrapper (compat)
        # 兼容 starter 的 Index 接口（内部会用到 register/unregister 等）
        self.index = Index(self)

    # -----------------
    # RID helpers
    # -----------------
    def alloc_base_rid(self) -> int:
        # 分配一个新的 rid，并把各个结构的长度都同步一下
        rid = self._next_rid
        self._next_rid += 1
        self.alive.append(True)
        self.versions.append([])
        for c in self.cols:
            c.append(0)  # 先占位，后面 insert 再写真实值
        return rid

    # -----------------
    # Core row/col ops
    # -----------------
    def set_row(self, rid: int, row: List[int]) -> None:
        # Set full row for rid (used by insert)
        # insert 时用：直接把整行写进列式存储
        for i, v in enumerate(row):
            self.cols[i][rid] = int(v)

        # Initialize version history with the inserted snapshot.
        # Store as tuple to avoid accidental mutation & extra copies.
        # 初始化版本历史：第一版就是插入时的整行快照
        self.versions[rid] = [list(map(int, row))]

    def get_projected_row(self, rid: int, projected_columns: List[int]) -> List[int]:
        # Hot path: bind locals
        # 只取需要的列（select 用），没被选中的列就不管
        cols = self.cols
        out: List[int] = []
        for i, flag in enumerate(projected_columns):
            if flag:
                out.append(int(cols[i][rid]))
        return out

    # -----------------
    # Key order + BIT maintenance
    # -----------------
    def _rebuild_bit_and_pos(self) -> None:
        """Rebuild rid2pos and BITs from current key order (O(n*cols)).

        Also compacts out soft-deleted rows so future scans are faster.

        简单说：BIT 不靠谱/失效了就会进来重建
        顺便把之前“软删除”留下的垃圾数据压缩掉，避免后面 range 扫描慢
        """
        if self._needs_compact:
            sk = self.sorted_keys
            sr = self.sorted_rids
            alive = self.alive
            new_sk = []
            new_sr = []
            # Keep only live rows（只保留还活着的）
            for k, rid in zip(sk, sr):
                if rid < len(alive) and alive[rid] and self.key2rid.get(k) == rid:
                    new_sk.append(k)
                    new_sr.append(rid)
            self.sorted_keys = new_sk
            self.sorted_rids = new_sr
            self._needs_compact = False
            self._last_key = new_sk[-1] if new_sk else None

        rids = self.sorted_rids
        # rid2pos 是给 BIT 用的：rid -> 它在 key-order 中的位置（1-indexed）
        self.rid2pos = {rid: i for i, rid in enumerate(rids, start=1)}
        n = len(rids)
        cols = self.cols
        # 给每一列重建 BIT（用 key-order 下的 values）
        for col in range(self.num_columns):
            vals = [int(cols[col][rid]) for rid in rids]
            self._bits[col].build_from(vals)
        self._bit_valid = True

    def _append_key_fast(self, key: int, rid: int) -> None:
        """Append key/rid assuming key is greater than all existing keys."""
        # 这是最常见的快路径：key 递增插入时，直接 append 就完事
        self.sorted_keys.append(key)
        self.sorted_rids.append(rid)
        pos = len(self.sorted_keys)  # 1-indexed
        self.rid2pos[rid] = pos

        # IMPORTANT:
        # A Fenwick tree cannot be safely "grown" by only appending zeros because
        # new internal nodes (e.g., tree[2], tree[4], ...) must include
        # contributions from earlier indices. To keep correctness simple and fast
        # enough for the testers, we invalidate BIT on structural growth and
        # rebuild lazily on the next SUM.
        # BIT 结构变了（长度变了）就别硬维护了，直接标记失效，下次 sum 再重建
        self._bit_valid = False
        self._last_key = key

    def register_key(self, key: int, rid: int) -> None:
        """Insert key into key-order lists.

        Fast path (common): keys are inserted in increasing order.
        Slow path: mid-list insert (rare) keeps sortedness.
        """
        keys = self.sorted_keys
        lk = self._last_key
        # 如果 key 是递增的，走 append 快路径
        if lk is None or key > lk:
            self._append_key_fast(key, rid)
            return

        # Mid-list insert: keep sorted lists correct, then rebuild BIT lazily
        # 如果有乱序插入，就用 bisect 插入到中间（但 BIT 必须标记失效）
        i = bisect_left(keys, key)
        keys.insert(i, key)
        self.sorted_rids.insert(i, rid)
        self._bit_valid = False  # positions shifted -> must rebuild before BIT use

    def unregister_key(self, key: int) -> None:
        """Soft delete: do NOT remove from sorted lists (avoids O(n)).

        We simply mark that compaction is needed and invalidate BIT.
        Next time we rebuild BIT, we will compact sorted_keys/rids to
        exclude dead rows.
        """
        # 删除时不做 O(n) 真删除，只做“标记”
        # 等到下次需要重建 BIT 时再顺手 compact
        self._needs_compact = True
        self._bit_valid = False

    # -----------------
    # Updates with BIT delta
    # -----------------
    def apply_update(self, rid: int, columns: List[Optional[int]]) -> None:
        """Update given rid with partial columns (None means unchanged).
        columns 里 None 就表示那列不改
        """
        cols = self.cols

        # Build the new snapshot from the latest one, but ONLY commit a new
        # version if something actually changes. The course testers sometimes
        # issue "no-op" updates (all None), and those should not advance the
        # version counter.
        # 这里专门防一下“空更新”：如果啥都没变，就别新增版本
        prev = self.versions[rid][-1]
        changed = False
        new = prev  # may stay aliased if unchanged
        for i, v in enumerate(columns):
            if v is None:
                continue
            iv = int(v)
            if iv != int(prev[i]):
                if not changed:
                    new = prev.copy()
                    changed = True
                new[i] = iv

        if not changed:
            return

        self.versions[rid].append(new)
        # Update the columnar latest storage and (if valid) the BIT.
        # 更新最新列存储；如果 BIT 还有效，就顺便做 delta 更新
        if not self._bit_valid:
            # No BIT maintenance; rebuild on next sum().
            # BIT 已经不可信，那就只更新 cols，sum 的时候再重建
            for i, v in enumerate(columns):
                if v is not None:
                    cols[i][rid] = int(v)
            return

        pos = self.rid2pos.get(rid)
        if pos is None:
            # 理论上不该出现，但防一手：位置没了就别维护 BIT 了
            for i, v in enumerate(columns):
                if v is not None:
                    cols[i][rid] = int(v)
            self._bit_valid = False
            return
        bits = self._bits
        for i, v in enumerate(columns):
            if v is None:
                continue
            newv = int(v)
            oldv = int(cols[i][rid])
            if newv != oldv:
                cols[i][rid] = newv
                # BIT 只需要加差值（delta），不用整棵重建
                bits[i].add(pos, newv - oldv)
    # Sums fast版本
    def sum_range(self, start_key: int, end_key: int, column: int) -> int:
        # key 范围不合法就直接 0
        if start_key > end_key:
            return 0
        # BIT 不可用就先重建一次（顺便 compact）
        if not self._bit_valid:
            self._rebuild_bit_and_pos()
        keys = self.sorted_keys
        # 用二分定位 key 的范围 [lo, hi]
        lo = bisect_left(keys, int(start_key))
        hi = bisect_right(keys, int(end_key)) - 1
        if lo > hi:
            return 0
        # Convert 0-indexed slice bounds to 1-indexed BIT positions
        # sorted_keys 是 0-index，但 BIT 是 1-index，所以要 +1
        left = lo + 1
        right = hi + 1
        return int(self._bits[int(column)].range_sum(left, right))
