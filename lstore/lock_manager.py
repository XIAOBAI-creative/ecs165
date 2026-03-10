from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Hashable
import threading


class LockConflict(Exception):
    # no-wait冲突时抛出
    pass


@dataclass
class _LockState:
    # 当前锁模式
    mode: str = "UNLOCKED"  # "UNLOCKED" | "S" | "X"
    # 排它锁持有（mode X）
    x_owner: Optional[int] = None
    # 共享锁持有集合（mode S）
    s_owners: Set[int] = field(default_factory=set)

    # txn 的持有次数（可重入 / 幂等）
    s_count: Dict[int, int] = field(default_factory=dict)
    x_count: Dict[int, int] = field(default_factory=dict)


class LockManager:
    """
    2PL + No-Wait 锁管理器

    对于 INSERT，因为记录还不存在，没有 baseRID，这里用虚拟资源
    ("PK", table_name, pk_value) 来避免同一个主键被并发插入导致重复。

    acquire_S / acquire_X：
      1. no-wait：一旦冲突立刻 LockConflict（不等待）
      2. 可重入（同一 txn 对同一资源重复 acquire）
      3. S -> X：只有该资源的共享锁仅被当前 txn 持有时才允许升级

    release_all：
      strict 2PL：只能在 commit/abort 时统一释放，中途不释放
    """

    def __init__(self):
        # 全局互斥，保护 lock table 的并发访问
        self._mu = threading.RLock()
        # lock table：resource_id -> LockState
        self._locks: Dict[Hashable, _LockState] = {}
        # 事务持有的资源集合：txn_id -> set(resource_id)，方便 release_all
        self._txn_resources: Dict[int, Set[Hashable]] = {}

    def acquire_S(self, txn_id: int, rid: Hashable) -> None:
        # 申请共享锁 S，冲突则 LockConflict
        with self._mu:
            st = self._locks.get(rid)
            if st is None:
                st = _LockState()
                self._locks[rid] = st

            # 如果当前是 X 锁：
            # 自己持有 X：允许（可重入），同时也可记录 S 计数
            # 别人持有 X：冲突，no-wait 失败
            if st.mode == "X":
                if st.x_owner == txn_id:
                    st.s_owners.add(txn_id)
                    st.s_count[txn_id] = st.s_count.get(txn_id, 0) + 1
                    self._txn_resources.setdefault(txn_id, set()).add(rid)
                    return
                raise LockConflict()

            # UNLOCKED 或 S：可共享
            if st.mode in ("UNLOCKED", "S"):
                st.mode = "S"
                st.s_owners.add(txn_id)
                st.s_count[txn_id] = st.s_count.get(txn_id, 0) + 1
                self._txn_resources.setdefault(txn_id, set()).add(rid)
                return

            raise LockConflict()

    def acquire_X(self, txn_id: int, rid: Hashable) -> None:
        # 申请排它锁 X，冲突则 LockConflict，同时支持 S -> X
        with self._mu:
            st = self._locks.get(rid)
            if st is None:
                st = _LockState()
                self._locks[rid] = st

            # 资源空闲则直接 X
            if st.mode == "UNLOCKED":
                st.mode = "X"
                st.x_owner = txn_id
                st.x_count[txn_id] = st.x_count.get(txn_id, 0) + 1
                self._txn_resources.setdefault(txn_id, set()).add(rid)
                return

            # 已经是 X
            if st.mode == "X":
                # 自己持有：可重入
                if st.x_owner == txn_id:
                    st.x_count[txn_id] = st.x_count.get(txn_id, 0) + 1
                    self._txn_resources.setdefault(txn_id, set()).add(rid)
                    return
                # 别人持有：冲突
                raise LockConflict()

            # 当前是 S：尝试升级到 X
            if st.mode == "S":
                # 条件：共享锁持有者只有自己
                if st.s_owners == {txn_id}:
                    st.s_owners.discard(txn_id)
                    st.s_count.pop(txn_id, None)
                    st.mode = "X"
                    st.x_owner = txn_id
                    st.x_count[txn_id] = st.x_count.get(txn_id, 0) + 1
                    self._txn_resources.setdefault(txn_id, set()).add(rid)
                    return

                # 其他 txn 也持有 S，no-wait 直接失败
                raise LockConflict()

            raise LockConflict()

    def release_all(self, txn_id: int) -> None:
        """
        释放某个事务持有的全部锁。strict 2PL 下只在 commit/abort 调用。

        注意：事务可能对同一 rid 重复 acquire，会导致 s_count/x_count > 1，
        但 txn_resources 只是 set。release_all 这里必须清空该 txn 在该 rid 上
        的所有持有，而不是只减 1。
        """
        with self._mu:
            resources = self._txn_resources.pop(txn_id, set())
            for rid in list(resources):
                st = self._locks.get(rid)
                if st is None:
                    continue

                # 清掉该 txn 的 X
                if st.x_owner == txn_id:
                    st.x_owner = None
                st.x_count.pop(txn_id, None)

                # 清掉该 txn 的 S
                st.s_owners.discard(txn_id)
                st.s_count.pop(txn_id, None)

                # 重算 mode
                if st.x_owner is None:
                    st.mode = "S" if st.s_owners else "UNLOCKED"
                else:
                    st.mode = "X"

                # 完全空闲则移除
                if st.mode == "UNLOCKED" and not st.s_owners and st.x_owner is None:
                    self._locks.pop(rid, None)

    def has_x_lock(self, rid: Hashable) -> bool:
        with self._mu:
            st = self._locks.get(rid)
            if st is None:
                return False
            return st.mode == "X" and st.x_owner is not None
