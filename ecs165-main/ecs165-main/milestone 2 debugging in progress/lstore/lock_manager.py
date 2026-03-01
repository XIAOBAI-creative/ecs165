from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Hashable
import threading


class LockConflict(Exception):
    #no-wait冲突时抛出
    pass

@dataclass
class _LockState:
    #当前锁模式
    mode: str = "UNLOCKED"  # "UNLOCKED" | "S" | "X"
    #排它锁持有（modeX）
    x_owner: Optional[int] = None
    #共享锁持有集合（modeS）
    s_owners: Set[int] = field(default_factory=set)

    #txn 的持有次数(可重入/幂)
    s_count: Dict[int, int] = field(default_factory=dict)
    x_count: Dict[int, int] = field(default_factory=dict)


class LockManager:
    """
    2PL+No-Wait锁管理器
    对于 INSERT就是记录还不存在，没有 baseRID的，这里用一个虚拟资源("PK", pk_value)来避免同一个主键被并发插入导致重复。
    acquire_S / acquire_X：1.no-wait：一旦冲突，立刻LockConflict（不等）2.可重入（同一 txn 对同一资源重复 acquire）3.S->X：只有当该资源的共享锁仅被当前 txn 持有时才允许升级
    release_all：必须2PL：只能在 commit/abort 时统一释放锁也就是中途不释放
    """

    def __init__(self):
        # 全局互斥，保护 lock table的并发访问
        self._mu = threading.RLock()
        # lock table：resource_id -> LockState
        self._locks: Dict[Hashable, _LockState] = {}
        #事务持有的资源集合：txn_id -> set(resource_id)，好release_all
        self._txn_resources: Dict[int, Set[Hashable]] = {}

    def acquire_S(self, txn_id: int, rid: Hashable) -> None:
        #申请共享锁S。冲突则LockConflict
        with self._mu:
            st = self._locks.get(rid)
            if st is None:
                st = _LockState()
                self._locks[rid] = st

            # 如果当前是 X 锁：自己持有 X：允许也就是可重入，同时也可记录 S 计数，别人持有 X：冲突，no-wait失败
            if st.mode == "X":
                if st.x_owner == txn_id:
                    st.s_owners.add(txn_id)
                    st.s_count[txn_id] = st.s_count.get(txn_id, 0) + 1
                    self._txn_resources.setdefault(txn_id, set()).add(rid)
                    return
                raise LockConflict()

            # UNLOCKED或S：可共享锁
            if st.mode in ("UNLOCKED", "S"):
                st.mode = "S"
                st.s_owners.add(txn_id)
                st.s_count[txn_id] = st.s_count.get(txn_id, 0) + 1
                self._txn_resources.setdefault(txn_id, set()).add(rid)
                return
            raise LockConflict()

    def acquire_X(self, txn_id: int, rid: Hashable) -> None:
        #申请排它锁X，冲突则抛LockConflict，同时可以S->X
        with self._mu:
            st = self._locks.get(rid)
            if st is None:
                st = _LockState()
                self._locks[rid] = st
            # 资源空闲则直接X
            if st.mode == "UNLOCKED":
                st.mode = "X"
                st.x_owner = txn_id
                st.x_count[txn_id] = st.x_count.get(txn_id, 0) + 1
                self._txn_resources.setdefault(txn_id, set()).add(rid)
                return
            # 如果已经是 X：1.自己持有：可重入 count++， 2.别人持有：冲突，no-wait失败
            if st.mode == "X":
                if st.x_owner == txn_id:
                    st.x_count[txn_id] = st.x_count.get(txn_id, 0) + 1
                    self._txn_resources.setdefault(txn_id, set()).add(rid)
                    return
                raise LockConflict()

            # 当前是 S：尝试升到 X
            if st.mode == "S":
                # 条件为共享锁持有者只有自己
                if st.s_owners == {txn_id}:
                    # 清S，再授X
                    st.s_owners.discard(txn_id)
                    st.s_count.pop(txn_id, None)
                    st.mode = "X"
                    st.x_owner = txn_id
                    st.x_count[txn_id] = st.x_count.get(txn_id, 0) + 1
                    self._txn_resources.setdefault(txn_id, set()).add(rid)
                    return
                # 如果别的txn 持有S则冲突，no-wait 失败
                raise LockConflict()

            raise LockConflict()

    def release_all(self, txn_id: int) -> None:
        """
        释放某个事务持有的全部锁， 2PL只在commit/abort 调用
        修复：事务可能对同一 rid 重复 acquire，会导致 s_count/x_count > 1，但 txn_resources 只是 set，release_all 只遍历一次，会只减 1，造成锁残留，strict 2PL 下 release_all 应该清空该 txn 在该 rid 上的所有持有，而不是计数-1
        """
        with self._mu:
            resources = self._txn_resources.pop(txn_id, set())
            for rid in list(resources):
                st = self._locks.get(rid)
                if st is None:
                    continue
                # 1，清掉该 txn 的 X，不管重入次数
                if st.x_owner == txn_id:
                    st.x_owner = None
                st.x_count.pop(txn_id, None)
                # 2， 清掉该 txn 的 S，不管重入
                st.s_owners.discard(txn_id)
                st.s_count.pop(txn_id, None)
                # 3，重新算mode
                if st.x_owner is None:
                    st.mode = "S" if st.s_owners else "UNLOCKED"
                else:
                    st.mode = "X"
                # 4，完全空闲则从表里删除
                if st.mode == "UNLOCKED" and not st.s_owners and st.x_owner is None:
                    self._locks.pop(rid, None)
