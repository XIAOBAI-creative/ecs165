from __future__ import annotations
from collections import defaultdict
import threading


class LockConflict(Exception):
    pass


class LockManager:
    """
    Strict no-wait 2PL lock manager with simple table hierarchy awareness.

    Supported resources:
      ("TABLE", table_name)
      ("PK", table_name, pk)
      ("RID", table_name, rid)

    Compatibility rules:
      - same txn re-entrant acquire succeeds
      - S conflicts with another txn's X on the same resource
      - X conflicts with another txn's S/X on the same resource
      - TABLE locks also conflict with row/key locks from other txns on the same table:
          * TABLE-S conflicts with any row/key TABLE-X on same table by others
          * TABLE-X conflicts with any lock on same table by others
          * row/key-S conflicts with TABLE-X by others
          * row/key-X conflicts with TABLE-S/TABLE-X by others
      - no waiting: any conflict raises LockConflict immediately
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._shared_holders = defaultdict(set)   # resource -> {txn_id}
        self._exclusive_holder = {}               # resource -> txn_id
        self._txn_to_resources = defaultdict(set) # txn_id -> {resource}

    def _table_name(self, resource):
        if isinstance(resource, tuple) and len(resource) >= 2:
            return resource[1]
        return None

    def _kind(self, resource):
        if isinstance(resource, tuple) and len(resource) >= 1:
            return resource[0]
        return None

    def _other_holders_same_resource(self, txn_id: int, resource, want_x: bool) -> bool:
        x_holder = self._exclusive_holder.get(resource)
        if x_holder is not None and x_holder != txn_id:
            return True
        if want_x:
            for holder in self._shared_holders.get(resource, set()):
                if holder != txn_id:
                    return True
        return False

    def _table_level_conflict(self, txn_id: int, resource, want_x: bool) -> bool:
        table = self._table_name(resource)
        kind = self._kind(resource)
        if table is None or kind is None:
            return False

        # Check exclusive holders on same table
        for other_res, holder in list(self._exclusive_holder.items()):
            other_kind = self._kind(other_res)
            other_table = self._table_name(other_res)
            if other_table != table or holder == txn_id:
                continue

            if kind == "TABLE":
                return True
            if other_kind == "TABLE":
                return True

        # Shared holders only matter when we want X, or when TABLE-S must see TABLE-level readers/writers
        if want_x or kind == "TABLE":
            for other_res, holders in list(self._shared_holders.items()):
                other_kind = self._kind(other_res)
                other_table = self._table_name(other_res)
                if other_table != table:
                    continue
                for holder in holders:
                    if holder == txn_id:
                        continue
                    if kind == "TABLE":
                        return True
                    if other_kind == "TABLE":
                        return True
        return False

    def acquire_S(self, txn_id: int, resource) -> None:
        txn_id = int(txn_id)
        with self._lock:
            if self._other_holders_same_resource(txn_id, resource, want_x=False):
                raise LockConflict()
            if self._table_level_conflict(txn_id, resource, want_x=False):
                raise LockConflict()

            self._shared_holders[resource].add(txn_id)
            self._txn_to_resources[txn_id].add(resource)

    def acquire_X(self, txn_id: int, resource) -> None:
        txn_id = int(txn_id)
        with self._lock:
            if self._exclusive_holder.get(resource) == txn_id:
                self._txn_to_resources[txn_id].add(resource)
                return

            if self._other_holders_same_resource(txn_id, resource, want_x=True):
                raise LockConflict()
            if self._table_level_conflict(txn_id, resource, want_x=True):
                raise LockConflict()

            self._exclusive_holder[resource] = txn_id

            if txn_id in self._shared_holders.get(resource, set()):
                self._shared_holders[resource].discard(txn_id)
                if not self._shared_holders[resource]:
                    self._shared_holders.pop(resource, None)

            self._txn_to_resources[txn_id].add(resource)

    def release_all(self, txn_id: int) -> None:
        txn_id = int(txn_id)
        with self._lock:
            resources = list(self._txn_to_resources.get(txn_id, set()))

            for resource in resources:
                s_holders = self._shared_holders.get(resource)
                if s_holders is not None:
                    s_holders.discard(txn_id)
                    if not s_holders:
                        self._shared_holders.pop(resource, None)

                if self._exclusive_holder.get(resource) == txn_id:
                    self._exclusive_holder.pop(resource, None)

            self._txn_to_resources.pop(txn_id, None)

    def has_x_lock(self, resource) -> bool:
        with self._lock:
            return resource in self._exclusive_holder
