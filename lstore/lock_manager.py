from __future__ import annotations
from collections import defaultdict
import threading


class LockConflict(Exception):
    pass


class LockManager:
    """
    Strict no-wait 2PL lock manager.

    Resource examples:
      ("PK", table_name, pk)
      ("RID", table_name, base_rid)

    Semantics:
      - S conflicts with another txn's X
      - X conflicts with another txn's S/X
      - same txn re-entrant acquire succeeds
      - S -> X upgrade succeeds only if no other txn holds S/X
      - no waiting: conflict => raise LockConflict immediately
    """

    def __init__(self):
        self._lock = threading.RLock()

        # resource -> set(txn_id)
        self._shared_holders = defaultdict(set)

        # resource -> txn_id
        self._exclusive_holder = {}

        # txn_id -> set(resource)
        self._txn_to_resources = defaultdict(set)

    def acquire_S(self, txn_id: int, resource) -> None:
        txn_id = int(txn_id)
        with self._lock:
            x_holder = self._exclusive_holder.get(resource)

            # another txn already holds X
            if x_holder is not None and x_holder != txn_id:
                raise LockConflict()

            # same txn re-entrant S is fine
            self._shared_holders[resource].add(txn_id)
            self._txn_to_resources[txn_id].add(resource)

    def acquire_X(self, txn_id: int, resource) -> None:
        txn_id = int(txn_id)
        with self._lock:
            x_holder = self._exclusive_holder.get(resource)
            s_holders = self._shared_holders.get(resource, set())

            # same txn already holds X
            if x_holder == txn_id:
                self._txn_to_resources[txn_id].add(resource)
                return

            # another txn holds X
            if x_holder is not None and x_holder != txn_id:
                raise LockConflict()

            # any other txn holds S
            for holder in s_holders:
                if holder != txn_id:
                    raise LockConflict()

            # upgrade or fresh X
            self._exclusive_holder[resource] = txn_id

            # if same txn previously had S, remove it from S set
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
