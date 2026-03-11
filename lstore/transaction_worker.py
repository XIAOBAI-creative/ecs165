from __future__ import annotations
from typing import List, Optional
import threading
import time
import random


class TransactionWorker:

    def __init__(self, transactions: Optional[List[object]] = None):
        self.stats: List[bool] = []
        self.transactions = list(transactions) if transactions is not None else []
        self.result: int = 0

        self._thread: Optional[threading.Thread] = None
        self._aborts: int = 0
        self._commits: int = 0

    def add_transaction(self, t) -> None:
        self.transactions.append(t)

    def run(self) -> None:
        self._thread = threading.Thread(target=self.__run, daemon=True)
        self._thread.start()

    def join(self) -> None:
        if self._thread is not None:
            self._thread.join()

    def __run(self) -> None:
        for txn in self.transactions:
            attempts = 0
            while True:
                ok = bool(txn.run())
                if ok:
                    self.stats.append(True)
                    self._commits += 1
                    break

                self._aborts += 1
                attempts += 1
                reason = getattr(txn, "_last_abort_reason", None)

                if reason != "LOCK":
                    break

                capped = min(attempts, 6)
                upper = min(0.002 * (2 ** capped), 0.05)
                time.sleep(random.uniform(0.0005, upper))

                reset_fn = getattr(txn, "reset_for_retry", None)
                if callable(reset_fn):
                    reset_fn()

        self.result = len(list(filter(lambda x: x, self.stats)))
