from __future__ import annotations
from typing import List, Optional
import threading
import time
import random


class TransactionWorker:

# 如果无法授予锁，事务就中止，对于已中止的事务，工作进程会持续重试，直到成功提交。
    def __init__(self, transactions: List[object] = []):
        self.stats: List[bool] = []
        self.transactions = transactions
        self.result: int = 0

        self._thread: Optional[threading.Thread] = None
        self._aborts: int = 0
        self._commits: int = 0

    def add_transaction(self, t) -> None:
        self.transactions.append(t)

    def run(self) -> None:
        # create and start thread
        self._thread = threading.Thread(target=self.__run, daemon=True)
        self._thread.start()

    def join(self) -> None:
        if self._thread is not None:
            self._thread.join()

    def __run(self) -> None:
        for txn in self.transactions:
            # retry until commit
            while True:
                ok = bool(txn.run())
                if ok:
                    self.stats.append(True)
                    self._commits += 1
                    break
                else:
                    self._aborts += 1
                    # tiny randomized backoff to reduce livelock under contention
                    time.sleep(random.uniform(0.0005, 0.003))

        self.result = len(list(filter(lambda x: x, self.stats)))
