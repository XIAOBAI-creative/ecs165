from __future__ import annotations
import threading
import time
import random


class TransactionWorker:

    def __init__(self, transactions=None):
        self.stats = []
        self.transactions = list(transactions) if transactions is not None else []
        self.result = 0
        self._thread = None
        self._aborts = 0
        self._commits = 0

    def add_transaction(self, t):
        self.transactions.append(t)

    def run(self):
        self._thread = threading.Thread(target=self.__run, daemon=True)
        self._thread.start()

    def join(self):
        if self._thread is not None:
            self._thread.join()

    def __run(self):
        for txn in self.transactions:
            attempts = 0
            # Handout 要求：abort 的事务必须一直重试直到 commit
            while True:
                ok = bool(txn.run())
                if ok:
                    self.stats.append(True)
                    self._commits += 1
                    break
                else:
                    self._aborts += 1
                    attempts += 1
                    capped = min(attempts, 6)
                    max_wait = min(0.002 * (2 ** capped), 0.05)
                    time.sleep(random.uniform(0.0005, max_wait))

        self.result = len(list(filter(lambda x: x, self.stats)))
