from __future__ import annotations
from typing import List, Optional
import sys
from array import array


class Page:

    PAGE_SIZE = 4096
    INT_SIZE = 8  # 64-bit
    SLOTS = PAGE_SIZE // INT_SIZE  # 512

    HEADER_SLOTS = 1              # reserve slot[0] for num_records
    CAPACITY = SLOTS - HEADER_SLOTS  # 511

    __slots__ = ("num_records", "data", "column_id", "is_tail")

    def __init__(self, column_id: int, is_tail: bool = False):
        self.num_records = 0
        self.data: List[int] = []
        self.column_id: int = int(column_id)
        self.is_tail: bool = bool(is_tail)

    def has_capacity(self) -> bool:
        return self.num_records < self.CAPACITY

    def write(self, value: Optional[int]) -> int:
        """Append a value. Return logical offset (0..CAPACITY-1)."""
        if not self.has_capacity():
            raise OverflowError("Page is full")
        if value is None:
            value = 0
        off = self.num_records
        self.data.append(int(value))
        self.num_records += 1
        return off

    def read(self, offset: int) -> int:
        """Read by logical offset."""
        if offset < 0 or offset >= self.num_records:
            raise IndexError("Offset out of range in Page")
        return self.data[offset]

    def overwrite(self, offset: int, value: Optional[int]) -> None:
        """Overwrite by logical offset."""
        if offset < 0 or offset >= self.num_records:
            raise IndexError("Offset out of range in Page")
        if value is None:
            value = 0
        self.data[offset] = int(value)

    def to_bytes(self) -> bytes:
        """
        Serialize to EXACTLY 4096 bytes:
          [num_records][data...][padding...]
        """
        n = int(self.num_records)
        if n < 0 or n > self.CAPACITY:
            raise ValueError(f"Invalid num_records={n}")

        # Build 512-slot payload
        # slot0 = n, slots1.. = data padded
        slots = [0] * self.SLOTS
        slots[0] = n  # store count in-page

        # copy data
        # mask to uint64
        for i in range(n):
            slots[1 + i] = int(self.data[i]) & 0xFFFFFFFFFFFFFFFF

        arr = array("Q", slots)
        if sys.byteorder != "little":
            arr.byteswap()

        b = arr.tobytes()
        # must be exactly 4096
        if len(b) != self.PAGE_SIZE:
            raise ValueError(f"Serialized page size {len(b)} != {self.PAGE_SIZE}")
        return b

    def from_bytes(self, raw: bytes) -> None:
        """
        Deserialize from EXACTLY 4096 bytes (or best-effort normalize).
        Restores num_records from slot[0].
        """
        if not raw:
            self.num_records = 0
            self.data = []
            return

        # normalize to 4096 bytes
        if len(raw) < self.PAGE_SIZE:
            raw = raw.ljust(self.PAGE_SIZE, b"\x00")
        elif len(raw) > self.PAGE_SIZE:
            raw = raw[: self.PAGE_SIZE]

        arr = array("Q")
        arr.frombytes(raw)
        if sys.byteorder != "little":
            arr.byteswap()

        n = int(arr[0])
        if n < 0:
            n = 0
        if n > self.CAPACITY:
            # 文件损坏或旧格式混入，直接截断，避免崩
            n = self.CAPACITY

        self.num_records = n
        self.data = list(arr[1 : 1 + n])
