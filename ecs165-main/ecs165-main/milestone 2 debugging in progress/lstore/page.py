from __future__ import annotations
from typing import List, Optional
import struct
import sys
from array import array

class Page:
    """
(一次修改)改python list
    """
    PAGE_SIZE = 4096
    INT_SIZE = 8 # 64bit
    CAPACITY = PAGE_SIZE // INT_SIZE  # 一页

    HEADER_FMT = "<I"            # num_records: little-endian
    HEADER_SIZE = struct.calcsize(HEADER_FMT)

    __slots__ = ("num_records",  "data", "column_id", "is_tail")

    def __init__(self, column_id: int, is_tail: bool = False):
        #已写入记录数
        self.num_records = 0
        #存整数，bytearray加struct太垃圾了
        self.data: List[int] = []
        self.column_id: int = int(column_id)
        self.is_tail: bool = bool(is_tail)

    def has_capacity(self) -> bool:
        return self.num_records < self.CAPACITY
    
    def write(self, value: Optional[int]) -> int:
        """Append a value. Return offset."""
        if not self.has_capacity():
            raise OverflowError("Page is full")
        if value is None:
            value = 0
        offset = self.num_records
        self.data.append(int(value))
        self.num_records += 1
        return offset
    
    def read(self, offset: int) -> int:
        """
        读取
        """
        if offset < 0 or offset >= self.num_records:#边界
            raise IndexError("Offset out of range in Page")
        return self.data[offset]

    def overwrite(self, offset: int, value: Optional[int]) -> None:
        #（二次修改）覆盖写：update 时需要改 base 的 indirection / schema encoding
        if offset < 0 or offset >= self.num_records: # 邊界
            raise IndexError("Offset out of range in Page")
        # 给None兼容
        if value is None:
            value = 0
        self.data[offset] = int(value)

    def to_bytes(self) -> bytes:
        n = int(self.num_records)

        # body fix to 4096 bytes：insufficient CAPACITY use 0 padding
        padded = self.data + [0] * (self.CAPACITY - len(self.data))
        arr = array("Q", padded)   # uint64
        if sys.byteorder != "little":
            arr.byteswap()
        body = arr.tobytes()      

        return struct.pack(self.HEADER_FMT, n) + body

    def from_bytes(self, raw: bytes) -> None:
        if not raw:
            self.num_records = 0
            self.data = []
            return

        if len(raw) < self.HEADER_SIZE:
            raise ValueError("corrupted page bytes (too small)")

        (n,) = struct.unpack_from(self.HEADER_FMT, raw, 0)
        n = int(n)

        body = raw[self.HEADER_SIZE:self.HEADER_SIZE + self.PAGE_SIZE]
        body = body.ljust(self.PAGE_SIZE, b"\x00")  

        arr = array("Q")
        arr.frombytes(body[:self.PAGE_SIZE])
        if sys.byteorder != "little":
            arr.byteswap()

        self.num_records = n
        self.data = list(arr[:n])