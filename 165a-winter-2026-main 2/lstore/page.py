import struct

class Page:
    PAGE_SIZE = 4096
    INT_SIZE = 8
    CAPACITY = PAGE_SIZE // INT_SIZE

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self) -> bool:
        return self.num_records < self.CAPACITY

# append 8-byte signed integer to the page
    def write(self, value: int) -> int:
        if not self.has_capacity():
            raise OverflowError("Page is full")
        if value is None:
            value = 0

        offset = self.num_records
        start = offset * self.INT_SIZE
        self.data[start : start + self.INT_SIZE] = struct.pack("q", int(value))
        self.num_records += 1
        return offset

# read one 8-byte signed integer
    def read(self, offset: int) -> int:
        if offset < 0 or offset >= self.num_records:
            raise IndexError("Offset out of range for page read")
        start = offset * self.INT_SIZE
        return struct.unpack("q", self.data[start : start + self.INT_SIZE])[0]