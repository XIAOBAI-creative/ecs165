class Page:
    """
改python list
    """
    PAGE_SIZE = 4096
    INT_SIZE = 8 # 64bit
    CAPACITY = PAGE_SIZE // INT_SIZE  # 一页
    __slots__ = ("num_records", "data")
    def __init__(self):
        self.num_records = 0
        self.data = []

    def has_capacity(self) -> bool:
        return self.num_records < self.CAPACITY

    def write(self, value: int) -> int:
        ...
    
    def read(self, offset: int) -> int:
        ...

    def overwrite(self, offset: int, value: int) -> None:
        #（二次修改）覆盖写，update base 的 indirection/scheme
        if offset < 0 or offset >= self.num_records:
            raise IndexError("Offset out of range in Page")
        if value is None:
            value = 0
        self.data[offset] = int(value)
