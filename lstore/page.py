class Page:
    """
    改 python list
    """
    PAGE_SIZE = 4096
    INT_SIZE = 8  # 64bit
    CAPACITY = PAGE_SIZE // INT_SIZE
    __slots__ = ("num_records", "data")

    def __init__(self):
        self.num_records = 0
        self.data = []

    def has_capacity(self) -> bool:
        return self.num_records < self.CAPACITY

    def write(self, value: int) -> int:
        """
        追加一个64bit int, 返回该值页内offset
        """
        if self.num_records >= self.CAPACITY:
            raise OverflowError("Page is full")
        if value is None:
            value = 0
        offset = self.num_records
        self.data.append(int(value))
        self.num_records += 1
        return offset

    def read(self, offset: int) -> int:
        if offset < 0 or offset >= self.num_records:
            raise IndexError("Offset out of range in Page")
        return self.data[offset]

    def overwrite(self, offset: int, value: int) -> None:
        # 【新增】支持覆盖写（update base 的 indirection / schema encoding 必须）
        if offset < 0 or offset >= self.num_records:
            raise IndexError("Offset out of range in Page")
        if value is None:
            value = 0
        self.data[offset] = int(value)
