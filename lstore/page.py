class Page:
    """
改python list
    """
    PAGE_SIZE = 4096
    INT_SIZE = 8 # 64bit
    CAPACITY = PAGE_SIZE // INT_SIZE  # 一页
    __slots__ = ("num_records", "data")
    def __init__(self):
        #已写入记录数
        self.num_records = 0
        #存整数，bytearray加struct太垃圾了
        self.data = []
    def has_capacity(self) -> bool:
        return self.num_records < self.CAPACITY
    def write(self, value: int) -> int:
        """
        追加一个64bit int, 返回该值页内offset
        """
        if self.num_records >= self.CAPACITY:
            raise OverflowError("Page is full")
        # 给None兼容
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

    def overwrite(self, offset: int, value: int) -> None:
        #（二次修改）覆盖写：update 时需要改 base 的 indirection / schema encoding
        if offset < 0 or offset >= self.num_records:  # 边界
            raise IndexError("Offset out of range in Page")
        # 给None兼容
        if value is None:
            value = 0
        self.data[offset] = int(value)
