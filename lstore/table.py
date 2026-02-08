class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.total_columns = 4 + num_columns
        #RID -> RecordLocator
        self.page_directory: Dict[int, RecordLocator] = {}
        #默认对 key列弄索引
        self.index = Index(self)
        #  +++++++++++++++++++++++之前merge_threshold_pages同名方法会被这个 int 覆盖导致int不能被调用，以后千万别这么搞，不然milestone2肯定不过测试
        self.merge_threshold_pages_count = 50#命名改了
        #rid的分配，base 从小到大，tail用大数省的冲突
        self._next_base_rid = 1
        self._next_tail_rid = 10_000_000
        # 存储
        self._storage = StorageEngine(self.total_columns)

    def __merge(self):
        print("merge is happening")
        pass

    def new_merge_threshold_pages(self):#命名改了
        print("merge_threshold_pages is happening")
        pass

# allocate a new RID for a base record
    def alloc_base_rid(self) -> int:
        rid = self._next_base_rid
        self._next_base_rid += 1
        return rid

# allocate a new RID for a tail record
    def alloc_tail_rid(self) -> int:
        rid = self._next_tail_rid
        self._next_tail_rid += 1
        return rid

# return the record by the given RID
    def lookup_locator(self, rid: int) -> Optional[RecordLocator]:
        return self.page_directory.get(rid, None)

# register a new record’s location
    def install_locator(self, rid: int, locator: RecordLocator) -> None:
        self.page_directory[rid] = locator

# return the locator of a record. includes metadate and user columns
    def write_record(self, is_tail: bool, rid: int, columns: List[int]) -> RecordLocator:
        if len(columns) != self.total_columns:
            raise ValueError(f"columns length {len(columns)} != total_columns {self.total_columns}")
        norm = [0 if v is None else int(v) for v in columns]
        locator = self._storage.append_record(is_tail=is_tail, values=norm)
        self.install_locator(rid, locator)
        return locator

# return the record columns by the given locator
    def read_record(self, locator: RecordLocator, projected_cols: Optional[List[int]] = None) -> List[int]:
        row = self._storage.read_record(locator)
        if projected_cols is None:
            return row
        if len(projected_cols) != self.num_columns:
            raise ValueError("projected_cols must be length num_columns")
        user_vals = row[4:]
        out = []
        for i, keep in enumerate(projected_cols):
            out.append(user_vals[i] if keep else None)
        return out

    def overwrite_value_at(self, locator: RecordLocator, column_index: int, value: int) -> None:
        # （二次修改）统一按 locator 覆盖写，别在 Query 里整了
        ranges = self._storage.tail_ranges if locator.is_tail else self._storage.base_ranges
        pr = ranges[locator.page_range_id]
        page_index = locator.offset // Page.CAPACITY
        in_page_offset = locator.offset % Page.CAPACITY

        pr.pages_by_col[column_index][page_index].overwrite(in_page_offset, value)
