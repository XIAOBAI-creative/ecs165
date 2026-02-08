from lstore.index import Index
from lstore.page import Page
from time import time
from dataclasses import dataclass
from typing import Optional, List, Dict

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3



class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __getitem__(self, idx):
        return self.columns[idx]

# Find a record for a given RID
@dataclass
class RecordLocator:
    is_tail: bool # whether it's a tail record
    page_range_id: int # Find the page range
    offset: int


# for each column, maintain a list of pages
class PageRange:

    def __init__(self, total_columns: int):
        self.total_columns = total_columns
        self.pages_by_col: List[List[Page]] = [[Page()] for _ in range(total_columns)]
        self.num_rows = 0

# make a new page if the last one is full
    def _ensure_capacity_for_next_row(self):
        """
        不要每插一行就对每一列都检查一次 has_capacity()， 因为每一行都会对所有列都写一次，所以所有列的页增长是同步的
        所以检查任意一列就行，比如第 0 列满了，那么所有列都会满，写的时候能不能稍微思考思考，这种地方就不要浪费我的时间去修正了
        """
        # 0 列最后一页
        if not self.pages_by_col[0][-1].has_capacity():
            for c in range(self.total_columns):
                self.pages_by_col[c].append(Page())

    def append_row(self, values: List[int]) -> int:
        """
        追加total_columns个值，返回该行在本 PageRange的row_offset
        """
        if len(values) != self.total_columns:
            raise ValueError("Row length does not match total_columns")
        self._ensure_capacity_for_next_row()
        # 逐列写入最后一页
        pages_by_col = self.pages_by_col  # 缓存一下能快点
        for c in range(self.total_columns):
            pages_by_col[c][-1].write(values[c])
        row_offset = self.num_rows
        self.num_rows += 1
        return row_offset

    def read_row(self, row_offset: int) -> List[int]:
        """
        读一行，返回 total_columns个值
        """
        if row_offset < 0 or row_offset >= self.num_rows:
            raise IndexError("Row offset out of range for page range")
        # 落在哪个 page以及页内 offset
        page_index = row_offset // Page.CAPACITY
        in_page_offset = row_offset % Page.CAPACITY
        pages_by_col = self.pages_by_col
        total_cols = self.total_columns
        # 预分配 list，这样append开销能少点
        result = [0] * total_cols
        for c in range(total_cols):
            page_list = pages_by_col[c]
            #保留一下保护吧
            if page_index >= len(page_list):
                raise IndexError("Page index out of range in column")
            result[c] = page_list[page_index].read(in_page_offset)
        return result



#in-memory storage to satisfy base page ranges and tail page ranges
class StorageEngine:

    def __init__(self, total_columns: int):
        self.total_columns = total_columns
        self.base_ranges: List[PageRange] = [PageRange(total_columns)]
        self.tail_ranges: List[PageRange] = [PageRange(total_columns)]

    def _get_ranges(self, is_tail: bool) -> List[PageRange]:
        return self.tail_ranges if is_tail else self.base_ranges

    def append_record(self, is_tail: bool, values: List[int]) -> RecordLocator:
        ranges = self._get_ranges(is_tail)
        pr = ranges[-1]
        offset = pr.append_row(values)
        return RecordLocator(is_tail=is_tail, page_range_id=len(ranges) - 1, offset=offset)

    def read_record(self, locator: RecordLocator) -> List[int]:
        ranges = self._get_ranges(locator.is_tail)
        pr = ranges[locator.page_range_id]
        return pr.read_row(locator.offset)



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
        # （二次修改）统一按 locator 覆盖写，别在 Query里pages_by_col + struct
        ranges = self._storage.tail_ranges if locator.is_tail else self._storage.base_ranges
        pr = ranges[locator.page_range_id]
        page_index = locator.offset // Page.CAPACITY
        in_page_offset = locator.offset % Page.CAPACITY

        pr.pages_by_col[column_index][page_index].overwrite(in_page_offset, value)
