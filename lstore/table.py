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
        for c in range(self.total_columns):
            if not self.pages_by_col[c][-1].has_capacity():
                self.pages_by_col[c].append(Page())

    def append_row(self, values: List[int]) -> int:
        if len(values) != self.total_columns:
            raise ValueError("Row length does not match total_columns")
        self._ensure_capacity_for_next_row()
        for c in range(self.total_columns):
            self.pages_by_col[c][-1].write(values[c])
        row_offset = self.num_rows
        self.num_rows += 1
        return row_offset

    def read_row(self, row_offset: int) -> List[int]:
        if row_offset < 0 or row_offset >= self.num_rows:
            raise IndexError("Row offset out of range for page range")
        result = []
        page_index = row_offset // Page.CAPACITY
        in_page_offset = row_offset % Page.CAPACITY
        for c in range(self.total_columns):
            page_list = self.pages_by_col[c]
            if page_index >= len(page_list):
                raise IndexError("Page index out of range in column")
            result.append(page_list[page_index].read(in_page_offset))
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
        self.page_directory: Dict[int, RecordLocator] = {}
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        self._next_base_rid = 1
        self._next_tail_rid = 10_000_000
        self._storage = StorageEngine(self.total_columns)

    def __merge(self):
        print("merge is happening")
        pass

    def merge_threshold_pages(self):
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