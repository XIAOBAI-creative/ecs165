from __future__ import annotations
from collections import defaultdict
from bisect import bisect_left, bisect_right

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        self.table = table
        self.num_columns = table.num_columns
        self.indices = [None] * self.num_columns
        self.create_index(self.table.key)

    """
    # returns the location of all records with the given value on column "column"
    """

# Return list of RIDs for records whose column == value.
    def locate(self, column, value):
        idx = self.indices[column]
        if idx is None:
            raise ValueError(f"Index on column {column} not created.")
        rids = idx.get(value, None)
        return list(rids) if rids else []

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

# Return list of RIDs for records whose column value in [begin, end].
    def locate_range(self, begin: int, end: int, column: int) -> list[int]:
        idx = self.indices[column]
        if idx is None:
            raise ValueError(f"Index on column {column} not created.")
        out = []
        for val, rids in idx.items():
            if begin <= val <= end:
                out.extend(list(rids))
        return out

    """
    # optional: Create index on specific column
    """

# Create an index for a column
    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = defaultdict(set)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        if column_number == self.table.key:
            raise ValueError("Cannot drop primary key index.")
        self.indices[column_number] = None

# Add (value -> rid) mapping to index
    def insert_entry(self, column: int, value: int, rid: int) -> None:
        idx = self.indices[column]
        if idx is None:
            return
        idx[value].add(rid)

# Remove (value -> rid) mapping from index
    def delete_entry(self, column: int, value: int, rid: int) -> None:
        idx = self.indices[column]
        if idx is None:
            return
        if value in idx and rid in idx[value]:
            idx[value].remove(rid)
            if not idx[value]:
                del idx[value]