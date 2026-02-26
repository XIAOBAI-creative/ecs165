"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from __future__ import annotations
from dataclasses import dataclass
from bisect import bisect_left, bisect_right
from typing import Any, Dict, List, Optional, Tuple

@dataclass
class Leaf:
    keys: List[int]
    # each key maps to a list of RIDs
    vals: List[List[int]]          
    next: Optional["Leaf"] = None

    @property
    def is_leaf(self) -> bool:
        return True
    
@dataclass
class Internal:
    keys: List[int]
    children: List[Any]            
    # children are Internal or Leaf

    @property
    def is_leaf(self) -> bool:
        return False

class BPlusTree:
    """
    B+Tree supporting so far:
      - insert(key, rid)
      - find(key) -> list[rid]
      - range(begin, end) -> list[rid]
    order = max number of keys in a node before split 
    """

    def __init__(self, order: int = 32):
        if order < 3:
            raise ValueError("order must be >= 3")
            # 太少的order会导致频繁分裂，影响性能
        self.order = order
        self.root: Any = Leaf(keys=[], vals=[], next=None)

    def _split_internal(self, internal: Internal, path: List[Tuple[Internal, int]]) -> None:
        mid = len(internal.keys) // 2
        # 二分算法加快分裂过程，使得运算时间更短
        promoted_key = internal.keys[mid]

        left_keys = internal.keys[:mid]
        right_keys = internal.keys[mid+1:]
        left_children = internal.children[:mid+1]
        right_children = internal.children[mid+1:]

        # 创建新的内部节点
        new_internal = Internal(keys=right_keys, children=right_children)
        internal.keys = left_keys
        internal.children = left_children

        self._insert_in_parent(promoted_key, internal, new_internal, path)


    def _insert_in_parent(
        self,
        key: int,
        left_child: Any,
        right_child: Any,
        path: List[Tuple[Internal, int]],
    ) -> None:
        # if splitting root
        if not path:
            self.root = Internal(keys=[key], children=[left_child, right_child])
            return

        parent, child_index = path.pop()  # where left_child was
        # insert key into parent.keys at position child_index
        parent.keys.insert(child_index, key)
        parent.children.insert(child_index + 1, right_child)

        if len(parent.keys) > self.order:
            self._split_internal(parent, path)

            
    def _split_leaf(self, leaf: Leaf, path: List[Tuple[Internal, int]]) -> None:
        mid = len(leaf.keys) // 2
        new_leaf = Leaf(keys=leaf.keys[mid:], vals=leaf.vals[mid:], next=leaf.next)
        leaf.keys = leaf.keys[:mid]
        leaf.vals = leaf.vals[:mid]
        leaf.next = new_leaf

        # promote the first key of new leaf
        promoted_key = new_leaf.keys[0]
        self._insert_in_parent(promoted_key, leaf, new_leaf, path)



    def insert(self, key: int, rid: int) -> None:
        # Path stack for split propagation
        path: List[Tuple[Internal, int]] = []
        node = self.root

        # descend to leaf
        while not node.is_leaf:
            internal: Internal = node
            idx = bisect_right(internal.keys, key)
            path.append((internal, idx))
            node = internal.children[idx]

        leaf: Leaf = node
        i = bisect_left(leaf.keys, key)
        if i < len(leaf.keys) and leaf.keys[i] == key:
            # duplicate key -> append rid
            leaf.vals[i].append(rid)
        else:
            leaf.keys.insert(i, key)
            leaf.vals.insert(i, [rid])

        # split leaf if overflow
        if len(leaf.keys) > self.order:
            self._split_leaf(leaf, path)

    def delete(self, key: int, rid: int | None = None) -> bool:
        leaf = self._find_leaf(key)
        i = bisect_left(leaf.keys, key)
        if i >= len(leaf.keys) or leaf.keys[i] != key:
            return False

        if rid is None:
            # delete all key
            del leaf.keys[i]
            del leaf.vals[i]
            return True

        # delete special rid
        try:
            leaf.vals[i].remove(rid)
        except ValueError:
            return False

        if not leaf.vals[i]:
            del leaf.keys[i]
            del leaf.vals[i]
        return True



    def _find_leaf(self, key: int) -> Leaf:
        node = self.root
        while not node.is_leaf:
            internal: Internal = node
            idx = bisect_right(internal.keys, key)
            node = internal.children[idx]
        return node

    def find(self, key: int) -> List[int]:
        leaf = self._find_leaf(key)
        i = bisect_left(leaf.keys, key)
        if i < len(leaf.keys) and leaf.keys[i] == key:
            return list(leaf.vals[i])
        return []


    def range(self, begin: int, end: int) -> List[int]:
        if begin > end:
            return []
        out: List[int] = []
        leaf = self._find_leaf(begin)
        while leaf is not None:
            # start at first key >= begin
            i = bisect_left(leaf.keys, begin)
            while i < len(leaf.keys) and leaf.keys[i] <= end:
                out.extend(leaf.vals[i])
                i += 1
            # If leaf has keys and the last key > end, we can stop early
            if leaf.keys and leaf.keys[-1] > end:
                break
            leaf = leaf.next
        return out

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = [None] *  table.num_columns

        # key column indexed by default
        self.create_index(table.key)
        self.key_to_rid: dict[int, int] = {}
 
    def is_indexed(self, column_number: int) -> bool:
        return bool(self.indices[int(column_number)] is not None)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        tree = self.indices[column]
        if tree is None:
            return []
        return tree.find(int(value))
 
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        tree = self.indices[column]
        if tree is None:
            return []
        return tree.range(int(begin), int(end))

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = BPlusTree( )
            # build by scanning all base records
            for base_rid in self.table.all_base_rids():
                if self.table.is_deleted_rid(base_rid):
                    continue
                val = self.table.read_latest_user_value(base_rid, column_number)
                self.insert_entry(column_number, val, base_rid)
    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None

    def insert_entry(self, column: int, value: int, rid: int) -> None:
        tree = self.indices[column]
        if tree is None:
            return
        tree.insert(int(value), int(rid))

    def delete_entry(self, column: int, value: int, rid: int) -> None:
        tree = self.indices[column]
        if tree is None:
            return
        tree.delete(int(value), int(rid))

    def update_entry(self, base_rid: int, old_user_cols: List[int], new_user_cols: List[int]) -> None:
        rid = int(base_rid)
        for col, tree in enumerate(self.indices):
            if tree is None:
                continue
            old_val = int(old_user_cols[col])
            new_val = int(new_user_cols[col])
            if old_val == new_val:
                continue
            tree.delete(old_val, rid)
            tree.insert(new_val, rid)