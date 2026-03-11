"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from __future__ import annotations
from dataclasses import dataclass
from bisect import bisect_left, bisect_right
import threading
from typing import Any, Dict, List, Optional, Tuple

@dataclass
class Leaf:
    keys: List[int]
    vals: List[List[int]]
    next: Optional["Leaf"] = None

    @property
    def is_leaf(self) -> bool:
        return True

@dataclass
class Internal:
    keys: List[int]
    children: List[Any]

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
        self.order = order
        self.root: Any = Leaf(keys=[], vals=[], next=None)

    def _split_internal(self, internal: Internal, path: List[Tuple[Internal, int]]) -> None:
        mid = len(internal.keys) // 2
        promoted_key = internal.keys[mid]

        left_keys = internal.keys[:mid]
        right_keys = internal.keys[mid + 1:]
        left_children = internal.children[:mid + 1]
        right_children = internal.children[mid + 1:]

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
        if not path:
            self.root = Internal(keys=[key], children=[left_child, right_child])
            return

        parent, child_index = path.pop()
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

        promoted_key = new_leaf.keys[0]
        self._insert_in_parent(promoted_key, leaf, new_leaf, path)

    def insert(self, key: int, rid: int) -> None:
        path: List[Tuple[Internal, int]] = []
        node = self.root

        while not node.is_leaf:
            internal: Internal = node
            idx = bisect_right(internal.keys, key)
            path.append((internal, idx))
            node = internal.children[idx]

        leaf: Leaf = node
        i = bisect_left(leaf.keys, key)
        if i < len(leaf.keys) and leaf.keys[i] == key:
            leaf.vals[i].append(rid)
        else:
            leaf.keys.insert(i, key)
            leaf.vals.insert(i, [rid])

        if len(leaf.keys) > self.order:
            self._split_leaf(leaf, path)

    def delete(self, key: int, rid: int | None = None) -> bool:
        leaf = self._find_leaf(key)
        i = bisect_left(leaf.keys, key)
        if i >= len(leaf.keys) or leaf.keys[i] != key:
            return False

        if rid is None:
            del leaf.keys[i]
            del leaf.vals[i]
            return True

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
            i = bisect_left(leaf.keys, begin)
            while i < len(leaf.keys) and leaf.keys[i] <= end:
                out.extend(leaf.vals[i])
                i += 1
            if leaf.keys and leaf.keys[-1] > end:
                break
            leaf = leaf.next
        return out

class Index:

    def __init__(self, table):
        self.table = table
        self.indices = [None] * table.num_columns

        self._lock = threading.RLock()
        self._cv = threading.Condition(self._lock)
        self.BUILDING = object()
        self.create_index(table.key)

    def is_indexed(self, column_number: int) -> bool:
        col = int(column_number)
        with self._cv:
            while self.indices[col] is self.BUILDING:
                self._cv.wait()
            tree = self.indices[col]
            return (tree is not None) and (tree is not self.BUILDING)

    def locate(self, column, value):
        col = int(column)
        with self._cv:
            while self.indices[col] is self.BUILDING:
                self._cv.wait()
            tree = self.indices[col]
            if tree is None:
                return []
            return tree.find(int(value))

    def locate_range(self, begin, end, column):
        col = int(column)
        with self._cv:
            while self.indices[col] is self.BUILDING:
                self._cv.wait()
            tree = self.indices[col]
            if tree is None:
                return []
            return tree.range(int(begin), int(end))

    def create_index(self, column_number):
        col = int(column_number)
        with self._cv:
            while self.indices[col] is self.BUILDING:
                self._cv.wait()
            if self.indices[col] is not None:
                return
            self.indices[col] = self.BUILDING

        tree = BPlusTree()
        try:
            for base_rid in self.table.all_base_rids():
                if self.table.is_deleted_rid(base_rid):
                    continue
                if col == self.table.key:
                    val = self.table.read_base_user_value(base_rid, col)
                else:
                    val = self.table.read_latest_user_value(base_rid, col)
                if val is None:
                    continue
                tree.insert(int(val), int(base_rid))
        except Exception:
            with self._cv:
                self.indices[col] = None
                self._cv.notify_all()
            raise

        with self._cv:
            self.indices[col] = tree
            self._cv.notify_all()

    def drop_index(self, column_number):
        col = int(column_number)
        with self._cv:
            while self.indices[col] is self.BUILDING:
                self._cv.wait()
            self.indices[col] = None
            self._cv.notify_all()

    def insert_entry(self, column: int, value: int, rid: int) -> None:
        if value is None:
            return
        col = int(column)
        with self._cv:
            while self.indices[col] is self.BUILDING:
                self._cv.wait()
            tree = self.indices[col]
            if tree is None:
                return
            tree.insert(int(value), int(rid))

    def delete_entry(self, column: int, value: int, rid: int) -> None:
        if value is None:
            return
        col = int(column)
        with self._cv:
            while self.indices[col] is self.BUILDING:
                self._cv.wait()
            tree = self.indices[col]
            if tree is None:
                return
            tree.delete(int(value), int(rid))

    def update_entry(self, base_rid: int, old_user_cols: List[int], new_user_cols: List[int]) -> None:
        rid = int(base_rid)
        for col in range(len(self.indices)):
            with self._cv:
                while self.indices[col] is self.BUILDING:
                    self._cv.wait()
                tree = self.indices[col]
                if tree is None:
                    continue

                old_val = old_user_cols[col]
                new_val = new_user_cols[col]
                if old_val is None or new_val is None:
                    continue

                old_val = int(old_val)
                new_val = int(new_val)
                if old_val == new_val:
                    continue

                tree.delete(old_val, rid)
                tree.insert(new_val, rid)

    def add_to_index(self, column: int, value: int, rid: int) -> None:
        self.insert_entry(column, value, rid)

    def remove_from_index(self, column: int, value: int, rid: int) -> None:
        self.delete_entry(column, value, rid)

    def update_index(self, column: int, old_value: int, new_value: int, rid: int) -> None:
        if old_value is None or new_value is None:
            return
        old_v = int(old_value)
        new_v = int(new_value)
        if old_v == new_v:
            return
        self.delete_entry(int(column), new_v, int(rid))
        self.insert_entry(int(column), old_v, int(rid))
