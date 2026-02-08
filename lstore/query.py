from lstore.table import Table, Record, INDIRECTION_COLUMN, RID_COLUMN, TIMESTAMP_COLUMN, SCHEMA_ENCODING_COLUMN  
from lstore.index import Index
from time import time


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        self._num_cols = table.num_columns
        # 五次修改：用 bitmask 表示“全列有效”，tail 存全量快照时直接用它
        self._all_ones = (1 << table.num_columns) - 1
        # 五次修改：预计算每列对应的 bit（避免 update 里反复 shift）
        self._col_bits = [1 << (table.num_columns - 1 - i) for i in range(table.num_columns)]


    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key: int):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False
            rid = rids[0]
            # Remove from index
            self.table.index.delete_entry(self.table.key, primary_key, rid)
            # Remove from page directory
            if rid in self.table.page_directory:
                del self.table.page_directory[rid]
            return True
        except:
            return False


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        try:
            schema_encoding = 0
            rid = self.table.alloc_base_rid()
            # 五次修改：不用 time()，用自增 timestamp
            timestamp = self.table.next_timestamp()
            indirection = 0  # No tail record yet

            # Build full record: [indirection, rid, timestamp, schema_encoding, ...user_columns]
            full_record = [indirection, rid, timestamp, schema_encoding] + list(columns)

            # 五次修改：insert 也是纯 int，可直接 fast 写（绕过 norm 扫描）
            self.table.write_record_fast(is_tail=False, rid=rid, columns=full_record)

            # Update index for primary key
            primary_key = columns[self.table.key]
            self.table.index.insert_entry(self.table.key, primary_key, rid)

            return True
        except:
            return False


    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key: int, search_key_index: int, projected_columns_index):
        try:
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                return []

            lookup = self.table.lookup_locator
            storage = self.table._storage
            num_cols = self._num_cols
            key_col = self.table.key

            results = []
            for rid in rids:
                locator = lookup(rid)
                if locator is None:
                    continue

                base_row = storage.read_record(locator)

                # 五次修改：base 不再被 update 覆盖写用户列，所以这里要看 indirection
                indirection = base_row[INDIRECTION_COLUMN]
                if indirection != 0:
                    tail_locator = lookup(indirection)
                    if tail_locator is not None:
                        row = storage.read_record(tail_locator)
                    else:
                        row = base_row
                else:
                    row = base_row

                # Apply projection（避免 row[4:] 切片分配）
                projected = [None] * num_cols
                base_idx = 4
                for i, keep in enumerate(projected_columns_index):
                    if keep:
                        projected[i] = row[base_idx + i]

                primary_key = row[base_idx + key_col]
                results.append(Record(rid, primary_key, projected))

            return results
        except:
            return False


    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key: int, search_key_index: int,
                       projected_columns_index, relative_version: int):
        try:
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                return []

            num_cols = self._num_cols
            key_col = self.table.key

            results = []
            for rid in rids:
                user_columns = self._get_version_values(rid, relative_version)
                if user_columns is None:
                    continue

                projected = [None] * num_cols
                for i, keep in enumerate(projected_columns_index):
                    if keep:
                        projected[i] = user_columns[i]

                primary_key = user_columns[key_col]
                results.append(Record(rid, primary_key, projected))

            return results
        except:
            return False


    def _get_version_values(self, base_rid: int, relative_version: int):
        """
        Helper: get values at a specific version.
        relative_version = 0 means latest, -1 means one version back, etc.
        """
        lookup = self.table.lookup_locator
        storage = self.table._storage

        locator = lookup(base_rid)
        if locator is None:
            return None

        base_row = storage.read_record(locator)
        indirection = base_row[INDIRECTION_COLUMN]

        # 没有 tail：只有 base
        if indirection == 0:
            return list(base_row[4:])

        # 五次修改：tail 链结构：base.indirection -> 最新 tail，tail.indirection -> 上一条 tail（第一次为0）
        k = abs(relative_version)
        current_rid = indirection
        last_tail_row = None

        while current_rid != 0 and current_rid >= 10_000_000:
            tail_locator = lookup(current_rid)
            if tail_locator is None:
                break
            tail_row = storage.read_record(tail_locator)
            last_tail_row = tail_row

            if k == 0:
                return list(tail_row[4:])

            current_rid = tail_row[INDIRECTION_COLUMN]
            k -= 1

        # 版本往回走过头：回到 base（因为 base 保存最老版本）
        if k >= 0:
            return list(base_row[4:])

        # 理论上不会到这里，保底
        if last_tail_row is not None:
            return list(last_tail_row[4:])
        return list(base_row[4:])


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key: int, *columns):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False

            base_rid = rids[0]
            base_locator = self.table.lookup_locator(base_rid)
            if base_locator is None:
                return False

            lookup = self.table.lookup_locator
            storage = self.table._storage
            base_row = storage.read_record(base_locator)
            old_indirection = base_row[INDIRECTION_COLUMN]

            num_cols = self._num_cols
            # 五次修改：不用 time()，用自增 timestamp（update 会被测很多次）
            timestamp = self.table.next_timestamp()

            # 五次修改：取“当前最新行”作为快照源（第一次 update 用 base，否则用最新 tail）
            if old_indirection == 0:
                src_row = base_row
            else:
                tail_locator = lookup(old_indirection)
                src_row = storage.read_record(tail_locator) if tail_locator is not None else base_row

            # 五次修改：先直接复制最新快照（切片复制），然后只改需要更新的列
            base_idx = 4
            new_user_values = src_row[base_idx:base_idx + num_cols].copy()

            updated_bits = 0
            col_bits = self._col_bits
            for i in range(num_cols):
                v = columns[i]
                if v is not None:
                    updated_bits |= col_bits[i]
                    new_user_values[i] = v

            # 五次修改：预分配 tail_record，避免 list 拼接
            tail_rid = self.table.alloc_tail_rid()
            tail_record = [0] * (4 + num_cols)
            tail_record[0] = old_indirection          # tail.indirection 指向上一条 tail（第一次就是 0）
            tail_record[1] = tail_rid
            tail_record[2] = timestamp
            tail_record[3] = self._all_ones           # tail 存全量快照
            tail_record[4:] = new_user_values

            # 五次修改：fast 写入，绕开 norm/int/None 扫描（update 最主要提速点之一）
            self.table.write_record_fast(is_tail=True, rid=tail_rid, columns=tail_record)

            # 五次修改：base 只覆盖写 2 个 int（indirection + schema）
            new_base_schema = base_row[SCHEMA_ENCODING_COLUMN] | updated_bits
            self.table.overwrite_values_at(
                base_locator,
                [INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN],
                [tail_rid, new_base_schema]
            )

            return True
        except:
            return False


    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range: int, end_range: int, aggregate_column_index: int):
        try:
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids:
                return False

            lookup = self.table.lookup_locator
            storage = self.table._storage

            # 五次修改：sum 也要读“最新行”（base 或最新 tail），避免 row[4:] 切片
            total = 0
            col = 4 + aggregate_column_index
            for rid in rids:
                locator = lookup(rid)
                if locator is None:
                    continue
                base_row = storage.read_record(locator)
                indirection = base_row[INDIRECTION_COLUMN]
                if indirection != 0:
                    tail_locator = lookup(indirection)
                    if tail_locator is not None:
                        row = storage.read_record(tail_locator)
                    else:
                        row = base_row
                else:
                    row = base_row

                total += row[col]

            return total
        except:
            return False


    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range: int, end_range: int,
                    aggregate_column_index: int, relative_version: int):
        try:
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids:
                return False

            total = 0
            for rid in rids:
                user_columns = self._get_version_values(rid, relative_version)
                if user_columns is not None:
                    total += user_columns[aggregate_column_index]

            return total
        except:
            return False


    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
