from lstore.table import Record, INDIRECTION_COLUMN, RID_COLUMN, TIMESTAMP_COLUMN, SCHEMA_ENCODING_COLUMN
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
            timestamp = int(time())
            indirection = 0  # No tail record yet

            # Build full record: [indirection, rid, timestamp, schema_encoding, ...user_columns]
            full_record = [indirection, rid, timestamp, schema_encoding] + list(columns)

            # Write to storage
            self.table.write_record(is_tail=False, rid=rid, columns=full_record)

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

            # 缓存常用的东西，少做属性查找
            lookup = self.table.lookup_locator
            storage = self.table._storage
            num_cols = self.table.num_columns
            key_col = self.table.key

            results = []
            for rid in rids:
                locator = lookup(rid)
                if locator is None:
                    continue

                base_row = storage.read_record(locator)

                #四次修  update把 base 的用户列覆盖成最新：select 永远直接读base，不管tail
                user_columns = base_row[4:]

                projected = [None] * num_cols
                for i, keep in enumerate(projected_columns_index):
                    if keep:
                        projected[i] = user_columns[i]

                primary_key = user_columns[key_col]
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

            num_cols = self.table.num_columns
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
        if indirection == 0:
            return list(base_row[4:])

        #四次修改 直接走链第 k 条 tail，返回那条的快照
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

        # k 走过头：返回最老快照 tail
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

            storage = self.table._storage
            base_row = storage.read_record(base_locator)
            old_indirection = base_row[INDIRECTION_COLUMN]

            num_cols = self.table.num_columns
            timestamp = int(time())

            #四次修改 base现在是最新，所以 current_values 直接取 base 的用户列，不管tail
            current_values = base_row[4:]

            schema_bits = 0
            new_user_values = [0] * num_cols
            for i in range(num_cols):
                v = columns[i]
                if v is not None:
                    schema_bits |= (1 << (num_cols - 1 - i))
                    new_user_values[i] = v
                else:
                    new_user_values[i] = current_values[i]

            # 四次修 第一次 update先写原始 base 快照 tail，这样旧版本不丢
            prev_rid = old_indirection
            if old_indirection == 0:
                snapshot_rid = self.table.alloc_tail_rid()
                all_ones = (1 << num_cols) - 1
                snapshot_record = [0, snapshot_rid, timestamp, all_ones] + list(current_values)
                self.table.write_record(is_tail=True, rid=snapshot_rid, columns=snapshot_record)
                prev_rid = snapshot_rid

            # 写新版本 tail（全列快照）
            tail_rid = self.table.alloc_tail_rid()
            tail_record = [prev_rid, tail_rid, timestamp, schema_bits] + new_user_values
            self.table.write_record(is_tail=True, rid=tail_rid, columns=tail_record)

            #四次修改 用 table.overwrite_values_at批量覆盖，减少函数调用和重复算偏移
            new_base_schema = base_row[SCHEMA_ENCODING_COLUMN] | schema_bits
            self.table.overwrite_values_at(
                base_locator,
                [INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN],
                [tail_rid, new_base_schema]
            )

            #四次修改 批量覆盖 base 的用户列成最新，select/sum 直接读 base
            user_col_indices = list(range(4, 4 + num_cols))
            self.table.overwrite_values_at(base_locator, user_col_indices, new_user_values)

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

            total = 0
            col = 4 + aggregate_column_index

            #四次修改 base 永远最新sum 只读 base 一次
            for rid in rids:
                locator = lookup(rid)
                if locator is None:
                    continue
                base_row = storage.read_record(locator)
                total += base_row[col]

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
