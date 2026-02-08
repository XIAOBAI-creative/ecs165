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
            schema_encoding = int('0' * self.table.num_columns, 2)
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


    def _get_latest_values(self, base_rid: int):
        """
        Helper: follow indirection chain to get the latest values for a base record.
        Returns the user columns (not metadata).
        """
        locator = self.table.lookup_locator(base_rid)
        if locator is None:
            return None

        base_row = self.table._storage.read_record(locator)
        indirection = base_row[INDIRECTION_COLUMN]

        if indirection == 0:
            # 别list复制了，直接切片返回，这样少一次构建Python list
            return base_row[4:]

        # Get the latest tail record - it contains all current values
        tail_locator = self.table.lookup_locator(indirection)
        if tail_locator is None:
            return base_row[4:]

        tail_row = self.table._storage.read_record(tail_locator)
        return tail_row[4:]


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

            # （二次修改）缓存常用的东西，少做属性查找
            lookup = self.table.lookup_locator
            storage = self.table._storage
            num_cols = self.table.num_columns
            key_col = self.table.key

            results = []
            for rid in rids:
                # （二次修改）把 _get_latest_values 展开到这里，这样少一次函数调用就少一次 list 构造
                locator = lookup(rid)
                if locator is None:
                    continue
                base_row = storage.read_record(locator)
                indirection = base_row[INDIRECTION_COLUMN]
                if indirection == 0:
                    user_columns = base_row[4:]
                else:
                    tail_locator = lookup(indirection)
                    if tail_locator is None:
                        user_columns = base_row[4:]
                    else:
                        tail_row = storage.read_record(tail_locator)
                        user_columns = tail_row[4:]

                # Apply projection
                # （二次修改）不append 循环，直接一次性分配 list再按位往里填
                projected = [None] * num_cols
                for i, keep in enumerate(projected_columns_index):
                    if keep:
                        projected[i] = user_columns[i]
                primary_key = user_columns[key_col]
                record = Record(rid, primary_key, projected)
                results.append(record)

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

            # 还是少属性查找
            num_cols = self.table.num_columns
            key_col = self.table.key

            results = []
            for rid in rids:
                user_columns = self._get_version_values(rid, relative_version)
                if user_columns is None:
                    continue

                # Apply projection
                #还是一次性分配list再按位填
                projected = [None] * num_cols
                for i, keep in enumerate(projected_columns_index):
                    if keep:
                        projected[i] = user_columns[i]

                primary_key = user_columns[key_col]
                record = Record(rid, primary_key, projected)
                results.append(record)

            return results
        except:
            return False


    def _get_version_values(self, base_rid: int, relative_version: int):
        """
        Helper: get values at a specific version.
        relative_version = 0 means latest, -1 means one version back, etc.
        """
        #以后再有类似的东西都换成我这样的，属性查找太慢了
        lookup = self.table.lookup_locator
        storage = self.table._storage
        num_cols = self.table.num_columns

        locator = lookup(base_rid)
        if locator is None:
            return None

        base_row = storage.read_record(locator)
        base_user_columns = base_row[4:]   #（二次修改）别 list复制，后面需要copy的时候再copy
        indirection = base_row[INDIRECTION_COLUMN]
        if indirection == 0:
            # No updates at all, return base
            return list(base_user_columns)  #(erxiciugai)这里再copy

        # 三次修改 update 里 new_user_values已经补全了 None，所以版本查询不需要 schema apply
    #三次修改 直接从最新 tail 开始往后走 k 步，找到对应 tail，返回那条 tail 的 user columns
        k = abs(relative_version)
        current_rid = indirection

        while k > 0 and current_rid != 0 and current_rid >= 10_000_000:
            tail_locator = lookup(current_rid)
            if tail_locator is None:
                current_rid = 0
                break
            tail_row = storage.read_record(tail_locator)
            current_rid = tail_row[INDIRECTION_COLUMN]
            k -= 1

        if current_rid == 0 or current_rid < 10_000_000:
            # Asking for version before any updates, return base
            return list(base_user_columns)

        tail_locator = lookup(current_rid)
        if tail_locator is None:
            return list(base_user_columns)

        tail_row = storage.read_record(tail_locator)
        return list(tail_row[4:])


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

            # Read current base record
            base_row = self.table._storage.read_record(base_locator)
            old_indirection = base_row[INDIRECTION_COLUMN]

            # Get current values (latest)
            current_values = self._get_latest_values(base_rid)

            # Build schema encoding and new values
            schema_bits = 0
            new_user_values = []
            #缓存 num_cols，少属性查找
            num_cols = self.table.num_columns
            for i in range(num_cols):
                if columns[i] is not None:
                    schema_bits |= (1 << (num_cols - 1 - i))
                    new_user_values.append(columns[i])
                else:
                    new_user_values.append(current_values[i])

            # Allocate tail RID
            tail_rid = self.table.alloc_tail_rid()
            timestamp = int(time())

            # （二次修改）tail 的 indirection 永远指向上一条 tail（第一次为0）
            tail_indirection = old_indirection

            # Build tail record
            tail_record = [tail_indirection, tail_rid, timestamp, schema_bits] + new_user_values

            # Write tail record
            self.table.write_record(is_tail=True, rid=tail_rid, columns=tail_record)
            # 你 Page.data 是 list[int]，不能用 struct 往里塞 bytes；要用覆盖写
            self.table.overwrite_value_at(base_locator, INDIRECTION_COLUMN, tail_rid)
            new_base_schema = base_row[SCHEMA_ENCODING_COLUMN] | schema_bits
            self.table.overwrite_value_at(base_locator, SCHEMA_ENCODING_COLUMN, new_base_schema)

            # 三次修改 把 base 的用户列也覆盖成最新值，这样 select和sum不用每条记录再去读tail
            for i in range(num_cols):
                self.table.overwrite_value_at(base_locator, 4 + i, new_user_values[i])

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
    def sum(self, start_range: int, end_range: int, aggregate_column_index: int):#（二次修改）同select一样，别属性查找然后少repeat不必要的东西
        try:
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids:
                return False
            lookup = self.table.lookup_locator
            storage = self.table._storage

            total = 0
            for rid in rids:
                # （三次修改）既然 update 已经把 base 的用户列覆盖成最新，那 sum 直接用base，不再读 tail
                locator = lookup(rid)
                if locator is None:
                    continue
                base_row = storage.read_record(locator)
                total += base_row[4 + aggregate_column_index]

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
