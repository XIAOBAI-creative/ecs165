from dataclasses import dataclass
from collections import OrderedDict

"""
LRU Buffer Pool Manager
"""

@dataclass
class Frame:
    page_id: object = None
    pin: int = 0
    dirty: bool = False
    page: object = None

class BufferPool:
    def __init__ (self, capacity, loader, flusher):
        # capacity: number of frames in the buffer pool
        self.capacity = capacity
        # loader: function that takes a page_id and returns the page data as bytes
        self.loader = loader
        # flusher: function that takes a page_id and page data, and writes it back to storage
        self.flusher = flusher
        
        self.frames = [Frame() for _ in range(capacity)]
        # dict: page_id -> frame index
        self.page_table = {}

        self.free_list = list(range(capacity))
        self.lru = OrderedDict()
        # lru = Least Recently Used
        # evictable pages only pin==0

    def get_frame(self):
        # if there is a free frame
        if self.free_list:
            return self.free_list.pop()
        
        # lru only contains evictable pages
        if not self.lru:
            raise RuntimeError("Buffer pool is full, no evictable page")
    
        # pop the least recently used page
        victim_id, _ = self.lru.popitem(last = False)
        idx = self.page_table[victim_id]
        fr = self.frames[idx]

        if fr.pin != 0:
            raise RuntimeError("LRU contains a pinned page (bug)")

        if fr.dirty:
            self.flusher(victim_id, fr.page)

        del self.page_table[victim_id]
        fr.page_id = None
        fr.page = None
        fr.pin = 0
        fr.dirty = False
        return idx
        

    def fetch_page(self, page_id):
        # hit
        if page_id in self.page_table:
            idx = self.page_table[page_id]
            fr = self.frames[idx]
            fr.pin += 1
        
            # pinned -> not evictable, remove from lru if present
            self.lru.pop(page_id, None)
            return fr.page

        
        # miss, pick one frame
        idx = self.get_frame()
        fr = self.frames[idx]

        page = self.loader(page_id)
        fr.page_id = page_id
        fr.page = page
        fr.pin = 1
        fr.dirty = False

        # map page_id to frame index
        self.page_table[page_id] = idx

        return page

    def unpin_page(self, page_id, is_dirty = False):
        idx = self.page_table.get(page_id)
        if idx is None:
            raise KeyError(f"page_id {page_id} not in buffer pool")

        fr = self.frames[idx]
        if fr.pin <= 0:
            raise RuntimeError("unpin_page called when pin is already 0")

        fr.pin -= 1
        if is_dirty:
            fr.dirty = True
        if fr.pin == 0:
            # move page_id to end of list
            self.lru.pop(page_id, None)
            self.lru[page_id] = True

    def flush_page(self, page_id):
        if page_id not in self.page_table:
            return
        idx = self.page_table[page_id]
        fr = self.frames[idx]
        if fr.dirty:
            self.flusher(page_id, fr.page)
            fr.dirty = False

    def flush_all(self):
        for fr in self.frames:
            if fr.page_id is not None and fr.dirty:
                self.flusher(fr.page_id, fr.page)
                fr.dirty = False
                
    # Merge snapshot helper (contention-free):
    def read_page_bytes(self, page_id) -> bytes:
        # 1) snapshot from in-memory page if present (latest, may be dirty)
        idx = self.page_table.get(page_id)
        if idx is not None:
            fr = self.frames[idx]
            page = fr.page
            if isinstance(page, (bytes, bytearray)):
                return bytes(page)
            if hasattr(page, "to_bytes"):
                return page.to_bytes()
            raise TypeError(
                f"in-buffer page has unsupported type: {type(page)}; "
                "expected Page with to_bytes() or raw bytes."
            )

        # 2) otherwise snapshot from disk
        try:
            page = self.loader(page_id)
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"read_page_bytes cannot find page on disk: {page_id}"
            ) from e

        if isinstance(page, (bytes, bytearray)):
            return bytes(page)
        if hasattr(page, "to_bytes"):
            return page.to_bytes()
        raise TypeError(
            f"loader(page_id) returned unsupported type: {type(page)}; "
            "expected Page with to_bytes() or raw bytes."
        )

    def read_page(self, page_id):
        """
        Optional convenience: read a Page object directly from disk,
        without caching it in the buffer pool.
        """
        return self.loader(page_id)


