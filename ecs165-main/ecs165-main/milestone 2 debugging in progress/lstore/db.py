from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional
import json

from lstore.table import Table
from lstore.bufferpool import BufferPool
from lstore.page import Page


class Database:
    def __init__(self):
        self.tables: Dict[str, Table] = {}
        self.path: Optional[str] = None
        self.bp: Optional[BufferPool] = None

    def open(self, path: str):
        self.path = str(path)
        base = Path(self.path)
        (base / "pages").mkdir(parents=True, exist_ok=True)

        # Grading config: bufferpool size = 32 frames
        self.bp = BufferPool(
            capacity=32,
            loader=self._load_page,
            flusher=self._flush_page,
        )

        # Load catalog (tables metadata)
        catalog_path = base / "catalog.json"
        if catalog_path.exists():
            meta = json.loads(catalog_path.read_text())
            for name, tmeta in meta.get("tables", {}).items():
                t = Table.from_metadata(tmeta, buffer_pool=self.bp)
                self.tables[name] = t

    def close(self):
        # Flush all dirty pages
        if self.bp is not None:
            self.bp.flush_all()

        # Persist catalog
        if self.path is None:
            return
        base = Path(self.path)
        meta = {"tables": {name: t.to_metadata() for name, t in self.tables.items()}}
        (base / "catalog.json").write_text(json.dumps(meta))

    # ------------------------------------------------------------------
    # Disk layout:
    #   pages/<table>/pr_<pr>/
    #       base/v_<base_ver>/c_<col>/p_<pid>.bin     (versioned base pages)
    #       base/c_<col>/p_<pid>.bin                  (legacy base pages)
    #       tail/c_<col>/p_<pid>.bin                  (tail pages)
    #
    # page_id formats supported:
    #   (table, pr, is_tail, col, pid)                     len=5  [legacy]
    #   (table, pr, False, base_ver, col, pid)             len=6  [versioned base]
    # ------------------------------------------------------------------
    def _page_file(self, page_id):
        if self.path is None:
            raise RuntimeError("Database not opened")

        if not isinstance(page_id, tuple):
            raise ValueError(f"page_id must be a tuple, got: {type(page_id)}")

        # Legacy format: (table, pr, is_tail, col, pid)
        if len(page_id) == 5:
            table, pr, is_tail, col, pid = page_id
            kind = "tail" if bool(is_tail) else "base"
            return (
                Path(self.path)
                / "pages"
                / str(table)
                / f"pr_{int(pr)}"
                / kind
                / f"c_{int(col)}"
                / f"p_{int(pid)}.bin"
            )

        # Versioned base format: (table, pr, False, base_ver, col, pid)
        if len(page_id) == 6:
            table, pr, is_tail, base_ver, col, pid = page_id
            kind = "tail" if bool(is_tail) else "base"

            # If it's a base page, keep versions separate
            if not bool(is_tail):
                return (
                    Path(self.path)
                    / "pages"
                    / str(table)
                    / f"pr_{int(pr)}"
                    / kind
                    / f"v_{int(base_ver)}"
                    / f"c_{int(col)}"
                    / f"p_{int(pid)}.bin"
                )

            # Tail pages shouldn't normally be versioned, but handle gracefully
            return (
                Path(self.path)
                / "pages"
                / str(table)
                / f"pr_{int(pr)}"
                / kind
                / f"c_{int(col)}"
                / f"p_{int(pid)}.bin"
            )

        raise ValueError(f"Unsupported page_id format (len={len(page_id)}): {page_id!r}")

    def _load_page(self, page_id):
        f = self._page_file(page_id)

        # Extract (is_tail, col) for Page constructor
        if len(page_id) == 5:
            _, _, is_tail, col, _ = page_id
        else:
            _, _, is_tail, _, col, _ = page_id  # skip base_ver

        p = Page(int(col), is_tail=bool(is_tail))
        if f.exists():
            p.from_bytes(f.read_bytes())
        return p

    def _flush_page(self, page_id, page: Page):
        f = self._page_file(page_id)
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(page.to_bytes())

    # ------------------------------------------------------------------
    # Public table APIs
    # ------------------------------------------------------------------
    def create_table(self, name: str, num_columns: int, key_index: int) -> Table:
        if self.bp is None:
            # Open wasn't called; still allow table creation (in-memory)
            table = Table(name, num_columns, key_index, buffer_pool=None)
        else:
            table = Table(name, num_columns, key_index, buffer_pool=self.bp)
        self.tables[name] = table
        return table

    def drop_table(self, name: str) -> None:
        if name in self.tables:
            del self.tables[name]

    def get_table(self, name: str) -> Optional[Table]:
        return self.tables.get(name, None)
