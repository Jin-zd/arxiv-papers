"""tqdm progress reporting for OAI-PMH synchronization."""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field
from typing import TextIO

from tqdm import tqdm

from arxiv_papers.oai import OAIPage


@dataclass(slots=True)
class ProgressDisplay:
    """Render record progress and transfer statistics with ``tqdm``."""

    stream: TextIO = field(default_factory=lambda: sys.stderr)
    bar: tqdm | None = field(default=None, init=False)
    started_at: float = field(default_factory=time.monotonic, init=False)
    downloaded_bytes: int = field(default=0, init=False)
    processed_records: int = field(default=0, init=False)
    total_records: int | None = field(default=None, init=False)
    pages: int = field(default=0, init=False)
    current_page: int | None = field(default=None, init=False)
    current_page_bytes: int = field(default=0, init=False)
    _saw_stream_bytes: bool = field(default=False, init=False)
    _last_refresh_at: float = field(default=0.0, init=False)

    def start(self) -> None:
        """Reset counters and create a fresh progress bar."""

        if self.bar is not None:
            self.bar.close()
        self.started_at = time.monotonic()
        self.downloaded_bytes = 0
        self.processed_records = 0
        self.total_records = None
        self.pages = 0
        self.current_page = None
        self.current_page_bytes = 0
        self._saw_stream_bytes = False
        self._last_refresh_at = 0.0
        self.bar = tqdm(
            total=None,
            desc="sync",
            unit="record",
            dynamic_ncols=True,
            file=self.stream,
            leave=True,
        )

    def requesting(self, *, page_number: int) -> None:
        """Show the page currently being requested."""

        self._ensure_bar()
        bar = self.bar
        assert bar is not None
        self.current_page = page_number
        self.current_page_bytes = 0
        self._saw_stream_bytes = False
        bar.set_description(f"sync page {page_number}")
        self._refresh(force=True)

    def received_bytes(self, count: int) -> None:
        """Update transfer statistics while an OAI response is streaming."""

        if count <= 0:
            return
        self._ensure_bar()
        self.downloaded_bytes += count
        self.current_page_bytes += count
        self._saw_stream_bytes = True
        self._refresh()

    def update(self, page: OAIPage, *, page_number: int) -> None:
        """Record one checkpointed page."""

        self._ensure_bar()
        bar = self.bar
        assert bar is not None
        self.pages += 1
        if not self._saw_stream_bytes:
            self.downloaded_bytes += len(page.raw_xml)
        if page.complete_list_size is not None:
            self.total_records = page.complete_list_size
            bar.total = page.complete_list_size

        if page.cursor is not None:
            processed = max(self.processed_records, page.cursor + len(page.records))
        else:
            processed = self.processed_records + len(page.records)
        bar.update(processed - self.processed_records)
        self.processed_records = processed
        self.current_page = page_number
        bar.set_description(f"sync page {page_number}")
        self._set_postfix()
        bar.refresh()
        self.current_page_bytes = 0
        self._saw_stream_bytes = False

    def finish(self, *, success: bool) -> None:
        """Close the progress bar and leave the next output on a new line."""

        if self.bar is None:
            return
        self.bar.set_description("sync complete" if success else "sync failed")
        self._set_postfix()
        self.bar.close()
        self.bar = None

    def _ensure_bar(self) -> None:
        if self.bar is None:
            self.start()

    def _refresh(self, *, force: bool = False) -> None:
        if self.bar is None:
            return
        now = time.monotonic()
        if not force and now - self._last_refresh_at < 0.1:
            return
        self._set_postfix()
        self.bar.refresh()
        self._last_refresh_at = now

    def _set_postfix(self) -> None:
        if self.bar is None:
            return
        elapsed = max(time.monotonic() - self.started_at, 0.001)
        speed = self.downloaded_bytes / elapsed
        self.bar.set_postfix(
            downloaded=self._format_bytes(self.downloaded_bytes),
            rate=f"{self._format_bytes(speed)}/s",
            page=self.current_page or 0,
        )

    @staticmethod
    def _format_bytes(value: float | None) -> str:
        if value is None:
            return "?"
        amount = float(value)
        for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
            if abs(amount) < 1024 or unit == "TiB":
                return f"{amount:.1f}{unit}"
            amount /= 1024
        return f"{amount:.1f}TiB"


__all__ = ["ProgressDisplay"]
