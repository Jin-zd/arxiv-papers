"""Terminal progress reporting for OAI-PMH synchronization."""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field
from typing import TextIO

from arxiv_papers.oai import OAIPage


@dataclass(slots=True)
class ProgressDisplay:
    """Render record progress and transfer estimates on one terminal line."""

    stream: TextIO = field(default_factory=lambda: sys.stderr)
    bar_width: int = 28
    started_at: float = field(default_factory=time.monotonic, init=False)
    downloaded_bytes: int = field(default=0, init=False)
    processed_records: int = field(default=0, init=False)
    total_records: int | None = field(default=None, init=False)
    pages: int = field(default=0, init=False)
    current_page: int | None = field(default=None, init=False)
    current_page_bytes: int = field(default=0, init=False)
    _saw_stream_bytes: bool = field(default=False, init=False)
    _last_line_length: int = field(default=0, init=False)
    _last_render_at: float = field(default=0.0, init=False)

    def requesting(self, *, page_number: int) -> None:
        """Show immediate feedback while the next OAI page is in flight."""

        self.current_page = page_number
        self.current_page_bytes = 0
        self._saw_stream_bytes = False
        self._last_render_at = time.monotonic()
        line = f"requesting OAI page {page_number} ..."
        padding = max(0, self._last_line_length - len(line))
        self.stream.write(f"\r{line}{' ' * padding}")
        self.stream.flush()
        self._last_line_length = len(line)

    def received_bytes(self, count: int) -> None:
        """Update transfer statistics while an OAI response is streaming."""

        if count <= 0:
            return
        self.downloaded_bytes += count
        self.current_page_bytes += count
        self._saw_stream_bytes = True
        now = time.monotonic()
        if now - self._last_render_at < 0.1 and self.current_page_bytes < 1024 * 1024:
            return
        elapsed = max(time.monotonic() - self.started_at, 0.001)
        speed = self.downloaded_bytes / elapsed
        page = self.current_page or 0
        line = (
            f"downloading page {page} | current {self._format_bytes(self.current_page_bytes)}"
            f" | total {self._format_bytes(self.downloaded_bytes)}"
            f" | {self._format_rate(speed)}"
        )
        padding = max(0, self._last_line_length - len(line))
        self.stream.write(f"\r{line}{' ' * padding}")
        self.stream.flush()
        self._last_line_length = len(line)
        self._last_render_at = now

    def start(self) -> None:
        """Reset counters when a runner is reused for another sync."""

        self.started_at = time.monotonic()
        self.downloaded_bytes = 0
        self.processed_records = 0
        self.total_records = None
        self.pages = 0
        self.current_page = None
        self.current_page_bytes = 0
        self._saw_stream_bytes = False
        self._last_line_length = 0
        self._last_render_at = 0.0

    def update(self, page: OAIPage, *, page_number: int) -> None:
        """Record one checkpointed page and redraw the progress line."""

        self.pages += 1
        if not self._saw_stream_bytes:
            self.downloaded_bytes += len(page.raw_xml)
        if page.complete_list_size is not None:
            self.total_records = page.complete_list_size

        page_cursor = page.cursor
        if page_cursor is not None:
            self.processed_records = max(
                self.processed_records, page_cursor + len(page.records)
            )
        else:
            self.processed_records += len(page.records)

        elapsed = max(time.monotonic() - self.started_at, 0.001)
        speed = self.downloaded_bytes / elapsed
        remaining_bytes = self._remaining_bytes()
        line = self._format_line(page_number, speed, remaining_bytes)
        padding = max(0, self._last_line_length - len(line))
        self.stream.write(f"\r{line}{' ' * padding}")
        self.stream.flush()
        self._last_line_length = len(line)
        self.current_page_bytes = 0
        self._saw_stream_bytes = False

    def finish(self, *, success: bool) -> None:
        """End the current line so the final JSON starts on a fresh line."""

        elapsed = max(time.monotonic() - self.started_at, 0.001)
        speed = self.downloaded_bytes / elapsed
        status = "done" if success else "failed"
        summary = (
            f"{status} | {self._format_bytes(self.downloaded_bytes)} downloaded"
            f" | {self._format_rate(speed)}"
        )
        padding = max(0, self._last_line_length - len(summary))
        self.stream.write(f"\r{summary}{' ' * padding}\n")
        self.stream.flush()
        self._last_line_length = 0

    def _remaining_bytes(self) -> int | None:
        if self.total_records is None or self.processed_records <= 0:
            return None
        remaining_records = max(self.total_records - self.processed_records, 0)
        average_bytes = self.downloaded_bytes / self.processed_records
        return round(remaining_records * average_bytes)

    def _format_line(
        self, page_number: int, speed: float, remaining_bytes: int | None
    ) -> str:
        if self.total_records and self.total_records > 0:
            ratio = min(self.processed_records / self.total_records, 1.0)
            filled = round(self.bar_width * ratio)
            bar = "=" * max(filled - 1, 0) + (">" if filled else "")
            bar = bar.ljust(self.bar_width, "-")
            progress = (
                f"[{bar}] {ratio * 100:6.2f}% "
                f"records {self.processed_records:,}/{self.total_records:,}"
            )
        else:
            progress = f"records {self.processed_records:,}/?"
        remaining = (
            f"remaining ~{self._format_bytes(remaining_bytes)}"
            if remaining_bytes is not None
            else "remaining ?"
        )
        return (
            f"{progress} | downloaded {self._format_bytes(self.downloaded_bytes)}"
            f" | {self._format_rate(speed)} | {remaining} | page {page_number}"
        )

    @staticmethod
    def _format_bytes(value: float | None) -> str:
        if value is None:
            return "?"
        amount = float(value)
        for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
            if abs(amount) < 1024 or unit == "TiB":
                return f"{amount:.1f} {unit}"
            amount /= 1024
        return f"{amount:.1f} TiB"

    @staticmethod
    def _format_rate(value: float) -> str:
        return f"{ProgressDisplay._format_bytes(value)}/s"


__all__ = ["ProgressDisplay"]
