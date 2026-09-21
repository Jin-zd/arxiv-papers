"""Read-only query benchmarks for a local DuckDB snapshot."""

from __future__ import annotations

import math
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from arxiv_papers.query import DuckDBQueryService, SearchPage, SearchPosition


class BenchmarkError(ValueError):
    """Raised when benchmark settings are invalid."""


def benchmark_snapshot(
    snapshot_path: Path | str,
    *,
    iterations: int = 5,
    limit: int = 20,
) -> dict[str, Any]:
    """Measure representative local query shapes without changing the snapshot."""

    if iterations < 1:
        raise BenchmarkError("iterations must be positive")
    if limit < 1 or limit > DuckDBQueryService.MAX_LIMIT:
        raise BenchmarkError(
            f"limit must be between 1 and {DuckDBQueryService.MAX_LIMIT}"
        )

    with DuckDBQueryService(snapshot_path) as service:
        values = _representative_values(service)
        cases: dict[str, dict[str, Any]] = {}

        operations: list[tuple[str, Callable[[SearchPosition | None], SearchPage]]] = [
            (
                "keyword",
                lambda after: service.search_papers(
                    query=values["keyword"], limit=limit, after=after
                ),
            ),
            (
                "author",
                lambda after: service.search_by_author(
                    values["author"], limit=limit, after=after
                ),
            ),
            (
                "category",
                lambda after: service.search_by_category(
                    values["category"], limit=limit, after=after
                ),
            ),
        ]
        for name, operation in operations:
            first_page = operation(None)
            cases[name] = _measure_page(operation, iterations)
            if first_page.has_more and first_page.next_position is not None:
                cases[f"{name}_page_2"] = _measure_page(
                    lambda _after, operation=operation, position=first_page.next_position: (
                        operation(position)
                    ),
                    iterations,
                )
            else:
                cases[f"{name}_page_2"] = {"skipped": True, "reason": "no next page"}

        arxiv_id = values["arxiv_id"]
        cases["id"] = _measure_value(lambda: service.get_paper(arxiv_id), iterations)

        return {
            "snapshot": str(Path(snapshot_path)),
            "iterations": iterations,
            "limit": limit,
            "inputs": values,
            "cases": cases,
        }


def _measure_page(
    operation: Callable[[SearchPosition | None], SearchPage], iterations: int
) -> dict[str, Any]:
    durations: list[float] = []
    page: SearchPage | None = None
    for _ in range(iterations):
        started = time.perf_counter()
        page = operation(None)
        durations.append((time.perf_counter() - started) * 1000)
    assert page is not None
    return {
        **_duration_summary(durations),
        "result_count": len(page.items),
        "has_more": page.has_more,
    }


def _measure_value(operation: Callable[[], object], iterations: int) -> dict[str, Any]:
    durations: list[float] = []
    value: object = None
    for _ in range(iterations):
        started = time.perf_counter()
        value = operation()
        durations.append((time.perf_counter() - started) * 1000)
    return {
        **_duration_summary(durations),
        "result_count": 1 if value is not None else 0,
        "has_more": False,
    }


def _duration_summary(durations: list[float]) -> dict[str, float]:
    ordered = sorted(durations)
    p95_index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * 0.95) - 1))
    return {
        "min_ms": round(ordered[0], 3),
        "median_ms": round(ordered[len(ordered) // 2], 3),
        "p95_ms": round(ordered[p95_index], 3),
    }


def _representative_values(service: DuckDBQueryService) -> dict[str, str]:
    connection = service.connection
    title_row = connection.execute(
        "SELECT title FROM papers WHERE title IS NOT NULL ORDER BY arxiv_id LIMIT 1"
    ).fetchone()
    title = str(title_row[0]) if title_row else "arxiv"
    words = [word for word in re.findall(r"[A-Za-z]{4,}", title)]
    author_row = connection.execute(
        "SELECT display_name FROM authors ORDER BY display_name LIMIT 1"
    ).fetchone()
    category_row = connection.execute(
        "SELECT category FROM categories ORDER BY category LIMIT 1"
    ).fetchone()
    id_row = connection.execute(
        "SELECT arxiv_id FROM papers ORDER BY arxiv_id LIMIT 1"
    ).fetchone()
    return {
        "keyword": words[0] if words else "arxiv",
        "author": str(author_row[0]) if author_row else "author",
        "category": str(category_row[0]) if category_row else "cs.",
        "arxiv_id": str(id_row[0]) if id_row else "0000.00000",
    }


__all__ = ["BenchmarkError", "benchmark_snapshot"]
