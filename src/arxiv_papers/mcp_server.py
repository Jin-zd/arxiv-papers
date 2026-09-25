"""Local read-only MCP server for an arXiv DuckDB snapshot."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import logging
import math
import threading
import time
from collections import OrderedDict
from collections.abc import Mapping
from concurrent.futures import Future
from datetime import date, datetime
from pathlib import Path
from typing import Any, Self

import duckdb
from mcp.server.mcpserver import MCPServer

from arxiv_papers.query import (
    DuckDBQueryService,
    QueryError,
    SearchPage,
    SearchPosition,
)
from arxiv_papers.runtime import DEFAULT_RUNTIME, DuckDBRuntimeConfig
from arxiv_papers.snapshots import SnapshotCatalog, SnapshotCatalogError

LOGGER = logging.getLogger(__name__)
_CACHE_MISS = object()


class _SnapshotRetired(QueryError):
    """Internal signal to retry acquisition on the newly published generation."""


class SnapshotResolutionError(RuntimeError):
    """Raised when no usable local query snapshot can be found."""


class _SnapshotLease:
    def __init__(self, handle: _SnapshotHandle, query: DuckDBQueryService) -> None:
        self._handle = handle
        self.query = query

    @property
    def generation(self) -> str:
        return self._handle.generation

    @property
    def snapshot_path(self) -> Path:
        return self._handle.snapshot_path

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self._handle.release(self.query)


class _SnapshotHandle:
    """Own a bounded pool of read-only connections for one immutable snapshot."""

    def __init__(
        self,
        snapshot_path: Path,
        generation: str,
        max_connections: int,
        *,
        pool_timeout_seconds: float,
        runtime: DuckDBRuntimeConfig,
    ) -> None:
        self.snapshot_path = snapshot_path
        self.generation = generation
        self.max_connections = max_connections
        self.pool_timeout_seconds = pool_timeout_seconds
        self.runtime = runtime
        self._condition = threading.Condition()
        self._available: list[DuckDBQueryService] = []
        self._created = 0
        self._active = 0
        self._retiring = False

    def acquire(self) -> _SnapshotLease:
        create_connection = False
        started_at = time.monotonic()
        deadline = started_at + self.pool_timeout_seconds
        with self._condition:
            while True:
                if self._retiring:
                    raise _SnapshotRetired("snapshot generation is no longer active")
                if self._available:
                    query = self._available.pop()
                    self._active += 1
                    _log_pool_acquire(self.generation, started_at)
                    return _SnapshotLease(self, query)
                if self._created < self.max_connections:
                    self._created += 1
                    self._active += 1
                    create_connection = True
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise QueryError("MCP snapshot connection pool is saturated")
                self._condition.wait(remaining)

        if create_connection:
            try:
                query = DuckDBQueryService(self.snapshot_path, runtime=self.runtime)
            except Exception:
                with self._condition:
                    self._created -= 1
                    self._active -= 1
                    self._condition.notify()
                raise
            _log_pool_acquire(self.generation, started_at)
            return _SnapshotLease(self, query)
        raise AssertionError("snapshot connection acquisition did not complete")

    def release(self, query: DuckDBQueryService) -> None:
        close_query = False
        with self._condition:
            self._active -= 1
            if self._retiring:
                self._created -= 1
                close_query = True
            else:
                self._available.append(query)
            self._condition.notify()
        if close_query:
            query.close()

    def retire(self) -> None:
        available: list[DuckDBQueryService]
        with self._condition:
            if self._retiring:
                return
            self._retiring = True
            available = self._available
            self._available = []
            self._created -= len(available)
            self._condition.notify_all()
        for query in available:
            query.close()


class _SnapshotManager:
    """Publish immutable snapshot handles without holding a query-wide lock."""

    def __init__(
        self,
        snapshot_path: Path,
        catalog: SnapshotCatalog | None,
        *,
        max_connections: int,
        runtime: DuckDBRuntimeConfig,
    ) -> None:
        if max_connections < 1:
            raise ValueError("max_connections must be positive")
        self.catalog = catalog
        self.max_connections = max_connections
        self.runtime = runtime
        self._lock = threading.Lock()
        self._closed = False
        self._current = _SnapshotHandle(
            snapshot_path,
            self._initial_generation(snapshot_path, catalog),
            max_connections,
            pool_timeout_seconds=runtime.pool_timeout_seconds,
            runtime=runtime,
        )
        self._pointer_checked_at = 0.0
        self._pointer_mtime_ns: int | None = None

    @staticmethod
    def _initial_generation(
        snapshot_path: Path, catalog: SnapshotCatalog | None
    ) -> str:
        if catalog is not None:
            try:
                pointer = catalog.read_current()
            except SnapshotCatalogError:
                pointer = None
            if (
                pointer is not None
                and pointer.snapshot_path.is_file()
                and _same_path(pointer.snapshot_path, snapshot_path)
            ):
                return pointer.generation
        return str(snapshot_path.resolve())

    def acquire(self) -> _SnapshotLease:
        self._refresh()
        while True:
            with self._lock:
                if self._closed:
                    raise QueryError("MCP snapshot manager is closed")
                current = self._current
            try:
                return current.acquire()
            except _SnapshotRetired:
                # A refresh may retire the handle after it was observed.
                continue

    def generation(self) -> str:
        """Return the current generation without consuming a pool slot."""

        self._refresh()
        with self._lock:
            if self._closed:
                raise QueryError("MCP snapshot manager is closed")
            return self._current.generation

    def _refresh(self) -> None:
        if self.catalog is None:
            return
        now = time.monotonic()
        if now - self._pointer_checked_at < self.runtime.pointer_ttl_seconds:
            return
        self._pointer_checked_at = now
        try:
            mtime_ns = self.catalog.pointer_path.stat().st_mtime_ns
        except OSError:
            mtime_ns = None
        if mtime_ns is not None and mtime_ns == self._pointer_mtime_ns:
            return
        self._pointer_mtime_ns = mtime_ns
        try:
            pointer = self.catalog.read_current()
        except SnapshotCatalogError as error:
            LOGGER.warning("ignoring invalid current snapshot pointer: %s", error)
            return
        if pointer is None or not pointer.snapshot_path.is_file():
            return

        with self._lock:
            current = self._current
            if (
                _same_path(pointer.snapshot_path, current.snapshot_path)
                and pointer.generation == current.generation
            ):
                return
            self._current = _SnapshotHandle(
                pointer.snapshot_path,
                pointer.generation,
                self.max_connections,
                pool_timeout_seconds=self.runtime.pool_timeout_seconds,
                runtime=self.runtime,
            )
        current.retire()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            current = self._current
        current.retire()


class LocalArxivMCPServer:
    """Expose local snapshot queries as MCP tools over STDIO."""

    def __init__(
        self,
        snapshot_path: Path | str,
        *,
        catalog: SnapshotCatalog | None = None,
        max_connections: int | None = None,
        cache_size: int | None = None,
        runtime: DuckDBRuntimeConfig = DEFAULT_RUNTIME,
    ) -> None:
        effective_connections = (
            runtime.max_connections if max_connections is None else max_connections
        )
        effective_cache_bytes = (
            runtime.paper_cache_bytes if cache_size is None else cache_size
        )
        if effective_cache_bytes < 0:
            raise ValueError("cache_size must be non-negative")
        initial_path = Path(snapshot_path)
        self.catalog = catalog
        self._snapshots = _SnapshotManager(
            initial_path,
            catalog,
            max_connections=effective_connections,
            runtime=runtime,
        )
        self._cache_size_bytes = effective_cache_bytes
        self._search_cache_size = runtime.search_cache_size
        self._search_cache_ttl = runtime.search_cache_ttl_seconds
        self._cache_lock = threading.Lock()
        self._paper_cache: OrderedDict[
            tuple[str, str, int | None], dict[str, object] | None
        ] = OrderedDict()
        self._paper_cache_sizes: dict[tuple[str, str, int | None], int] = {}
        self._paper_cache_bytes = 0
        self._search_cache: OrderedDict[
            str, tuple[float, dict[str, Any], int]
        ] = OrderedDict()
        self._search_cache_bytes = 0
        # Coalesce identical paper requests before they acquire a snapshot
        # connection.  This prevents a burst of duplicate model calls from
        # occupying every pool slot while waiting for the first query.
        self._inflight: dict[
            tuple[str, str, int | None], Future[dict[str, object] | None]
        ] = {}
        self.server = MCPServer(
            name="arxiv-papers",
            version="0.1.0",
            description="Read-only search over a locally synchronized arXiv metadata snapshot.",
            instructions=(
                "This server only queries its local DuckDB snapshot. "
                "It never calls arXiv or executes arbitrary SQL."
            ),
        )
        self.server.add_tool(
            self.search_papers,
            description=(
                "Search current local arXiv papers by keyword and optional author, "
                "category, and date filters. Results are cursor-paginated."
            ),
            structured_output=True,
        )
        self.server.add_tool(
            self.get_paper,
            description=(
                "Get one local arXiv paper by ID, including current metadata, "
                "authors, categories, and available version facts."
            ),
            structured_output=True,
        )
        self.server.add_tool(
            self.get_papers,
            description=(
                "Get up to 100 local arXiv papers in one batch, including current "
                "metadata, authors, categories, and available version facts."
            ),
            structured_output=True,
        )
        self.server.add_tool(
            self.search_by_author,
            description="Search current local arXiv papers by author name with cursor pagination.",
            structured_output=True,
        )
        self.server.add_tool(
            self.search_by_category,
            description="Search current local arXiv papers by category with cursor pagination.",
            structured_output=True,
        )
        self.server.add_tool(
            self.library_status,
            description="Return local snapshot counts and search-index status.",
            structured_output=True,
        )

    def close(self) -> None:
        self._snapshots.close()

    def _cursor_fingerprint(
        self, filters: Mapping[str, object], *, snapshot_identity: str
    ) -> str:
        payload = {
            **filters,
            "snapshot": snapshot_identity,
        }
        encoded = json.dumps(
            payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def search_papers(
        self,
        query: str | None = None,
        author: str | None = None,
        category: str | None = None,
        primary_only: bool = False,
        created_from: str | None = None,
        created_until: str | None = None,
        updated_from: str | None = None,
        updated_until: str | None = None,
        limit: int = 20,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Search local current papers using keyword and structured filters."""

        with self._snapshots.acquire() as lease:
            search_key = self._search_key(
                "search",
                {
                    "query": query,
                    "author": author,
                    "category": category,
                    "primary_only": primary_only,
                    "created_from": created_from,
                    "created_until": created_until,
                    "updated_from": updated_from,
                    "updated_until": updated_until,
                    "limit": limit,
                    "cursor": cursor,
                },
                lease.generation,
            )
            cached_page = self._search_cache_get(search_key)
            if cached_page is not _CACHE_MISS:
                assert isinstance(cached_page, dict)
                LOGGER.debug("mcp_search_cache_hit", extra={"generation": lease.generation})
                return cached_page
            fingerprint = self._cursor_fingerprint(
                {
                    "query": query,
                    "author": author,
                    "category": category,
                    "primary_only": primary_only,
                    "created_from": created_from,
                    "created_until": created_until,
                    "updated_from": updated_from,
                    "updated_until": updated_until,
                },
                snapshot_identity=lease.generation,
            )
            after = _decode_cursor(cursor, fingerprint)
            started_at = time.monotonic()
            page = lease.query.search_papers(
                query=query,
                author=author,
                category=category,
                primary_only=primary_only,
                created_from=created_from,
                created_until=created_until,
                updated_from=updated_from,
                updated_until=updated_until,
                limit=limit,
                after=after,
            )
            LOGGER.debug(
                "mcp_search_completed",
                extra={
                    "generation": lease.generation,
                    "elapsed_seconds": round(time.monotonic() - started_at, 6),
                    "items": len(page.items),
                },
            )
        payload = _page_payload(page, fingerprint)
        self._search_cache_put(search_key, payload)
        return payload

    def get_paper(self, arxiv_id: str, version: int | None = None) -> dict[str, Any]:
        """Get one current local paper and its available version facts."""

        canonical_id = arxiv_id.strip()
        # Resolve generation without acquiring a DB connection.  Generation is
        # part of the key so a publication during an in-flight request cannot
        # make callers for the new snapshot reuse an old result.
        generation = self._snapshots.generation()
        request_key = (generation, canonical_id, version)
        cached = self._cache_get(request_key)
        if cached is not _CACHE_MISS:
            return {"paper": cached}
        with self._cache_lock:
            future = self._inflight.get(request_key)
            owner = future is None
            if owner:
                future = Future()
                self._inflight[request_key] = future

        assert future is not None
        if not owner:
            # Waiting happens without holding a snapshot lease, so duplicate
            # requests cannot consume connection-pool capacity.
            paper = future.result()
            return {"paper": copy.deepcopy(paper)}

        try:
            with self._snapshots.acquire() as lease:
                cache_key = (lease.generation, canonical_id, version)
                cached = self._cache_get(cache_key)
                if cached is _CACHE_MISS:
                    paper = _jsonable(
                        lease.query.get_paper(canonical_id, version=version)
                    )
                    assert paper is None or isinstance(paper, dict)
                    self._cache_put(cache_key, paper)
                else:
                    paper = cached
                    assert paper is None or isinstance(paper, dict)
            future.set_result(paper)
            return {"paper": copy.deepcopy(paper)}
        except BaseException as error:
            future.set_exception(error)
            raise
        finally:
            with self._cache_lock:
                if self._inflight.get(request_key) is future:
                    self._inflight.pop(request_key, None)

    def get_papers(
        self, arxiv_ids: list[str], version: int | None = None
    ) -> dict[str, Any]:
        """Get up to 100 current local papers in one batch."""

        if not arxiv_ids:
            raise QueryError("arxiv_ids must not be empty")
        if len(arxiv_ids) > 100:
            raise QueryError("at most 100 arxiv_ids may be requested")
        canonical_ids = tuple(item.strip() for item in arxiv_ids)
        if any(not item for item in canonical_ids):
            raise QueryError("arxiv_ids must not contain empty values")

        with self._snapshots.acquire() as lease:
            unique_ids = tuple(dict.fromkeys(canonical_ids))
            papers: dict[str, object] = {}
            missing: list[str] = []
            for arxiv_id in unique_ids:
                key = (lease.generation, arxiv_id, version)
                cached = self._cache_get(key)
                if cached is _CACHE_MISS:
                    missing.append(arxiv_id)
                else:
                    papers[arxiv_id] = cached

            if missing:
                loaded = lease.query.get_papers(missing, version=version)
                for arxiv_id in missing:
                    paper = _jsonable(loaded.get(arxiv_id))
                    papers[arxiv_id] = paper
                    self._cache_put((lease.generation, arxiv_id, version), paper)

            return {
                "papers": [
                    {"arxiv_id": arxiv_id, "paper": copy.deepcopy(papers[arxiv_id])}
                    for arxiv_id in canonical_ids
                ]
            }

    def search_by_author(
        self,
        author: str,
        limit: int = 20,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Search local current papers by normalized author display name."""

        with self._snapshots.acquire() as lease:
            search_key = self._search_key(
                "author", {"author": author, "limit": limit, "cursor": cursor}, lease.generation
            )
            cached_page = self._search_cache_get(search_key)
            if cached_page is not _CACHE_MISS:
                assert isinstance(cached_page, dict)
                return cached_page
            fingerprint = self._cursor_fingerprint(
                {"author": author}, snapshot_identity=lease.generation
            )
            after = _decode_cursor(cursor, fingerprint)
            page = lease.query.search_by_author(author, limit=limit, after=after)
        payload = _page_payload(page, fingerprint)
        self._search_cache_put(search_key, payload)
        return payload

    def search_by_category(
        self,
        category: str,
        primary_only: bool = False,
        limit: int = 20,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Search local current papers by exact category or category prefix."""

        with self._snapshots.acquire() as lease:
            search_key = self._search_key(
                "category",
                {
                    "category": category,
                    "primary_only": primary_only,
                    "limit": limit,
                    "cursor": cursor,
                },
                lease.generation,
            )
            cached_page = self._search_cache_get(search_key)
            if cached_page is not _CACHE_MISS:
                assert isinstance(cached_page, dict)
                return cached_page
            fingerprint = self._cursor_fingerprint(
                {"category": category, "primary_only": primary_only},
                snapshot_identity=lease.generation,
            )
            after = _decode_cursor(cursor, fingerprint)
            page = lease.query.search_by_category(
                category,
                primary_only=primary_only,
                limit=limit,
                after=after,
            )
        payload = _page_payload(page, fingerprint)
        self._search_cache_put(search_key, payload)
        return payload

    def library_status(self) -> dict[str, Any]:
        """Return local snapshot metadata without contacting the network."""

        with self._snapshots.acquire() as lease:
            query = lease.query
            snapshot_path = lease.snapshot_path
            generation = lease.generation
            rows = query.connection.execute(
                "SELECT key, value FROM library_metadata"
            ).fetchall()
            paper_count = query.connection.execute(
                "SELECT count(*) FROM papers"
            ).fetchone()
            version_count = query.connection.execute(
                "SELECT count(*) FROM paper_versions"
            ).fetchone()
            fts_enabled = query.fts_enabled
        metadata = {str(row[0]): str(row[1]) for row in rows}
        return {
            "snapshot_path": str(snapshot_path),
            "schema_version": metadata.get("schema_version"),
            "built_at": metadata.get("built_at"),
            "source": metadata.get("source"),
            "papers": int(paper_count[0]) if paper_count else 0,
            "paper_versions": int(version_count[0]) if version_count else 0,
            "fts_enabled": fts_enabled,
            "generation": generation,
        }

    def _cache_get(
        self, key: tuple[str, str, int | None]
    ) -> dict[str, object] | None | object:
        if self._cache_size_bytes == 0:
            return _CACHE_MISS
        with self._cache_lock:
            if key not in self._paper_cache:
                return _CACHE_MISS
            value = self._paper_cache.pop(key)
            self._paper_cache[key] = value
            return copy.deepcopy(value)

    def _cache_put(
        self, key: tuple[str, str, int | None], value: dict[str, object] | None
    ) -> None:
        if self._cache_size_bytes == 0:
            return
        with self._cache_lock:
            self._paper_cache.pop(key, None)
            self._paper_cache_bytes -= self._paper_cache_sizes.pop(key, 0)
            cached = copy.deepcopy(value)
            self._paper_cache[key] = cached
            size = _estimate_json_bytes(cached)
            self._paper_cache_sizes[key] = size
            self._paper_cache_bytes += size
            self._paper_cache.move_to_end(key)
            while self._paper_cache and self._paper_cache_bytes > self._cache_size_bytes:
                removed_key, _ = self._paper_cache.popitem(last=False)
                self._paper_cache_bytes -= self._paper_cache_sizes.pop(
                    removed_key, 0
                )

    @staticmethod
    def _search_key(operation: str, payload: Mapping[str, object], generation: str) -> str:
        return json.dumps(
            {"operation": operation, "generation": generation, **payload},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )

    def _search_cache_get(self, key: str) -> dict[str, Any] | object:
        if self._search_cache_size == 0:
            return _CACHE_MISS
        now = time.monotonic()
        with self._cache_lock:
            cached = self._search_cache.get(key)
            if cached is None:
                return _CACHE_MISS
            created, value, size = cached
            if now - created >= self._search_cache_ttl:
                self._search_cache.pop(key, None)
                self._search_cache_bytes -= size
                return _CACHE_MISS
            self._search_cache.move_to_end(key)
            return copy.deepcopy(value)

    def _search_cache_put(self, key: str, value: dict[str, Any]) -> None:
        if self._search_cache_size == 0:
            return
        cached = copy.deepcopy(value)
        size = _estimate_json_bytes(cached)
        with self._cache_lock:
            previous = self._search_cache.pop(key, None)
            if previous is not None:
                self._search_cache_bytes -= previous[2]
            self._search_cache[key] = (time.monotonic(), cached, size)
            self._search_cache_bytes += size
            self._search_cache.move_to_end(key)
            while len(self._search_cache) > self._search_cache_size:
                _, removed = self._search_cache.popitem(last=False)
                self._search_cache_bytes -= removed[2]


def resolve_snapshot_path(
    data_dir: Path | str = "data",
    snapshot: Path | str | None = None,
    sync_name: str | None = None,
) -> Path:
    """Resolve a snapshot without creating or modifying any local files."""

    if snapshot is not None:
        path = Path(snapshot).expanduser()
        if path.is_file():
            return path
        raise SnapshotResolutionError(f"snapshot does not exist: {path}")

    data_path = Path(data_dir)
    if sync_name is None:
        catalog = SnapshotCatalog(data_path / "duckdb")
        try:
            current_snapshot = catalog.current_snapshot()
        except SnapshotCatalogError as error:
            raise SnapshotResolutionError(str(error)) from error
        if current_snapshot is not None:
            return current_snapshot
    state_path = data_path / "state" / "sync.duckdb"
    if state_path.is_file():
        state_snapshot = _snapshot_from_state(state_path, sync_name)
        if state_snapshot is not None and state_snapshot.is_file():
            return state_snapshot

    duckdb_dir = data_path / "duckdb"
    candidates = tuple(duckdb_dir.glob("library-*.duckdb"))
    if candidates:
        return max(candidates, key=lambda path: path.stat().st_mtime_ns)
    raise SnapshotResolutionError(
        f"no completed local snapshot found under {data_path}; "
        "run sync full or pass --snapshot"
    )


def run_stdio(
    data_dir: Path | str = "data",
    snapshot: Path | str | None = None,
    sync_name: str | None = None,
) -> None:
    """Run the local MCP server over STDIO."""

    snapshot_path = resolve_snapshot_path(data_dir, snapshot, sync_name)
    catalog = (
        None
        if snapshot is not None or sync_name is not None
        else SnapshotCatalog(Path(data_dir) / "duckdb")
    )
    application = LocalArxivMCPServer(snapshot_path, catalog=catalog)
    try:
        application.server.run("stdio")
    finally:
        application.close()


def _snapshot_from_state(state_path: Path, sync_name: str | None) -> Path | None:
    try:
        with duckdb.connect(str(state_path), read_only=True) as connection:
            if sync_name:
                row = connection.execute(
                    """
                    SELECT snapshot_path
                    FROM sync_state
                    WHERE sync_name = ? AND status = 'completed'
                    """,
                    [sync_name],
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT snapshot_path
                    FROM sync_state
                    WHERE status = 'completed' AND snapshot_path IS NOT NULL
                    ORDER BY completed_at DESC NULLS LAST
                    LIMIT 1
                    """
                ).fetchone()
    except duckdb.Error as error:
        raise SnapshotResolutionError(
            f"failed to read sync state: {state_path}"
        ) from error
    if not row or not row[0]:
        return None
    return Path(str(row[0])).expanduser()


def _same_path(left: Path, right: Path) -> bool:
    return left.resolve() == right.resolve()


def _log_pool_acquire(generation: str, started_at: float) -> None:
    wait_seconds = time.monotonic() - started_at
    if wait_seconds >= 0.001:
        LOGGER.debug(
            "mcp_pool_acquired",
            extra={"generation": generation, "wait_seconds": round(wait_seconds, 6)},
        )


def _page_payload(page: SearchPage, fingerprint: str) -> dict[str, Any]:
    items = [_jsonable(item) for item in page.items]
    next_cursor = (
        _encode_cursor(page.next_position, fingerprint)
        if page.has_more and page.next_position is not None
        else None
    )
    return {
        "items": items,
        "limit": page.limit,
        "has_more": page.has_more,
        "next_cursor": next_cursor,
    }


def _encode_cursor(position: SearchPosition, fingerprint: str) -> str:
    payload = json.dumps(
        {
            "fingerprint": fingerprint,
            "position": {
                "relevance": position.relevance,
                "updated": position.updated.isoformat()
                if position.updated is not None
                else None,
                "arxiv_id": position.arxiv_id,
            },
        },
        separators=(",", ":"),
    ).encode("ascii")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_cursor(cursor: str | None, fingerprint: str) -> SearchPosition | None:
    if cursor is None:
        return None
    try:
        padding = "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(cursor + padding).decode("ascii"))
        if payload.get("fingerprint") != fingerprint:
            raise ValueError
        position = payload["position"]
        relevance = position["relevance"]
        if (
            isinstance(relevance, bool)
            or not isinstance(relevance, (int, float))
            or not math.isfinite(float(relevance))
        ):
            raise ValueError
        updated = position["updated"]
        parsed_updated = date.fromisoformat(updated) if updated is not None else None
        arxiv_id = position["arxiv_id"]
        if not isinstance(arxiv_id, str) or not arxiv_id:
            raise ValueError
        return SearchPosition(float(relevance), parsed_updated, arxiv_id)
    except (
        ValueError,
        KeyError,
        TypeError,
        UnicodeError,
        json.JSONDecodeError,
    ) as error:
        raise QueryError("invalid or mismatched cursor") from error


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_jsonable(item) for item in value]
    return value


def _estimate_json_bytes(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"))


__all__ = [
    "LocalArxivMCPServer",
    "SnapshotResolutionError",
    "resolve_snapshot_path",
    "run_stdio",
]
