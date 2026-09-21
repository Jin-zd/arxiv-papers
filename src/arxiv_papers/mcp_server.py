"""Local read-only MCP server for an arXiv DuckDB snapshot."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import math
import threading
from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
from mcp.server.mcpserver import MCPServer

from arxiv_papers.query import (
    DuckDBQueryService,
    QueryError,
    SearchPage,
    SearchPosition,
)
from arxiv_papers.snapshots import SnapshotCatalog, SnapshotCatalogError

LOGGER = logging.getLogger(__name__)


class SnapshotResolutionError(RuntimeError):
    """Raised when no usable local query snapshot can be found."""


class LocalArxivMCPServer:
    """Expose local snapshot queries as MCP tools over STDIO."""

    def __init__(
        self,
        snapshot_path: Path | str,
        *,
        catalog: SnapshotCatalog | None = None,
    ) -> None:
        self.snapshot_path = Path(snapshot_path)
        self.catalog = catalog
        self._generation: str | None = None
        if catalog is not None:
            try:
                pointer = catalog.read_current()
            except SnapshotCatalogError:
                pointer = None
            if (
                pointer is not None
                and pointer.snapshot_path.is_file()
                and _same_path(pointer.snapshot_path, self.snapshot_path)
            ):
                self._generation = pointer.generation
        self.query = DuckDBQueryService(self.snapshot_path)
        self._query_lock = threading.Lock()
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
        with self._query_lock:
            self.query.close()

    def _refresh_snapshot_locked(self) -> None:
        if self.catalog is None:
            return
        try:
            pointer = self.catalog.read_current()
        except SnapshotCatalogError as error:
            LOGGER.warning("ignoring invalid current snapshot pointer: %s", error)
            return
        if pointer is None or not pointer.snapshot_path.is_file():
            return
        current = pointer.snapshot_path
        if (
            _same_path(current, self.snapshot_path)
            and pointer.generation == self._generation
        ):
            return
        try:
            replacement = DuckDBQueryService(current)
        except QueryError as error:
            LOGGER.warning("keeping previous snapshot after failed reload: %s", error)
            return
        previous = self.query
        self.query = replacement
        self.snapshot_path = current
        self._generation = pointer.generation
        previous.close()

    def _cursor_fingerprint(self, filters: Mapping[str, object]) -> str:
        payload = {
            **filters,
            "snapshot": self._snapshot_identity(),
        }
        encoded = json.dumps(
            payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _snapshot_identity(self) -> str:
        return self._generation or str(self.snapshot_path.resolve())

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

        with self._query_lock:
            self._refresh_snapshot_locked()
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
                }
            )
            after = _decode_cursor(cursor, fingerprint)
            page = self.query.search_papers(
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
        return _page_payload(page, fingerprint)

    def get_paper(self, arxiv_id: str, version: int | None = None) -> dict[str, Any]:
        """Get one current local paper and its available version facts."""

        with self._query_lock:
            self._refresh_snapshot_locked()
            paper = self.query.get_paper(arxiv_id, version=version)
        return {"paper": _jsonable(paper)}

    def search_by_author(
        self,
        author: str,
        limit: int = 20,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Search local current papers by normalized author display name."""

        with self._query_lock:
            self._refresh_snapshot_locked()
            fingerprint = self._cursor_fingerprint({"author": author})
            after = _decode_cursor(cursor, fingerprint)
            page = self.query.search_by_author(author, limit=limit, after=after)
        return _page_payload(page, fingerprint)

    def search_by_category(
        self,
        category: str,
        primary_only: bool = False,
        limit: int = 20,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Search local current papers by exact category or category prefix."""

        with self._query_lock:
            self._refresh_snapshot_locked()
            fingerprint = self._cursor_fingerprint(
                {"category": category, "primary_only": primary_only}
            )
            after = _decode_cursor(cursor, fingerprint)
            page = self.query.search_by_category(
                category,
                primary_only=primary_only,
                limit=limit,
                after=after,
            )
        return _page_payload(page, fingerprint)

    def library_status(self) -> dict[str, Any]:
        """Return local snapshot metadata without contacting the network."""

        with self._query_lock:
            self._refresh_snapshot_locked()
            query = self.query
            snapshot_path = self.snapshot_path
            generation = self._generation
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


__all__ = [
    "LocalArxivMCPServer",
    "SnapshotResolutionError",
    "resolve_snapshot_path",
    "run_stdio",
]
