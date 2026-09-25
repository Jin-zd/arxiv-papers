"""Atomic publication and retention of local DuckDB query snapshots."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb

CURRENT_POINTER_NAME = "current.json"
POINTER_SCHEMA_VERSION = 1


class SnapshotCatalogError(RuntimeError):
    """Raised when a local snapshot catalog cannot be read or published."""


_BASE_SNAPSHOT_TABLES = (
    "papers",
    "paper_versions",
    "authors",
    "paper_authors",
    "categories",
    "deleted_records",
    "library_metadata",
)
_SEARCH_SNAPSHOT_TABLES = ("paper_search", "search_metadata")
_SUPPORTED_SNAPSHOT_SCHEMAS = frozenset({"4", "5"})


@dataclass(frozen=True, slots=True)
class SnapshotPointer:
    """The atomically published local query generation."""

    generation: str
    snapshot_path: Path
    published_at: str
    counts: dict[str, int]


class SnapshotCatalog:
    """Manage the current pointer and bounded snapshot retention."""

    def __init__(self, duckdb_dir: Path | str, *, retention: int = 3) -> None:
        if retention < 1:
            raise ValueError("retention must be at least 1")
        self.duckdb_dir = Path(duckdb_dir)
        self.retention = retention

    @property
    def pointer_path(self) -> Path:
        return self.duckdb_dir / CURRENT_POINTER_NAME

    def read_current(self) -> SnapshotPointer | None:
        """Read the current pointer, returning None when it has not been published."""

        if not self.pointer_path.exists():
            return None
        try:
            payload = json.loads(self.pointer_path.read_text(encoding="utf-8"))
            if payload.get("schema_version") != POINTER_SCHEMA_VERSION:
                raise ValueError("unsupported pointer schema")
            generation = payload["generation"]
            snapshot_name = payload["snapshot"]
            published_at = payload["published_at"]
            counts = payload.get("counts", {})
            if not isinstance(generation, str) or not generation:
                raise ValueError("pointer generation is invalid")
            if not isinstance(snapshot_name, str) or not snapshot_name:
                raise ValueError("pointer snapshot is invalid")
            if (
                Path(snapshot_name).is_absolute()
                or Path(snapshot_name).name != snapshot_name
            ):
                raise ValueError("pointer snapshot must be a file name")
            if not isinstance(published_at, str) or not isinstance(counts, dict):
                raise TypeError("pointer metadata is invalid")
            normalized_counts = {str(key): int(value) for key, value in counts.items()}
        except (
            OSError,
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
            json.JSONDecodeError,
        ) as error:
            raise SnapshotCatalogError(
                f"invalid snapshot pointer: {self.pointer_path}"
            ) from error

        snapshot_path = self.duckdb_dir / snapshot_name
        return SnapshotPointer(
            generation=generation,
            snapshot_path=snapshot_path,
            published_at=published_at,
            counts=normalized_counts,
        )

    def current_snapshot(self) -> Path | None:
        """Return the published snapshot only when its file still exists."""

        pointer = self.read_current()
        if pointer is None:
            return None
        return pointer.snapshot_path if pointer.snapshot_path.is_file() else None

    def publish(
        self,
        snapshot_path: Path | str,
        *,
        generation: str,
        counts: Mapping[str, int] | None = None,
    ) -> SnapshotPointer:
        """Validate and atomically publish a completed snapshot."""

        source_path = Path(snapshot_path)
        if not source_path.is_file():
            raise SnapshotCatalogError(f"snapshot does not exist: {source_path}")
        if source_path.parent.resolve() != self.duckdb_dir.resolve():
            raise SnapshotCatalogError(
                "published snapshots must be stored in the DuckDB snapshot directory"
            )
        self.validate_snapshot(source_path)
        self.duckdb_dir.mkdir(parents=True, exist_ok=True)
        pointer = SnapshotPointer(
            generation=generation,
            snapshot_path=source_path,
            published_at=datetime.now(UTC).isoformat(),
            counts={str(key): int(value) for key, value in (counts or {}).items()},
        )
        payload = {
            "schema_version": POINTER_SCHEMA_VERSION,
            "generation": pointer.generation,
            "snapshot": source_path.name,
            "published_at": pointer.published_at,
            "counts": pointer.counts,
        }
        self._atomic_write_json(payload)
        return pointer

    def prune(self) -> tuple[Path, ...]:
        """Remove snapshots older than the configured retention window.

        The currently published snapshot is always protected. A deletion failure is
        ignored so retention maintenance cannot make a valid published snapshot
        unavailable.
        """

        candidates = sorted(
            self.duckdb_dir.glob("library-*.duckdb"),
            key=lambda path: (path.stat().st_mtime_ns, path.name),
            reverse=True,
        )
        current = self.current_snapshot()
        keep = set(candidates[: self.retention])
        if current is not None:
            keep.add(current)
        removed: list[Path] = []
        for candidate in candidates:
            if candidate in keep:
                continue
            try:
                candidate.unlink()
            except OSError:
                continue
            removed.append(candidate)
        return tuple(removed)

    @staticmethod
    def validate_snapshot(snapshot_path: Path | str) -> None:
        """Validate that a DuckDB file is a usable query snapshot."""

        snapshot_path = Path(snapshot_path)
        try:
            with duckdb.connect(str(snapshot_path), read_only=True) as connection:
                available = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = 'main'"
                    ).fetchall()
                }
                missing = [
                    table for table in _BASE_SNAPSHOT_TABLES if table not in available
                ]
                if missing:
                    raise SnapshotCatalogError(
                        f"snapshot is missing required table: {missing[0]}"
                    )
                metadata = connection.execute(
                    "SELECT value FROM library_metadata WHERE key = 'schema_version'"
                ).fetchone()
                if metadata is None:
                    raise SnapshotCatalogError("snapshot is missing schema metadata")
                schema_version = str(metadata[0])
                if schema_version not in _SUPPORTED_SNAPSHOT_SCHEMAS:
                    raise SnapshotCatalogError(
                        f"snapshot uses unsupported schema version: {schema_version}"
                    )
                missing_search = [
                    table
                    for table in _SEARCH_SNAPSHOT_TABLES
                    if table not in available
                ]
                if missing_search:
                    raise SnapshotCatalogError(
                        f"snapshot is missing required table: {missing_search[0]}"
                    )
                connection.execute("SELECT count(*) FROM current_papers").fetchone()
                if "paper_details" not in available:
                    raise SnapshotCatalogError(
                        "snapshot is missing required table: paper_details"
                    )
        except duckdb.Error as error:
            raise SnapshotCatalogError(
                f"snapshot failed read-only validation: {snapshot_path}"
            ) from error

    def _atomic_write_json(self, payload: Mapping[str, object]) -> None:
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.duckdb_dir,
                prefix=f".{CURRENT_POINTER_NAME}.",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                json.dump(
                    payload, temporary, ensure_ascii=False, indent=2, sort_keys=True
                )
                temporary.write("\n")
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_path, self.pointer_path)
            _fsync_directory(self.duckdb_dir)
        except OSError as error:
            raise SnapshotCatalogError(
                f"failed to publish snapshot pointer: {self.pointer_path}"
            ) from error
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


__all__ = [
    "CURRENT_POINTER_NAME",
    "POINTER_SCHEMA_VERSION",
    "SnapshotCatalog",
    "SnapshotCatalogError",
    "SnapshotPointer",
]
