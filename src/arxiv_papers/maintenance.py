"""Operational checks, backups, compaction and scheduler plans."""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from arxiv_papers.snapshots import SnapshotCatalog, SnapshotCatalogError
from arxiv_papers.storage import TABLE_SCHEMAS


class MaintenanceError(RuntimeError):
    """Raised when a maintenance operation cannot complete safely."""


@dataclass(frozen=True, slots=True)
class CompactResult:
    source: Path
    output: Path
    tables: dict[str, int]
    files_written: int


@dataclass(frozen=True, slots=True)
class BackupResult:
    path: Path
    files: int
    bytes: int
    manifest: Path


def health_report(
    data_dir: Path | str, *, sync_name: str = "incremental"
) -> dict[str, object]:
    """Return a machine-readable health report without changing data."""

    root = Path(data_dir)
    state_path = root / "state" / "sync.duckdb"
    catalog = SnapshotCatalog(root / "duckdb")
    result: dict[str, object] = {
        "data_dir": str(root),
        "state_path": str(state_path),
        "sync_name": sync_name,
        "ok": True,
        "issues": [],
    }
    issues: list[str] = []
    if not state_path.is_file():
        issues.append("sync state database is missing")
    else:
        try:
            with duckdb.connect(str(state_path), read_only=True) as connection:
                row = connection.execute(
                    "SELECT status, snapshot_path FROM sync_state WHERE sync_name = ?",
                    [sync_name],
                ).fetchone()
            if row is None:
                issues.append(f"sync state is missing: {sync_name}")
            else:
                status, snapshot_path = row
                result["sync_status"] = status
                if status in {"failed", "running"}:
                    issues.append(f"sync state is {status}: {sync_name}")
                if snapshot_path and not Path(snapshot_path).is_file():
                    issues.append("sync state points to a missing snapshot")
        except duckdb.Error as error:
            issues.append(f"cannot read sync state: {error}")

    try:
        pointer = catalog.read_current()
        if pointer is None:
            issues.append("current snapshot pointer is missing")
        elif not pointer.snapshot_path.is_file():
            issues.append("current snapshot pointer targets a missing file")
        else:
            SnapshotCatalog.validate_snapshot(pointer.snapshot_path)
            result["generation"] = pointer.generation
            result["snapshot"] = str(pointer.snapshot_path)
    except SnapshotCatalogError as error:
        issues.append(str(error))
    result["issues"] = issues
    result["ok"] = not issues
    return result


def integrity_report(data_dir: Path | str) -> dict[str, object]:
    """Validate current snapshot tables and Parquet readability."""

    root = Path(data_dir)
    result: dict[str, object] = {"data_dir": str(root), "ok": True, "issues": []}
    issues: list[str] = []
    tables: dict[str, dict[str, int]] = {}
    parquet_root = root / "parquet"
    for table_name in TABLE_SCHEMAS:
        files = tuple(parquet_root.glob(f"**/{table_name}/*.parquet"))
        rows = 0
        readable = 0
        for path in files:
            try:
                metadata = pq.read_metadata(path)
                rows += metadata.num_rows
                readable += 1
            except (OSError, pa.ArrowException) as error:
                issues.append(f"unreadable Parquet file {path}: {error}")
        tables[table_name] = {"files": len(files), "readable": readable, "rows": rows}
    result["parquet"] = tables

    catalog = SnapshotCatalog(root / "duckdb")
    try:
        pointer = catalog.read_current()
        if pointer is None or not pointer.snapshot_path.is_file():
            issues.append("current snapshot is missing")
        else:
            SnapshotCatalog.validate_snapshot(pointer.snapshot_path)
            result["snapshot"] = _snapshot_counts(pointer.snapshot_path)
            result["generation"] = pointer.generation
            expected = pointer.counts
            actual = result["snapshot"]
            for key, value in expected.items():
                if key in actual and actual[key] != value:
                    issues.append(f"snapshot count mismatch for {key}")
    except SnapshotCatalogError as error:
        issues.append(str(error))
    result["issues"] = issues
    result["ok"] = not issues
    return result


def compact_parquet(
    source_root: Path | str,
    output_root: Path | str | None = None,
) -> CompactResult:
    """Rewrite each Parquet table into one compressed file per table.

    The source data is never modified. If output_root is omitted, a sibling
    ``<source>-compact`` directory is created.
    """

    source = Path(source_root)
    if not source.is_dir():
        raise MaintenanceError(f"Parquet source does not exist: {source}")
    output = (
        Path(output_root)
        if output_root is not None
        else source.with_name(f"{source.name}-compact")
    )
    if output.exists():
        raise MaintenanceError(f"compact output already exists: {output}")
    if _is_relative_to(output.resolve(), source.resolve()):
        raise MaintenanceError("compact output must not be inside source directory")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    table_counts: dict[str, int] = {}
    try:
        for table_name, schema in TABLE_SCHEMAS.items():
            files = tuple(source.glob(f"{table_name}/*.parquet"))
            destination = temporary / table_name
            destination.mkdir(parents=True, exist_ok=True)
            table_counts[table_name] = _compact_table(
                files,
                destination / "part-000001.parquet",
                schema,
            )
        parse_errors = source / "parse-errors.jsonl"
        if parse_errors.is_file():
            shutil.copy2(parse_errors, temporary / parse_errors.name)
        os.replace(temporary, output)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return CompactResult(
        source=source,
        output=output,
        tables=table_counts,
        files_written=sum(1 for _ in output.rglob("*.parquet")),
    )


def backup_data(
    data_dir: Path | str,
    output_dir: Path | str,
    *,
    include_raw: bool = False,
) -> BackupResult:
    """Create an atomic backup of Parquet, DuckDB and sync state files."""

    source = Path(data_dir)
    destination_root = Path(output_dir)
    if not source.is_dir():
        raise MaintenanceError(f"data directory does not exist: {source}")
    source_resolved = source.resolve()
    destination_root_resolved = destination_root.resolve()
    if _is_relative_to(destination_root_resolved, source_resolved):
        raise MaintenanceError("backup destination must not be inside data directory")
    destination_root.mkdir(parents=True, exist_ok=True)
    name = f"arxiv-papers-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    destination = destination_root / name
    if destination.exists():
        raise MaintenanceError(f"backup destination already exists: {destination}")
    temporary = Path(tempfile.mkdtemp(prefix=f".{name}.", dir=destination_root))
    included = ["parquet", "duckdb", "state"]
    if include_raw:
        included.append("raw")
    try:
        for relative in included:
            source_path = source / relative
            if source_path.is_dir():
                shutil.copytree(source_path, temporary / relative)
        manifest_entries = []
        total_bytes = 0
        for path in sorted(temporary.rglob("*")):
            if not path.is_file() or path.name == "manifest.json":
                continue
            digest, size = _file_digest(path)
            total_bytes += size
            manifest_entries.append(
                {
                    "path": str(path.relative_to(temporary)),
                    "bytes": size,
                    "sha256": digest,
                }
            )
        manifest = temporary / "manifest.json"
        manifest.write_text(
            json.dumps(
                {
                    "created_at": datetime.now(UTC).isoformat(),
                    "source": str(source_resolved),
                    "include_raw": include_raw,
                    "files": manifest_entries,
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return BackupResult(
        path=destination,
        files=len(manifest_entries),
        bytes=total_bytes,
        manifest=destination / "manifest.json",
    )


def scheduler_plan(
    data_dir: Path | str, *, hour: int = 2, minute: int = 0
) -> dict[str, str]:
    """Return a daily cron command and launchd-compatible program arguments."""

    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise MaintenanceError("hour must be 0-23 and minute must be 0-59")
    data = str(Path(data_dir).expanduser().resolve())
    command = f"uv run arxiv-papers sync incremental --data-dir {shlex.quote(data)} --no-progress"
    return {
        "cron": (
            f"{minute} {hour} * * * cd {shlex.quote(str(Path.cwd()))} && {command}"
        ),
        "command": command,
        "launchd_label": "com.arxiv-papers.incremental",
        "launchd_program_arguments": json.dumps(
            [
                "uv",
                "run",
                "arxiv-papers",
                "sync",
                "incremental",
                "--data-dir",
                data,
                "--no-progress",
            ],
            ensure_ascii=False,
        ),
    }


def _snapshot_counts(path: Path) -> dict[str, int]:
    try:
        with duckdb.connect(str(path), read_only=True) as connection:
            return {
                table: int(
                    connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                )
                for table in (
                    "papers",
                    "paper_versions",
                    "authors",
                    "categories",
                    "deleted_records",
                )
            }
    except duckdb.Error as error:
        raise SnapshotCatalogError(f"cannot read snapshot counts: {path}") from error


def _coerce_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    columns = []
    for field in schema:
        if field.name in table.column_names:
            columns.append(table[field.name])
        else:
            columns.append(pa.nulls(table.num_rows, type=field.type))
    return pa.Table.from_arrays(columns, schema=schema)


def _compact_table(files: tuple[Path, ...], output: Path, schema: pa.Schema) -> int:
    if not files:
        pq.write_table(
            pa.Table.from_pylist([], schema=schema), output, compression="zstd"
        )
        return 0

    row_count = 0
    writer: pq.ParquetWriter | None = None
    try:
        dataset = ds.dataset([str(path) for path in files], format="parquet")
        for batch in dataset.scanner(batch_size=65_536).to_batches():
            table = _coerce_schema(pa.Table.from_batches([batch]), schema)
            if writer is None:
                writer = pq.ParquetWriter(output, schema, compression="zstd")
            writer.write_table(table)
            row_count += table.num_rows
        if writer is None:
            pq.write_table(
                pa.Table.from_pylist([], schema=schema), output, compression="zstd"
            )
    finally:
        if writer is not None:
            writer.close()
    return row_count


def _file_digest(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


__all__ = [
    "BackupResult",
    "CompactResult",
    "MaintenanceError",
    "backup_data",
    "compact_parquet",
    "health_report",
    "integrity_report",
    "scheduler_plan",
]
