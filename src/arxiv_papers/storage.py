"""Parquet staging and DuckDB snapshot construction for synchronized metadata."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from arxiv_papers.metadata import (
    ArxivAuthor,
    ArxivPaperMetadata,
    ParsedArxivRecord,
    normalize_arxiv_id,
)
from arxiv_papers.runtime import DEFAULT_RUNTIME, DuckDBRuntimeConfig

PAPER_VERSION_SCHEMA = pa.schema(
    [
        pa.field("arxiv_id", pa.string(), nullable=False),
        pa.field("version", pa.int64()),
        pa.field("title", pa.string()),
        pa.field("abstract", pa.string()),
        pa.field("created", pa.date32()),
        pa.field("updated", pa.date32()),
        # Values are normalized to UTC by the parser; Parquet stores them as
        # UTC wall-clock timestamps so DuckDB does not need a timezone package.
        pa.field("first_submitted_at", pa.timestamp("us")),
        pa.field("submitted_at", pa.timestamp("us")),
        pa.field("size_kb", pa.int64()),
        pa.field("source_type", pa.string()),
        pa.field("is_current", pa.bool_(), nullable=False),
        pa.field("comments", pa.string()),
        pa.field("journal_ref", pa.string()),
        pa.field("doi", pa.string()),
        pa.field("license", pa.string()),
        pa.field("primary_category", pa.string()),
        pa.field("authors_raw", pa.string()),
        pa.field("report_numbers", pa.list_(pa.string())),
        pa.field("oai_identifier", pa.string(), nullable=False),
        pa.field("oai_datestamp", pa.string()),
        pa.field("metadata_hash", pa.string()),
    ]
)
AUTHOR_SCHEMA = pa.schema(
    [
        pa.field("author_key", pa.string(), nullable=False),
        pa.field("keyname", pa.string()),
        pa.field("forenames", pa.string()),
        pa.field("suffix", pa.string()),
        pa.field("display_name", pa.string(), nullable=False),
        pa.field("affiliations_json", pa.string(), nullable=False),
        pa.field("name_source", pa.string(), nullable=False),
    ]
)
PAPER_AUTHOR_SCHEMA = pa.schema(
    [
        pa.field("arxiv_id", pa.string(), nullable=False),
        pa.field("version", pa.int64()),
        pa.field("author_key", pa.string(), nullable=False),
        pa.field("author_order", pa.int64(), nullable=False),
        pa.field("oai_datestamp", pa.string()),
        pa.field("metadata_hash", pa.string()),
    ]
)
CATEGORY_SCHEMA = pa.schema(
    [
        pa.field("arxiv_id", pa.string(), nullable=False),
        pa.field("version", pa.int64()),
        pa.field("category", pa.string(), nullable=False),
        pa.field("is_primary", pa.bool_(), nullable=False),
        pa.field("oai_datestamp", pa.string()),
        pa.field("metadata_hash", pa.string()),
    ]
)
DELETED_SCHEMA = pa.schema(
    [
        pa.field("arxiv_id", pa.string(), nullable=False),
        pa.field("version", pa.int64()),
        pa.field("oai_identifier", pa.string(), nullable=False),
        pa.field("oai_datestamp", pa.string()),
    ]
)
EXCLUDED_SCHEMA = pa.schema(
    [
        pa.field("arxiv_id", pa.string(), nullable=False),
        pa.field("oai_datestamp", pa.string()),
    ]
)

TABLE_SCHEMAS = {
    "paper_versions": PAPER_VERSION_SCHEMA,
    "authors": AUTHOR_SCHEMA,
    "paper_authors": PAPER_AUTHOR_SCHEMA,
    "categories": CATEGORY_SCHEMA,
    "deleted_records": DELETED_SCHEMA,
    "excluded_records": EXCLUDED_SCHEMA,
}
SNAPSHOT_SCHEMA_VERSION = "6"


@dataclass(frozen=True, slots=True)
class SnapshotBuildResult:
    path: Path
    paper_versions: int
    papers: int
    authors: int
    categories: int
    deleted_records: int
    excluded_records: int


class ParquetBatchStore:
    """Write bounded Parquet batches while preserving page replayability."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        for table_name in TABLE_SCHEMAS:
            (self.root / table_name).mkdir(parents=True, exist_ok=True)
        self.errors_path = self.root / "parse-errors.jsonl"

    def write_batch(
        self, batch_number: int, records: Iterable[ParsedArxivRecord]
    ) -> None:
        records_tuple = tuple(records)
        rows = self._rows(records_tuple)
        for table_name, schema in TABLE_SCHEMAS.items():
            table_rows = rows[table_name]
            if not table_rows:
                continue
            path = self.root / table_name / f"batch-{batch_number:06d}.parquet"
            table = pa.Table.from_pylist(table_rows, schema=schema)
            pq.write_table(table, path, compression="zstd")

    def finalize(self) -> None:
        """Create typed empty parts when a sync returned no records."""

        for table_name, schema in TABLE_SCHEMAS.items():
            table_dir = self.root / table_name
            if not any(table_dir.glob("*.parquet")):
                pq.write_table(
                    pa.Table.from_pylist([], schema=schema),
                    table_dir / "part-000000.parquet",
                    compression="zstd",
                )

    def cleanup_after_page(self, last_successful_page: int) -> None:
        """Remove batch files created after the last durable checkpoint."""

        for path in self.root.glob("*/batch-*.parquet"):
            try:
                batch_page = int(path.stem.rsplit("-", 1)[-1])
            except ValueError:
                continue
            if batch_page > last_successful_page:
                path.unlink(missing_ok=True)

    def write_parse_error(
        self,
        *,
        page_number: int,
        oai_identifier: str,
        error: Exception,
    ) -> None:
        record = {
            "page_number": page_number,
            "oai_identifier": oai_identifier,
            "error_type": type(error).__name__,
            "error": str(error),
            "recorded_at": datetime.now(UTC).isoformat(),
        }
        with self.errors_path.open("a", encoding="utf-8") as output:
            output.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

    def _rows(
        self, records: tuple[ParsedArxivRecord, ...]
    ) -> dict[str, list[dict[str, object]]]:
        rows: dict[str, list[dict[str, object]]] = {
            table_name: [] for table_name in TABLE_SCHEMAS
        }
        for record in records:
            if record.excluded:
                if record.metadata is not None:
                    rows["excluded_records"].append(
                        {
                            "arxiv_id": record.metadata.arxiv_id,
                            "oai_datestamp": record.oai_datestamp,
                        }
                    )
                continue
            if record.deleted:
                raw_id = record.oai_identifier.rsplit(":", maxsplit=1)[-1]
                arxiv_id, version, _ = normalize_arxiv_id(raw_id)
                rows["deleted_records"].append(
                    {
                        "arxiv_id": arxiv_id,
                        "version": version,
                        "oai_identifier": record.oai_identifier,
                        "oai_datestamp": record.oai_datestamp,
                    }
                )
                continue
            if record.metadata is None or record.metadata_hash is None:
                continue

            metadata = record.metadata
            current_version = metadata.version
            version_history = metadata.version_history
            if version_history:
                for version in version_history:
                    is_current = version.version == current_version
                    rows["paper_versions"].append(
                        _version_row(
                            metadata=metadata,
                            record=record,
                            version=version.version,
                            submitted_at=version.submitted_at,
                            size_kb=version.size_kb,
                            source_type=version.source_type,
                            is_current=is_current,
                        )
                    )
            else:
                rows["paper_versions"].append(
                    _version_row(
                        metadata=metadata,
                        record=record,
                        version=current_version,
                        submitted_at=None,
                        size_kb=None,
                        source_type=None,
                        is_current=True,
                    )
                )

            for author_order, author in enumerate(metadata.authors):
                author_key = _author_key(author)
                rows["authors"].append(
                    {
                        "author_key": author_key,
                        "keyname": author.keyname,
                        "forenames": author.forenames,
                        "suffix": author.suffix,
                        "display_name": author.display_name,
                        "affiliations_json": json.dumps(
                            list(author.affiliations), ensure_ascii=False
                        ),
                        "name_source": author.name_source,
                    }
                )
                rows["paper_authors"].append(
                    {
                        "arxiv_id": metadata.arxiv_id,
                        "version": current_version,
                        "author_key": author_key,
                        "author_order": author_order,
                        "oai_datestamp": record.oai_datestamp,
                        "metadata_hash": record.metadata_hash,
                    }
                )
            rows["categories"].extend(
                {
                    "arxiv_id": metadata.arxiv_id,
                    "version": current_version,
                    "category": category.name,
                    "is_primary": category.is_primary,
                    "oai_datestamp": record.oai_datestamp,
                    "metadata_hash": record.metadata_hash,
                }
                for category in metadata.categories
            )
        return rows


def _deduplicated_deletions_sql(source: str) -> str:
    return f"""
        SELECT * EXCLUDE (dedupe_rank)
        FROM (
            SELECT *, row_number() OVER (
                PARTITION BY arxiv_id, coalesce(version, 0), oai_datestamp
                ORDER BY oai_identifier DESC
            ) AS dedupe_rank
            FROM {source} AS deletions
            WHERE EXISTS (SELECT 1 FROM paper_versions versions
                          WHERE versions.arxiv_id = deletions.arxiv_id)
        )
        WHERE dedupe_rank = 1
    """


def _current_papers_sql(extra_condition: str | None = None) -> str:
    changed_filter = f"AND {extra_condition}" if extra_condition else ""
    return f"""
        SELECT * EXCLUDE (current_rank), lower(doi) AS doi_normalized
        FROM (
            SELECT versions.*, row_number() OVER (
                PARTITION BY arxiv_id
                ORDER BY updated DESC NULLS LAST, created DESC NULLS LAST,
                         version DESC NULLS LAST, oai_datestamp DESC NULLS LAST,
                         oai_identifier DESC
            ) AS current_rank
            FROM paper_versions AS versions
            WHERE is_current AND title IS NOT NULL
              {changed_filter}
              AND NOT EXISTS (
                  SELECT 1 FROM deleted_records AS deleted
                  WHERE deleted.arxiv_id = versions.arxiv_id
                    AND (
                        deleted.oai_datestamp IS NULL
                        OR versions.oai_datestamp IS NULL
                        OR deleted.oai_datestamp >= versions.oai_datestamp
                    )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM excluded_records AS excluded
                  WHERE excluded.arxiv_id = versions.arxiv_id
                    AND (
                        excluded.oai_datestamp IS NULL
                        OR versions.oai_datestamp IS NULL
                        OR excluded.oai_datestamp >= versions.oai_datestamp
                    )
              )
        )
        WHERE current_rank = 1
    """


def _delta_winners_sql() -> str:
    # Relationship rows must come from the same OAI record as the selected version.
    return """
        SELECT DISTINCT selected.arxiv_id, selected.version,
               selected.oai_datestamp, selected.metadata_hash
        FROM paper_versions selected
        JOIN delta_paper_versions delta
          ON selected.arxiv_id = delta.arxiv_id
         AND selected.version IS NOT DISTINCT FROM delta.version
         AND selected.oai_datestamp IS NOT DISTINCT FROM delta.oai_datestamp
         AND selected.metadata_hash IS NOT DISTINCT FROM delta.metadata_hash
        WHERE delta.is_current AND delta.title IS NOT NULL
    """


def _delta_relationship_rows_sql(table: str) -> str:
    columns = {
        "paper_authors": ("arxiv_id", "version", "author_key", "author_order"),
        "categories": ("arxiv_id", "version", "category", "is_primary"),
    }[table]
    selected = ", ".join(f"delta.{column}" for column in columns)
    return f"""
        SELECT DISTINCT {selected}
        FROM delta_{table} delta
        JOIN delta_winners winner
          ON delta.arxiv_id = winner.arxiv_id
         AND delta.version IS NOT DISTINCT FROM winner.version
         AND delta.oai_datestamp IS NOT DISTINCT FROM winner.oai_datestamp
         AND delta.metadata_hash IS NOT DISTINCT FROM winner.metadata_hash
    """


def _paper_details_sql(*, changed_only: bool = False) -> str:
    author_filter = "WHERE pa.arxiv_id IN (SELECT arxiv_id FROM changed_ids)" if changed_only else ""
    row_filter = "WHERE arxiv_id IN (SELECT arxiv_id FROM changed_ids)" if changed_only else ""
    paper_filter = "WHERE p.arxiv_id IN (SELECT arxiv_id FROM changed_ids)" if changed_only else ""
    return f"""
        WITH current_authors AS (
            SELECT pa.arxiv_id, pa.version,
                   to_json(list(struct_pack(
                       display_name := a.display_name,
                       keyname := a.keyname,
                       forenames := a.forenames,
                       suffix := a.suffix,
                       affiliations := CAST(a.affiliations_json AS JSON),
                       name_source := a.name_source,
                       author_order := pa.author_order
                   ) ORDER BY pa.author_order)) AS authors_json
            FROM paper_authors pa
            JOIN authors a ON a.author_key = pa.author_key
            {author_filter}
            GROUP BY pa.arxiv_id, pa.version
        ), current_categories AS (
            SELECT arxiv_id, version,
                   to_json(list(struct_pack(
                       category := category, is_primary := is_primary
                   ) ORDER BY is_primary DESC, category)) AS categories_json
            FROM categories
            {row_filter}
            GROUP BY arxiv_id, version
        ), version_history AS (
            SELECT arxiv_id,
                   to_json(list(struct_pack(
                       arxiv_id := arxiv_id, version := version,
                       first_submitted_at := CASE
                           WHEN first_submitted_at IS NULL THEN NULL
                           ELSE strftime(first_submitted_at, '%Y-%m-%dT%H:%M:%S.%f')
                       END,
                       submitted_at := CASE
                           WHEN submitted_at IS NULL THEN NULL
                           ELSE strftime(submitted_at, '%Y-%m-%dT%H:%M:%S.%f')
                       END,
                       size_kb := size_kb, source_type := source_type,
                       is_current := is_current, oai_datestamp := oai_datestamp
                   ) ORDER BY version NULLS LAST)) AS versions_json
            FROM paper_versions
            {row_filter}
            GROUP BY arxiv_id
        )
        SELECT p.arxiv_id,
               coalesce(ca.authors_json, '[]') AS authors_json,
               coalesce(cc.categories_json, '[]') AS categories_json,
               coalesce(vh.versions_json, '[]') AS versions_json
        FROM papers p
        LEFT JOIN current_authors ca ON ca.arxiv_id = p.arxiv_id
            AND ca.version IS NOT DISTINCT FROM p.version
        LEFT JOIN current_categories cc ON cc.arxiv_id = p.arxiv_id
            AND cc.version IS NOT DISTINCT FROM p.version
        LEFT JOIN version_history vh ON vh.arxiv_id = p.arxiv_id
        {paper_filter}
    """


class DuckDBSnapshotBuilder:
    """Build a self-contained read-only query snapshot from Parquet parts."""

    def __init__(self, runtime: DuckDBRuntimeConfig = DEFAULT_RUNTIME) -> None:
        self.runtime = runtime

    def build(
        self,
        parquet_root: Path | str,
        output_path: Path | str,
        *,
        base_snapshot: Path | str | None = None,
    ) -> SnapshotBuildResult:
        parquet_root = Path(parquet_root)
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = _temporary_database_path(output_path)
        connection: duckdb.DuckDBPyConnection | None = None
        try:
            connection = duckdb.connect(str(temporary_path))
            self.runtime.configure(connection, role="build")
            self._build_tables(connection, parquet_root, base_snapshot)
            self._build_paper_details(connection)
            self._build_search_indexes(connection)
            self._run_query_smoke_test(connection)
            connection.execute("CHECKPOINT")
            connection.close()
            connection = None
            os.replace(temporary_path, output_path)
        except Exception:
            if connection is not None:
                connection.close()
            temporary_path.unlink(missing_ok=True)
            raise

        counts = {
            name: self._count(output_path, name)
            for name in (
                "paper_versions",
                "papers",
                "authors",
                "categories",
                "deleted_records",
                "excluded_records",
            )
        }
        return SnapshotBuildResult(path=output_path, **counts)

    def build_incremental(
        self,
        parquet_root: Path | str,
        output_path: Path | str,
        *,
        base_snapshot: Path | str,
    ) -> SnapshotBuildResult:
        """Patch a published snapshot using an APFS clone when available."""

        parquet_root = Path(parquet_root)
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = _temporary_database_path(output_path)
        connection: duckdb.DuckDBPyConnection | None = None
        try:
            _clone_database(Path(base_snapshot), temporary_path)
            connection = duckdb.connect(str(temporary_path))
            self.runtime.configure(connection, role="build")
            with suppress(duckdb.Error):
                connection.execute("PRAGMA drop_fts_index('papers')")
            self._patch_tables(connection, parquet_root)
            self._rebuild_search_indexes(connection)
            self._run_query_smoke_test(connection)
            connection.execute("CHECKPOINT")
            connection.close()
            connection = None
            os.replace(temporary_path, output_path)
        except Exception:
            if connection is not None:
                connection.close()
            temporary_path.unlink(missing_ok=True)
            raise

        counts = {
            name: self._count(output_path, name)
            for name in (
                "paper_versions",
                "papers",
                "authors",
                "categories",
                "deleted_records",
                "excluded_records",
            )
        }
        return SnapshotBuildResult(path=output_path, **counts)

    @staticmethod
    def incremental_change_ratio(
        parquet_root: Path | str, base_snapshot: Path | str
    ) -> float:
        """Estimate the changed-paper ratio before choosing patch vs rebuild."""

        root = Path(parquet_root)
        with duckdb.connect(":memory:") as connection:
            connection.execute(
                f"ATTACH '{_read_parquet_sql(Path(base_snapshot))}' "
                "AS base_snapshot (READ_ONLY)"
            )
            changed_row = connection.execute(
                """
                SELECT count(*) FROM (
                    SELECT DISTINCT arxiv_id FROM read_parquet(?)
                    UNION
                    SELECT DISTINCT deleted.arxiv_id FROM read_parquet(?) deleted
                    WHERE EXISTS (SELECT 1 FROM base_snapshot.paper_versions versions
                                  WHERE versions.arxiv_id = deleted.arxiv_id)
                    UNION
                    SELECT DISTINCT excluded.arxiv_id FROM read_parquet(?) excluded
                    WHERE EXISTS (SELECT 1 FROM base_snapshot.paper_versions versions
                                  WHERE versions.arxiv_id = excluded.arxiv_id)
                )
                """,
                [
                    str(root / "paper_versions" / "*.parquet"),
                    str(root / "deleted_records" / "*.parquet"),
                    str(root / "excluded_records" / "*.parquet"),
                ],
            ).fetchone()
            changed = int(changed_row[0]) if changed_row is not None else 0
        with duckdb.connect(str(base_snapshot), read_only=True) as connection:
            total_row = connection.execute("SELECT count(*) FROM papers").fetchone()
            total = int(total_row[0]) if total_row is not None else 0
        return float(changed) / max(total, 1)

    @staticmethod
    def supports_incremental_patch(snapshot_path: Path | str) -> bool:
        try:
            with duckdb.connect(str(snapshot_path), read_only=True) as connection:
                row = connection.execute(
                    """
                    SELECT count(*) FROM information_schema.tables
                    WHERE table_schema = 'main' AND table_name = 'paper_details'
                    """
                ).fetchone()
        except duckdb.Error:
            return False
        return bool(row and int(row[0]) == 1)

    @staticmethod
    def validate_base_snapshot(snapshot_path: Path | str) -> None:
        """Require the current metadata schema for incremental updates."""

        try:
            with duckdb.connect(str(snapshot_path), read_only=True) as connection:
                row = connection.execute(
                    "SELECT value FROM library_metadata WHERE key = 'schema_version'"
                ).fetchone()
        except duckdb.Error as error:
            raise RuntimeError(
                "base snapshot is not compatible with the version-history schema; "
                "run a new full sync in a new data directory"
            ) from error
        if row is None or str(row[0]) != SNAPSHOT_SCHEMA_VERSION:
            raise RuntimeError(
                "base snapshot uses an incompatible metadata schema; "
                "run a new full sync in a new data directory"
            )

    def _build_tables(
        self,
        connection: duckdb.DuckDBPyConnection,
        root: Path,
        base_snapshot: Path | str | None = None,
    ) -> None:
        paper_versions = _read_parquet_sql(root / "paper_versions" / "*.parquet")
        authors = _read_parquet_sql(root / "authors" / "*.parquet")
        paper_authors = _read_parquet_sql(root / "paper_authors" / "*.parquet")
        categories = _read_parquet_sql(root / "categories" / "*.parquet")
        deleted = _read_parquet_sql(root / "deleted_records" / "*.parquet")
        excluded = _read_parquet_sql(root / "excluded_records" / "*.parquet")

        self._create_source_views(
            connection,
            {
                "paper_versions": paper_versions,
                "deleted_records": deleted,
                "excluded_records": excluded,
                "authors": authors,
                "paper_authors": paper_authors,
                "categories": categories,
            },
            base_snapshot,
        )

    @staticmethod
    def _create_source_views(
        connection: duckdb.DuckDBPyConnection,
        parquet_paths: dict[str, str],
        base_snapshot: Path | str | None,
    ) -> None:
        if base_snapshot is not None:
            base_path = _read_parquet_sql(Path(base_snapshot))
            connection.execute(f"ATTACH '{base_path}' AS base_snapshot (READ_ONLY)")

        for table_name in (
            "paper_versions",
            "deleted_records",
            "excluded_records",
        ):
            current = f"SELECT * FROM read_parquet('{parquet_paths[table_name]}')"
            connection.execute(f"CREATE TEMP VIEW delta_{table_name} AS {current}")
            if base_snapshot is not None:
                if table_name == "paper_versions":
                    current = (
                        "SELECT *, 0 AS source_priority FROM base_snapshot.paper_versions "
                        f"UNION ALL SELECT *, 1 AS source_priority FROM delta_{table_name}"
                    )
                else:
                    current = f"SELECT * FROM base_snapshot.{table_name} UNION ALL {current}"
            elif table_name == "paper_versions":
                current = f"SELECT *, 1 AS source_priority FROM delta_{table_name}"
            connection.execute(f"CREATE TEMP VIEW source_{table_name} AS {current}")

        for table_name in ("paper_authors", "categories"):
            connection.execute(
                f"CREATE TEMP VIEW delta_{table_name} AS "
                f"SELECT * FROM read_parquet('{parquet_paths[table_name]}')"
            )

        current_authors = f"""
            SELECT author_key, keyname, forenames, suffix, display_name,
                   affiliations_json, name_source,
                   lower(regexp_replace(trim(display_name), '\\s+', ' ', 'g'))
                       AS display_name_normalized
            FROM read_parquet('{parquet_paths["authors"]}')
        """
        if base_snapshot is not None:
            current_authors = f"""
                SELECT author_key, keyname, forenames, suffix, display_name,
                       affiliations_json, name_source,
                       lower(regexp_replace(trim(display_name), '\\s+', ' ', 'g'))
                           AS display_name_normalized
                FROM base_snapshot.authors
                UNION ALL
                {current_authors}
            """
        connection.execute(f"CREATE TEMP VIEW source_authors AS {current_authors}")

        connection.execute(
            """
            CREATE TABLE paper_versions AS
            SELECT * EXCLUDE (dedupe_rank, source_priority)
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY arxiv_id, COALESCE(version, 0)
                    ORDER BY oai_datestamp DESC NULLS LAST,
                             source_priority DESC, metadata_hash DESC NULLS LAST,
                             oai_identifier DESC
                ) AS dedupe_rank
                FROM source_paper_versions
            )
            WHERE dedupe_rank = 1
            """
        )
        connection.execute(
            "CREATE TABLE deleted_records AS "
            + _deduplicated_deletions_sql("source_deleted_records")
        )
        connection.execute(
            "CREATE TABLE excluded_records AS "
            "SELECT arxiv_id, max(oai_datestamp) AS oai_datestamp "
            "FROM source_excluded_records excluded "
            "WHERE EXISTS (SELECT 1 FROM paper_versions versions "
            "WHERE versions.arxiv_id = excluded.arxiv_id) "
            "GROUP BY arxiv_id"
        )
        connection.execute(
            "DELETE FROM excluded_records WHERE EXISTS ("
            "SELECT 1 FROM delta_paper_versions d "
            "WHERE d.arxiv_id = excluded_records.arxiv_id AND d.is_current "
            "AND d.oai_datestamp >= excluded_records.oai_datestamp)"
        )
        connection.execute("CREATE TABLE papers AS " + _current_papers_sql())
        connection.execute("CREATE TEMP TABLE delta_winners AS " + _delta_winners_sql())
        connection.execute(
            """
            CREATE TABLE authors AS
            SELECT DISTINCT * FROM source_authors
            """
        )
        for table_name in ("paper_authors", "categories"):
            delta_rows = _delta_relationship_rows_sql(table_name)
            if base_snapshot is not None:
                source = f"""
                    SELECT base.* FROM base_snapshot.{table_name} base
                    WHERE NOT EXISTS (
                        SELECT 1 FROM delta_winners winner
                        WHERE winner.arxiv_id = base.arxiv_id
                          AND winner.version IS NOT DISTINCT FROM base.version
                    )
                    UNION ALL {delta_rows}
                """
            else:
                source = delta_rows
            connection.execute(
                f"CREATE TABLE {table_name} AS "
                f"SELECT DISTINCT * FROM ({source}) relationships "
                "WHERE EXISTS (SELECT 1 FROM papers "
                "WHERE papers.arxiv_id = relationships.arxiv_id)"
            )
        connection.execute(
            "DELETE FROM authors WHERE NOT EXISTS ("
            "SELECT 1 FROM paper_authors pa "
            "WHERE pa.author_key = authors.author_key)"
        )
        connection.execute(
            """
            CREATE TABLE library_metadata AS
            SELECT * FROM (VALUES
                ('schema_version', ?),
                ('built_at', ?),
                ('source', 'arXiv OAI-PMH')
            ) AS metadata(key, value)
            """,
            [SNAPSHOT_SCHEMA_VERSION, datetime.now(UTC).isoformat()],
        )
        for table, column in (
            ("papers", "arxiv_id"),
            ("papers", "doi_normalized"),
            ("paper_versions", "arxiv_id"),
            ("authors", "author_key"),
            ("paper_authors", "arxiv_id"),
            ("categories", "category"),
            ("categories", "arxiv_id"),
        ):
            connection.execute(
                f"CREATE INDEX idx_{table}_{column} ON {table} ({column})"
            )
        connection.execute(
            "CREATE INDEX idx_paper_authors_lookup "
            "ON paper_authors (arxiv_id, version, author_key)"
        )
        connection.execute(
            "CREATE INDEX idx_categories_lookup "
            "ON categories (arxiv_id, version, category, is_primary)"
        )

    def _build_search_indexes(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Build full-text search directly from the current-paper table."""

        connection.execute(
            """
            CREATE VIEW current_papers AS
            SELECT * FROM papers
            """
        )
        try:
            try:
                connection.execute("LOAD fts")
            except duckdb.Error:
                connection.execute("INSTALL fts")
                connection.execute("LOAD fts")
            connection.execute(
                """
                PRAGMA create_fts_index(
                    'papers', 'arxiv_id', 'title', 'abstract',
                    'comments', 'journal_ref',
                    stemmer='porter', stopwords='english', lower=1,
                    strip_accents=1, overwrite=1
                )
                """
            )
        except duckdb.Error as error:
            raise RuntimeError(
                "DuckDB FTS is required to build a query snapshot; "
                "install the matching fts extension or set up network access"
            ) from error

        connection.execute(
            """
            CREATE TABLE search_metadata AS
            SELECT * FROM (VALUES
                ('fts_enabled', ?),
                ('fts_stemmer', 'porter'),
                ('fts_stopwords', 'english'),
                ('search_fields', 'title,abstract,comments,journal_ref')
            ) AS metadata(key, value)
            """,
            ["true"],
        )

    def _rebuild_search_indexes(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Refresh search tables after an incremental table patch."""

        with suppress(duckdb.Error):
            connection.execute("PRAGMA drop_fts_index('papers')")
        connection.execute("DROP VIEW IF EXISTS current_papers")
        connection.execute("DROP TABLE IF EXISTS search_metadata")
        self._build_search_indexes(connection)

    def _patch_tables(
        self, connection: duckdb.DuckDBPyConnection, root: Path
    ) -> None:
        paths = {
            name: _read_parquet_sql(root / name / "*.parquet")
            for name in TABLE_SCHEMAS
        }
        for name, path in paths.items():
            connection.execute(
                f"CREATE OR REPLACE TEMP VIEW delta_{name} AS "
                f"SELECT * FROM read_parquet('{path}')"
            )
        connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE changed_ids AS
            SELECT DISTINCT arxiv_id FROM delta_paper_versions
            UNION
            SELECT DISTINCT deleted.arxiv_id FROM delta_deleted_records deleted
            WHERE EXISTS (SELECT 1 FROM paper_versions versions
                          WHERE versions.arxiv_id = deleted.arxiv_id)
            UNION
            SELECT DISTINCT excluded.arxiv_id FROM delta_excluded_records excluded
            WHERE EXISTS (SELECT 1 FROM paper_versions versions
                          WHERE versions.arxiv_id = excluded.arxiv_id)
            """
        )
        connection.execute(
            "CREATE OR REPLACE TEMP TABLE old_author_keys AS "
            "SELECT DISTINCT author_key FROM paper_authors "
            "WHERE arxiv_id IN (SELECT arxiv_id FROM changed_ids)"
        )
        connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE merged_paper_versions AS
            SELECT * EXCLUDE (dedupe_rank, source_priority)
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY arxiv_id, coalesce(version, 0)
                    ORDER BY oai_datestamp DESC NULLS LAST,
                             source_priority DESC, metadata_hash DESC NULLS LAST,
                             oai_identifier DESC
                ) AS dedupe_rank
                FROM (
                    SELECT versions.*, 0 AS source_priority
                    FROM paper_versions versions
                    WHERE versions.arxiv_id IN (SELECT arxiv_id FROM changed_ids)
                    UNION ALL
                    SELECT delta.*, 1 AS source_priority
                    FROM delta_paper_versions delta
                ) combined
            )
            WHERE dedupe_rank = 1
            """
        )
        connection.execute(
            "DELETE FROM paper_versions WHERE arxiv_id IN "
            "(SELECT arxiv_id FROM changed_ids)"
        )
        connection.execute(
            "INSERT INTO paper_versions SELECT * FROM merged_paper_versions"
        )
        connection.execute(
            "CREATE OR REPLACE TEMP TABLE delta_winners AS " + _delta_winners_sql()
        )
        connection.execute(
            "CREATE OR REPLACE TEMP TABLE merged_deleted_records AS "
            + _deduplicated_deletions_sql(
                "(SELECT deleted.* FROM deleted_records deleted "
                "WHERE deleted.arxiv_id IN (SELECT arxiv_id FROM changed_ids) "
                "UNION ALL SELECT delta.* FROM delta_deleted_records delta)"
            )
        )
        connection.execute(
            "DELETE FROM deleted_records WHERE arxiv_id IN "
            "(SELECT arxiv_id FROM changed_ids)"
        )
        connection.execute(
            "INSERT INTO deleted_records SELECT * FROM merged_deleted_records"
        )
        connection.execute(
            "CREATE OR REPLACE TEMP TABLE merged_excluded_records AS "
            "SELECT arxiv_id, max(oai_datestamp) AS oai_datestamp FROM ("
            "SELECT base.* FROM excluded_records base "
            "WHERE base.arxiv_id IN (SELECT arxiv_id FROM changed_ids) "
            "UNION ALL SELECT excluded.* FROM delta_excluded_records excluded "
            "WHERE EXISTS (SELECT 1 FROM paper_versions versions "
            "WHERE versions.arxiv_id = excluded.arxiv_id)) "
            "GROUP BY arxiv_id"
        )
        connection.execute(
            "DELETE FROM excluded_records WHERE arxiv_id IN "
            "(SELECT arxiv_id FROM changed_ids)"
        )
        connection.execute(
            "INSERT INTO excluded_records SELECT * FROM merged_excluded_records"
        )
        connection.execute(
            "DELETE FROM excluded_records WHERE EXISTS ("
            "SELECT 1 FROM delta_paper_versions d "
            "WHERE d.arxiv_id = excluded_records.arxiv_id AND d.is_current "
            "AND d.oai_datestamp >= excluded_records.oai_datestamp)"
        )
        for table_name in ("paper_authors", "categories"):
            connection.execute(
                f"DELETE FROM {table_name} WHERE EXISTS ("
                "SELECT 1 FROM delta_winners winner "
                f"WHERE winner.arxiv_id = {table_name}.arxiv_id "
                f"AND winner.version IS NOT DISTINCT FROM {table_name}.version)"
            )
            connection.execute(
                f"INSERT INTO {table_name} "
                + _delta_relationship_rows_sql(table_name)
            )
        connection.execute(
            """
            INSERT INTO authors (
                author_key, keyname, forenames, suffix, display_name,
                affiliations_json, name_source, display_name_normalized
            )
            SELECT author_key, keyname, forenames, suffix, display_name,
                   affiliations_json, name_source, display_name_normalized
            FROM (
                SELECT author_key, keyname, forenames, suffix, display_name,
                       affiliations_json, name_source,
                       lower(regexp_replace(trim(display_name), '\\s+', ' ', 'g'))
                           AS display_name_normalized,
                       row_number() OVER (
                           PARTITION BY author_key ORDER BY display_name
                       ) AS author_rank
                FROM delta_authors
            ) ranked
            WHERE author_rank = 1
              AND EXISTS (
                  SELECT 1 FROM paper_authors pa
                  WHERE pa.author_key = ranked.author_key
              )
              AND NOT EXISTS (
                  SELECT 1 FROM authors existing
                  WHERE existing.author_key = ranked.author_key
              )
            """
        )
        connection.execute(
            "DELETE FROM papers WHERE arxiv_id IN (SELECT arxiv_id FROM changed_ids)"
        )
        connection.execute(
            "INSERT INTO papers "
            + _current_papers_sql(
                "arxiv_id IN (SELECT arxiv_id FROM changed_ids)"
            )
        )
        for table_name in ("paper_authors", "categories"):
            connection.execute(
                f"DELETE FROM {table_name} WHERE arxiv_id IN "
                "(SELECT arxiv_id FROM changed_ids) AND NOT EXISTS "
                "(SELECT 1 FROM papers "
                f"WHERE papers.arxiv_id = {table_name}.arxiv_id)"
            )
        connection.execute(
            "DELETE FROM authors WHERE NOT EXISTS ("
            "SELECT 1 FROM paper_authors pa "
            "WHERE pa.author_key = authors.author_key) "
            "AND author_key IN (SELECT author_key FROM old_author_keys)"
        )
        connection.execute(
            "DELETE FROM paper_details WHERE arxiv_id IN (SELECT arxiv_id FROM changed_ids)"
        )
        connection.execute("INSERT INTO paper_details " + _paper_details_sql(changed_only=True))
        connection.execute(
            "UPDATE library_metadata SET value = ? WHERE key = 'schema_version'",
            [SNAPSHOT_SCHEMA_VERSION],
        )
        connection.execute(
            "UPDATE library_metadata SET value = ? WHERE key = 'built_at'",
            [datetime.now(UTC).isoformat()],
        )

    @staticmethod
    def _build_paper_details(connection: duckdb.DuckDBPyConnection) -> None:
        """Materialize current-paper relationships for single-row lookups."""

        connection.execute("CREATE TABLE paper_details AS " + _paper_details_sql())
        connection.execute(
            "CREATE UNIQUE INDEX idx_paper_details_arxiv_id "
            "ON paper_details (arxiv_id)"
        )

    @staticmethod
    def _run_query_smoke_test(connection: duckdb.DuckDBPyConnection) -> None:
        """Fail snapshot construction if the query schema is incomplete."""

        required_tables = (
            "papers",
            "paper_versions",
            "authors",
            "paper_authors",
            "categories",
            "deleted_records",
            "paper_details",
            "excluded_records",
            "search_metadata",
        )
        for table in required_tables:
            row = connection.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_catalog = current_database() "
                "AND table_schema = 'main' AND table_name = ?",
                [table],
            ).fetchone()
            if row is None or int(row[0]) != 1:
                raise RuntimeError(f"query snapshot smoke test missing table: {table}")
        connection.execute("SELECT count(*) FROM current_papers").fetchone()

    @staticmethod
    def _count(database_path: Path, table: str) -> int:
        with duckdb.connect(str(database_path), read_only=True) as connection:
            row = connection.execute(f"SELECT count(*) FROM {table}").fetchone()
            if row is None:
                raise RuntimeError(f"count query returned no row for table {table}")
            return int(row[0])


def _author_key(author: ArxivAuthor) -> str:
    canonical = json.dumps(
        author.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _version_row(
    *,
    metadata: ArxivPaperMetadata,
    record: ParsedArxivRecord,
    version: int | None,
    submitted_at: object,
    size_kb: int | None,
    source_type: str | None,
    is_current: bool,
) -> dict[str, object]:
    current_fields: dict[str, object] = {
        "title": metadata.title if is_current else None,
        "abstract": metadata.abstract if is_current else None,
        "created": metadata.created,
        "updated": metadata.updated if is_current else None,
        "comments": metadata.comments if is_current else None,
        "journal_ref": metadata.journal_ref if is_current else None,
        "doi": metadata.doi if is_current else None,
        "license": metadata.license if is_current else None,
        "primary_category": metadata.primary_category if is_current else None,
        "authors_raw": metadata.authors_raw if is_current else None,
        "report_numbers": list(metadata.report_numbers) if is_current else None,
        "metadata_hash": record.metadata_hash if is_current else None,
    }
    return {
        "arxiv_id": metadata.arxiv_id,
        "version": version,
        **current_fields,
        "first_submitted_at": metadata.first_submitted_at,
        "submitted_at": submitted_at,
        "size_kb": size_kb,
        "source_type": source_type,
        "is_current": is_current,
        "oai_identifier": record.oai_identifier,
        "oai_datestamp": record.oai_datestamp,
    }


def _read_parquet_sql(path: Path) -> str:
    return str(path).replace("\\", "\\\\").replace("'", "''")


def _temporary_database_path(output_path: Path) -> Path:
    with tempfile.NamedTemporaryFile(
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=output_path.parent,
        delete=False,
    ) as temporary:
        temporary_path = Path(temporary.name)
    temporary_path.unlink()
    return temporary_path


def _clone_database(source: Path, destination: Path) -> None:
    """Clone a DuckDB file with APFS copy-on-write when possible."""

    if not source.is_file():
        raise FileNotFoundError(source)
    if os.uname().sysname == "Darwin":
        try:
            import ctypes

            clonefile = ctypes.CDLL("libSystem.B.dylib").clonefile
            clonefile.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint32]
            clonefile.restype = ctypes.c_int
            if clonefile(
                os.fsencode(str(source)), os.fsencode(str(destination)), 0
            ) == 0:
                return
        except (AttributeError, OSError):
            pass
        destination.unlink(missing_ok=True)
    shutil.copy2(source, destination)


__all__ = [
    "DuckDBSnapshotBuilder",
    "ParquetBatchStore",
    "SnapshotBuildResult",
]
