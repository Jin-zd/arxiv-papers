"""Parquet staging and DuckDB snapshot construction for synchronized metadata."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from arxiv_papers.metadata import ParsedArxivRecord, normalize_arxiv_id

PAPER_VERSION_SCHEMA = pa.schema(
    [
        pa.field("arxiv_id", pa.string(), nullable=False),
        pa.field("version", pa.int64()),
        pa.field("title", pa.string(), nullable=False),
        pa.field("abstract", pa.string(), nullable=False),
        pa.field("created", pa.date32()),
        pa.field("updated", pa.date32()),
        pa.field("comments", pa.string()),
        pa.field("journal_ref", pa.string()),
        pa.field("doi", pa.string()),
        pa.field("license", pa.string()),
        pa.field("primary_category", pa.string()),
        pa.field("oai_identifier", pa.string(), nullable=False),
        pa.field("oai_datestamp", pa.string()),
        pa.field("metadata_hash", pa.string(), nullable=False),
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
    ]
)
PAPER_AUTHOR_SCHEMA = pa.schema(
    [
        pa.field("arxiv_id", pa.string(), nullable=False),
        pa.field("version", pa.int64()),
        pa.field("author_key", pa.string(), nullable=False),
        pa.field("author_order", pa.int64(), nullable=False),
    ]
)
CATEGORY_SCHEMA = pa.schema(
    [
        pa.field("arxiv_id", pa.string(), nullable=False),
        pa.field("version", pa.int64()),
        pa.field("category", pa.string(), nullable=False),
        pa.field("is_primary", pa.bool_(), nullable=False),
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

TABLE_SCHEMAS = {
    "paper_versions": PAPER_VERSION_SCHEMA,
    "authors": AUTHOR_SCHEMA,
    "paper_authors": PAPER_AUTHOR_SCHEMA,
    "categories": CATEGORY_SCHEMA,
    "deleted_records": DELETED_SCHEMA,
}


@dataclass(frozen=True, slots=True)
class SnapshotBuildResult:
    path: Path
    paper_versions: int
    papers: int
    authors: int
    categories: int
    deleted_records: int


class ParquetBatchStore:
    """Write one immutable Parquet part per OAI page."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        for table_name in TABLE_SCHEMAS:
            (self.root / table_name).mkdir(parents=True, exist_ok=True)
        self.errors_path = self.root / "parse-errors.jsonl"

    def write_page(
        self, page_number: int, records: Iterable[ParsedArxivRecord]
    ) -> None:
        records_tuple = tuple(records)
        rows = self._rows(records_tuple)
        for table_name, schema in TABLE_SCHEMAS.items():
            table_rows = rows[table_name]
            path = self.root / table_name / f"part-{page_number:06d}.parquet"
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
            rows["paper_versions"].append(
                {
                    "arxiv_id": metadata.arxiv_id,
                    "version": metadata.version,
                    "title": metadata.title,
                    "abstract": metadata.abstract,
                    "created": metadata.created,
                    "updated": metadata.updated,
                    "comments": metadata.comments,
                    "journal_ref": metadata.journal_ref,
                    "doi": metadata.doi,
                    "license": metadata.license,
                    "primary_category": metadata.primary_category,
                    "oai_identifier": record.oai_identifier,
                    "oai_datestamp": record.oai_datestamp,
                    "metadata_hash": record.metadata_hash,
                }
            )
            for author_order, author in enumerate(metadata.authors):
                author_key = _author_key(author.display_name)
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
                    }
                )
                rows["paper_authors"].append(
                    {
                        "arxiv_id": metadata.arxiv_id,
                        "version": metadata.version,
                        "author_key": author_key,
                        "author_order": author_order,
                    }
                )
            rows["categories"].extend(
                {
                    "arxiv_id": metadata.arxiv_id,
                    "version": metadata.version,
                    "category": category.name,
                    "is_primary": category.is_primary,
                }
                for category in metadata.categories
            )
        return rows


class DuckDBSnapshotBuilder:
    """Build a self-contained read-only query snapshot from Parquet parts."""

    def build(
        self, parquet_root: Path | str, output_path: Path | str
    ) -> SnapshotBuildResult:
        parquet_root = Path(parquet_root)
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = _temporary_database_path(output_path)
        connection: duckdb.DuckDBPyConnection | None = None
        try:
            connection = duckdb.connect(str(temporary_path))
            self._build_tables(connection, parquet_root)
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
            )
        }
        return SnapshotBuildResult(path=output_path, **counts)

    def _build_tables(self, connection: duckdb.DuckDBPyConnection, root: Path) -> None:
        paper_versions = _read_parquet_sql(root / "paper_versions" / "*.parquet")
        authors = _read_parquet_sql(root / "authors" / "*.parquet")
        paper_authors = _read_parquet_sql(root / "paper_authors" / "*.parquet")
        categories = _read_parquet_sql(root / "categories" / "*.parquet")
        deleted = _read_parquet_sql(root / "deleted_records" / "*.parquet")

        connection.execute(
            f"""
            CREATE TABLE paper_versions AS
            SELECT * EXCLUDE (dedupe_rank)
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY arxiv_id, COALESCE(version, 0), metadata_hash
                    ORDER BY oai_datestamp DESC NULLS LAST, oai_identifier DESC
                ) AS dedupe_rank
                FROM read_parquet('{paper_versions}')
            )
            WHERE dedupe_rank = 1
            """
        )
        connection.execute(
            f"""
            CREATE TABLE deleted_records AS
            SELECT * EXCLUDE (dedupe_rank)
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY arxiv_id, COALESCE(version, 0), oai_datestamp
                    ORDER BY oai_identifier DESC
                ) AS dedupe_rank
                FROM read_parquet('{deleted}')
            )
            WHERE dedupe_rank = 1
            """
        )
        connection.execute(
            """
            CREATE TABLE papers AS
            SELECT * EXCLUDE (current_rank)
            FROM (
                SELECT versions.*, row_number() OVER (
                    PARTITION BY arxiv_id
                    ORDER BY updated DESC NULLS LAST, created DESC NULLS LAST,
                             version DESC NULLS LAST, oai_datestamp DESC NULLS LAST,
                             oai_identifier DESC
                ) AS current_rank
                FROM paper_versions AS versions
                WHERE NOT EXISTS (
                    SELECT 1 FROM deleted_records AS deleted
                    WHERE deleted.arxiv_id = versions.arxiv_id
                      AND (
                          deleted.oai_datestamp IS NULL
                          OR versions.oai_datestamp IS NULL
                          OR deleted.oai_datestamp >= versions.oai_datestamp
                      )
                )
            )
            WHERE current_rank = 1
            """
        )
        connection.execute(
            f"""
            CREATE TABLE authors AS
            SELECT DISTINCT * FROM read_parquet('{authors}')
            """
        )
        connection.execute(
            f"""
            CREATE TABLE paper_authors AS
            SELECT DISTINCT * FROM read_parquet('{paper_authors}')
            """
        )
        connection.execute(
            f"""
            CREATE TABLE categories AS
            SELECT DISTINCT * FROM read_parquet('{categories}')
            """
        )
        connection.execute(
            """
            CREATE TABLE library_metadata AS
            SELECT * FROM (VALUES
                ('schema_version', '1'),
                ('built_at', ?),
                ('source', 'arXiv OAI-PMH')
            ) AS metadata(key, value)
            """,
            [datetime.now(UTC).isoformat()],
        )
        for table, column in (
            ("papers", "arxiv_id"),
            ("paper_versions", "arxiv_id"),
            ("authors", "author_key"),
            ("categories", "category"),
        ):
            connection.execute(
                f"CREATE INDEX idx_{table}_{column} ON {table} ({column})"
            )

    @staticmethod
    def _count(database_path: Path, table: str) -> int:
        with duckdb.connect(str(database_path), read_only=True) as connection:
            row = connection.execute(f"SELECT count(*) FROM {table}").fetchone()
            if row is None:
                raise RuntimeError(f"count query returned no row for table {table}")
            return int(row[0])


def _author_key(display_name: str) -> str:
    return hashlib.sha256(display_name.casefold().encode("utf-8")).hexdigest()


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


__all__ = [
    "DuckDBSnapshotBuilder",
    "ParquetBatchStore",
    "SnapshotBuildResult",
]
