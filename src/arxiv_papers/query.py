"""Read-only query service for a DuckDB arXiv metadata snapshot."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Self

import duckdb


class QueryError(Exception):
    """Base class for local snapshot query errors."""


class SnapshotNotFoundError(QueryError):
    """The requested local snapshot does not exist."""


@dataclass(frozen=True, slots=True)
class SearchPage:
    """A stable, offset-based page of current-paper search results."""

    items: tuple[dict[str, object], ...]
    limit: int
    offset: int
    total: int | None = None


class DuckDBQueryService:
    """Query one immutable snapshot without opening any network connection."""

    MAX_LIMIT = 100

    def __init__(self, snapshot_path: Path | str) -> None:
        self.snapshot_path = Path(snapshot_path)
        if not self.snapshot_path.exists():
            raise SnapshotNotFoundError(f"snapshot does not exist: {snapshot_path}")
        try:
            self.connection = duckdb.connect(str(self.snapshot_path), read_only=True)
        except duckdb.Error as error:
            raise QueryError(f"failed to open snapshot: {snapshot_path}") from error
        self.fts_enabled = self._read_fts_enabled()
        self._authors_have_normalized_name = self._has_column(
            "authors", "display_name_normalized"
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        self.connection.close()

    def search_papers(
        self,
        query: str | None = None,
        *,
        author: str | None = None,
        category: str | None = None,
        primary_only: bool = False,
        created_from: date | str | None = None,
        created_until: date | str | None = None,
        updated_from: date | str | None = None,
        updated_until: date | str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> SearchPage:
        """Search current, non-deleted papers with stable relevance ordering."""

        limit, offset = _validate_page(limit, offset, self.MAX_LIMIT)
        normalized_query = query.strip() if query else None
        conditions = ["1 = 1"]
        parameters: list[object] = []
        score_sql = "CAST(0.0 AS DOUBLE)"

        if normalized_query:
            if self.fts_enabled:
                score_sql = "search_score.relevance"
                parameters.append(normalized_query)
                conditions.append("search_score.relevance IS NOT NULL")
            else:
                pattern = f"%{normalized_query}%"
                conditions.append(
                    "(p.title ILIKE ? OR p.abstract ILIKE ? OR "
                    "p.comments ILIKE ? OR p.journal_ref ILIKE ?)"
                )
                parameters.extend([pattern] * 4)

        if author:
            conditions.append(
                "EXISTS ("
                "SELECT 1 FROM paper_authors pa "
                "JOIN authors a ON a.author_key = pa.author_key "
                "WHERE pa.arxiv_id = p.arxiv_id "
                "AND pa.version IS NOT DISTINCT FROM p.version "
                f"AND {self._author_name_expression()} LIKE ?"
                ")"
            )
            parameters.append(f"%{_normalize_search_text(author)}%")

        if category:
            category_condition = "c.category = ?"
            category_parameters: list[object] = [category]
            if category.endswith("."):
                category_condition = "c.category LIKE ?"
                category_parameters = [f"{category}%"]
            conditions.append(
                "EXISTS ("
                "SELECT 1 FROM categories c "
                "WHERE c.arxiv_id = p.arxiv_id "
                "AND c.version IS NOT DISTINCT FROM p.version "
                f"AND {category_condition}"
                + (" AND c.is_primary" if primary_only else "")
                + ")"
            )
            parameters.extend(category_parameters)

        for column, value, operator in (
            ("created", created_from, ">="),
            ("created", created_until, "<="),
            ("updated", updated_from, ">="),
            ("updated", updated_until, "<="),
        ):
            if value is not None:
                conditions.append(f"p.{column} {operator} ?")
                parameters.append(_date_value(value))

        where_sql = " AND ".join(conditions)
        author_list = (
            "COALESCE((SELECT list(a.display_name ORDER BY pa.author_order) "
            "FROM paper_authors pa JOIN authors a ON a.author_key = pa.author_key "
            "WHERE pa.arxiv_id = p.arxiv_id "
            "AND pa.version IS NOT DISTINCT FROM p.version), [])"
        )
        category_list = (
            "COALESCE((SELECT list(c.category ORDER BY c.is_primary DESC, c.category) "
            "FROM categories c WHERE c.arxiv_id = p.arxiv_id "
            "AND c.version IS NOT DISTINCT FROM p.version), [])"
        )
        from_sql = "papers p"
        if normalized_query and self.fts_enabled:
            from_sql = (
                "papers p JOIN ("
                "SELECT arxiv_id, "
                "fts_main_paper_search.match_bm25(arxiv_id, ?) AS relevance "
                "FROM paper_search"
                ") search_score ON search_score.arxiv_id = p.arxiv_id"
            )
        sql = f"""
            SELECT p.arxiv_id, p.version, p.title, p.abstract, p.created,
                   p.updated, p.doi, p.journal_ref, p.comments,
                   p.primary_category, {author_list} AS authors,
                   {category_list} AS categories, {score_sql} AS relevance,
                   count(*) OVER () AS total_count
            FROM {from_sql}
            WHERE {where_sql}
            ORDER BY relevance DESC NULLS LAST, p.updated DESC NULLS LAST,
                     p.arxiv_id ASC
            LIMIT ? OFFSET ?
        """
        parameters.extend([limit, offset])
        rows = self.connection.execute(sql, parameters).fetchall()
        columns = [item[0] for item in self.connection.description]
        item_columns = columns[:-1]
        total = int(rows[0][-1]) if rows else 0
        return SearchPage(
            items=tuple(_row_to_dict(item_columns, row[:-1]) for row in rows),
            limit=limit,
            offset=offset,
            total=total,
        )

    def search_by_author(
        self, author: str, *, limit: int = 20, offset: int = 0
    ) -> SearchPage:
        """Search papers by normalized author name, independent of FTS."""

        if not author.strip():
            raise QueryError("author must not be empty")
        return self.search_papers(author=author, limit=limit, offset=offset)

    def search_by_category(
        self,
        category: str,
        *,
        primary_only: bool = False,
        limit: int = 20,
        offset: int = 0,
    ) -> SearchPage:
        """Search papers by exact category or a trailing-dot prefix."""

        if not category.strip():
            raise QueryError("category must not be empty")
        return self.search_papers(
            category=category.strip(),
            primary_only=primary_only,
            limit=limit,
            offset=offset,
        )

    def get_paper(
        self, arxiv_id: str, *, version: int | None = None
    ) -> dict[str, object] | None:
        """Return current metadata and version facts for one visible paper."""

        canonical_id = arxiv_id.strip()
        if not canonical_id:
            raise QueryError("arxiv_id must not be empty")
        condition = "arxiv_id = ?"
        parameters: list[object] = [canonical_id]
        if version is not None:
            if version < 1:
                raise QueryError("version must be positive")
            condition += " AND version = ?"
            parameters.append(version)
        row = self.connection.execute(
            f"SELECT * FROM papers WHERE {condition} LIMIT 1", parameters
        ).fetchone()
        if row is None:
            return None
        columns = [item[0] for item in self.connection.description]
        paper = _row_to_dict(columns, row)
        paper_version = paper["version"]
        if paper_version is not None:
            paper_version = int(paper_version)
        paper["authors"] = self._authors(canonical_id, paper_version)
        paper["categories"] = self._categories(canonical_id, paper_version)
        paper["versions"] = self._versions(canonical_id)
        return paper

    def get_paper_by_doi(self, doi: str) -> dict[str, object] | None:
        """Return the current paper whose DOI matches exactly, case-insensitively."""

        normalized_doi = doi.strip()
        if not normalized_doi:
            raise QueryError("doi must not be empty")
        row = self.connection.execute(
            "SELECT arxiv_id FROM papers WHERE lower(doi) = lower(?) LIMIT 1",
            [normalized_doi],
        ).fetchone()
        if row is None:
            return None
        return self.get_paper(str(row[0]))

    def _authors(self, arxiv_id: str, version: int | None) -> list[dict[str, object]]:
        rows = self.connection.execute(
            """
            SELECT a.display_name, a.keyname, a.forenames, a.suffix,
                   a.affiliations_json, pa.author_order
            FROM paper_authors pa
            JOIN authors a ON a.author_key = pa.author_key
            WHERE pa.arxiv_id = ? AND pa.version IS NOT DISTINCT FROM ?
            ORDER BY pa.author_order
            """,
            [arxiv_id, version],
        ).fetchall()
        columns = [item[0] for item in self.connection.description]
        return [_row_to_dict(columns, row) for row in rows]

    def _categories(
        self, arxiv_id: str, version: int | None
    ) -> list[dict[str, object]]:
        rows = self.connection.execute(
            """
            SELECT category, is_primary
            FROM categories
            WHERE arxiv_id = ? AND version IS NOT DISTINCT FROM ?
            ORDER BY is_primary DESC, category
            """,
            [arxiv_id, version],
        ).fetchall()
        columns = [item[0] for item in self.connection.description]
        return [_row_to_dict(columns, row) for row in rows]

    def _versions(self, arxiv_id: str) -> list[dict[str, object]]:
        rows = self.connection.execute(
            """
            SELECT arxiv_id, version, first_submitted_at, submitted_at,
                   size_kb, source_type, is_current, oai_datestamp
            FROM paper_versions
            WHERE arxiv_id = ?
            ORDER BY version NULLS LAST
            """,
            [arxiv_id],
        ).fetchall()
        columns = [item[0] for item in self.connection.description]
        return [_row_to_dict(columns, row) for row in rows]

    def _read_fts_enabled(self) -> bool:
        try:
            row = self.connection.execute(
                "SELECT value FROM search_metadata WHERE key = 'fts_enabled'"
            ).fetchone()
        except duckdb.Error:
            return False
        return bool(row and str(row[0]).lower() == "true")

    def _has_column(self, table: str, column: str) -> bool:
        row = self.connection.execute(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_name = ? AND column_name = ?",
            [table, column],
        ).fetchone()
        return bool(row and row[0])

    def _author_name_expression(self) -> str:
        if self._authors_have_normalized_name:
            return "a.display_name_normalized"
        return "lower(regexp_replace(trim(a.display_name), '\\s+', ' ', 'g'))"


def _validate_page(limit: int, offset: int, max_limit: int) -> tuple[int, int]:
    if limit < 1 or limit > max_limit:
        raise QueryError(f"limit must be between 1 and {max_limit}")
    if offset < 0:
        raise QueryError("offset must be non-negative")
    return limit, offset


def _normalize_search_text(value: str) -> str:
    return " ".join(value.casefold().split())


def _date_value(value: date | str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as error:
        raise QueryError(f"invalid ISO date: {value}") from error


def _row_to_dict(columns: Sequence[str], row: Sequence[object]) -> dict[str, object]:
    return {column: value for column, value in zip(columns, row, strict=True)}


__all__ = [
    "DuckDBQueryService",
    "QueryError",
    "SearchPage",
    "SnapshotNotFoundError",
]
