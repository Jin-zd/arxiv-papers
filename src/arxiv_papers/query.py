"""Read-only query service for a DuckDB arXiv metadata snapshot."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Self

import duckdb

from arxiv_papers.runtime import DEFAULT_RUNTIME, DuckDBRuntimeConfig


class QueryError(Exception):
    """Base class for local snapshot query errors."""


class SnapshotNotFoundError(QueryError):
    """The requested local snapshot does not exist."""


@dataclass(frozen=True, slots=True)
class SearchPosition:
    """The final sort position needed for keyset pagination."""

    relevance: float
    updated: date | None
    arxiv_id: str


@dataclass(frozen=True, slots=True)
class SearchPage:
    """A stable keyset-paginated page of current-paper search results."""

    items: tuple[dict[str, object], ...]
    limit: int
    has_more: bool
    next_position: SearchPosition | None = None


_PAPER_COLUMNS = (
    "arxiv_id",
    "version",
    "title",
    "abstract",
    "created",
    "updated",
    "first_submitted_at",
    "submitted_at",
    "size_kb",
    "source_type",
    "is_current",
    "comments",
    "journal_ref",
    "doi",
    "license",
    "primary_category",
    "authors_raw",
    "oai_identifier",
    "oai_datestamp",
    "metadata_hash",
)


class DuckDBQueryService:
    """Query one immutable snapshot without opening any network connection."""

    MAX_LIMIT = 100

    def __init__(
        self,
        snapshot_path: Path | str,
        *,
        runtime: DuckDBRuntimeConfig = DEFAULT_RUNTIME,
    ) -> None:
        self.snapshot_path = Path(snapshot_path)
        if not self.snapshot_path.exists():
            raise SnapshotNotFoundError(f"snapshot does not exist: {snapshot_path}")
        try:
            self.connection = duckdb.connect(str(self.snapshot_path), read_only=True)
            runtime.configure(self.connection, role="query")
        except duckdb.Error as error:
            raise QueryError(f"failed to open snapshot: {snapshot_path}") from error
        self.fts_enabled = self._read_fts_enabled()
        if self.fts_enabled:
            try:
                self.connection.execute("LOAD fts")
            except duckdb.Error:
                try:
                    self.connection.execute("INSTALL fts")
                    self.connection.execute("LOAD fts")
                except duckdb.Error as error:
                    self.connection.close()
                    raise QueryError(
                        "snapshot requires the DuckDB FTS extension; "
                        "install the matching extension"
                    ) from error
        self._authors_have_normalized_name = self._has_column(
            "authors", "display_name_normalized"
        )
        if not self._has_table("paper_details"):
            self.connection.close()
            raise QueryError(
                "snapshot does not contain paper_details; "
                "run an incremental or full sync to rebuild it"
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
        after: SearchPosition | None = None,
    ) -> SearchPage:
        """Search current, non-deleted papers with stable relevance ordering."""

        limit = _validate_limit(limit, self.MAX_LIMIT)
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
                raise QueryError(
                    "full-text search is unavailable in this snapshot; "
                    "rebuild it with the DuckDB FTS extension"
                )

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
            "WHERE pa.arxiv_id = w.arxiv_id "
            "AND pa.version IS NOT DISTINCT FROM w.version), [])"
        )
        category_list = (
            "COALESCE((SELECT list(c.category ORDER BY c.is_primary DESC, c.category) "
            "FROM categories c WHERE c.arxiv_id = w.arxiv_id "
            "AND c.version IS NOT DISTINCT FROM w.version), [])"
        )
        from_sql = "papers p"
        search_cte = ""
        if normalized_query and self.fts_enabled:
            search_cte = """
                search_candidates AS (
                    SELECT arxiv_id, relevance
                    FROM (
                        SELECT arxiv_id,
                               fts_main_paper_search.match_bm25(
                                   arxiv_id, ?
                               ) AS relevance
                        FROM paper_search
                    ) candidates
                    WHERE relevance IS NOT NULL
                ),
            """
            from_sql = (
                "papers p JOIN search_candidates search_score "
                "ON search_score.arxiv_id = p.arxiv_id"
            )
        position_condition = "1 = 1"
        position_parameters: list[object] = []
        if after is not None:
            position_condition = """
                (
                    ranked.relevance < ?
                    OR (
                        ranked.relevance = ?
                        AND (
                            ranked.updated_is_null > ?
                            OR (
                                ranked.updated_is_null = ?
                                AND (
                                    ranked.updated < ?
                                    OR (
                                        ranked.updated IS NOT DISTINCT FROM ?
                                        AND ranked.arxiv_id > ?
                                    )
                                )
                            )
                        )
                    )
                )
            """
            position_parameters.extend(
                [
                    after.relevance,
                    after.relevance,
                    int(after.updated is None),
                    int(after.updated is None),
                    after.updated,
                    after.updated,
                    after.arxiv_id,
                ]
            )

        sql = f"""
            WITH {search_cte} ranked AS (
                SELECT p.arxiv_id, p.version, p.title, p.abstract, p.created,
                       p.updated, p.doi, p.journal_ref, p.comments,
                       p.primary_category, {score_sql} AS relevance,
                       CASE WHEN p.updated IS NULL THEN 1 ELSE 0 END
                           AS updated_is_null
                FROM {from_sql}
                WHERE {where_sql}
            ), windowed AS (
                SELECT *
                FROM ranked
                WHERE {position_condition}
                ORDER BY relevance DESC NULLS LAST,
                         updated_is_null ASC,
                         updated DESC NULLS LAST,
                         arxiv_id ASC
                LIMIT ?
            )
            SELECT w.arxiv_id, w.version, w.title, w.abstract, w.created,
                   w.updated, w.doi, w.journal_ref, w.comments,
                   w.primary_category, {author_list} AS authors,
                   {category_list} AS categories,
                   w.relevance
            FROM windowed w
            ORDER BY w.relevance DESC NULLS LAST,
                     w.updated_is_null ASC,
                     w.updated DESC NULLS LAST,
                     w.arxiv_id ASC
        """
        parameters.extend(position_parameters)
        parameters.append(limit + 1)
        rows = self.connection.execute(sql, parameters).fetchall()
        columns = [item[0] for item in self.connection.description]
        has_more = len(rows) > limit
        rows = rows[:limit]
        item_columns = columns[:-1]
        next_position = None
        if has_more and rows:
            last_row = rows[-1]
            next_position = SearchPosition(
                relevance=float(last_row[-1]),
                updated=last_row[5],
                arxiv_id=str(last_row[0]),
            )
        return SearchPage(
            items=tuple(_row_to_dict(item_columns, row[:-1]) for row in rows),
            limit=limit,
            has_more=has_more,
            next_position=next_position,
        )

    def search_by_author(
        self, author: str, *, limit: int = 20, after: SearchPosition | None = None
    ) -> SearchPage:
        """Search papers by normalized author name, independent of FTS."""

        if not author.strip():
            raise QueryError("author must not be empty")
        return self.search_papers(author=author, limit=limit, after=after)

    def search_by_category(
        self,
        category: str,
        *,
        primary_only: bool = False,
        limit: int = 20,
        after: SearchPosition | None = None,
    ) -> SearchPage:
        """Search papers by exact category or a trailing-dot prefix."""

        if not category.strip():
            raise QueryError("category must not be empty")
        return self.search_papers(
            category=category.strip(),
            primary_only=primary_only,
            limit=limit,
            after=after,
        )

    def get_paper(
        self, arxiv_id: str, *, version: int | None = None
    ) -> dict[str, object] | None:
        """Return current metadata and version facts for one visible paper."""

        canonical_id = arxiv_id.strip()
        if not canonical_id:
            raise QueryError("arxiv_id must not be empty")
        if version is not None and version < 1:
            raise QueryError("version must be positive")
        return self.get_papers((canonical_id,), version=version).get(canonical_id)

    def get_papers(
        self, arxiv_ids: Sequence[str], *, version: int | None = None
    ) -> dict[str, dict[str, object] | None]:
        """Return multiple current papers using one materialized details query."""

        if not arxiv_ids:
            raise QueryError("arxiv_ids must not be empty")
        if len(arxiv_ids) > 100:
            raise QueryError("at most 100 arxiv_ids may be requested")
        if version is not None and version < 1:
            raise QueryError("version must be positive")
        canonical_ids = tuple(item.strip() for item in arxiv_ids)
        if any(not item for item in canonical_ids):
            raise QueryError("arxiv_ids must not contain empty values")

        placeholders = ", ".join("?" for _ in canonical_ids)
        select_columns = ", ".join(f"p.{column}" for column in _PAPER_COLUMNS)
        rows = self.connection.execute(
            f"""
            SELECT {select_columns}, d.authors_json, d.categories_json,
                   d.versions_json
            FROM papers p
            LEFT JOIN paper_details d ON d.arxiv_id = p.arxiv_id
            WHERE p.arxiv_id IN ({placeholders})
            """,
            list(dict.fromkeys(canonical_ids)),
        ).fetchall()
        papers = {
            str(row[0]): self._paper_from_details_row(row, version=version)
            for row in rows
        }
        return {arxiv_id: papers.get(arxiv_id) for arxiv_id in dict.fromkeys(canonical_ids)}

    def _paper_from_details_row(
        self, row: Sequence[object], *, version: int | None
    ) -> dict[str, object] | None:
        paper = _row_to_dict(_PAPER_COLUMNS, row[: len(_PAPER_COLUMNS)])
        authors = _json_list(row[len(_PAPER_COLUMNS)])
        categories = _json_list(row[len(_PAPER_COLUMNS) + 1])
        versions = _version_list(row[len(_PAPER_COLUMNS) + 2])
        current_version = paper["version"]
        if current_version is not None:
            current_version = int(str(current_version))
        selected_version = next(
            (
                item
                for item in versions
                if item.get("version")
                == (current_version if version is None else version)
            ),
            None,
        )
        if selected_version is None:
            return None
        paper["selected_version"] = selected_version
        paper["authors"] = authors
        paper["categories"] = categories
        paper["versions"] = versions
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

    def _has_table(self, table: str) -> bool:
        row = self.connection.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name = ?",
            [table],
        ).fetchone()
        return bool(row and row[0])

    def _author_name_expression(self) -> str:
        if self._authors_have_normalized_name:
            return "a.display_name_normalized"
        return "lower(regexp_replace(trim(a.display_name), '\\s+', ' ', 'g'))"


def _validate_limit(limit: int, max_limit: int) -> int:
    if limit < 1 or limit > max_limit:
        raise QueryError(f"limit must be between 1 and {max_limit}")
    return limit


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


def _json_list(value: object) -> list[dict[str, object]]:
    if value is None:
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError) as error:
        raise QueryError("snapshot contains invalid paper detail JSON") from error
    if not isinstance(parsed, list) or any(not isinstance(item, dict) for item in parsed):
        raise QueryError("snapshot contains invalid paper detail JSON")
    return [dict(item) for item in parsed]


def _version_list(value: object) -> list[dict[str, object]]:
    versions = _json_list(value)
    for version in versions:
        for field in ("first_submitted_at", "submitted_at"):
            timestamp = version.get(field)
            if isinstance(timestamp, str):
                try:
                    version[field] = datetime.fromisoformat(timestamp)
                except ValueError as error:
                    raise QueryError(
                        "snapshot contains invalid version timestamp"
                    ) from error
    return versions


__all__ = [
    "DuckDBQueryService",
    "QueryError",
    "SearchPage",
    "SearchPosition",
    "SnapshotNotFoundError",
]
