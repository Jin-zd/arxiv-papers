"""Runtime limits shared by query, sync and snapshot connections."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import duckdb


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if parsed < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return parsed


@dataclass(frozen=True, slots=True)
class DuckDBRuntimeConfig:
    """Bounded defaults for the 20-core/24-GB local deployment."""

    query_threads: int = 2
    build_threads: int = 10
    query_memory_limit: str = "1536MB"
    build_memory_limit: str = "12GB"
    temp_directory: Path | None = Path("/tmp/arxiv-papers-duckdb")
    max_connections: int = 8
    pool_timeout_seconds: float = 5.0
    pointer_ttl_seconds: float = 1.0
    paper_cache_bytes: int = 128 * 1024 * 1024
    search_cache_size: int = 256
    search_cache_ttl_seconds: float = 5.0
    checkpoint_pages: int = 10
    checkpoint_records: int = 50_000
    checkpoint_bytes: int = 256 * 1024 * 1024

    @classmethod
    def from_environment(cls) -> DuckDBRuntimeConfig:
        temp = os.environ.get("ARXIV_DUCKDB_TEMP_DIR", "/tmp/arxiv-papers-duckdb")
        return cls(
            query_threads=_env_int("ARXIV_DUCKDB_QUERY_THREADS", 2),
            build_threads=_env_int("ARXIV_DUCKDB_BUILD_THREADS", 10),
            query_memory_limit=os.environ.get(
                "ARXIV_DUCKDB_QUERY_MEMORY", "1536MB"
            ),
            build_memory_limit=os.environ.get("ARXIV_DUCKDB_BUILD_MEMORY", "12GB"),
            temp_directory=Path(temp).expanduser() if temp else None,
            max_connections=_env_int("ARXIV_MCP_MAX_CONNECTIONS", 8),
            pool_timeout_seconds=float(
                os.environ.get("ARXIV_MCP_POOL_TIMEOUT", "5")
            ),
            pointer_ttl_seconds=float(
                os.environ.get("ARXIV_MCP_POINTER_TTL", "1")
            ),
            paper_cache_bytes=_env_int(
                "ARXIV_MCP_PAPER_CACHE_BYTES", 128 * 1024 * 1024
            ),
            search_cache_size=_env_int("ARXIV_MCP_SEARCH_CACHE_SIZE", 256),
            search_cache_ttl_seconds=float(
                os.environ.get("ARXIV_MCP_SEARCH_CACHE_TTL", "5")
            ),
            checkpoint_pages=_env_int("ARXIV_SYNC_CHECKPOINT_PAGES", 10),
            checkpoint_records=_env_int("ARXIV_SYNC_CHECKPOINT_RECORDS", 50_000),
            checkpoint_bytes=_env_int(
                "ARXIV_SYNC_CHECKPOINT_BYTES", 256 * 1024 * 1024
            ),
        )

    def configure(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        role: str = "query",
    ) -> None:
        """Apply bounded settings before a connection is used."""

        threads = self.build_threads if role == "build" else self.query_threads
        memory = self.build_memory_limit if role == "build" else self.query_memory_limit
        if threads < 1:
            raise ValueError("DuckDB thread count must be positive")
        connection.execute(f"SET threads TO {int(threads)}")
        connection.execute(
            "SET memory_limit TO " + _quote_setting(memory)
        )
        connection.execute("SET preserve_insertion_order TO false")
        if self.temp_directory is not None:
            self.temp_directory.mkdir(parents=True, exist_ok=True)
            connection.execute(
                "SET home_directory TO "
                + _quote_setting(str(self.temp_directory.resolve()))
            )
            connection.execute(
                "SET temp_directory TO "
                + _quote_setting(str(self.temp_directory.resolve()))
            )


def _quote_setting(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


DEFAULT_RUNTIME = DuckDBRuntimeConfig.from_environment()


__all__ = ["DEFAULT_RUNTIME", "DuckDBRuntimeConfig"]
