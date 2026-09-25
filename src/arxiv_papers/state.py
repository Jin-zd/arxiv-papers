"""Persistent synchronization checkpoints and recovery state."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Self

import duckdb

from arxiv_papers.runtime import DEFAULT_RUNTIME, DuckDBRuntimeConfig


@dataclass(frozen=True, slots=True)
class SyncState:
    sync_name: str
    run_id: str
    mode: str
    status: str
    from_date: str | None
    until_date: str | None
    set_name: str | None
    metadata_prefix: str | None
    created_from: str | None
    created_until: str | None
    category_prefixes: tuple[str, ...]
    next_resumption_token: str | None
    last_successful_page: int
    last_successful_datestamp: str | None
    recovery_count: int
    started_at: str
    updated_at: str
    completed_at: str | None
    snapshot_path: str | None
    error: str | None


@dataclass(frozen=True, slots=True)
class SyncContext:
    sync_name: str
    run_id: str
    from_date: str | None
    until_date: str | None
    set_name: str | None
    metadata_prefix: str | None
    created_from: str | None
    created_until: str | None
    category_prefixes: tuple[str, ...]
    next_resumption_token: str | None
    next_page: int
    last_successful_page: int
    last_successful_datestamp: str | None
    resumed: bool

    @property
    def has_checkpointed_pages(self) -> bool:
        return self.last_successful_page > 0


class SyncStateStore:
    """Store checkpoints separately from the query snapshot database."""

    def __init__(
        self, path: Path | str, *, runtime: DuckDBRuntimeConfig = DEFAULT_RUNTIME
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(str(self.path)) as connection:
            runtime.configure(connection, role="query")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_state (
                    sync_name VARCHAR PRIMARY KEY,
                    run_id VARCHAR NOT NULL,
                    mode VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    from_date VARCHAR,
                    until_date VARCHAR,
                    set_name VARCHAR,
                    metadata_prefix VARCHAR,
                    created_from VARCHAR,
                    created_until VARCHAR,
                    category_prefixes VARCHAR,
                    next_resumption_token VARCHAR,
                    last_successful_page BIGINT NOT NULL,
                    last_successful_datestamp VARCHAR,
                    recovery_count BIGINT NOT NULL,
                    started_at VARCHAR NOT NULL,
                    updated_at VARCHAR NOT NULL,
                    completed_at VARCHAR,
                    snapshot_path VARCHAR,
                    error VARCHAR
                )
                """
            )

            for column, definition in (
                ("metadata_prefix", "VARCHAR"),
                ("created_from", "VARCHAR"),
                ("created_until", "VARCHAR"),
                ("category_prefixes", "VARCHAR"),
            ):
                connection.execute(
                    f"ALTER TABLE sync_state ADD COLUMN IF NOT EXISTS {column} {definition}"
                )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_batches (
                    run_id VARCHAR NOT NULL,
                    page_number BIGINT NOT NULL,
                    status VARCHAR NOT NULL,
                    request_params_json VARCHAR NOT NULL,
                    response_sha256 VARCHAR,
                    records BIGINT NOT NULL,
                    parsed_records BIGINT NOT NULL,
                    parse_errors BIGINT NOT NULL,
                    warnings BIGINT NOT NULL,
                    max_datestamp VARCHAR,
                    next_resumption_token VARCHAR,
                    started_at VARCHAR NOT NULL,
                    completed_at VARCHAR,
                    error VARCHAR,
                    PRIMARY KEY (run_id, page_number)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS dead_letters (
                    id BIGINT PRIMARY KEY,
                    run_id VARCHAR NOT NULL,
                    page_number BIGINT,
                    oai_identifier VARCHAR,
                    error_type VARCHAR NOT NULL,
                    error VARCHAR NOT NULL,
                    raw_xml_path VARCHAR,
                    created_at VARCHAR NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE SEQUENCE IF NOT EXISTS dead_letter_id_seq START 1
                """
            )

    def checkpoint_session(
        self,
        *,
        run_id: str,
        sync_name: str = "full",
        runtime: DuckDBRuntimeConfig = DEFAULT_RUNTIME,
    ) -> SyncCheckpointSession:
        return SyncCheckpointSession(
            self, run_id=run_id, sync_name=sync_name, runtime=runtime
        )

    def begin_full(
        self,
        *,
        run_id: str,
        from_date: str | None,
        until_date: str | None,
        set_name: str | None,
        metadata_prefix: str | None,
        created_from: str | None,
        created_until: str | None,
        category_prefixes: tuple[str, ...],
        resume: bool = True,
    ) -> SyncContext:
        """Create a run or resume the incomplete full-sync run."""

        return self._begin(
            sync_name="full",
            mode="full",
            run_id=run_id,
            from_date=from_date,
            until_date=until_date,
            set_name=set_name,
            metadata_prefix=metadata_prefix,
            created_from=created_from,
            created_until=created_until,
            category_prefixes=category_prefixes,
            resume=resume,
            initial_datestamp=None,
        )

    def begin_incremental(
        self,
        *,
        run_id: str,
        from_date: str | None,
        until_date: str | None,
        set_name: str | None,
        metadata_prefix: str | None,
        created_from: str | None,
        created_until: str | None,
        category_prefixes: tuple[str, ...],
        initial_datestamp: str | None,
        resume: bool = True,
    ) -> SyncContext:
        return self._begin(
            sync_name="incremental",
            mode="incremental",
            run_id=run_id,
            from_date=from_date,
            until_date=until_date,
            set_name=set_name,
            metadata_prefix=metadata_prefix,
            created_from=created_from,
            created_until=created_until,
            category_prefixes=category_prefixes,
            resume=resume,
            initial_datestamp=initial_datestamp,
        )

    def _begin(
        self,
        *,
        sync_name: str,
        mode: str,
        run_id: str,
        from_date: str | None,
        until_date: str | None,
        set_name: str | None,
        metadata_prefix: str | None,
        created_from: str | None,
        created_until: str | None,
        category_prefixes: tuple[str, ...],
        resume: bool,
        initial_datestamp: str | None,
    ) -> SyncContext:
        now = _now()
        with duckdb.connect(str(self.path)) as connection:
            existing_row = connection.execute(
                f"SELECT {_STATE_COLUMNS} FROM sync_state WHERE sync_name = ?",
                [sync_name],
            ).fetchone()
            existing = _state_from_row(existing_row) if existing_row else None
            can_resume = (
                resume
                and existing is not None
                and existing.status in {"running", "failed"}
            )
            if can_resume:
                assert existing is not None
                _validate_same_scope(
                    existing,
                    from_date,
                    until_date,
                    set_name,
                    metadata_prefix,
                    created_from,
                    created_until,
                    category_prefixes,
                )
                connection.execute(
                    """
                    UPDATE sync_state
                    SET status = 'running', updated_at = ?, error = NULL
                    WHERE sync_name = ?
                    """,
                    [now, sync_name],
                )
                return _context_from_state(existing, resumed=True)

            connection.execute(
                "DELETE FROM sync_state WHERE sync_name = ?", [sync_name]
            )
            connection.execute(
                """
                INSERT INTO sync_state (
                    sync_name, run_id, mode, status, from_date, until_date,
                    set_name, metadata_prefix, created_from, created_until,
                    category_prefixes, next_resumption_token, last_successful_page,
                    last_successful_datestamp, recovery_count, started_at,
                    updated_at, completed_at, snapshot_path, error
                ) VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, NULL, 0,
                          ?, 0, ?, ?, NULL, NULL, NULL)
                """,
                [
                    sync_name,
                    run_id,
                    mode,
                    from_date,
                    until_date,
                    set_name,
                    metadata_prefix,
                    created_from,
                    created_until,
                    json.dumps(category_prefixes),
                    initial_datestamp,
                    now,
                    now,
                ],
            )
        return SyncContext(
            sync_name=sync_name,
            run_id=run_id,
            from_date=from_date,
            until_date=until_date,
            set_name=set_name,
            metadata_prefix=metadata_prefix,
            created_from=created_from,
            created_until=created_until,
            category_prefixes=category_prefixes,
            next_resumption_token=None,
            next_page=1,
            last_successful_page=0,
            last_successful_datestamp=initial_datestamp,
            resumed=False,
        )

    def begin_batch(
        self,
        *,
        run_id: str,
        page_number: int,
        request_params: dict[str, str],
    ) -> None:
        now = _now()
        with duckdb.connect(str(self.path)) as connection:
            connection.execute(
                """
                INSERT INTO sync_batches (
                    run_id, page_number, status, request_params_json,
                    response_sha256, records, parsed_records, parse_errors,
                    warnings, max_datestamp, next_resumption_token,
                    started_at, completed_at, error
                ) VALUES (?, ?, 'started', ?, NULL, 0, 0, 0, 0, NULL,
                          NULL, ?, NULL, NULL)
                ON CONFLICT (run_id, page_number) DO UPDATE SET
                    status = 'started',
                    request_params_json = excluded.request_params_json,
                    response_sha256 = NULL,
                    records = 0,
                    parsed_records = 0,
                    parse_errors = 0,
                    warnings = 0,
                    max_datestamp = NULL,
                    next_resumption_token = NULL,
                    started_at = excluded.started_at,
                    completed_at = NULL,
                    error = NULL
                """,
                [run_id, page_number, json.dumps(request_params, sort_keys=True), now],
            )

    def complete_batch(
        self,
        *,
        run_id: str,
        page_number: int,
        response_sha256: str,
        records: int,
        parsed_records: int,
        parse_errors: int,
        warnings: int,
        max_datestamp: str | None,
        next_resumption_token: str | None,
        sync_name: str = "full",
    ) -> None:
        now = _now()
        with duckdb.connect(str(self.path)) as connection:
            connection.execute("BEGIN TRANSACTION")
            connection.execute(
                """
                UPDATE sync_batches
                SET status = 'completed', response_sha256 = ?, records = ?,
                    parsed_records = ?, parse_errors = ?, warnings = ?,
                    max_datestamp = ?, next_resumption_token = ?,
                    completed_at = ?, error = NULL
                WHERE run_id = ? AND page_number = ?
                """,
                [
                    response_sha256,
                    records,
                    parsed_records,
                    parse_errors,
                    warnings,
                    max_datestamp,
                    next_resumption_token,
                    now,
                    run_id,
                    page_number,
                ],
            )
            connection.execute(
                """
                UPDATE sync_state
                SET updated_at = ?, next_resumption_token = ?,
                    last_successful_page = ?,
                    last_successful_datestamp = CASE
                        WHEN last_successful_datestamp IS NULL THEN ?
                        WHEN ? IS NULL THEN last_successful_datestamp
                        WHEN ? > last_successful_datestamp THEN ?
                        ELSE last_successful_datestamp
                    END,
                    error = NULL
                WHERE sync_name = ? AND run_id = ?
                """,
                [
                    now,
                    next_resumption_token,
                    page_number,
                    max_datestamp,
                    max_datestamp,
                    max_datestamp,
                    max_datestamp,
                    sync_name,
                    run_id,
                ],
            )
            connection.execute("COMMIT")

    def mark_token_invalid(
        self, *, run_id: str, error: str, sync_name: str = "full"
    ) -> None:
        with duckdb.connect(str(self.path)) as connection:
            connection.execute(
                """
                UPDATE sync_state
                SET status = 'running', error = ?, updated_at = ?,
                    next_resumption_token = NULL,
                    recovery_count = recovery_count + 1
                WHERE sync_name = ? AND run_id = ?
                """,
                [error, _now(), sync_name, run_id],
            )

    def mark_completed(
        self, *, run_id: str, snapshot_path: Path, sync_name: str = "full"
    ) -> None:
        now = _now()
        with duckdb.connect(str(self.path)) as connection:
            connection.execute(
                """
                UPDATE sync_state
                SET status = 'completed', completed_at = ?, updated_at = ?,
                    snapshot_path = ?, error = NULL
                WHERE sync_name = ? AND run_id = ?
                """,
                [now, now, str(snapshot_path), sync_name, run_id],
            )

    def mark_failed(self, *, run_id: str, error: str, sync_name: str = "full") -> None:
        with duckdb.connect(str(self.path)) as connection:
            connection.execute(
                """
                UPDATE sync_state
                SET status = 'failed', updated_at = ?, error = ?
                WHERE sync_name = ? AND run_id = ?
                """,
                [_now(), error[:4000], sync_name, run_id],
            )

    def record_dead_letter(
        self,
        *,
        run_id: str,
        page_number: int | None,
        oai_identifier: str | None,
        error_type: str,
        error: str,
        raw_xml_path: Path | None = None,
    ) -> None:
        with duckdb.connect(str(self.path)) as connection:
            sequence_row = connection.execute(
                "SELECT nextval('dead_letter_id_seq')"
            ).fetchone()
            if sequence_row is None:
                raise RuntimeError("dead-letter sequence returned no value")
            letter_id = int(sequence_row[0])
            connection.execute(
                """
                INSERT INTO dead_letters (
                    id, run_id, page_number, oai_identifier, error_type,
                    error, raw_xml_path, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    letter_id,
                    run_id,
                    page_number,
                    oai_identifier,
                    error_type,
                    error[:4000],
                    str(raw_xml_path) if raw_xml_path else None,
                    _now(),
                ],
            )

    def get_state(self, sync_name: str = "full") -> SyncState | None:
        with duckdb.connect(str(self.path), read_only=True) as connection:
            row = connection.execute(
                f"SELECT {_STATE_COLUMNS} FROM sync_state WHERE sync_name = ?",
                [sync_name],
            ).fetchone()
        return _state_from_row(row) if row else None

    def latest_completed(self) -> SyncState | None:
        with duckdb.connect(str(self.path), read_only=True) as connection:
            row = connection.execute(
                f"""
                SELECT {_STATE_COLUMNS}
                FROM sync_state
                WHERE status = 'completed' AND snapshot_path IS NOT NULL
                ORDER BY completed_at DESC NULLS LAST
                LIMIT 1
                """
            ).fetchone()
        return _state_from_row(row) if row else None

    def status(self, sync_name: str | None = None) -> dict[str, object]:
        with duckdb.connect(str(self.path), read_only=True) as connection:
            state_rows = connection.execute(
                f"SELECT {_STATE_COLUMNS} FROM sync_state ORDER BY sync_name"
            ).fetchall()
            batch_rows = connection.execute(
                """
                SELECT status, count(*) AS count
                FROM sync_batches
                GROUP BY status
                ORDER BY status
                """
            ).fetchall()
            dead_letter_row = connection.execute(
                "SELECT count(*) FROM dead_letters"
            ).fetchone()
        states = [_state_from_row(row) for row in state_rows]
        selected = next(
            (state for state in states if state and state.sync_name == sync_name),
            None,
        )
        return {
            "state_path": str(self.path),
            "state": _state_to_dict(selected),
            "states": [_state_to_dict(state) for state in states if state],
            "batches": [
                {"status": str(row[0]), "count": int(row[1])} for row in batch_rows
            ],
            "dead_letters": int(dead_letter_row[0]) if dead_letter_row else 0,
        }

    def verify(self, sync_name: str = "full") -> dict[str, object]:
        state = self.get_state(sync_name)
        checks: dict[str, object] = {
            "state_exists": state is not None,
            "status_completed": bool(state and state.status == "completed"),
            "snapshot_exists": bool(
                state and state.snapshot_path and Path(state.snapshot_path).exists()
            ),
        }
        if state:
            with duckdb.connect(str(self.path), read_only=True) as connection:
                batch_row = connection.execute(
                    """
                    SELECT count(*) FROM sync_batches
                    WHERE run_id = ? AND status = 'completed'
                    """,
                    [state.run_id],
                ).fetchone()
            batch_counts = int(batch_row[0]) if batch_row else 0
            checks["completed_batches"] = batch_counts
            checks["last_successful_page"] = state.last_successful_page
            checks["checkpoint_consistent"] = batch_counts == state.last_successful_page
        checks["ok"] = all(
            checks.get(key, False)
            for key in ("state_exists", "status_completed", "snapshot_exists")
        ) and checks.get("checkpoint_consistent", False)
        return checks


def overlap_datestamp(
    last_successful_datestamp: str | None,
    original_from: str | None,
    *,
    overlap_days: int = 3,
) -> str | None:
    """Return a conservative restart date for an invalid token."""

    if overlap_days < 0:
        raise ValueError("overlap_days must be non-negative")
    if not last_successful_datestamp:
        return original_from
    try:
        last_date = datetime.fromisoformat(last_successful_datestamp[:10]).date()
    except ValueError:
        return original_from
    recovery_date = last_date - timedelta(days=overlap_days)
    recovery_value = recovery_date.isoformat()
    if original_from is None:
        return recovery_value
    return max(original_from[:10], recovery_value)


class SyncCheckpointSession:
    """Reuse one state connection and commit completed pages in batches."""

    def __init__(
        self,
        store: SyncStateStore,
        *,
        run_id: str,
        sync_name: str,
        runtime: DuckDBRuntimeConfig,
    ) -> None:
        self.store = store
        self.run_id = run_id
        self.sync_name = sync_name
        self.connection = duckdb.connect(str(store.path))
        runtime.configure(self.connection, role="query")
        self._closed = False

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def begin_page(
        self, *, page_number: int, request_params: dict[str, str]
    ) -> None:
        now = _now()
        self.connection.execute(
            """
            INSERT INTO sync_batches (
                run_id, page_number, status, request_params_json,
                response_sha256, records, parsed_records, parse_errors,
                warnings, max_datestamp, next_resumption_token,
                started_at, completed_at, error
            ) VALUES (?, ?, 'started', ?, NULL, 0, 0, 0, 0, NULL,
                      NULL, ?, NULL, NULL)
            ON CONFLICT (run_id, page_number) DO UPDATE SET
                status = 'started', request_params_json = excluded.request_params_json,
                response_sha256 = NULL, records = 0, parsed_records = 0,
                parse_errors = 0, warnings = 0, max_datestamp = NULL,
                next_resumption_token = NULL, started_at = excluded.started_at,
                completed_at = NULL, error = NULL
            """,
            [self.run_id, page_number, json.dumps(request_params, sort_keys=True), now],
        )

    def complete_pages(self, pages: list[dict[str, object]]) -> None:
        if not pages:
            return
        now = _now()
        last = pages[-1]
        max_datestamp = max(
            (str(page["max_datestamp"]) for page in pages if page["max_datestamp"]),
            default=None,
        )
        self.connection.execute("BEGIN TRANSACTION")
        try:
            for page in pages:
                self.connection.execute(
                    """
                    UPDATE sync_batches
                    SET status = 'completed', response_sha256 = ?, records = ?,
                        parsed_records = ?, parse_errors = ?, warnings = ?,
                        max_datestamp = ?, next_resumption_token = ?,
                        completed_at = ?, error = NULL
                    WHERE run_id = ? AND page_number = ?
                    """,
                    [
                        page["response_sha256"],
                        page["records"],
                        page["parsed_records"],
                        page["parse_errors"],
                        page["warnings"],
                        page["max_datestamp"],
                        page["next_resumption_token"],
                        now,
                        self.run_id,
                        page["page_number"],
                    ],
                )
            self.connection.execute(
                """
                UPDATE sync_state
                SET updated_at = ?, next_resumption_token = ?,
                    last_successful_page = ?,
                    last_successful_datestamp = CASE
                        WHEN last_successful_datestamp IS NULL THEN ?
                        WHEN ? IS NULL THEN last_successful_datestamp
                        WHEN ? > last_successful_datestamp THEN ?
                        ELSE last_successful_datestamp
                    END,
                    error = NULL
                WHERE sync_name = ? AND run_id = ?
                """,
                [
                    now,
                    last["next_resumption_token"],
                    last["page_number"],
                    max_datestamp,
                    max_datestamp,
                    max_datestamp,
                    max_datestamp,
                    self.sync_name,
                    self.run_id,
                ],
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

    def record_dead_letter(
        self,
        *,
        page_number: int,
        oai_identifier: str,
        error_type: str,
        error: str,
        raw_xml_path: Path | None = None,
    ) -> None:
        sequence_row = self.connection.execute(
            "SELECT nextval('dead_letter_id_seq')"
        ).fetchone()
        if sequence_row is None:
            raise RuntimeError("dead-letter sequence returned no value")
        self.connection.execute(
            """
            INSERT INTO dead_letters (
                id, run_id, page_number, oai_identifier, error_type,
                error, raw_xml_path, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                int(sequence_row[0]),
                self.run_id,
                page_number,
                oai_identifier,
                error_type,
                error[:4000],
                str(raw_xml_path) if raw_xml_path else None,
                _now(),
            ],
        )

    def close(self) -> None:
        if not self._closed:
            self.connection.close()
            self._closed = True


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _state_from_row(row: tuple[object, ...] | None) -> SyncState | None:
    if row is None:
        return None
    return SyncState(
        sync_name=str(row[0]),
        run_id=str(row[1]),
        mode=str(row[2]),
        status=str(row[3]),
        from_date=_optional_string(row[4]),
        until_date=_optional_string(row[5]),
        set_name=_optional_string(row[6]),
        metadata_prefix=_optional_string(row[7]),
        created_from=_optional_string(row[8]),
        created_until=_optional_string(row[9]),
        category_prefixes=_category_prefixes(row[10]),
        next_resumption_token=_optional_string(row[11]),
        last_successful_page=_as_int(row[12]),
        last_successful_datestamp=_optional_string(row[13]),
        recovery_count=_as_int(row[14]),
        started_at=str(row[15]),
        updated_at=str(row[16]),
        completed_at=_optional_string(row[17]),
        snapshot_path=_optional_string(row[18]),
        error=_optional_string(row[19]),
    )


def _state_to_dict(state: SyncState | None) -> dict[str, object] | None:
    if state is None:
        return None
    return {
        "sync_name": state.sync_name,
        "run_id": state.run_id,
        "mode": state.mode,
        "status": state.status,
        "from_date": state.from_date,
        "until_date": state.until_date,
        "set_name": state.set_name,
        "metadata_prefix": state.metadata_prefix,
        "created_from": state.created_from,
        "created_until": state.created_until,
        "category_prefixes": list(state.category_prefixes),
        "next_resumption_token": state.next_resumption_token,
        "last_successful_page": state.last_successful_page,
        "last_successful_datestamp": state.last_successful_datestamp,
        "recovery_count": state.recovery_count,
        "started_at": state.started_at,
        "updated_at": state.updated_at,
        "completed_at": state.completed_at,
        "snapshot_path": state.snapshot_path,
        "error": state.error,
    }


def _context_from_state(state: SyncState, *, resumed: bool) -> SyncContext:
    return SyncContext(
        sync_name=state.sync_name,
        run_id=state.run_id,
        from_date=state.from_date,
        until_date=state.until_date,
        set_name=state.set_name,
        metadata_prefix=state.metadata_prefix,
        created_from=state.created_from,
        created_until=state.created_until,
        category_prefixes=state.category_prefixes,
        next_resumption_token=state.next_resumption_token,
        next_page=state.last_successful_page + 1,
        last_successful_page=state.last_successful_page,
        last_successful_datestamp=state.last_successful_datestamp,
        resumed=resumed,
    )


def _validate_same_scope(
    state: SyncState,
    from_date: str | None,
    until_date: str | None,
    set_name: str | None,
    metadata_prefix: str | None,
    created_from: str | None,
    created_until: str | None,
    category_prefixes: tuple[str, ...],
) -> None:
    requested = (
        from_date,
        until_date,
        set_name,
        metadata_prefix,
        created_from,
        created_until,
        category_prefixes,
    )
    existing = (
        state.from_date,
        state.until_date,
        state.set_name,
        state.metadata_prefix,
        state.created_from,
        state.created_until,
        state.category_prefixes,
    )
    if all(value is None for value in requested):
        return
    if requested != existing:
        raise ValueError(
            "an incomplete sync exists with different source or filter parameters"
        )


def _optional_string(value: object) -> str | None:
    return None if value is None else str(value)


def _as_int(value: object) -> int:
    if value is None:
        raise ValueError("required integer value is NULL")
    return int(str(value))


def _category_prefixes(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return ()
    if not isinstance(parsed, list):
        return ()
    return tuple(str(item) for item in parsed)


_STATE_COLUMNS = (
    "sync_name, run_id, mode, status, from_date, until_date, set_name, "
    "metadata_prefix, created_from, created_until, category_prefixes, "
    "next_resumption_token, last_successful_page, last_successful_datestamp, "
    "recovery_count, started_at, updated_at, completed_at, snapshot_path, error"
)


__all__ = [
    "SyncCheckpointSession",
    "SyncContext",
    "SyncState",
    "SyncStateStore",
    "overlap_datestamp",
]
