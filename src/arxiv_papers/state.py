"""Persistent synchronization checkpoints and recovery state."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb


@dataclass(frozen=True, slots=True)
class SyncState:
    sync_name: str
    run_id: str
    mode: str
    status: str
    from_date: str | None
    until_date: str | None
    set_name: str | None
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
    run_id: str
    from_date: str | None
    until_date: str | None
    set_name: str | None
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

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(str(self.path)) as connection:
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

    def begin_full(
        self,
        *,
        run_id: str,
        from_date: str | None,
        until_date: str | None,
        set_name: str | None,
        resume: bool = True,
    ) -> SyncContext:
        """Create a run or resume the incomplete full-sync run."""

        now = _now()
        with duckdb.connect(str(self.path)) as connection:
            existing_row = connection.execute(
                "SELECT * FROM sync_state WHERE sync_name = 'full'"
            ).fetchone()
            existing = _state_from_row(existing_row) if existing_row else None
            can_resume = (
                resume
                and existing is not None
                and existing.status in {"running", "failed"}
            )
            if can_resume:
                assert existing is not None
                _validate_same_scope(existing, from_date, until_date, set_name)
                connection.execute(
                    """
                    UPDATE sync_state
                    SET status = 'running', updated_at = ?, error = NULL
                    WHERE sync_name = 'full'
                    """,
                    [now],
                )
                return _context_from_state(existing, resumed=True)

            connection.execute("DELETE FROM sync_state WHERE sync_name = 'full'")
            connection.execute(
                """
                INSERT INTO sync_state (
                    sync_name, run_id, mode, status, from_date, until_date,
                    set_name, next_resumption_token, last_successful_page,
                    last_successful_datestamp, recovery_count, started_at,
                    updated_at, completed_at, snapshot_path, error
                ) VALUES ('full', ?, 'full', 'running', ?, ?, ?, NULL, 0,
                          NULL, 0, ?, ?, NULL, NULL, NULL)
                """,
                [run_id, from_date, until_date, set_name, now, now],
            )
        return SyncContext(
            run_id=run_id,
            from_date=from_date,
            until_date=until_date,
            set_name=set_name,
            next_resumption_token=None,
            next_page=1,
            last_successful_page=0,
            last_successful_datestamp=None,
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
                WHERE sync_name = 'full' AND run_id = ?
                """,
                [
                    now,
                    next_resumption_token,
                    page_number,
                    max_datestamp,
                    max_datestamp,
                    max_datestamp,
                    max_datestamp,
                    run_id,
                ],
            )
            connection.execute("COMMIT")

    def mark_token_invalid(self, *, run_id: str, error: str) -> None:
        with duckdb.connect(str(self.path)) as connection:
            connection.execute(
                """
                UPDATE sync_state
                SET status = 'running', error = ?, updated_at = ?,
                    next_resumption_token = NULL,
                    recovery_count = recovery_count + 1
                WHERE sync_name = 'full' AND run_id = ?
                """,
                [error, _now(), run_id],
            )

    def mark_completed(self, *, run_id: str, snapshot_path: Path) -> None:
        now = _now()
        with duckdb.connect(str(self.path)) as connection:
            connection.execute(
                """
                UPDATE sync_state
                SET status = 'completed', completed_at = ?, updated_at = ?,
                    snapshot_path = ?, error = NULL
                WHERE sync_name = 'full' AND run_id = ?
                """,
                [now, now, str(snapshot_path), run_id],
            )

    def mark_failed(self, *, run_id: str, error: str) -> None:
        with duckdb.connect(str(self.path)) as connection:
            connection.execute(
                """
                UPDATE sync_state
                SET status = 'failed', updated_at = ?, error = ?
                WHERE sync_name = 'full' AND run_id = ?
                """,
                [_now(), error[:4000], run_id],
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
                "SELECT * FROM sync_state WHERE sync_name = ?", [sync_name]
            ).fetchone()
        return _state_from_row(row) if row else None

    def status(self) -> dict[str, object]:
        state = self.get_state()
        result: dict[str, object] = {
            "state_path": str(self.path),
            "state": _state_to_dict(state) if state else None,
        }
        with duckdb.connect(str(self.path), read_only=True) as connection:
            batch_rows = connection.execute(
                """
                SELECT status, count(*) AS count
                FROM sync_batches
                GROUP BY status
                ORDER BY status
                """
            ).fetchall()
            result["batches"] = [
                {"status": str(row[0]), "count": int(row[1])} for row in batch_rows
            ]
            dead_letter_row = connection.execute(
                "SELECT count(*) FROM dead_letters"
            ).fetchone()
            result["dead_letters"] = int(dead_letter_row[0]) if dead_letter_row else 0
        return result

    def verify(self) -> dict[str, object]:
        state = self.get_state()
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
        next_resumption_token=_optional_string(row[7]),
        last_successful_page=_as_int(row[8]),
        last_successful_datestamp=_optional_string(row[9]),
        recovery_count=_as_int(row[10]),
        started_at=str(row[11]),
        updated_at=str(row[12]),
        completed_at=_optional_string(row[13]),
        snapshot_path=_optional_string(row[14]),
        error=_optional_string(row[15]),
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
        run_id=state.run_id,
        from_date=state.from_date,
        until_date=state.until_date,
        set_name=state.set_name,
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
) -> None:
    requested = (from_date, until_date, set_name)
    existing = (state.from_date, state.until_date, state.set_name)
    if all(value is None for value in requested):
        return
    if requested != existing:
        raise ValueError(
            "an incomplete full sync exists with different from/until/set parameters"
        )


def _optional_string(value: object) -> str | None:
    return None if value is None else str(value)


def _as_int(value: object) -> int:
    if value is None:
        raise ValueError("required integer value is NULL")
    return int(str(value))


__all__ = ["SyncContext", "SyncState", "SyncStateStore", "overlap_datestamp"]
