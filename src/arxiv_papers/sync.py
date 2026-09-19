"""Full OAI-PMH synchronization pipeline with durable checkpoints."""

from __future__ import annotations

import logging
import re
import uuid
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from arxiv_papers.archive import RawArchiveEntry, RawXMLArchive
from arxiv_papers.filters import (
    DEFAULT_CATEGORY_PREFIXES,
    DEFAULT_CREATED_FROM,
    PaperScope,
)
from arxiv_papers.metadata import (
    MetadataParseError,
    ParsedArxivRecord,
    parse_oai_record,
)
from arxiv_papers.oai import OAIClient, OAIPage, OAIProtocolError
from arxiv_papers.progress import ProgressDisplay
from arxiv_papers.snapshots import SnapshotCatalog
from arxiv_papers.state import SyncStateStore, overlap_datestamp
from arxiv_papers.storage import (
    DuckDBSnapshotBuilder,
    ParquetBatchStore,
    SnapshotBuildResult,
)

LOGGER = logging.getLogger(__name__)
RUN_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")


@dataclass(slots=True)
class SyncStats:
    run_id: str
    pages: int = 0
    records: int = 0
    parsed_records: int = 0
    accepted_records: int = 0
    filtered_records: int = 0
    deleted_records: int = 0
    parse_errors: int = 0
    warnings: int = 0
    token_recoveries: int = 0

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class FullSyncResult:
    run_id: str
    stats: SyncStats
    parquet_root: Path
    snapshot: SnapshotBuildResult
    resumed: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "resumed": self.resumed,
            "stats": self.stats.as_dict(),
            "parquet_root": str(self.parquet_root),
            "snapshot": {
                "path": str(self.snapshot.path),
                "paper_versions": self.snapshot.paper_versions,
                "papers": self.snapshot.papers,
                "authors": self.snapshot.authors,
                "categories": self.snapshot.categories,
                "deleted_records": self.snapshot.deleted_records,
            },
        }


class FullSyncRunner:
    """Connect the OAI client, archive, Parquet staging and snapshot builder."""

    def __init__(
        self,
        client: OAIClient,
        data_dir: Path | str,
        *,
        archive: RawXMLArchive | None = None,
        snapshot_builder: DuckDBSnapshotBuilder | None = None,
        state_store: SyncStateStore | None = None,
        overlap_days: int = 3,
        progress: ProgressDisplay | None = None,
        snapshot_retention: int = 3,
    ) -> None:
        if overlap_days < 0:
            raise ValueError("overlap_days must be non-negative")
        self.client = client
        self.data_dir = Path(data_dir)
        self.archive = archive or RawXMLArchive(self.data_dir / "raw")
        self.snapshot_builder = snapshot_builder or DuckDBSnapshotBuilder()
        self.state_store = state_store or SyncStateStore(
            self.data_dir / "state" / "sync.duckdb"
        )
        self.overlap_days = overlap_days
        self.progress = progress
        self.snapshot_catalog = SnapshotCatalog(
            self.data_dir / "duckdb", retention=snapshot_retention
        )

    def run(
        self,
        *,
        run_id: str | None = None,
        from_: str | None = None,
        until: str | None = None,
        set_name: str | None = None,
        metadata_prefix: str | None = None,
        created_from: str | None = DEFAULT_CREATED_FROM.isoformat(),
        created_until: str | None = None,
        category_prefixes: tuple[str, ...] = DEFAULT_CATEGORY_PREFIXES,
    ) -> FullSyncResult:
        requested_run_id = run_id or _new_run_id()
        _validate_run_id(requested_run_id)
        effective_prefix = metadata_prefix or self.client.config.metadata_prefix
        effective_set = set_name or "cs"
        scope = _build_scope(created_from, created_until, category_prefixes)
        context = self.state_store.begin_full(
            run_id=requested_run_id,
            from_date=from_,
            until_date=until,
            set_name=effective_set,
            metadata_prefix=effective_prefix,
            created_from=_date_string(scope.created_from),
            created_until=_date_string(scope.created_until),
            category_prefixes=scope.category_prefixes,
        )
        stats = SyncStats(run_id=context.run_id)
        parquet_root = self.data_dir / "parquet" / "full" / context.run_id
        store = ParquetBatchStore(parquet_root)
        run_date = datetime.now(UTC).date()

        LOGGER.info(
            "full_sync_started",
            extra={
                "run_id": context.run_id,
                "resumed": context.resumed,
                "from": context.from_date,
                "until": context.until_date,
                "set": context.set_name,
                "metadata_prefix": context.metadata_prefix,
                "created_from": context.created_from,
                "created_until": context.created_until,
                "category_prefixes": context.category_prefixes,
                "next_page": context.next_page,
            },
        )
        if self.progress is not None:
            self.progress.start()
        next_page = context.next_page
        try:
            should_fetch = (
                not context.has_checkpointed_pages
                or context.next_resumption_token is not None
            )
            if should_fetch:
                try:
                    next_page = self._consume_pages(
                        client_pages=self.client.list_records(
                            from_=context.from_date,
                            until=context.until_date,
                            set_name=context.set_name,
                            metadata_prefix=context.metadata_prefix,
                            resumption_token=context.next_resumption_token,
                        ),
                        start_page=next_page,
                        run_id=context.run_id,
                        run_date=run_date,
                        store=store,
                        stats=stats,
                        scope=scope,
                    )
                except OAIProtocolError as error:
                    if error.code != "badResumptionToken":
                        raise
                    current = self.state_store.get_state()
                    if current is None:
                        raise
                    recovery_from = overlap_datestamp(
                        current.last_successful_datestamp,
                        current.from_date,
                        overlap_days=self.overlap_days,
                    )
                    self.state_store.mark_token_invalid(
                        run_id=context.run_id, error=str(error)
                    )
                    stats.token_recoveries += 1
                    recovered = self.state_store.get_state()
                    if recovered is None:
                        raise
                    LOGGER.warning(
                        "oai_resumption_token_invalid",
                        extra={
                            "run_id": context.run_id,
                            "recovery_from": recovery_from,
                            "last_successful_page": recovered.last_successful_page,
                        },
                    )
                    next_page = self._consume_pages(
                        client_pages=self.client.list_records(
                            from_=recovery_from,
                            until=recovered.until_date,
                            set_name=recovered.set_name,
                            metadata_prefix=recovered.metadata_prefix,
                        ),
                        start_page=recovered.last_successful_page + 1,
                        run_id=context.run_id,
                        run_date=run_date,
                        store=store,
                        stats=stats,
                        scope=scope,
                    )

            store.finalize()
            snapshot_path = (
                self.data_dir / "duckdb" / f"library-{context.run_id}.duckdb"
            )
            snapshot = self.snapshot_builder.build(parquet_root, snapshot_path)
            self.snapshot_catalog.publish(
                snapshot.path,
                generation=context.run_id,
                counts={
                    "paper_versions": snapshot.paper_versions,
                    "papers": snapshot.papers,
                    "authors": snapshot.authors,
                    "categories": snapshot.categories,
                    "deleted_records": snapshot.deleted_records,
                },
            )
            self.state_store.mark_completed(
                run_id=context.run_id, snapshot_path=snapshot.path
            )
            self.snapshot_catalog.prune()
            result = FullSyncResult(
                run_id=context.run_id,
                stats=stats,
                parquet_root=parquet_root,
                snapshot=snapshot,
                resumed=context.resumed,
            )
            if self.progress is not None:
                self.progress.finish(success=True)
            LOGGER.info("full_sync_finished", extra=result.as_dict())
            return result
        except Exception as error:
            if self.progress is not None:
                self.progress.finish(success=False)
            self.state_store.mark_failed(run_id=context.run_id, error=str(error))
            if not isinstance(error, MetadataParseError):
                self.state_store.record_dead_letter(
                    run_id=context.run_id,
                    page_number=next_page,
                    oai_identifier=None,
                    error_type=type(error).__name__,
                    error=str(error),
                )
            LOGGER.exception("full_sync_failed", extra={"run_id": context.run_id})
            raise

    def _consume_pages(
        self,
        *,
        client_pages: Iterator[OAIPage],
        start_page: int,
        run_id: str,
        run_date: date,
        store: ParquetBatchStore,
        stats: SyncStats,
        scope: PaperScope,
        sync_name: str = "full",
    ) -> int:
        if not hasattr(client_pages, "__iter__"):
            raise TypeError("client_pages must be iterable")
        page_number = start_page
        page_iterator = iter(client_pages)
        while True:
            if self.progress is not None:
                self.progress.requesting(page_number=page_number)
            try:
                page = next(page_iterator)
            except StopIteration:
                break
            stats.pages += 1
            stats.records += len(page.records)
            request_params = dict(page.request_params)
            self.state_store.begin_batch(
                run_id=run_id,
                page_number=page_number,
                request_params=request_params,
            )
            archive_entry = self.archive.save_response(
                run_id=run_id,
                page_number=page_number,
                raw_xml=page.raw_xml,
                request_params=request_params,
                run_date=run_date,
                http_status=page.http_status,
                elapsed_seconds=page.elapsed_seconds,
            )
            parse_errors_before = stats.parse_errors
            parsed_records = self._parse_page_records(
                page=page,
                page_number=page_number,
                run_id=run_id,
                store=store,
                archive_entry=archive_entry,
                stats=stats,
                scope=scope,
            )
            store.write_page(page_number, parsed_records)
            max_datestamp = _max_datestamp(page)
            page_parse_errors = stats.parse_errors - parse_errors_before
            self.state_store.complete_batch(
                run_id=run_id,
                page_number=page_number,
                response_sha256=archive_entry.sha256,
                records=len(page.records),
                parsed_records=len(parsed_records),
                parse_errors=page_parse_errors,
                warnings=sum(len(record.warnings) for record in parsed_records),
                max_datestamp=max_datestamp,
                next_resumption_token=page.resumption_token,
                sync_name=sync_name,
            )
            LOGGER.info(
                "full_sync_page_checkpointed",
                extra={
                    "run_id": run_id,
                    "page_number": page_number,
                    "records": len(page.records),
                    "parsed_records": len(parsed_records),
                    "next_resumption_token": bool(page.resumption_token),
                },
            )
            if self.progress is not None:
                self.progress.update(page, page_number=page_number)
            page_number += 1
        return page_number

    def _parse_page_records(
        self,
        *,
        page: OAIPage,
        page_number: int,
        run_id: str,
        store: ParquetBatchStore,
        archive_entry: RawArchiveEntry,
        stats: SyncStats,
        scope: PaperScope,
    ) -> list[ParsedArxivRecord]:
        parsed_records: list[ParsedArxivRecord] = []
        for record in page.records:
            try:
                parsed = parse_oai_record(record)
            except MetadataParseError as error:
                stats.parse_errors += 1
                store.write_parse_error(
                    page_number=page_number,
                    oai_identifier=record.identifier,
                    error=error,
                )
                self.state_store.record_dead_letter(
                    run_id=run_id,
                    page_number=page_number,
                    oai_identifier=record.identifier,
                    error_type=type(error).__name__,
                    error=str(error),
                    raw_xml_path=archive_entry.xml_path,
                )
                LOGGER.error(
                    "metadata_record_parse_failed",
                    extra={
                        "run_id": run_id,
                        "page_number": page_number,
                        "oai_identifier": record.identifier,
                        "error": str(error),
                    },
                )
                continue
            parsed_records.append(parsed)
            stats.parsed_records += 1
            stats.warnings += len(parsed.warnings)
            if parsed.deleted:
                stats.deleted_records += 1
            if scope.accepts(parsed):
                stats.accepted_records += 1
            else:
                stats.filtered_records += 1
                parsed_records.pop()
        return parsed_records


class IncrementalSyncRunner(FullSyncRunner):
    """Synchronize an OAI datestamp window onto the latest local snapshot."""

    def run(
        self,
        *,
        run_id: str | None = None,
        from_: str | None = None,
        until: str | None = None,
        set_name: str | None = None,
        metadata_prefix: str | None = None,
        created_from: str | None = None,
        created_until: str | None = None,
        category_prefixes: tuple[str, ...] | None = None,
        safety_lag_days: int = 1,
    ) -> FullSyncResult:
        if safety_lag_days < 0:
            raise ValueError("safety_lag_days must be non-negative")
        existing_incremental = self.state_store.get_state("incremental")
        resuming = existing_incremental is not None and existing_incremental.status in {
            "running",
            "failed",
        }
        base_state = self.state_store.latest_completed()
        if base_state is None or base_state.snapshot_path is None:
            raise RuntimeError(
                "incremental sync requires a completed full or incremental snapshot"
            )
        base_snapshot = Path(base_state.snapshot_path)
        if not base_snapshot.exists():
            raise RuntimeError(f"base snapshot does not exist: {base_snapshot}")
        self.snapshot_builder.validate_base_snapshot(base_snapshot)

        if resuming:
            assert existing_incremental is not None
            effective_from = existing_incremental.from_date
            effective_until = existing_incremental.until_date
            effective_set = existing_incremental.set_name
            effective_prefix = existing_incremental.metadata_prefix
            effective_created_from = existing_incremental.created_from
            effective_created_until = existing_incremental.created_until
            effective_category_prefixes = existing_incremental.category_prefixes
            requested_run_id = existing_incremental.run_id
        else:
            safe_until = (
                datetime.now(UTC).date() - timedelta(days=safety_lag_days)
            ).isoformat()
            effective_until = (
                min(until, safe_until) if until is not None else safe_until
            )
            effective_from = overlap_datestamp(
                base_state.last_successful_datestamp,
                from_,
                overlap_days=self.overlap_days,
            )
            effective_set = set_name or base_state.set_name
            effective_prefix = metadata_prefix or base_state.metadata_prefix
            effective_created_from = (
                created_from
                if created_from is not None
                else base_state.created_from or DEFAULT_CREATED_FROM.isoformat()
            )
            effective_created_until = (
                created_until if created_until is not None else base_state.created_until
            )
            effective_category_prefixes = (
                category_prefixes
                if category_prefixes is not None
                else base_state.category_prefixes or DEFAULT_CATEGORY_PREFIXES
            )
            requested_run_id = run_id or _new_run_id(prefix="incremental")
        effective_set = effective_set or "cs"
        effective_prefix = effective_prefix or self.client.config.metadata_prefix
        scope = _build_scope(
            effective_created_from,
            effective_created_until,
            effective_category_prefixes or DEFAULT_CATEGORY_PREFIXES,
        )
        _validate_run_id(requested_run_id)
        context = self.state_store.begin_incremental(
            run_id=requested_run_id,
            from_date=effective_from,
            until_date=effective_until,
            set_name=effective_set,
            metadata_prefix=effective_prefix,
            created_from=_date_string(scope.created_from),
            created_until=_date_string(scope.created_until),
            category_prefixes=scope.category_prefixes,
            initial_datestamp=base_state.last_successful_datestamp,
        )
        stats = SyncStats(run_id=context.run_id)
        parquet_root = self.data_dir / "parquet" / "incremental" / context.run_id
        store = ParquetBatchStore(parquet_root)
        run_date = datetime.now(UTC).date()
        if self.progress is not None:
            self.progress.start()
        next_page = context.next_page
        try:
            should_fetch = (
                not context.has_checkpointed_pages
                or context.next_resumption_token is not None
            )
            if should_fetch:
                try:
                    next_page = self._consume_pages(
                        client_pages=self.client.list_records(
                            from_=context.from_date,
                            until=context.until_date,
                            set_name=context.set_name,
                            metadata_prefix=context.metadata_prefix,
                            resumption_token=context.next_resumption_token,
                        ),
                        start_page=next_page,
                        run_id=context.run_id,
                        run_date=run_date,
                        store=store,
                        stats=stats,
                        scope=scope,
                        sync_name="incremental",
                    )
                except OAIProtocolError as error:
                    if error.code != "badResumptionToken":
                        raise
                    current = self.state_store.get_state("incremental")
                    if current is None:
                        raise
                    recovery_from = overlap_datestamp(
                        current.last_successful_datestamp,
                        current.from_date,
                        overlap_days=self.overlap_days,
                    )
                    self.state_store.mark_token_invalid(
                        run_id=context.run_id,
                        error=str(error),
                        sync_name="incremental",
                    )
                    stats.token_recoveries += 1
                    recovered = self.state_store.get_state("incremental")
                    if recovered is None:
                        raise
                    next_page = self._consume_pages(
                        client_pages=self.client.list_records(
                            from_=recovery_from,
                            until=recovered.until_date,
                            set_name=recovered.set_name,
                            metadata_prefix=recovered.metadata_prefix,
                        ),
                        start_page=recovered.last_successful_page + 1,
                        run_id=context.run_id,
                        run_date=run_date,
                        store=store,
                        stats=stats,
                        scope=scope,
                        sync_name="incremental",
                    )

            store.finalize()
            snapshot_path = (
                self.data_dir / "duckdb" / f"library-{context.run_id}.duckdb"
            )
            snapshot = self.snapshot_builder.build(
                parquet_root, snapshot_path, base_snapshot=base_snapshot
            )
            self.snapshot_catalog.publish(
                snapshot.path,
                generation=context.run_id,
                counts={
                    "paper_versions": snapshot.paper_versions,
                    "papers": snapshot.papers,
                    "authors": snapshot.authors,
                    "categories": snapshot.categories,
                    "deleted_records": snapshot.deleted_records,
                },
            )
            self.state_store.mark_completed(
                run_id=context.run_id,
                snapshot_path=snapshot.path,
                sync_name="incremental",
            )
            self.snapshot_catalog.prune()
            if self.progress is not None:
                self.progress.finish(success=True)
            return FullSyncResult(
                run_id=context.run_id,
                stats=stats,
                parquet_root=parquet_root,
                snapshot=snapshot,
                resumed=context.resumed,
            )
        except Exception as error:
            if self.progress is not None:
                self.progress.finish(success=False)
            self.state_store.mark_failed(
                run_id=context.run_id, error=str(error), sync_name="incremental"
            )
            self.state_store.record_dead_letter(
                run_id=context.run_id,
                page_number=next_page,
                oai_identifier=None,
                error_type=type(error).__name__,
                error=str(error),
            )
            raise


def _new_run_id(prefix: str = "full") -> str:
    return f"{prefix}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def _validate_run_id(run_id: str) -> None:
    if not run_id or RUN_ID_RE.fullmatch(run_id) is None:
        raise ValueError(
            "run_id must contain only letters, digits, dot, underscore or hyphen"
        )


def _max_datestamp(page: OAIPage) -> str | None:
    values = [record.datestamp for record in page.records if record.datestamp]
    return max(values) if values else None


def _build_scope(
    created_from: str | None,
    created_until: str | None,
    category_prefixes: tuple[str, ...],
) -> PaperScope:
    return PaperScope(
        created_from=_parse_date(created_from),
        created_until=_parse_date(created_until),
        category_prefixes=tuple(category_prefixes),
    )


def _parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError as exc:
        raise ValueError(f"invalid ISO date: {value}") from exc


def _date_string(value: date | None) -> str | None:
    return value.isoformat() if value else None


__all__ = ["FullSyncResult", "FullSyncRunner", "IncrementalSyncRunner", "SyncStats"]
