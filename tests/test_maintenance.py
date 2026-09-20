from __future__ import annotations

import json
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest
from test_query import QUERY_PAGE, build_query_snapshot

from arxiv_papers.logging_config import JsonFormatter
from arxiv_papers.maintenance import (
    MaintenanceError,
    backup_data,
    compact_parquet,
    health_report,
    integrity_report,
    scheduler_plan,
)
from arxiv_papers.oai import OAIClient, OAIClientConfig


def test_json_formatter_emits_structured_record() -> None:
    import logging

    record = logging.LogRecord(
        "arxiv_papers.test",
        logging.INFO,
        __file__,
        1,
        "sync_finished",
        (),
        None,
    )
    record.run_id = "run-1"  # type: ignore[attr-defined]

    payload = json.loads(JsonFormatter().format(record))

    assert payload["event"] == "sync_finished"
    assert payload["run_id"] == "run-1"
    assert payload["level"] == "INFO"


def test_oai_metrics_count_requests_retries_and_bytes() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, content=b"busy", request=request)
        return httpx.Response(200, content=QUERY_PAGE, request=request)

    config = OAIClientConfig(
        endpoint="https://example.test/oai",
        request_delay=0,
        backoff_factor=0,
        max_backoff=0,
        max_retries=1,
    )
    with OAIClient(
        config, http_client=httpx.Client(transport=httpx.MockTransport(handler))
    ) as client:
        list(client.list_records())
        metrics = client.metrics.as_dict()

    assert metrics == {
        "requests": 2,
        "retries": 1,
        "response_bytes": len(b"busy") + len(QUERY_PAGE),
        "transport_errors": 0,
        "http_errors": 1,
    }


def test_health_and_integrity_reports_for_completed_snapshot(tmp_path: Path) -> None:
    build_query_snapshot(tmp_path)

    health = health_report(tmp_path, sync_name="full")
    integrity = integrity_report(tmp_path)

    assert health["ok"] is True
    assert health["generation"] == "query-run"
    assert integrity["ok"] is True
    assert integrity["snapshot"] == {
        "papers": 2,
        "paper_versions": 2,
        "authors": 2,
        "categories": 3,
        "deleted_records": 1,
    }


def test_health_report_does_not_create_missing_state_database(tmp_path: Path) -> None:
    report = health_report(tmp_path)

    assert report["ok"] is False
    assert not (tmp_path / "state" / "sync.duckdb").exists()


def test_compact_parquet_writes_readable_copy_without_changing_source(
    tmp_path: Path,
) -> None:
    build_query_snapshot(tmp_path)
    source = tmp_path / "parquet" / "full" / "query-run"
    source_files = sorted(source.rglob("*.parquet"))
    output = tmp_path / "compacted" / "run"

    result = compact_parquet(source, output)

    assert result.output == output
    assert result.tables["paper_versions"] == 2
    assert result.files_written == 5
    assert sorted(source.rglob("*.parquet")) == source_files
    assert (
        pq.read_table(output / "paper_versions" / "part-000001.parquet").num_rows == 2
    )


def test_backup_manifest_contains_sha256_and_respects_raw_flag(tmp_path: Path) -> None:
    build_query_snapshot(tmp_path)
    raw_file = tmp_path / "raw" / "response.xml"
    raw_file.parent.mkdir(parents=True, exist_ok=True)
    raw_file.write_bytes(b"raw metadata")

    without_raw = backup_data(tmp_path, tmp_path.parent / "backups-one")
    with_raw = backup_data(tmp_path, tmp_path.parent / "backups-two", include_raw=True)

    manifest = json.loads(without_raw.manifest.read_text(encoding="utf-8"))
    assert manifest["include_raw"] is False
    assert not (without_raw.path / "raw").exists()
    assert (with_raw.path / "raw" / "response.xml").read_bytes() == b"raw metadata"
    assert all("sha256" in entry for entry in manifest["files"])


def test_backup_rejects_destination_inside_source(tmp_path: Path) -> None:
    build_query_snapshot(tmp_path)
    destination = tmp_path / "backups"

    with pytest.raises(MaintenanceError, match="inside data directory"):
        backup_data(tmp_path, destination)

    assert not destination.exists()


def test_scheduler_plan_and_time_validation(tmp_path: Path) -> None:
    plan = scheduler_plan(tmp_path / "data", hour=5, minute=7)
    arguments = json.loads(plan["launchd_program_arguments"])

    assert plan["cron"].startswith("7 5 * * * cd ")
    assert "--no-progress" in plan["command"]
    assert arguments[-1] == "--no-progress"
    assert arguments[arguments.index("--data-dir") + 1] == str(
        (tmp_path / "data").resolve()
    )

    with pytest.raises(MaintenanceError, match="hour"):
        scheduler_plan(tmp_path, hour=24)
    with pytest.raises(MaintenanceError, match="minute"):
        scheduler_plan(tmp_path, minute=60)
