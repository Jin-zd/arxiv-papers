from datetime import date
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from arxiv_papers.metadata import (
    ArxivAuthor,
    ArxivCategory,
    ArxivPaperMetadata,
    MetadataParseError,
    ParsedArxivRecord,
    parse_oai_record,
)
from arxiv_papers.oai import (
    OAIClient,
    OAIPage,
    OAIProtocolError,
    OAIRecord,
)
from arxiv_papers.query import DuckDBQueryService
from arxiv_papers.runtime import DuckDBRuntimeConfig
from arxiv_papers.snapshots import SnapshotCatalog
from arxiv_papers.state import SyncStateStore
from arxiv_papers.storage import DuckDBSnapshotBuilder, ParquetBatchStore
from arxiv_papers.sync import FullSyncRunner, IncrementalSyncRunner


def _record(
    author: str,
    category: str,
    datestamp: str = "2026-09-25",
    affiliation: str | None = None,
) -> ParsedArxivRecord:
    metadata = ArxivPaperMetadata(
        arxiv_id="2609.12345",
        version=1,
        title="A paper",
        abstract="An abstract",
        authors=(
            ArxivAuthor(
                display_name=author,
                affiliations=(affiliation,) if affiliation else (),
            ),
        ),
        categories=(ArxivCategory(name=category, is_primary=True),),
        created=date(2026, 9, 1),
        updated=date(2026, 9, 25),
    )
    return ParsedArxivRecord(
        oai_identifier="oai:arXiv.org:2609.12345",
        oai_datestamp=datestamp,
        metadata=metadata,
        metadata_hash=metadata.metadata_hash,
    )


def _stage(root: Path, records: list[ParsedArxivRecord]) -> Path:
    store = ParquetBatchStore(root)
    store.write_batch(1, records)
    store.finalize()
    return root


def test_latest_record_replaces_same_version_relationships(tmp_path: Path) -> None:
    runtime = DuckDBRuntimeConfig(
        build_threads=1, query_threads=1, temp_directory=tmp_path / "duckdb-temp"
    )
    builder = DuckDBSnapshotBuilder(runtime)
    base = builder.build(
        _stage(tmp_path / "base-parquet", [_record("Old Author", "cs.AI")]),
        tmp_path / "base.duckdb",
    ).path
    delta = _stage(tmp_path / "delta-parquet", [_record("New Author", "cs.LG")])

    for name, build in (
        (
            "patch",
            lambda path: builder.build_incremental(delta, path, base_snapshot=base),
        ),
        ("rebuild", lambda path: builder.build(delta, path, base_snapshot=base)),
    ):
        snapshot = build(tmp_path / f"{name}.duckdb").path
        with duckdb.connect(str(snapshot), read_only=True) as connection:
            authors = connection.execute(
                "SELECT display_name FROM authors JOIN paper_authors USING (author_key) "
                "WHERE arxiv_id = '2609.12345'"
            ).fetchall()
            categories = connection.execute(
                "SELECT category FROM categories WHERE arxiv_id = '2609.12345'"
            ).fetchall()
        assert authors == [("New Author",)]
        assert categories == [("cs.LG",)]


def test_latest_record_within_one_archive_batch_controls_relationships(
    tmp_path: Path,
) -> None:
    runtime = DuckDBRuntimeConfig(
        build_threads=1, query_threads=1, temp_directory=tmp_path / "duckdb-temp"
    )
    staged = _stage(
        tmp_path / "parquet",
        [
            _record("Earlier Author", "cs.AI", "2026-09-24"),
            _record("Latest Author", "cs.LG", "2026-09-25"),
        ],
    )
    snapshot = (
        DuckDBSnapshotBuilder(runtime).build(staged, tmp_path / "library.duckdb").path
    )
    with duckdb.connect(str(snapshot), read_only=True) as connection:
        authors = connection.execute(
            "SELECT display_name FROM authors JOIN paper_authors USING (author_key)"
        ).fetchall()
        categories = connection.execute("SELECT category FROM categories").fetchall()
    assert authors == [("Latest Author",)]
    assert categories == [("cs.LG",)]


def test_same_name_author_affiliation_is_replaced(tmp_path: Path) -> None:
    runtime = DuckDBRuntimeConfig(
        build_threads=1, query_threads=1, temp_directory=tmp_path / "duckdb-temp"
    )
    builder = DuckDBSnapshotBuilder(runtime)
    base = builder.build(
        _stage(
            tmp_path / "base-parquet",
            [_record("Same Author", "cs.AI", affiliation="Old University")],
        ),
        tmp_path / "base.duckdb",
    ).path
    snapshot = builder.build_incremental(
        _stage(
            tmp_path / "delta-parquet",
            [_record("Same Author", "cs.AI", affiliation="New University")],
        ),
        tmp_path / "updated.duckdb",
        base_snapshot=base,
    ).path
    with duckdb.connect(str(snapshot), read_only=True) as connection:
        authors = connection.execute(
            "SELECT affiliations_json FROM authors "
            "JOIN paper_authors USING (author_key) "
            "WHERE arxiv_id = '2609.12345'"
        ).fetchall()
    assert authors == [('["New University"]',)]


def test_no_records_match_is_empty_only_for_initial_request() -> None:
    raw = b'<OAI-PMH><error code="noRecordsMatch">empty</error></OAI-PMH>'
    client = OAIClient()
    try:
        page = client._parse_page(raw, request_params={"verb": "ListRecords"})
        assert page.records == ()
        assert page.resumption_token is None
        with pytest.raises(OAIProtocolError):
            client._parse_page(
                raw,
                request_params={"verb": "ListRecords", "resumptionToken": "token"},
            )
    finally:
        client.close()


def test_empty_window_completes_and_records_scan_date(tmp_path: Path) -> None:
    raw = b'<OAI-PMH><error code="noRecordsMatch">empty</error></OAI-PMH>'
    client_parser = OAIClient()
    try:
        page = client_parser._parse_page(
            raw, request_params={"verb": "ListRecords", "until": "2026-09-26"}
        )
    finally:
        client_parser.close()
    runtime = DuckDBRuntimeConfig(
        build_threads=1, query_threads=1, temp_directory=tmp_path / "duckdb-temp"
    )
    with OAIClient() as client:
        runner = FullSyncRunner(client, tmp_path, runtime=runtime)
        with patch.object(client, "list_records", return_value=iter((page,))):
            result = runner.run(until="2026-09-26", created_from=None)
    assert result.snapshot.papers == 0
    state = runner.state_store.get_state("full")
    assert state is not None and state.status == "completed"
    assert state.last_scanned_until == "2026-09-26"


def test_empty_incremental_keeps_base_papers(tmp_path: Path) -> None:
    runtime = DuckDBRuntimeConfig(
        build_threads=1, query_threads=1, temp_directory=tmp_path / "duckdb-temp"
    )
    base = DuckDBSnapshotBuilder(runtime).build(
        _stage(tmp_path / "base-parquet", [_record("Original Author", "cs.AI")]),
        tmp_path / "duckdb" / "library-base.duckdb",
    ).path
    state_store = SyncStateStore(tmp_path / "state" / "sync.duckdb", runtime=runtime)
    state_store.begin_full(
        run_id="full-1",
        from_date=None,
        until_date="2026-09-26",
        set_name="cs",
        metadata_prefix="arXivRaw",
        created_from="2023-01-01",
        created_until=None,
        category_prefixes=("cs.",),
    )
    state_store.mark_completed(run_id="full-1", snapshot_path=base)
    raw = b'<OAI-PMH><error code="noRecordsMatch">empty</error></OAI-PMH>'
    with OAIClient() as client:
        page = client._parse_page(
            raw, request_params={"verb": "ListRecords", "from": "2026-09-23"}
        )
        runner = IncrementalSyncRunner(
            client, tmp_path, state_store=state_store, runtime=runtime
        )
        with patch.object(client, "list_records", return_value=iter((page,))):
            result = runner.run(until="2026-09-27", safety_lag_days=0)
    assert result.snapshot.papers == 1
    state = state_store.get_state("incremental")
    assert state is not None and state.last_scanned_until == "2026-09-27"


def test_parse_error_does_not_checkpoint_page(tmp_path: Path) -> None:
    record = OAIRecord("oai:arXiv.org:2609.12345", "2026-09-25", None)
    page = OAIPage(
        raw_xml=b"<OAI-PMH/>",
        records=(record,),
        resumption_token=None,
        cursor=None,
        complete_list_size=1,
    )
    with OAIClient() as client:
        runner = FullSyncRunner(client, tmp_path)
        with (
            patch.object(client, "list_records", return_value=iter((page,))),
            pytest.raises(MetadataParseError),
        ):
            runner.run(created_from=None, category_prefixes=("cs.",))
    state = runner.state_store.get_state("full")
    assert state is not None
    assert state.status == "failed"
    assert state.last_successful_page == 0
    assert runner.state_store.status()["dead_letters"] == 1


def test_scan_watermark_and_protected_snapshot(tmp_path: Path) -> None:
    store = SyncStateStore(tmp_path / "state.duckdb")
    store.begin_full(
        run_id="full-1",
        from_date=None,
        until_date="2026-09-26",
        set_name="cs",
        metadata_prefix="arXivRaw",
        created_from=None,
        created_until=None,
        category_prefixes=("cs.",),
    )
    duckdb_dir = tmp_path / "duckdb"
    duckdb_dir.mkdir()
    base = duckdb_dir / "library-full-1.duckdb"
    base.touch()
    store.mark_completed(run_id="full-1", snapshot_path=base)
    state = store.latest_completed()
    assert state is not None and state.last_scanned_until == "2026-09-26"

    with (
        OAIClient() as client,
        patch.object(DuckDBSnapshotBuilder, "validate_base_snapshot"),
        patch.object(IncrementalSyncRunner, "_execute_run") as execute,
    ):
        IncrementalSyncRunner(client, tmp_path, state_store=store).run(
            until="2026-09-27", safety_lag_days=0
        )
    context = execute.call_args.args[0]
    assert context.from_date == "2026-09-23"
    assert context.set_name is None

    catalog = SnapshotCatalog(duckdb_dir, retention=1)
    for name in ("library-new-1.duckdb", "library-new-2.duckdb"):
        (duckdb_dir / name).touch()
    catalog.prune(protected_paths=store.completed_snapshot_paths())
    assert base.exists()


def test_old_state_schema_can_be_read_before_migration(tmp_path: Path) -> None:
    store = SyncStateStore(tmp_path / "state.duckdb")
    store.begin_full(
        run_id="full-1",
        from_date=None,
        until_date="2026-09-26",
        set_name="cs",
        metadata_prefix="arXivRaw",
        created_from=None,
        created_until=None,
        category_prefixes=("cs.",),
    )
    store.mark_completed(run_id="full-1", snapshot_path=tmp_path / "library.duckdb")
    with duckdb.connect(str(store.path)) as connection:
        connection.execute("ALTER TABLE sync_state DROP COLUMN last_scanned_until")
    state = store.get_state("full")
    assert state is not None and state.last_scanned_until is None
    store.initialize()
    migrated = store.get_state("full")
    assert migrated is not None and migrated.last_scanned_until == "2026-09-26"


def test_raw_authors_are_candidates_with_source_text_preserved(tmp_path: Path) -> None:
    parsed = parse_oai_record(
        OAIRecord(
            identifier="oai:arXiv.org:2609.12345",
            datestamp="2026-09-25",
            metadata_xml=(
                b"<arXivRaw><id>2609.12345</id><version version='v1'>"
                b"<date>Tue, 01 Sep 2026 12:00:00 GMT</date></version>"
                b"<title>A paper</title><abstract>An abstract</abstract>"
                b"<authors>Ada Smith, Ben Jones and Cora Lee</authors>"
                b"<categories>cs.AI</categories><report-no>ABC-123</report-no>"
                b"</arXivRaw>"
            ),
        )
    )
    assert parsed.metadata is not None
    assert parsed.metadata.authors_raw == "Ada Smith, Ben Jones and Cora Lee"
    assert [author.display_name for author in parsed.metadata.authors] == [
        "Ada Smith", "Ben Jones", "Cora Lee"
    ]
    assert {author.name_source for author in parsed.metadata.authors} == {
        "raw_heuristic"
    }
    assert parsed.metadata.report_numbers == ("ABC-123",)

    runtime = DuckDBRuntimeConfig(
        build_threads=1, query_threads=1, temp_directory=tmp_path / "duckdb-temp"
    )
    snapshot = DuckDBSnapshotBuilder(runtime).build(
        _stage(tmp_path / "parquet", [parsed]), tmp_path / "library.duckdb"
    ).path
    with DuckDBQueryService(snapshot, runtime=runtime) as query:
        paper = query.get_paper("2609.12345")
        assert paper is not None
        assert paper["authors_raw"] == "Ada Smith, Ben Jones and Cora Lee"
        assert paper["report_numbers"] == ["ABC-123"]
        authors = paper["authors"]
        assert isinstance(authors, list)
        assert [author["display_name"] for author in authors] == [
            "Ada Smith", "Ben Jones", "Cora Lee"
        ]
        assert authors[0]["affiliations"] == []
        assert query.get_papers(["2609.12345", "2609.12345"])["2609.12345"]
        assert query.search_by_author("Ben Jones").items


def test_scope_exit_and_reentry_are_applied_by_both_build_paths(tmp_path: Path) -> None:
    runtime = DuckDBRuntimeConfig(
        build_threads=1, query_threads=1, temp_directory=tmp_path / "duckdb-temp"
    )
    builder = DuckDBSnapshotBuilder(runtime)
    base = builder.build(
        _stage(tmp_path / "base-parquet", [_record("Author", "cs.AI")]),
        tmp_path / "base.duckdb",
    ).path
    excluded = _record("Author", "math.CO", "2026-09-26").model_copy(
        update={"excluded": True}
    )
    delta = _stage(tmp_path / "excluded-parquet", [excluded])
    for name, build in (
        ("patch", lambda path: builder.build_incremental(delta, path, base_snapshot=base)),
        ("rebuild", lambda path: builder.build(delta, path, base_snapshot=base)),
    ):
        snapshot = build(tmp_path / f"{name}.duckdb").path
        with duckdb.connect(str(snapshot), read_only=True) as connection:
            assert connection.execute("SELECT count(*) FROM papers").fetchone() == (0,)
            assert connection.execute("SELECT count(*) FROM authors").fetchone() == (0,)
            assert connection.execute("SELECT count(*) FROM excluded_records").fetchone() == (1,)

    reentry = builder.build_incremental(
        _stage(tmp_path / "reentry-parquet", [_record("Author", "cs.AI", "2026-09-27")]),
        tmp_path / "reentry.duckdb",
        base_snapshot=tmp_path / "patch.duckdb",
    ).path
    with duckdb.connect(str(reentry), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM papers").fetchone() == (1,)
        assert connection.execute("SELECT count(*) FROM excluded_records").fetchone() == (0,)

    unrelated = excluded.model_copy(
        update={
            "metadata": excluded.metadata.model_copy(update={"arxiv_id": "2609.99999"})
            if excluded.metadata is not None
            else None,
            "oai_identifier": "oai:arXiv.org:2609.99999",
        }
    )
    unrelated_delta = _stage(tmp_path / "unrelated-parquet", [unrelated])
    assert builder.incremental_change_ratio(unrelated_delta, reentry) == 0
    unrelated_snapshot = builder.build_incremental(
        unrelated_delta, tmp_path / "unrelated.duckdb", base_snapshot=reentry
    ).path
    with duckdb.connect(str(unrelated_snapshot), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM excluded_records").fetchone() == (0,)


def test_incremental_stages_out_of_scope_record(tmp_path: Path) -> None:
    runtime = DuckDBRuntimeConfig(
        build_threads=1, query_threads=1, temp_directory=tmp_path / "duckdb-temp"
    )
    base = DuckDBSnapshotBuilder(runtime).build(
        _stage(tmp_path / "base-parquet", [_record("Author", "cs.AI")]),
        tmp_path / "duckdb" / "library-base.duckdb",
    ).path
    state_store = SyncStateStore(tmp_path / "state" / "sync.duckdb", runtime=runtime)
    state_store.begin_full(
        run_id="full-1",
        from_date=None,
        until_date="2026-09-25",
        set_name="cs",
        metadata_prefix="arXivRaw",
        created_from="2023-01-01",
        created_until=None,
        category_prefixes=("cs.",),
    )
    state_store.mark_completed(run_id="full-1", snapshot_path=base)
    page = OAIPage(
        raw_xml=b"<OAI-PMH/>",
        records=(
            OAIRecord(
                identifier="oai:arXiv.org:2609.12345",
                datestamp="2026-09-27",
                metadata_xml=(
                    b"<arXivRaw><id>2609.12345</id><version version='v1'>"
                    b"<date>Tue, 01 Sep 2026 12:00:00 GMT</date></version>"
                    b"<title>A paper</title><abstract>An abstract</abstract>"
                    b"<authors>Author</authors><categories>math.CO</categories>"
                    b"</arXivRaw>"
                ),
            ),
        ),
        resumption_token=None,
        cursor=None,
        complete_list_size=1,
    )
    with OAIClient() as client:
        runner = IncrementalSyncRunner(
            client, tmp_path, state_store=state_store, runtime=runtime
        )
        with patch.object(client, "list_records", return_value=iter((page,))) as list_records:
            result = runner.run(until="2026-09-27", safety_lag_days=0)
    assert list_records.call_args.kwargs["set_name"] is None
    assert result.snapshot.papers == 0
    assert result.snapshot.excluded_records == 1


def test_unrelated_global_deletion_does_not_change_snapshot(tmp_path: Path) -> None:
    runtime = DuckDBRuntimeConfig(
        build_threads=1, query_threads=1, temp_directory=tmp_path / "duckdb-temp"
    )
    builder = DuckDBSnapshotBuilder(runtime)
    base = builder.build(
        _stage(tmp_path / "base-parquet", [_record("Author", "cs.AI")]),
        tmp_path / "base.duckdb",
    ).path
    delta = _stage(
        tmp_path / "delta-parquet",
        [
            ParsedArxivRecord(
                oai_identifier="oai:arXiv.org:2609.99999",
                oai_datestamp="2026-09-27",
                deleted=True,
            )
        ],
    )
    assert builder.incremental_change_ratio(delta, base) == 0
    snapshot = builder.build_incremental(
        delta, tmp_path / "updated.duckdb", base_snapshot=base
    ).path
    with duckdb.connect(str(snapshot), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM papers").fetchone() == (1,)
        assert connection.execute("SELECT count(*) FROM deleted_records").fetchone() == (0,)
