from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pytest
from test_query import QUERY_PAGE, build_query_snapshot

from arxiv_papers.snapshots import SnapshotCatalog, SnapshotCatalogError


def test_snapshot_catalog_publishes_pointer_and_rejects_bad_snapshot(
    tmp_path: Path,
) -> None:
    snapshot = build_query_snapshot(tmp_path)
    catalog = SnapshotCatalog(tmp_path / "duckdb")

    pointer = catalog.read_current()
    assert pointer is not None
    assert pointer.generation == "query-run"
    assert pointer.snapshot_path == snapshot

    bad_snapshot = tmp_path / "duckdb" / "library-bad.duckdb"
    bad_snapshot.write_bytes(b"not a duckdb database")
    with pytest.raises(SnapshotCatalogError, match="validation"):
        catalog.publish(bad_snapshot, generation="bad")
    assert catalog.current_snapshot() == snapshot


def test_snapshot_catalog_retains_current_and_recent_generations(
    tmp_path: Path,
) -> None:
    source = build_query_snapshot(tmp_path / "source")
    catalog = SnapshotCatalog(tmp_path / "catalog", retention=2)
    catalog.duckdb_dir.mkdir(parents=True)
    paths: list[Path] = []
    for index in range(4):
        target = catalog.duckdb_dir / f"library-generation-{index}.duckdb"
        shutil.copy2(source, target)
        paths.append(target)
        catalog.publish(target, generation=f"generation-{index}")
        catalog.prune()

    assert catalog.current_snapshot() == paths[-1]
    assert paths[-1].exists()
    assert paths[-2].exists()
    assert not paths[0].exists()
    assert len(tuple(catalog.duckdb_dir.glob("library-*.duckdb"))) == 2


def test_snapshot_catalog_current_pointer_is_atomic_file(tmp_path: Path) -> None:
    snapshot = build_query_snapshot(tmp_path)
    catalog = SnapshotCatalog(tmp_path / "duckdb")
    pointer_text = catalog.pointer_path.read_text(encoding="utf-8")
    assert '"generation": "query-run"' in pointer_text
    assert not tuple(catalog.duckdb_dir.glob(".current.json.*.tmp"))
    assert catalog.current_snapshot() == snapshot


def test_snapshot_catalog_validates_legacy_schema_two_snapshot(tmp_path: Path) -> None:
    snapshot = tmp_path / "duckdb" / "library-legacy.duckdb"
    snapshot.parent.mkdir(parents=True)
    with duckdb.connect(str(snapshot)) as connection:
        for table in (
            "papers",
            "paper_versions",
            "authors",
            "paper_authors",
            "categories",
            "deleted_records",
        ):
            connection.execute(f"CREATE TABLE {table} (id INTEGER)")
        connection.execute("CREATE TABLE library_metadata (key VARCHAR, value VARCHAR)")
        connection.execute(
            "INSERT INTO library_metadata VALUES ('schema_version', '2')"
        )

    SnapshotCatalog.validate_snapshot(snapshot)


def test_snapshot_catalog_supports_switching_generations(tmp_path: Path) -> None:
    first = build_query_snapshot(tmp_path, run_id="generation-one")
    second_page = QUERY_PAGE.replace(
        b"Local Search Systems", b"A New Generation Search System"
    )
    second = build_query_snapshot(tmp_path, page=second_page, run_id="generation-two")
    catalog = SnapshotCatalog(tmp_path / "duckdb")

    assert catalog.read_current() is not None
    assert catalog.read_current().generation == "generation-two"
    assert first.exists()
    assert second.exists()
