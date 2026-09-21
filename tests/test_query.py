from __future__ import annotations

from pathlib import Path

import duckdb
import httpx
import pytest

from arxiv_papers.oai import OAIClient, OAIClientConfig
from arxiv_papers.query import DuckDBQueryService, QueryError
from arxiv_papers.sync import FullSyncRunner

QUERY_PAGE = b"""<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <ListRecords>
    <record>
      <header>
        <identifier>oai:arXiv.org:2401.00001</identifier>
        <datestamp>2024-02-03</datestamp>
      </header>
      <metadata>
        <arXiv xmlns="http://arxiv.org/OAI/arXiv/">
          <id>2401.00001</id>
          <created>2024-01-02</created>
          <updated>2024-02-03</updated>
          <authors>
            <author><keyname>Doe</keyname><forenames>Jane</forenames></author>
          </authors>
          <title>Local Search Systems</title>
          <categories>cs.AI cs.LG</categories>
          <comments>Includes a retrieval benchmark.</comments>
          <journal-ref>Journal of Local Search</journal-ref>
          <doi>10.1234/LOCAL.1</doi>
          <abstract>A local abstract about ranking and retrieval.</abstract>
        </arXiv>
      </metadata>
    </record>
    <record>
      <header>
        <identifier>oai:arXiv.org:2401.00002</identifier>
        <datestamp>2024-02-03</datestamp>
      </header>
      <metadata>
        <arXiv xmlns="http://arxiv.org/OAI/arXiv/">
          <id>2401.00002</id>
          <created>2024-01-03</created>
          <updated>2024-02-03</updated>
          <authors>
            <author><keyname>Roe</keyname><forenames>Richard</forenames></author>
          </authors>
          <title>Another Local Study</title>
          <categories>cs.CV</categories>
          <abstract>A second local abstract.</abstract>
        </arXiv>
      </metadata>
    </record>
    <record>
      <header status="deleted">
        <identifier>oai:arXiv.org:2401.00003</identifier>
        <datestamp>2024-02-03</datestamp>
      </header>
    </record>
    <resumptionToken />
  </ListRecords>
</OAI-PMH>"""

VERSIONED_QUERY_PAGE = QUERY_PAGE.replace(
    b"<created>2024-01-02</created>",
    b'<version version="v1"><date>Tue, 02 Jan 2024 05:00:00 GMT</date></version>'
    b'<version version="v2"><date>Sat, 03 Feb 2024 05:00:00 GMT</date></version>'
    b"<created>2024-01-02</created>",
)


def build_query_snapshot(
    tmp_path: Path,
    *,
    page: bytes = QUERY_PAGE,
    run_id: str = "query-run",
) -> Path:
    config = OAIClientConfig(
        endpoint="https://example.test/oai",
        request_delay=0,
        max_retries=0,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=page, request=request)

    with OAIClient(
        config,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as client:
        result = FullSyncRunner(client, tmp_path).run(run_id=run_id)
    return result.snapshot.path


@pytest.fixture
def query_service(tmp_path: Path):
    snapshot_path = build_query_snapshot(tmp_path)
    with DuckDBQueryService(snapshot_path) as service:
        yield service


def test_query_snapshot_has_search_tables_and_structural_indexes(
    tmp_path: Path,
) -> None:
    snapshot_path = build_query_snapshot(tmp_path)

    with duckdb.connect(str(snapshot_path), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM paper_search").fetchone() == (
            2,
        )
        assert connection.execute(
            "SELECT value FROM search_metadata WHERE key = 'search_fields'"
        ).fetchone() == ("title,abstract,comments,journal_ref",)
        assert connection.execute("SELECT count(*) FROM current_papers").fetchone() == (
            2,
        )


def test_search_by_text_author_category_and_date(query_service) -> None:
    text_results = query_service.search_papers(query="ranking")
    assert [item["arxiv_id"] for item in text_results.items] == ["2401.00001"]
    assert text_results.has_more is False

    author_results = query_service.search_by_author("  JANE   DOE ")
    assert [item["arxiv_id"] for item in author_results.items] == ["2401.00001"]
    assert author_results.items[0]["authors"] == ["Jane Doe"]

    category_results = query_service.search_by_category("cs.AI")
    assert [item["arxiv_id"] for item in category_results.items] == ["2401.00001"]
    prefix_results = query_service.search_by_category("cs.")
    assert [item["arxiv_id"] for item in prefix_results.items] == [
        "2401.00001",
        "2401.00002",
    ]

    date_results = query_service.search_papers(
        created_from="2024-01-03", updated_until="2024-02-03"
    )
    assert [item["arxiv_id"] for item in date_results.items] == ["2401.00002"]


def test_keyset_pagination_has_no_duplicates_or_total(query_service) -> None:
    first = query_service.search_by_category("cs.", limit=1)
    assert first.has_more is True
    assert first.next_position is not None
    assert not hasattr(first, "total")

    second = query_service.search_by_category("cs.", limit=1, after=first.next_position)
    assert [item["arxiv_id"] for item in first.items + second.items] == [
        "2401.00001",
        "2401.00002",
    ]
    assert second.has_more is False


def test_get_historical_version_keeps_current_metadata(tmp_path: Path) -> None:
    snapshot_path = build_query_snapshot(tmp_path, page=VERSIONED_QUERY_PAGE)
    with DuckDBQueryService(snapshot_path) as service:
        paper = service.get_paper("2401.00001", version=1)

    assert paper is not None
    assert paper["title"] == "Local Search Systems"
    assert paper["version"] == 2
    assert paper["selected_version"]["version"] == 1
    assert paper["selected_version"]["is_current"] is False
    assert len(paper["versions"]) == 2


def test_get_paper_by_id_doi_and_version_facts(query_service) -> None:
    paper = query_service.get_paper("2401.00001")
    assert paper is not None
    assert paper["title"] == "Local Search Systems"
    assert paper["doi"] == "10.1234/LOCAL.1"
    assert paper["categories"] == [
        {"category": "cs.AI", "is_primary": True},
        {"category": "cs.LG", "is_primary": False},
    ]
    assert len(paper["versions"]) == 1

    by_doi = query_service.get_paper_by_doi("10.1234/local.1")
    assert by_doi is not None
    assert by_doi["arxiv_id"] == "2401.00001"
    assert query_service.get_paper_by_doi("10.1234/missing") is None

    assert query_service.get_paper("2401.00003") is None


def test_query_validation(query_service) -> None:
    with pytest.raises(QueryError, match="limit"):
        query_service.search_papers(limit=101)
    with pytest.raises(QueryError, match="author"):
        query_service.search_by_author(" ")
    with pytest.raises(QueryError, match="category"):
        query_service.search_by_category(" ")
    with pytest.raises(QueryError, match="invalid ISO date"):
        query_service.search_papers(created_from="not-a-date")
