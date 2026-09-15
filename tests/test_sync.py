from __future__ import annotations

from pathlib import Path

import duckdb
import httpx
import pytest

from arxiv_papers.oai import OAIClient, OAIClientConfig
from arxiv_papers.sync import FullSyncRunner

PAGE = b"""<?xml version="1.0" encoding="UTF-8"?>
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
          <authors><author><keyname>Doe</keyname><forenames>Jane</forenames></author></authors>
          <title>A local paper</title>
          <categories>cs.AI</categories>
          <abstract>A local abstract.</abstract>
        </arXiv>
      </metadata>
    </record>
    <record>
      <header status="deleted">
        <identifier>oai:arXiv.org:2401.00002</identifier>
        <datestamp>2024-02-03</datestamp>
      </header>
    </record>
    <resumptionToken />
  </ListRecords>
</OAI-PMH>"""

PAGE_WITH_TOKEN = PAGE.replace(
    b"<resumptionToken />",
    b'<resumptionToken cursor="0" completeListSize="2">next-token</resumptionToken>',
)
BAD_TOKEN_ERROR = b"""<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <error code="badResumptionToken">expired</error>
</OAI-PMH>"""


def mock_transport(
    responses: list[httpx.Response | Exception],
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        response.request = request
        return response

    return httpx.MockTransport(handler)


def test_full_sync_archives_stages_and_builds_snapshot(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=PAGE, request=request)

    config = OAIClientConfig(
        endpoint="https://example.test/oai",
        request_delay=0,
        max_retries=0,
    )
    with OAIClient(
        config, http_client=httpx.Client(transport=httpx.MockTransport(handler))
    ) as client:
        result = FullSyncRunner(client, tmp_path).run(
            run_id="run-001",
            from_="2024-01-01",
            until="2024-12-31",
        )

    assert len(requests) == 1
    assert result.stats.pages == 1
    assert result.stats.records == 2
    assert result.stats.parsed_records == 2
    assert result.stats.deleted_records == 1
    assert result.snapshot.paper_versions == 1
    assert result.snapshot.papers == 1
    assert result.snapshot.authors == 1
    assert result.snapshot.categories == 1
    assert result.snapshot.deleted_records == 1
    assert result.snapshot.path.exists()

    raw_files = list((tmp_path / "raw").rglob("*.xml.zst"))
    assert len(raw_files) == 1
    assert (result.parquet_root / "paper_versions" / "part-000001.parquet").exists()
    assert dict(requests[0].url.params) == {
        "verb": "ListRecords",
        "metadataPrefix": "arXiv",
        "from": "2024-01-01",
        "until": "2024-12-31",
    }

    with duckdb.connect(str(result.snapshot.path), read_only=True) as connection:
        paper = connection.execute("SELECT arxiv_id, title FROM papers").fetchone()
        assert paper == ("2401.00001", "A local paper")
        assert connection.execute(
            "SELECT count(*) FROM deleted_records"
        ).fetchone() == (1,)


def test_full_sync_continues_after_bad_metadata_and_records_error(
    tmp_path: Path,
) -> None:
    bad_page = PAGE.replace(
        b"<id>2401.00001</id>",
        b"<missing-id>2401.00001</missing-id>",
    )
    config = OAIClientConfig(
        endpoint="https://example.test/oai",
        request_delay=0,
        max_retries=0,
    )
    transport = mock_transport([httpx.Response(200, content=bad_page)])

    with OAIClient(config, http_client=httpx.Client(transport=transport)) as client:
        result = FullSyncRunner(client, tmp_path).run(run_id="run-errors")

    assert result.stats.records == 2
    assert result.stats.parsed_records == 1
    assert result.stats.parse_errors == 1
    errors_path = result.parquet_root / "parse-errors.jsonl"
    assert errors_path.exists()
    assert "no arXiv id" in errors_path.read_text(encoding="utf-8")
    assert result.snapshot.papers == 0


def test_full_sync_resumes_from_saved_token_after_failure(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []
    responses: list[httpx.Response | Exception] = [
        httpx.Response(200, content=PAGE_WITH_TOKEN),
        httpx.ConnectError("offline"),
        httpx.Response(200, content=PAGE),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        response.request = request
        return response

    config = OAIClientConfig(
        endpoint="https://example.test/oai",
        request_delay=0,
        max_retries=0,
    )
    with OAIClient(
        config, http_client=httpx.Client(transport=httpx.MockTransport(handler))
    ) as client:
        with pytest.raises(Exception, match="offline"):
            FullSyncRunner(client, tmp_path).run(run_id="resume-run")
        result = FullSyncRunner(client, tmp_path).run()

    assert result.resumed is True
    assert result.run_id == "resume-run"
    assert result.snapshot.papers == 1
    assert dict(requests[1].url.params) == {
        "verb": "ListRecords",
        "resumptionToken": "next-token",
    }
    assert dict(requests[2].url.params) == {
        "verb": "ListRecords",
        "resumptionToken": "next-token",
    }


def test_full_sync_recovers_from_invalid_token_with_overlap_window(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []
    responses = [
        httpx.Response(200, content=PAGE_WITH_TOKEN),
        httpx.Response(200, content=BAD_TOKEN_ERROR),
        httpx.Response(200, content=PAGE),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        response = responses.pop(0)
        response.request = request
        return response

    config = OAIClientConfig(
        endpoint="https://example.test/oai",
        request_delay=0,
        max_retries=0,
    )
    with OAIClient(
        config, http_client=httpx.Client(transport=httpx.MockTransport(handler))
    ) as client:
        result = FullSyncRunner(client, tmp_path, overlap_days=3).run(
            run_id="token-run"
        )

    assert result.stats.token_recoveries == 1
    assert result.snapshot.papers == 1
    assert dict(requests[2].url.params) == {
        "verb": "ListRecords",
        "metadataPrefix": "arXiv",
        "from": "2024-01-31",
    }
