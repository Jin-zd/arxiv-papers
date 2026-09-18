from __future__ import annotations

import httpx
import pytest
import respx

from arxiv_papers.oai import (
    OAIClient,
    OAIClientConfig,
    OAIHTTPError,
    OAIParseError,
    OAIProtocolError,
    OAITransportError,
)

PAGE_ONE = b"""<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <ListRecords>
    <record>
      <header>
        <identifier>oai:arXiv.org:2401.00001</identifier>
        <datestamp>2024-01-02</datestamp>
      </header>
      <metadata><arXiv xmlns="http://arxiv.org/OAI/arXiv/"><id>2401.00001</id></arXiv></metadata>
    </record>
    <resumptionToken cursor="1" completeListSize="2">next-page-token</resumptionToken>
  </ListRecords>
</OAI-PMH>"""

PAGE_TWO = b"""<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <ListRecords>
    <record>
      <header status="deleted">
        <identifier>oai:arXiv.org:2401.00002</identifier>
        <datestamp>2024-01-03</datestamp>
      </header>
    </record>
    <resumptionToken />
  </ListRecords>
</OAI-PMH>"""


def make_config(**overrides: object) -> OAIClientConfig:
    values: dict[str, object] = {
        "request_delay": 0,
        "backoff_factor": 0,
        "max_backoff": 0,
        "max_retries": 2,
    }
    values.update(overrides)
    return OAIClientConfig(**values)  # type: ignore[arg-type]


@respx.mock
def test_list_records_follows_resumption_token_and_parses_records() -> None:
    route = respx.get("https://oaipmh.arxiv.org/oai").mock(
        side_effect=[
            httpx.Response(200, content=PAGE_ONE),
            httpx.Response(200, content=PAGE_TWO),
        ]
    )

    with OAIClient(make_config()) as client:
        pages = list(
            client.list_records(
                from_="2024-01-01",
                until="2024-01-31",
                set_name="physics:hep-th",
            )
        )

    assert len(pages) == 2
    assert pages[0].records[0].identifier == "oai:arXiv.org:2401.00001"
    assert pages[0].records[0].metadata_xml is not None
    assert pages[1].records[0].deleted is True
    assert pages[1].resumption_token is None
    assert route.call_count == 2
    assert route.calls[0].request.url.params["metadataPrefix"] == "arXivRaw"
    assert route.calls[0].request.url.params["from"] == "2024-01-01"
    assert route.calls[0].request.url.params["until"] == "2024-01-31"
    assert route.calls[0].request.url.params["set"] == "physics:hep-th"
    assert dict(route.calls[1].request.url.params) == {
        "verb": "ListRecords",
        "resumptionToken": "next-page-token",
    }


@respx.mock
def test_can_resume_from_an_existing_token() -> None:
    route = respx.get("https://oaipmh.arxiv.org/oai").mock(
        return_value=httpx.Response(200, content=PAGE_TWO)
    )

    with OAIClient(make_config()) as client:
        pages = list(client.list_records(resumption_token="saved-token"))

    assert len(pages) == 1
    assert dict(route.calls[0].request.url.params) == {
        "verb": "ListRecords",
        "resumptionToken": "saved-token",
    }


@respx.mock
def test_retries_transient_http_status() -> None:
    route = respx.get("https://oaipmh.arxiv.org/oai").mock(
        side_effect=[
            httpx.Response(503, text="busy"),
            httpx.Response(200, content=PAGE_TWO),
        ]
    )

    with OAIClient(make_config()) as client:
        pages = list(client.list_records())

    assert len(pages) == 1
    assert route.call_count == 2


@respx.mock
def test_raises_http_error_for_non_retryable_status() -> None:
    respx.get("https://oaipmh.arxiv.org/oai").mock(
        return_value=httpx.Response(400, text="bad request")
    )

    with OAIClient(make_config()) as client, pytest.raises(OAIHTTPError) as error:
        list(client.list_records())

    assert error.value.status_code == 400


@respx.mock
def test_raises_protocol_error() -> None:
    body = b"""<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><error code="badResumptionToken">expired</error></OAI-PMH>"""
    respx.get("https://oaipmh.arxiv.org/oai").mock(
        return_value=httpx.Response(200, content=body)
    )

    with (
        OAIClient(make_config()) as client,
        pytest.raises(OAIProtocolError, match="badResumptionToken"),
    ):
        list(client.list_records(resumption_token="expired-token"))


@respx.mock
def test_rejects_malformed_xml() -> None:
    respx.get("https://oaipmh.arxiv.org/oai").mock(
        return_value=httpx.Response(200, content=b"<not-xml")
    )

    with OAIClient(make_config()) as client, pytest.raises(OAIParseError):
        list(client.list_records())


@respx.mock
def test_exhausted_transport_retries_raise_client_error() -> None:
    respx.get("https://oaipmh.arxiv.org/oai").mock(
        side_effect=httpx.ConnectError("offline")
    )

    with (
        OAIClient(make_config(max_retries=1)) as client,
        pytest.raises(OAITransportError),
    ):
        list(client.list_records())
