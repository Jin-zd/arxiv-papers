from __future__ import annotations

import json
from datetime import date

from arxiv_papers.archive import RawXMLArchive


def test_raw_xml_archive_writes_compressed_response_and_sidecar(tmp_path) -> None:
    raw_xml = (
        b'<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><Identify /></OAI-PMH>'
    )
    archive = RawXMLArchive(tmp_path)

    entry = archive.save_response(
        run_id="run-001",
        page_number=1,
        raw_xml=raw_xml,
        request_params={"verb": "ListRecords", "metadataPrefix": "arXiv"},
        run_date=date(2026, 9, 15),
        http_status=200,
        elapsed_seconds=0.25,
    )

    assert (
        entry.xml_path
        == tmp_path / "2026-09-15" / "run-001" / "response-000001.xml.zst"
    )
    assert RawXMLArchive.read_response(entry.xml_path) == raw_xml
    sidecar = json.loads(entry.metadata_path.read_text(encoding="utf-8"))
    assert sidecar["sha256"] == entry.sha256
    assert sidecar["size_bytes"] == len(raw_xml)
    assert sidecar["request_params"]["verb"] == "ListRecords"
    assert sidecar["http_status"] == 200


def test_raw_xml_archive_replaces_same_page_atomically(tmp_path) -> None:
    archive = RawXMLArchive(tmp_path)
    first = archive.save_response(
        run_id="run-001",
        page_number=1,
        raw_xml=b"first",
        request_params={},
        run_date=date(2026, 9, 15),
    )
    second = archive.save_response(
        run_id="run-001",
        page_number=1,
        raw_xml=b"second",
        request_params={},
        run_date=date(2026, 9, 15),
    )

    assert first.xml_path == second.xml_path
    assert RawXMLArchive.read_response(second.xml_path) == b"second"
