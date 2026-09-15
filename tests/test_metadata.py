from __future__ import annotations

from datetime import date

import pytest

from arxiv_papers.metadata import (
    MetadataParseError,
    normalize_arxiv_id,
    parse_oai_record,
)
from arxiv_papers.oai import OAIRecord

METADATA = b"""<arXiv xmlns="http://arxiv.org/OAI/arXiv/">
  <id>arXiv:2401.00001v2</id>
  <created>2024-01-02</created>
  <updated>2024-02-03</updated>
  <authors>
    <author>
      <keyname>Doe</keyname><forenames>Jane</forenames>
      <affiliation>Example  University</affiliation>
    </author>
    <author><keyname>Roe</keyname><forenames>Richard</forenames><suffix>Jr.</suffix></author>
  </authors>
  <title> A title\nwith   whitespace </title>
  <categories>cs.AI stat.ML</categories>
  <comments>  12 pages\nwith figures  </comments>
  <journal-ref> Journal of Testing, 2024 </journal-ref>
  <doi>https://doi.org/10.1234/example</doi>
  <license>http://creativecommons.org/licenses/by/4.0/</license>
  <report-no>REPORT-1 REPORT-2</report-no>
  <abstract> An abstract\nwith multiple   spaces. </abstract>
</arXiv>"""


def test_parse_oai_record_normalizes_metadata_and_hashes_it() -> None:
    parsed = parse_oai_record(
        OAIRecord(
            identifier="oai:arXiv.org:2401.00001",
            datestamp="2024-02-03",
            metadata_xml=METADATA,
        )
    )

    assert parsed.metadata is not None
    assert parsed.metadata.arxiv_id == "2401.00001"
    assert parsed.metadata.version == 2
    assert parsed.metadata.title == "A title with whitespace"
    assert parsed.metadata.abstract == "An abstract with multiple spaces."
    assert parsed.metadata.created == date(2024, 1, 2)
    assert parsed.metadata.updated == date(2024, 2, 3)
    assert [author.display_name for author in parsed.metadata.authors] == [
        "Jane Doe",
        "Richard Roe Jr.",
    ]
    assert parsed.metadata.authors[0].affiliations == ("Example University",)
    assert parsed.metadata.categories[0].is_primary is True
    assert parsed.metadata.categories[1].is_primary is False
    assert parsed.metadata.doi == "10.1234/example"
    assert parsed.metadata.journal_ref == "Journal of Testing, 2024"
    assert parsed.metadata.report_numbers == ("REPORT-1", "REPORT-2")
    assert parsed.metadata_hash == parsed.metadata.metadata_hash
    assert "version suffix normalized from metadata id" in parsed.warnings


def test_deleted_record_is_preserved_without_metadata() -> None:
    parsed = parse_oai_record(
        OAIRecord(
            identifier="oai:arXiv.org:2401.00001",
            datestamp="2024-02-03",
            metadata_xml=None,
            deleted=True,
        )
    )

    assert parsed.deleted is True
    assert parsed.metadata is None
    assert parsed.metadata_hash is None


def test_missing_optional_fields_are_reported_as_warnings() -> None:
    parsed = parse_oai_record(
        OAIRecord(
            identifier="oai:arXiv.org:2401.00001",
            datestamp="2024-02-03",
            metadata_xml=b"""<arXiv xmlns=\"http://arxiv.org/OAI/arXiv/\"><id>2401.00001</id><title>X</title></arXiv>""",
        )
    )

    assert parsed.metadata is not None
    assert "missing abstract" in parsed.warnings
    assert "missing authors" in parsed.warnings
    assert "missing categories" in parsed.warnings
    assert "missing created date" in parsed.warnings


def test_invalid_record_shape_raises_parse_error() -> None:
    with pytest.raises(MetadataParseError, match="no arXiv id"):
        parse_oai_record(
            OAIRecord(
                identifier="oai:arXiv.org:unknown",
                datestamp=None,
                metadata_xml=b"<arXiv><title>No ID</title></arXiv>",
            )
        )


def test_normalize_arxiv_id_supports_new_and_legacy_ids() -> None:
    assert normalize_arxiv_id("https://arxiv.org/abs/2401.00001v3")[:2] == (
        "2401.00001",
        3,
    )
    assert normalize_arxiv_id("hep-th/9901001")[:2] == ("hep-th/9901001", None)
