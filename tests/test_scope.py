from __future__ import annotations

from arxiv_papers.filters import PaperScope
from arxiv_papers.metadata import ParsedArxivRecord, parse_oai_record
from arxiv_papers.oai import OAIRecord


def _record(xml: bytes) -> ParsedArxivRecord:
    return parse_oai_record(
        OAIRecord(
            identifier="oai:arXiv.org:2301.00001",
            datestamp="2023-11-20",
            metadata_xml=xml,
        )
    )


def test_scope_uses_v1_date_and_any_category() -> None:
    parsed = _record(
        b"""<arXivRaw xmlns="http://arxiv.org/OAI/arXivRaw/">
          <id>2301.00001</id>
          <version version="v1"><date>Sat, 31 Dec 2022 05:01:37 GMT</date></version>
          <version version="v2"><date>Mon, 02 Jan 2023 05:01:37 GMT</date></version>
          <categories>stat.ML cs.AI</categories>
          <title>Old paper</title><abstract>Abstract</abstract>
        </arXivRaw>"""
    )
    assert not PaperScope().accepts(parsed)


def test_scope_accepts_secondary_cs_category_after_2023() -> None:
    parsed = _record(
        b"""<arXivRaw xmlns="http://arxiv.org/OAI/arXivRaw/">
          <id>2301.00001</id>
          <version version="v1"><date>Sun, 01 Jan 2023 05:01:37 GMT</date></version>
          <categories>stat.ML cs.AI</categories>
          <title>New paper</title><abstract>Abstract</abstract>
        </arXivRaw>"""
    )
    assert PaperScope().accepts(parsed)
