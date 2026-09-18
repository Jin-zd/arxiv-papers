from __future__ import annotations

from io import StringIO

from arxiv_papers.oai import OAIPage, OAIRecord
from arxiv_papers.progress import ProgressDisplay


def _page(*, cursor: int, raw_size: int) -> OAIPage:
    records = tuple(
        OAIRecord(
            identifier=f"oai:arXiv.org:2301.0000{index}",
            datestamp="2023-01-01",
            metadata_xml=b"<metadata />",
        )
        for index in range(2)
    )
    return OAIPage(
        raw_xml=b"x" * raw_size,
        records=records,
        resumption_token="next" if cursor == 0 else None,
        cursor=cursor,
        complete_list_size=10,
    )


def test_progress_displays_records_speed_and_estimated_remaining_bytes() -> None:
    output = StringIO()
    display = ProgressDisplay(stream=output)

    display.start()
    display.update(_page(cursor=0, raw_size=100), page_number=1)
    display.update(_page(cursor=2, raw_size=200), page_number=2)
    display.finish(success=True)

    text = output.getvalue()
    assert "records 4/10" in text
    assert "downloaded" in text
    assert "remaining ~" in text
    assert "/s" in text
    assert text.endswith("\n")


def test_progress_shows_request_before_first_page_arrives() -> None:
    output = StringIO()
    display = ProgressDisplay(stream=output)

    display.start()
    display.requesting(page_number=1)

    assert "requesting OAI page 1" in output.getvalue()


def test_progress_updates_while_response_bytes_are_received() -> None:
    output = StringIO()
    display = ProgressDisplay(stream=output)

    display.start()
    display.requesting(page_number=1)
    display.received_bytes(1024)

    assert "downloading page 1" in output.getvalue()
    assert "1.0 KiB" in output.getvalue()
