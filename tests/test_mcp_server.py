from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from test_query import QUERY_PAGE, build_query_snapshot

from arxiv_papers.mcp_server import (
    LocalArxivMCPServer,
    SnapshotResolutionError,
    _decode_cursor,
    _encode_cursor,
    resolve_snapshot_path,
)
from arxiv_papers.snapshots import SnapshotCatalog


def test_mcp_registers_read_only_tools_and_calls_local_query(tmp_path: Path) -> None:
    snapshot = build_query_snapshot(tmp_path)
    application = LocalArxivMCPServer(snapshot)
    try:
        tools = asyncio.run(application.server.list_tools())
        assert {tool.name for tool in tools} == {
            "search_papers",
            "get_paper",
            "search_by_author",
            "search_by_category",
            "library_status",
        }
        result = asyncio.run(
            application.server.call_tool(
                "search_papers", {"query": "ranking", "limit": 1}
            )
        )
        assert result.structured_content is not None
        assert result.structured_content["total"] == 1
        assert result.structured_content["items"][0]["arxiv_id"] == "2401.00001"

        status = asyncio.run(application.server.call_tool("library_status", {}))
        assert status.structured_content["papers"] == 2
        assert status.structured_content["source"] == "arXiv OAI-PMH"
    finally:
        application.close()


def test_mcp_cursor_paginates_and_rejects_changed_filters(tmp_path: Path) -> None:
    snapshot = build_query_snapshot(tmp_path)
    application = LocalArxivMCPServer(snapshot)
    try:
        first = asyncio.run(
            application.server.call_tool(
                "search_by_category", {"category": "cs.", "limit": 1}
            )
        )
        first_payload = first.structured_content
        assert first_payload["next_cursor"]
        assert len(first_payload["items"]) == 1

        second = asyncio.run(
            application.server.call_tool(
                "search_by_category",
                {
                    "category": "cs.",
                    "limit": 1,
                    "cursor": first_payload["next_cursor"],
                },
            )
        )
        assert second.structured_content["items"][0]["arxiv_id"] == "2401.00002"
        assert second.structured_content["next_cursor"] is None

        with pytest.raises(Exception, match="Error executing tool search_by_category"):
            asyncio.run(
                application.server.call_tool(
                    "search_by_category",
                    {
                        "category": "cs.CV",
                        "limit": 1,
                        "cursor": first_payload["next_cursor"],
                    },
                )
            )
    finally:
        application.close()


def test_snapshot_resolution_is_read_only_and_prefers_latest_completed(
    tmp_path: Path,
) -> None:
    snapshot = build_query_snapshot(tmp_path)
    data_dir = tmp_path / "data"
    snapshot_dir = data_dir / "duckdb"
    snapshot_dir.mkdir(parents=True)
    copied = snapshot_dir / "library-copy.duckdb"
    copied.write_bytes(snapshot.read_bytes())
    explicit = resolve_snapshot_path(snapshot=copied)
    assert explicit == copied
    assert resolve_snapshot_path(data_dir=data_dir) == copied
    with pytest.raises(SnapshotResolutionError, match="no completed"):
        resolve_snapshot_path(data_dir=tmp_path / "missing")


def test_mcp_switches_generation_at_request_boundary(tmp_path: Path) -> None:
    first = build_query_snapshot(tmp_path, run_id="generation-one")
    application = LocalArxivMCPServer(
        first, catalog=SnapshotCatalog(tmp_path / "duckdb")
    )
    old_query = application.query
    try:
        assert application.get_paper("2401.00001")["paper"]["title"] == (
            "Local Search Systems"
        )
        second_page = QUERY_PAGE.replace(
            b"Local Search Systems", b"A New Generation Search System"
        )
        second = build_query_snapshot(
            tmp_path, page=second_page, run_id="generation-two"
        )

        assert old_query.get_paper("2401.00001")["title"] == "Local Search Systems"
        assert application.get_paper("2401.00001")["paper"]["title"] == (
            "A New Generation Search System"
        )
        assert application.snapshot_path == second
    finally:
        application.close()


def test_cursor_encoding_is_opaque_and_validated() -> None:
    cursor = _encode_cursor(20, "fingerprint")
    assert (
        json.loads(__import__("base64").urlsafe_b64decode(cursor + "=="))["offset"]
        == 20
    )
    assert _decode_cursor(cursor, "fingerprint") == 20
    with pytest.raises(Exception, match="invalid or mismatched cursor"):
        _decode_cursor(cursor, "other")


@pytest.mark.asyncio
async def test_mcp_stdio_process_supports_initialize_list_and_call(
    tmp_path: Path,
) -> None:
    snapshot = build_query_snapshot(tmp_path)
    executable = Path(sys.executable).with_name("arxiv-papers")
    parameters = StdioServerParameters(
        command=str(executable),
        args=["mcp", "--snapshot", str(snapshot)],
    )

    async with (
        stdio_client(parameters) as (read_stream, write_stream),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        tools = await session.list_tools()
        assert {tool.name for tool in tools.tools} == {
            "search_papers",
            "get_paper",
            "search_by_author",
            "search_by_category",
            "library_status",
        }
        result = await session.call_tool(
            "search_papers", {"query": "ranking", "limit": 1}
        )
        assert result.is_error is not True
        assert result.structured_content["items"][0]["arxiv_id"] == "2401.00001"
