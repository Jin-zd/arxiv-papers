"""Command-line entry points for local arXiv data maintenance."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from arxiv_papers.oai import OAIClient, OAIClientConfig
from arxiv_papers.state import SyncStateStore
from arxiv_papers.sync import FullSyncRunner

DEFAULT_DATA_DIR = Path("data")
app = typer.Typer(no_args_is_help=True, add_completion=False)
sync_app = typer.Typer(no_args_is_help=True, add_completion=False)
app.add_typer(sync_app, name="sync")


@sync_app.command("full")
def full_sync(
    endpoint: str = typer.Option(
        "https://export.arxiv.org/oai2", help="OAI-PMH endpoint."
    ),
    data_dir: Path | None = typer.Option(  # noqa: B008
        None, help="Local data directory."
    ),
    set_name: str | None = typer.Option(None, "--set", help="Optional OAI set."),
    from_: str | None = typer.Option(None, "--from", help="Inclusive OAI date."),
    until: str | None = typer.Option(None, help="Inclusive OAI date."),
    request_delay: float = typer.Option(
        3.0, min=0, help="Minimum seconds between OAI requests."
    ),
) -> None:
    """Run a full OAI-PMH ListRecords synchronization."""

    config = OAIClientConfig(endpoint=endpoint, request_delay=request_delay)
    with OAIClient(config) as client:
        result = FullSyncRunner(client, data_dir or DEFAULT_DATA_DIR).run(
            from_=from_, until=until, set_name=set_name
        )
    typer.echo(json.dumps(result.as_dict(), ensure_ascii=False, indent=2))


@sync_app.command("status")
def sync_status(
    data_dir: Path = typer.Option(  # noqa: B008
        DEFAULT_DATA_DIR, help="Local data directory."
    ),
) -> None:
    """Show the latest checkpoint and batch status."""

    status = SyncStateStore(data_dir / "state" / "sync.duckdb").status()
    typer.echo(json.dumps(status, ensure_ascii=False, indent=2))


@sync_app.command("verify")
def sync_verify(
    data_dir: Path = typer.Option(  # noqa: B008
        DEFAULT_DATA_DIR, help="Local data directory."
    ),
) -> None:
    """Verify checkpoint and completed snapshot consistency."""

    verification = SyncStateStore(data_dir / "state" / "sync.duckdb").verify()
    typer.echo(json.dumps(verification, ensure_ascii=False, indent=2))


__all__ = ["app"]
