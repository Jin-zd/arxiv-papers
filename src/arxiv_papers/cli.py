"""Command-line entry points for local arXiv data maintenance."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from arxiv_papers.oai import OAIClient, OAIClientConfig
from arxiv_papers.progress import ProgressDisplay
from arxiv_papers.state import SyncStateStore
from arxiv_papers.sync import FullSyncRunner, IncrementalSyncRunner

DEFAULT_DATA_DIR = Path("data")
app = typer.Typer(no_args_is_help=True, add_completion=False)
sync_app = typer.Typer(no_args_is_help=True, add_completion=False)
app.add_typer(sync_app, name="sync")


@sync_app.command("full")
def full_sync(
    endpoint: str = typer.Option(
        "https://oaipmh.arxiv.org/oai", help="OAI-PMH endpoint."
    ),
    proxy: str | None = typer.Option(
        None,
        "--proxy",
        help="HTTP proxy, e.g. http://127.0.0.1:7890. Uses environment proxy when omitted.",
    ),
    metadata_prefix: str = typer.Option(
        "arXivRaw", "--metadata-prefix", help="OAI metadata prefix."
    ),
    data_dir: Path | None = typer.Option(  # noqa: B008
        None, help="Local data directory."
    ),
    set_name: str = typer.Option("cs", "--set", help="OAI set."),
    from_: str | None = typer.Option(None, "--from", help="Inclusive OAI date."),
    until: str | None = typer.Option(None, help="Inclusive OAI date."),
    created_from: str | None = typer.Option(
        "2023-01-01", help="Inclusive local first-version date."
    ),
    created_until: str | None = typer.Option(
        None, help="Inclusive local first-version date limit."
    ),
    category_prefix: str = typer.Option(
        "cs.", help="Keep records with any category using this prefix."
    ),
    request_delay: float = typer.Option(
        3.0, min=0, help="Minimum seconds between OAI requests."
    ),
    progress: bool = typer.Option(
        True, "--progress/--no-progress", help="Show live sync progress on stderr."
    ),
) -> None:
    """Run a full OAI-PMH ListRecords synchronization."""

    config = OAIClientConfig(
        endpoint=endpoint,
        proxy=proxy,
        metadata_prefix=metadata_prefix,
        request_delay=request_delay,
    )
    progress_display = ProgressDisplay() if progress else None
    if progress_display is not None:
        progress_display.start()
    with OAIClient(
        config,
        on_bytes_received=(
            progress_display.received_bytes if progress_display else None
        ),
    ) as client:
        result = FullSyncRunner(
            client,
            data_dir or DEFAULT_DATA_DIR,
            progress=ProgressDisplay() if progress else None,
        ).run(
            from_=from_,
            until=until,
            set_name=set_name,
            metadata_prefix=metadata_prefix,
            created_from=created_from,
            created_until=created_until,
            category_prefixes=(category_prefix,),
        )
    typer.echo(json.dumps(result.as_dict(), ensure_ascii=False, indent=2))


@sync_app.command("status")
def sync_status(
    data_dir: Path = typer.Option(  # noqa: B008
        DEFAULT_DATA_DIR, help="Local data directory."
    ),
    sync_name: str | None = typer.Option(
        None, "--sync-name", help="Show one sync state, or all when omitted."
    ),
) -> None:
    """Show the latest checkpoint and batch status."""

    status = SyncStateStore(data_dir / "state" / "sync.duckdb").status(sync_name)
    typer.echo(json.dumps(status, ensure_ascii=False, indent=2))


@sync_app.command("verify")
def sync_verify(
    data_dir: Path = typer.Option(  # noqa: B008
        DEFAULT_DATA_DIR, help="Local data directory."
    ),
    sync_name: str = typer.Option("full", "--sync-name", help="State to verify."),
) -> None:
    """Verify checkpoint and completed snapshot consistency."""

    verification = SyncStateStore(data_dir / "state" / "sync.duckdb").verify(sync_name)
    typer.echo(json.dumps(verification, ensure_ascii=False, indent=2))


@sync_app.command("incremental")
def incremental_sync(
    endpoint: str = typer.Option(
        "https://oaipmh.arxiv.org/oai", help="OAI-PMH endpoint."
    ),
    proxy: str | None = typer.Option(
        None,
        "--proxy",
        help="HTTP proxy, e.g. http://127.0.0.1:7890. Uses environment proxy when omitted.",
    ),
    metadata_prefix: str = typer.Option(
        "arXivRaw", "--metadata-prefix", help="OAI metadata prefix."
    ),
    data_dir: Path | None = typer.Option(  # noqa: B008
        None, help="Local data directory."
    ),
    set_name: str | None = typer.Option(None, "--set", help="Override OAI set."),
    from_: str | None = typer.Option(None, "--from", help="Override start date."),
    until: str | None = typer.Option(None, help="Cap the end date."),
    created_from: str | None = typer.Option(
        None, help="Override local first-version date lower bound."
    ),
    created_until: str | None = typer.Option(
        None, help="Override local first-version date upper bound."
    ),
    category_prefix: str | None = typer.Option(
        None, help="Override local category prefix."
    ),
    request_delay: float = typer.Option(
        3.0, min=0, help="Minimum seconds between OAI requests."
    ),
    overlap_days: int = typer.Option(
        3, min=0, help="Days to overlap with the previous checkpoint."
    ),
    safety_lag_days: int = typer.Option(
        1, min=0, help="Days withheld from the current date."
    ),
    progress: bool = typer.Option(
        True, "--progress/--no-progress", help="Show live sync progress on stderr."
    ),
) -> None:
    """Run an incremental OAI-PMH synchronization."""

    config = OAIClientConfig(
        endpoint=endpoint,
        proxy=proxy,
        metadata_prefix=metadata_prefix,
        request_delay=request_delay,
    )
    progress_display = ProgressDisplay() if progress else None
    if progress_display is not None:
        progress_display.start()
    with OAIClient(
        config,
        on_bytes_received=(
            progress_display.received_bytes if progress_display else None
        ),
    ) as client:
        result = IncrementalSyncRunner(
            client,
            data_dir or DEFAULT_DATA_DIR,
            overlap_days=overlap_days,
            progress=ProgressDisplay() if progress else None,
        ).run(
            from_=from_,
            until=until,
            set_name=set_name,
            metadata_prefix=metadata_prefix,
            created_from=created_from,
            created_until=created_until,
            category_prefixes=(category_prefix,) if category_prefix else None,
            safety_lag_days=safety_lag_days,
        )
    typer.echo(json.dumps(result.as_dict(), ensure_ascii=False, indent=2))


__all__ = ["app"]
