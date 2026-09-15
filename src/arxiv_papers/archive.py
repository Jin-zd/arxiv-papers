"""Durable storage for raw OAI-PMH responses."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import zstandard


class RawArchiveError(Exception):
    """Raised when a raw OAI-PMH response cannot be archived safely."""


@dataclass(frozen=True, slots=True)
class RawArchiveEntry:
    """Paths and checksums for one archived OAI-PMH response."""

    xml_path: Path
    metadata_path: Path
    sha256: str
    size_bytes: int


class RawXMLArchive:
    """Store immutable, compressed OAI-PMH responses with sidecar metadata."""

    def __init__(self, root: Path | str, *, compression_level: int = 3) -> None:
        self.root = Path(root)
        if not 1 <= compression_level <= 22:
            raise ValueError("compression_level must be between 1 and 22")
        self.compression_level = compression_level

    def save_response(
        self,
        *,
        run_id: str,
        page_number: int,
        raw_xml: bytes,
        request_params: Mapping[str, str],
        run_date: date | None = None,
        http_status: int | None = None,
        elapsed_seconds: float | None = None,
    ) -> RawArchiveEntry:
        """Atomically save one response and its JSON sidecar.

        The checksum is calculated over the original XML bytes, not the
        compressed representation, so it can be used to validate replays and
        reparsing independently of the compression implementation.
        """

        if not raw_xml:
            raise RawArchiveError("cannot archive an empty response")
        if page_number < 1:
            raise ValueError("page_number must be positive")

        safe_run_id = _safe_component(run_id, "run_id")
        effective_date = run_date or datetime.now(UTC).date()
        date_dir = effective_date.isoformat()
        run_dir = self.root / date_dir / safe_run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        basename = f"response-{page_number:06d}"
        xml_path = run_dir / f"{basename}.xml.zst"
        metadata_path = run_dir / f"{basename}.json"
        sha256 = hashlib.sha256(raw_xml).hexdigest()
        compressed = zstandard.ZstdCompressor(level=self.compression_level).compress(
            raw_xml
        )

        metadata: dict[str, object] = {
            "run_id": safe_run_id,
            "page_number": page_number,
            "request_params": dict(request_params),
            "received_at": datetime.now(UTC).isoformat(),
            "sha256": sha256,
            "size_bytes": len(raw_xml),
            "compressed_size_bytes": len(compressed),
        }
        if http_status is not None:
            metadata["http_status"] = http_status
        if elapsed_seconds is not None:
            metadata["elapsed_seconds"] = elapsed_seconds

        _atomic_write_bytes(xml_path, compressed)
        _atomic_write_bytes(
            metadata_path,
            json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True).encode(
                "utf-8"
            ),
        )
        return RawArchiveEntry(
            xml_path=xml_path,
            metadata_path=metadata_path,
            sha256=sha256,
            size_bytes=len(raw_xml),
        )

    @staticmethod
    def read_response(path: Path | str) -> bytes:
        """Read and decompress an archived response."""

        response_path = Path(path)
        try:
            return zstandard.ZstdDecompressor().decompress(response_path.read_bytes())
        except (OSError, zstandard.ZstdError) as exc:
            raise RawArchiveError(
                f"could not read archived response {response_path}: {exc}"
            ) from exc


def _safe_component(value: str, field_name: str) -> str:
    if not value or value in {".", ".."} or not re.fullmatch(r"[A-Za-z0-9._-]+", value):
        raise ValueError(f"{field_name} contains unsafe path characters")
    return value


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(content)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, path)
    except OSError as exc:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise RawArchiveError(f"could not atomically write {path}: {exc}") from exc


__all__ = ["RawArchiveEntry", "RawArchiveError", "RawXMLArchive"]
