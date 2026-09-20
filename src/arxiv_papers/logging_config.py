"""Structured logging configuration for synchronization and maintenance jobs."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar


class JsonFormatter(logging.Formatter):
    """Render one JSON object per log record."""

    _reserved: ClassVar[set[str]] = set(
        logging.LogRecord(None, 0, "", 0, "", (), None).__dict__
    )

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in self._reserved or key.startswith("_"):
                continue
            payload[key] = _jsonable(value)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def configure_logging(
    *,
    log_file: Path | str | None = None,
    level: int = logging.INFO,
    stream: Any = None,
) -> logging.Logger:
    """Configure the application logger without changing third-party handlers."""

    root = logging.getLogger("arxiv_papers")
    root.setLevel(level)
    root.propagate = False
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        handler.close()
    formatter = JsonFormatter()
    output_stream = stream if stream is not None else sys.stderr
    stream_handler = logging.StreamHandler(output_stream)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)
    if log_file is not None:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
    return root


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    return value


__all__ = ["JsonFormatter", "configure_logging"]
