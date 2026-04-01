from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys
import threading
import time
from typing import Any


_LOG_LOCK = threading.Lock()


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float):
        return f"{value:.3f}"
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (list, tuple, set)):
        return ",".join(_format_value(item) for item in value)
    return str(value).replace("\n", "\\n")


def log_event(event: str, /, **fields: Any) -> None:
    timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    parts = [timestamp, f"event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={_format_value(value)}")
    with _LOG_LOCK:
        sys.stderr.write(" ".join(parts) + "\n")
        sys.stderr.flush()


def elapsed_ms(started_at: float) -> int:
    return int(round((time.perf_counter() - started_at) * 1000))
