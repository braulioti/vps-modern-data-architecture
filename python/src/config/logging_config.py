"""
Central logging configuration for the Python project.

Writes logs in logfmt-style (Grafana Loki/Promtail friendly).
- Console (stdout): always enabled so logs appear in docker logs.
- File: optional; only when log_file is non-empty.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path


def setup_logging(log_file: str, level: str = "INFO") -> None:
    """
    Configure root logger: stdout (for docker logs) and optionally a file.

    Args:
        log_file: Path to log file; if empty, only console is used.
        level: Log level name (e.g. "DEBUG", "INFO", "WARNING").
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    formatter = logging.Formatter(
        fmt=(
            "ts=%(asctime)s "
            "level=%(levelname)s "
            "logger=%(name)s "
            "msg=\"%(message)s\""
        ),
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # Console handler -> stdout so docker logs (and docker logs -f) show all output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)

    root = logging.getLogger()
    if not root.handlers:
        root.setLevel(log_level)
        root.addHandler(console_handler)
        if log_file and log_file.strip():
            path = Path(log_file.strip())
            path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(path, encoding="utf-8")
            file_handler.setFormatter(formatter)
            file_handler.setLevel(log_level)
            root.addHandler(file_handler)
    else:
        root.setLevel(log_level)

