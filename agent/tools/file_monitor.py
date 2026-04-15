"""Scans data/bronze/ for CSV files that have not yet been processed."""

from __future__ import annotations

import fnmatch
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class FileMonitorResult:
    unprocessed: List[str] = field(default_factory=list)
    already_processed: List[str] = field(default_factory=list)


def scan_for_new_files(
    bronze_path: str,
    file_pattern: str,
    memory_path: str,
) -> FileMonitorResult:
    """Return CSV files in *bronze_path* that do not appear in run history.

    Args:
        bronze_path: Directory to scan (relative to repo root).
        file_pattern: Glob pattern, e.g. ``"trades_*.csv"``.
        memory_path: Path to ``run_history.json`` used to detect already-processed files.

    Returns:
        :class:`FileMonitorResult` with two lists of absolute file paths.
    """
    bronze_dir = Path(bronze_path)
    if not bronze_dir.exists():
        raise FileNotFoundError(f"Bronze directory not found: {bronze_dir.resolve()}")

    candidates = sorted(
        str(p.resolve())
        for p in bronze_dir.iterdir()
        if p.is_file() and fnmatch.fnmatch(p.name, file_pattern)
    )

    processed_files: set[str] = set()
    memory_file = Path(memory_path)
    if memory_file.exists():
        history = json.loads(memory_file.read_text(encoding="utf-8"))
        for run in history:
            src = run.get("source_file")
            if src:
                processed_files.add(os.path.abspath(src))

    result = FileMonitorResult()
    for path in candidates:
        if path in processed_files:
            result.already_processed.append(path)
        else:
            result.unprocessed.append(path)

    return result
