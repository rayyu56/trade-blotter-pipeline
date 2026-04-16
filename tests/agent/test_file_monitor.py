"""Tests for agent/tools/file_monitor.py."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from agent.tools.file_monitor import scan_for_new_files, FileMonitorResult


@pytest.fixture
def bronze_dir(tmp_path: Path) -> Path:
    d = tmp_path / "bronze"
    d.mkdir()
    return d


@pytest.fixture
def memory_file(tmp_path: Path) -> Path:
    f = tmp_path / "run_history.json"
    f.write_text("[]")
    return f


def _csv(bronze_dir: Path, name: str = "trades_20260401.csv") -> Path:
    p = bronze_dir / name
    p.write_text("trade_id\nTRD-001\n")
    return p


def _history(memory_file: Path, records: list) -> None:
    memory_file.write_text(json.dumps(records))


# ---------------------------------------------------------------------------
# Basic scanning
# ---------------------------------------------------------------------------

class TestScanForNewFiles:
    def test_empty_directory_returns_nothing(self, bronze_dir, memory_file):
        result = scan_for_new_files(str(bronze_dir), "trades_*.csv", str(memory_file))
        assert result.unprocessed == []
        assert result.already_processed == []

    def test_new_file_is_unprocessed(self, bronze_dir, memory_file):
        _csv(bronze_dir)
        result = scan_for_new_files(str(bronze_dir), "trades_*.csv", str(memory_file))
        assert len(result.unprocessed) == 1
        assert result.already_processed == []

    def test_non_matching_file_is_ignored(self, bronze_dir, memory_file):
        (bronze_dir / "readme.txt").write_text("ignore me")
        result = scan_for_new_files(str(bronze_dir), "trades_*.csv", str(memory_file))
        assert result.unprocessed == []

    def test_missing_bronze_dir_raises(self, tmp_path, memory_file):
        with pytest.raises(FileNotFoundError):
            scan_for_new_files(str(tmp_path / "nonexistent"), "*.csv", str(memory_file))

    def test_missing_memory_file_treats_all_as_new(self, bronze_dir, tmp_path):
        _csv(bronze_dir)
        result = scan_for_new_files(str(bronze_dir), "trades_*.csv",
                                     str(tmp_path / "no_history.json"))
        assert len(result.unprocessed) == 1


# ---------------------------------------------------------------------------
# Deduplication — only status=success blocks re-processing
# ---------------------------------------------------------------------------

class TestDeduplication:
    def test_success_status_marks_file_as_processed(self, bronze_dir, memory_file):
        f = _csv(bronze_dir)
        _history(memory_file, [{"source_file": str(f), "status": "success"}])
        result = scan_for_new_files(str(bronze_dir), "trades_*.csv", str(memory_file))
        assert result.already_processed != []
        assert result.unprocessed == []

    def test_dry_run_status_does_not_block_live_run(self, bronze_dir, memory_file):
        f = _csv(bronze_dir)
        _history(memory_file, [{"source_file": str(f), "status": "dry_run"}])
        result = scan_for_new_files(str(bronze_dir), "trades_*.csv", str(memory_file))
        assert len(result.unprocessed) == 1

    def test_error_status_does_not_block_retry(self, bronze_dir, memory_file):
        f = _csv(bronze_dir)
        _history(memory_file, [{"source_file": str(f), "status": "error"}])
        result = scan_for_new_files(str(bronze_dir), "trades_*.csv", str(memory_file))
        assert len(result.unprocessed) == 1

    def test_failed_status_does_not_block_retry(self, bronze_dir, memory_file):
        f = _csv(bronze_dir)
        _history(memory_file, [{"source_file": str(f), "status": "failed"}])
        result = scan_for_new_files(str(bronze_dir), "trades_*.csv", str(memory_file))
        assert len(result.unprocessed) == 1

    def test_dry_run_followed_by_success_blocks_further_runs(self, bronze_dir, memory_file):
        f = _csv(bronze_dir)
        _history(memory_file, [
            {"source_file": str(f), "status": "dry_run"},
            {"source_file": str(f), "status": "success"},
        ])
        result = scan_for_new_files(str(bronze_dir), "trades_*.csv", str(memory_file))
        assert result.unprocessed == []

    def test_two_files_one_processed(self, bronze_dir, memory_file):
        f1 = _csv(bronze_dir, "trades_20260401.csv")
        _csv(bronze_dir, "trades_20260402.csv")
        _history(memory_file, [{"source_file": str(f1), "status": "success"}])
        result = scan_for_new_files(str(bronze_dir), "trades_*.csv", str(memory_file))
        assert len(result.unprocessed) == 1
        assert len(result.already_processed) == 1
