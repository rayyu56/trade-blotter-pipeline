"""Tests for the silver persistence layer."""

from pathlib import Path

import pandas as pd
import pytest

from trade_blotter.silver.writer import (
    REJECTED_DATASET,
    VALID_DATASET,
    WARNINGS_DATASET,
    write_silver_outputs,
)


@pytest.fixture
def valid_df() -> pd.DataFrame:
    return pd.DataFrame({
        "trade_id": ["TRD-001", "TRD-002"],
        "symbol": ["AAPL", "MSFT"],
        "side": ["Buy", "Sell"],
        "quantity": [5000, 3000],
        "price": [213.45, 415.20],
        "notional": [1_067_250.0, 1_245_600.0],
    })


@pytest.fixture
def rejected_df() -> pd.DataFrame:
    return pd.DataFrame({
        "trade_id": ["TRD-006"],
        "symbol": ["TSLA"],
        "side": ["Buy"],
        "quantity": ["4000"],
        "price": ["172.65"],
        "_validation_issues": ["|REJECT:missing_counterparty"],
    })


@pytest.fixture
def warnings_df() -> pd.DataFrame:
    return pd.DataFrame({
        "trade_id": ["TRD-008"],
        "symbol": ["EUR/USD"],
        "side": ["Buy"],
        "quantity": ["2000000"],
        "price": ["1.0842"],
        "_validation_issues": ["|WARN:missing_broker"],
    })


# ---------------------------------------------------------------------------
# File creation
# ---------------------------------------------------------------------------

class TestWriteSilverOutputs:
    def test_creates_three_parquet_files(self, tmp_path, valid_df, rejected_df, warnings_df):
        write_silver_outputs(valid_df, rejected_df, warnings_df, tmp_path)
        assert (tmp_path / "valid_trades.parquet").exists()
        assert (tmp_path / "rejected_trades.parquet").exists()
        assert (tmp_path / "warnings.parquet").exists()

    def test_returns_dict_of_three_paths(self, tmp_path, valid_df, rejected_df, warnings_df):
        paths = write_silver_outputs(valid_df, rejected_df, warnings_df, tmp_path)
        assert set(paths.keys()) == {VALID_DATASET, REJECTED_DATASET, WARNINGS_DATASET}

    def test_returned_paths_are_path_objects(self, tmp_path, valid_df, rejected_df, warnings_df):
        paths = write_silver_outputs(valid_df, rejected_df, warnings_df, tmp_path)
        assert all(isinstance(p, Path) for p in paths.values())

    def test_creates_output_dir_if_missing(self, tmp_path, valid_df, rejected_df, warnings_df):
        nested = tmp_path / "silver" / "nested"
        write_silver_outputs(valid_df, rejected_df, warnings_df, nested)
        assert nested.exists()


# ---------------------------------------------------------------------------
# Data roundtrip
# ---------------------------------------------------------------------------

class TestRoundtrip:
    def test_valid_trades_roundtrip(self, tmp_path, valid_df, rejected_df, warnings_df):
        paths = write_silver_outputs(valid_df, rejected_df, warnings_df, tmp_path)
        loaded = pd.read_parquet(paths[VALID_DATASET])
        assert list(loaded["trade_id"]) == ["TRD-001", "TRD-002"]
        assert len(loaded) == 2

    def test_rejected_trades_preserves_validation_issues(
        self, tmp_path, valid_df, rejected_df, warnings_df
    ):
        paths = write_silver_outputs(valid_df, rejected_df, warnings_df, tmp_path)
        loaded = pd.read_parquet(paths[REJECTED_DATASET])
        assert "_validation_issues" in loaded.columns
        assert "REJECT:missing_counterparty" in loaded.iloc[0]["_validation_issues"]

    def test_warnings_preserves_validation_issues(
        self, tmp_path, valid_df, rejected_df, warnings_df
    ):
        paths = write_silver_outputs(valid_df, rejected_df, warnings_df, tmp_path)
        loaded = pd.read_parquet(paths[WARNINGS_DATASET])
        assert "_validation_issues" in loaded.columns

    def test_empty_rejected_writes_empty_parquet(self, tmp_path, valid_df, warnings_df):
        paths = write_silver_outputs(valid_df, pd.DataFrame(), warnings_df, tmp_path)
        loaded = pd.read_parquet(paths[REJECTED_DATASET])
        assert len(loaded) == 0

    def test_empty_warnings_writes_empty_parquet(self, tmp_path, valid_df, rejected_df):
        paths = write_silver_outputs(valid_df, rejected_df, pd.DataFrame(), tmp_path)
        loaded = pd.read_parquet(paths[WARNINGS_DATASET])
        assert len(loaded) == 0
