"""Tests for the gold writer layer."""

import datetime
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from trade_blotter.gold.writer import write_parquet, write_database, write_gold_outputs


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({
        "symbol": ["AAPL", "MSFT"],
        "net_pnl": [15_000.0, -8_000.0],
        "trade_count": [3, 2],
    })


@pytest.fixture
def sqlite_engine():
    return create_engine("sqlite:///:memory:")


# ---------------------------------------------------------------------------
# write_parquet
# ---------------------------------------------------------------------------

class TestWriteParquet:
    def test_creates_parquet_file(self, tmp_path, sample_df):
        path = write_parquet(sample_df, tmp_path, "pnl_by_symbol")
        assert path.exists()
        assert path.suffix == ".parquet"

    def test_roundtrip_data_matches(self, tmp_path, sample_df):
        path = write_parquet(sample_df, tmp_path, "pnl_by_symbol")
        loaded = pd.read_parquet(path)
        assert list(loaded["symbol"]) == ["AAPL", "MSFT"]
        assert loaded["net_pnl"].tolist() == pytest.approx([15_000.0, -8_000.0])

    def test_creates_output_dir_if_missing(self, tmp_path, sample_df):
        new_dir = tmp_path / "gold" / "nested"
        write_parquet(sample_df, new_dir, "test")
        assert new_dir.exists()

    def test_returns_path_object(self, tmp_path, sample_df):
        result = write_parquet(sample_df, tmp_path, "test")
        assert isinstance(result, Path)

    def test_filename_matches_dataset_name(self, tmp_path, sample_df):
        path = write_parquet(sample_df, tmp_path, "pnl_summary")
        assert path.name == "pnl_summary.parquet"


# ---------------------------------------------------------------------------
# write_database
# ---------------------------------------------------------------------------

class TestWriteDatabase:
    def test_writes_rows_to_table(self, sqlite_engine, sample_df):
        write_database(sample_df, sqlite_engine, "pnl_by_symbol")
        with sqlite_engine.connect() as conn:
            result = pd.read_sql(text("SELECT * FROM pnl_by_symbol"), conn)
        assert len(result) == 2

    def test_replace_overwrites_existing_data(self, sqlite_engine, sample_df):
        write_database(sample_df, sqlite_engine, "pnl_by_symbol")
        write_database(sample_df.head(1), sqlite_engine, "pnl_by_symbol", if_exists="replace")
        with sqlite_engine.connect() as conn:
            result = pd.read_sql(text("SELECT * FROM pnl_by_symbol"), conn)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# write_gold_outputs
# ---------------------------------------------------------------------------

class TestWriteGoldOutputs:
    def test_writes_multiple_parquet_datasets(self, tmp_path, sample_df):
        outputs = {"pnl_by_symbol": sample_df, "pnl_by_trader": sample_df}
        results = write_gold_outputs(outputs, tmp_path, fmt="parquet")
        assert len(results) == 2
        for path in results.values():
            assert Path(path).exists()

    def test_writes_multiple_database_datasets(self, sqlite_engine, sample_df, tmp_path):
        outputs = {"pnl_by_symbol": sample_df, "positions": sample_df}
        results = write_gold_outputs(outputs, tmp_path, fmt="database", engine=sqlite_engine)
        assert set(results.keys()) == {"pnl_by_symbol", "positions"}

    def test_database_fmt_without_engine_raises(self, tmp_path, sample_df):
        with pytest.raises(ValueError, match="engine is required"):
            write_gold_outputs({"x": sample_df}, tmp_path, fmt="database")

    def test_invalid_fmt_raises(self, tmp_path, sample_df):
        with pytest.raises(ValueError, match="Unsupported format"):
            write_gold_outputs({"x": sample_df}, tmp_path, fmt="csv")
