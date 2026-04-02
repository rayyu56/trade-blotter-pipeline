"""Integration and unit tests for pipeline.py."""

import textwrap
from pathlib import Path

import pandas as pd
import pytest
import yaml

from trade_blotter.pipeline import (
    PipelineResult,
    run,
    run_bronze,
    run_silver,
    run_gold,
    _load_config,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CLEAN_CSV = textwrap.dedent("""\
    trade_id,trade_date,settlement_date,trader,counterparty,asset_class,symbol,side,quantity,price,currency,broker,status
    TRD-001,2026-03-28,2026-04-01,Sarah Chen,Goldman Sachs,Equity,AAPL,Buy,5000,213.45,USD,ITG,Confirmed
    TRD-002,2026-03-28,2026-04-01,James Park,Morgan Stanley,Equity,MSFT,Sell,3000,415.20,USD,Instinet,Confirmed
    TRD-003,2026-03-28,2026-04-01,Sarah Chen,Barclays,FX,EUR/USD,Buy,1000000,1.0842,USD,Bloomberg,Confirmed
""")

DIRTY_CSV = textwrap.dedent("""\
    trade_id,trade_date,settlement_date,trader,counterparty,asset_class,symbol,side,quantity,price,currency,broker,status
    TRD-001,2026-03-28,2026-04-01,Sarah Chen,Goldman Sachs,Equity,AAPL,Buy,5000,213.45,USD,ITG,Confirmed
    TRD-002,2026-03-28,2026-04-01,,Morgan Stanley,Equity,MSFT,Sell,3000,415.20,USD,Instinet,Confirmed
""")


def _write_config(tmp_path: Path, source_path: Path, fail_on_error: bool = False,
                  target_type: str = "parquet") -> Path:
    cfg = {
        "ingest": {"source_type": "csv", "source_path": str(source_path)},
        "silver": {"fail_on_validation_error": fail_on_error,
                   "output_path": str(tmp_path / "silver")},
        "gold": {
            "outputs": ["pnl", "positions"],
            "target_type": target_type,
            "output_path": str(tmp_path / "gold"),
        },
    }
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(yaml.dump(cfg))
    return config_path


def _silver_cfg(tmp_path: Path, fail_on_error: bool = False) -> dict:
    return {
        "silver": {
            "fail_on_validation_error": fail_on_error,
            "output_path": str(tmp_path / "silver"),
        }
    }


@pytest.fixture
def clean_csv(tmp_path):
    p = tmp_path / "bronze" / "trades.csv"
    p.parent.mkdir()
    p.write_text(CLEAN_CSV)
    return p


@pytest.fixture
def dirty_csv(tmp_path):
    p = tmp_path / "bronze" / "trades.csv"
    p.parent.mkdir()
    p.write_text(DIRTY_CSV)
    return p


# ---------------------------------------------------------------------------
# _load_config
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_loads_yaml(self, tmp_path):
        cfg_file = tmp_path / "pipeline.yaml"
        cfg_file.write_text("ingest:\n  source_type: csv\n")
        cfg = _load_config(cfg_file)
        assert cfg["ingest"]["source_type"] == "csv"

    def test_missing_file_returns_empty_dict(self, tmp_path):
        cfg = _load_config(tmp_path / "nonexistent.yaml")
        assert cfg == {}


# ---------------------------------------------------------------------------
# run_bronze
# ---------------------------------------------------------------------------

class TestRunBronze:
    def test_loads_csv_directory(self, clean_csv, tmp_path):
        cfg = {"ingest": {"source_type": "csv", "source_path": str(clean_csv.parent)}}
        result = run_bronze(cfg)
        assert result.row_count == 3

    def test_loads_single_csv_file(self, clean_csv):
        cfg = {"ingest": {"source_type": "csv", "source_path": str(clean_csv)}}
        result = run_bronze(cfg)
        assert result.row_count == 3

    def test_unsupported_source_type_raises(self, tmp_path):
        cfg = {"ingest": {"source_type": "database", "source_path": str(tmp_path)}}
        with pytest.raises(ValueError, match="Unsupported source_type"):
            run_bronze(cfg)


# ---------------------------------------------------------------------------
# run_silver
# ---------------------------------------------------------------------------

class TestRunSilver:
    def test_returns_three_tuple(self, clean_csv, tmp_path):
        from trade_blotter.bronze.loader import load_csv
        bronze_df = load_csv(clean_csv).dataframe
        result = run_silver(bronze_df, _silver_cfg(tmp_path))
        assert len(result) == 3

    def test_valid_count_and_dataframe(self, clean_csv, tmp_path):
        from trade_blotter.bronze.loader import load_csv
        bronze_df = load_csv(clean_csv).dataframe
        val_result, silver_df, _ = run_silver(bronze_df, _silver_cfg(tmp_path))
        assert val_result.valid_count == 3
        assert len(silver_df) == 3

    def test_exits_on_rejection_when_fail_on_error_true(self, dirty_csv, tmp_path):
        from trade_blotter.bronze.loader import load_csv
        bronze_df = load_csv(dirty_csv).dataframe
        with pytest.raises(SystemExit):
            run_silver(bronze_df, _silver_cfg(tmp_path, fail_on_error=True))

    def test_continues_when_fail_on_error_false(self, dirty_csv, tmp_path):
        from trade_blotter.bronze.loader import load_csv
        bronze_df = load_csv(dirty_csv).dataframe
        val_result, silver_df, _ = run_silver(bronze_df, _silver_cfg(tmp_path))
        assert val_result.rejected_count == 1
        assert val_result.valid_count == 1

    def test_writes_valid_trades_parquet(self, clean_csv, tmp_path):
        from trade_blotter.bronze.loader import load_csv
        bronze_df = load_csv(clean_csv).dataframe
        _, _, paths = run_silver(bronze_df, _silver_cfg(tmp_path))
        assert paths["valid_trades"].exists()

    def test_writes_rejected_trades_parquet(self, dirty_csv, tmp_path):
        from trade_blotter.bronze.loader import load_csv
        bronze_df = load_csv(dirty_csv).dataframe
        _, _, paths = run_silver(bronze_df, _silver_cfg(tmp_path))
        assert paths["rejected_trades"].exists()

    def test_writes_warnings_parquet(self, clean_csv, tmp_path):
        from trade_blotter.bronze.loader import load_csv
        bronze_df = load_csv(clean_csv).dataframe
        _, _, paths = run_silver(bronze_df, _silver_cfg(tmp_path))
        assert paths["warnings"].exists()

    def test_valid_parquet_row_count_matches(self, clean_csv, tmp_path):
        from trade_blotter.bronze.loader import load_csv
        bronze_df = load_csv(clean_csv).dataframe
        val_result, _, paths = run_silver(bronze_df, _silver_cfg(tmp_path))
        loaded = pd.read_parquet(paths["valid_trades"])
        assert len(loaded) == val_result.valid_count

    def test_rejected_parquet_preserves_validation_issues(self, dirty_csv, tmp_path):
        from trade_blotter.bronze.loader import load_csv
        bronze_df = load_csv(dirty_csv).dataframe
        _, _, paths = run_silver(bronze_df, _silver_cfg(tmp_path))
        loaded = pd.read_parquet(paths["rejected_trades"])
        assert "_validation_issues" in loaded.columns


# ---------------------------------------------------------------------------
# run_gold
# ---------------------------------------------------------------------------

class TestRunGold:
    def _make_silver_df(self):
        from tests.gold.test_pnl import _silver_df
        import datetime
        return _silver_df([
            {"trade_id": "TRD-001", "side": "Buy",  "quantity": 5000,
             "price": 213.45, "notional": 5000 * 213.45,
             "trade_date": datetime.date(2026, 3, 28)},
            {"trade_id": "TRD-002", "side": "Sell", "quantity": 3000,
             "price": 415.20, "notional": 3000 * 415.20,
             "trade_date": datetime.date(2026, 3, 28), "symbol": "MSFT"},
        ])

    def test_returns_gold_dataset_counts(self, tmp_path):
        cfg = {
            "gold": {
                "outputs": ["pnl", "positions"],
                "target_type": "parquet",
                "output_path": str(tmp_path / "gold"),
            }
        }
        counts = run_gold(self._make_silver_df(), cfg)
        assert "pnl_by_symbol" in counts
        assert "net_positions" in counts

    def test_pnl_only_output(self, tmp_path):
        cfg = {
            "gold": {
                "outputs": ["pnl"],
                "target_type": "parquet",
                "output_path": str(tmp_path / "gold"),
            }
        }
        counts = run_gold(self._make_silver_df(), cfg)
        assert "pnl_by_symbol" in counts
        assert "net_positions" not in counts

    def test_parquet_files_created(self, tmp_path):
        gold_dir = tmp_path / "gold"
        cfg = {
            "gold": {
                "outputs": ["pnl", "positions"],
                "target_type": "parquet",
                "output_path": str(gold_dir),
            }
        }
        run_gold(self._make_silver_df(), cfg)
        parquet_files = list(gold_dir.glob("*.parquet"))
        assert len(parquet_files) == 5  # 3 pnl + 2 positions


# ---------------------------------------------------------------------------
# run (end-to-end integration)
# ---------------------------------------------------------------------------

class TestRunEndToEnd:
    def test_returns_pipeline_result(self, clean_csv, tmp_path):
        config_path = _write_config(tmp_path, clean_csv.parent)
        result = run(config_path)
        assert isinstance(result, PipelineResult)

    def test_bronze_row_count(self, clean_csv, tmp_path):
        config_path = _write_config(tmp_path, clean_csv.parent)
        result = run(config_path)
        assert result.bronze_row_count == 3

    def test_all_rows_valid_on_clean_data(self, clean_csv, tmp_path):
        config_path = _write_config(tmp_path, clean_csv.parent)
        result = run(config_path)
        assert result.silver_valid_count == 3
        assert result.silver_rejected_count == 0

    def test_gold_datasets_produced(self, clean_csv, tmp_path):
        config_path = _write_config(tmp_path, clean_csv.parent)
        result = run(config_path)
        assert "pnl_by_symbol" in result.gold_datasets
        assert "net_positions" in result.gold_datasets

    def test_rejected_rows_counted_when_not_failing(self, dirty_csv, tmp_path):
        config_path = _write_config(tmp_path, dirty_csv.parent, fail_on_error=False)
        result = run(config_path)
        assert result.silver_rejected_count == 1
        assert result.silver_valid_count == 1

    def test_silver_paths_in_result(self, clean_csv, tmp_path):
        config_path = _write_config(tmp_path, clean_csv.parent)
        result = run(config_path)
        assert "valid_trades" in result.silver_paths
        assert "rejected_trades" in result.silver_paths
        assert "warnings" in result.silver_paths

    def test_silver_parquet_files_exist_on_disk(self, clean_csv, tmp_path):
        config_path = _write_config(tmp_path, clean_csv.parent)
        result = run(config_path)
        for path in result.silver_paths.values():
            assert path.exists()

    def test_valid_trades_parquet_matches_silver_valid_count(self, clean_csv, tmp_path):
        config_path = _write_config(tmp_path, clean_csv.parent)
        result = run(config_path)
        loaded = pd.read_parquet(result.silver_paths["valid_trades"])
        assert len(loaded) == result.silver_valid_count
