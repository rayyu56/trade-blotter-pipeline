"""Tests for the bronze ingestion layer."""

import textwrap
from pathlib import Path

import pandas as pd
import pytest

from trade_blotter.bronze.loader import (
    load_csv,
    load_directory,
    load_excel,
    _check_columns,
)
from trade_blotter.models.trade import IngestResult, REQUIRED_COLUMNS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    content = textwrap.dedent("""\
        trade_id,trade_date,settlement_date,trader,counterparty,asset_class,symbol,side,quantity,price,currency,broker,status
        TRD-001,2026-03-28,2026-04-01,Sarah Chen,Goldman Sachs,Equity,AAPL,Buy,5000,213.45,USD,ITG,Confirmed
        TRD-002,2026-03-28,2026-04-01,James Park,Morgan Stanley,Equity,MSFT,Sell,3000,415.20,USD,Instinet,Confirmed
    """)
    f = tmp_path / "trades.csv"
    f.write_text(content)
    return f


@pytest.fixture
def missing_column_csv(tmp_path: Path) -> Path:
    """CSV missing the 'broker' and 'status' columns."""
    content = textwrap.dedent("""\
        trade_id,trade_date,settlement_date,trader,counterparty,asset_class,symbol,side,quantity,price,currency
        TRD-003,2026-03-28,2026-04-01,Sarah Chen,Barclays,Equity,GOOGL,Buy,1200,175.80,USD
    """)
    f = tmp_path / "trades_missing_cols.csv"
    f.write_text(content)
    return f


@pytest.fixture
def sample_excel(tmp_path: Path, sample_csv: Path) -> Path:
    df = pd.read_csv(sample_csv)
    out = tmp_path / "trades.xlsx"
    df.to_excel(out, index=False)
    return out


# ---------------------------------------------------------------------------
# load_csv
# ---------------------------------------------------------------------------

class TestLoadCsv:
    def test_returns_ingest_result(self, sample_csv):
        result = load_csv(sample_csv)
        assert isinstance(result, IngestResult)

    def test_row_count(self, sample_csv):
        result = load_csv(sample_csv)
        assert result.row_count == 2

    def test_all_columns_present(self, sample_csv):
        result = load_csv(sample_csv)
        assert all(col in result.columns for col in REQUIRED_COLUMNS)

    def test_all_values_are_strings(self, sample_csv):
        result = load_csv(sample_csv)
        assert all(pd.api.types.is_string_dtype(result.dataframe[col]) for col in REQUIRED_COLUMNS)

    def test_empty_fields_not_converted_to_nan(self, tmp_path):
        content = "trade_id,trade_date,settlement_date,trader,counterparty,asset_class,symbol,side,quantity,price,currency,broker,status\nTRD-006,2026-03-31,2026-04-02,Marcus Webb,,Equity,TSLA,Buy,4000,172.65,USD,ITG,Pending\n"
        f = tmp_path / "trades_empty.csv"
        f.write_text(content)
        result = load_csv(f)
        assert result.dataframe.loc[0, "counterparty"] == ""

    def test_source_path_recorded(self, sample_csv):
        result = load_csv(sample_csv)
        assert str(sample_csv) in result.source

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_csv(tmp_path / "nonexistent.csv")
    
    def test_dataframe_cell_value(self, sample_csv):
        result = load_csv(sample_csv)
        assert result.dataframe.loc[0, "trader"] == "Sarah Chen"

# ---------------------------------------------------------------------------
# load_excel
# ---------------------------------------------------------------------------

class TestLoadExcel:
    def test_returns_ingest_result(self, sample_excel):
        result = load_excel(sample_excel)
        assert isinstance(result, IngestResult)

    def test_row_count(self, sample_excel):
        result = load_excel(sample_excel)
        assert result.row_count == 2

    def test_all_values_are_strings(self, sample_excel):
        result = load_excel(sample_excel)
        assert all(pd.api.types.is_string_dtype(result.dataframe[col]) for col in REQUIRED_COLUMNS)
    
    def test_sheet_name_parameter(self, sample_excel):
        result = load_excel(sample_excel, sheet_name="Sheet1")
        assert result.row_count == 2


# ---------------------------------------------------------------------------
# load_directory
# ---------------------------------------------------------------------------

class TestLoadDirectory:
    def test_concatenates_multiple_files(self, tmp_path):
        for i, name in enumerate(["a.csv", "b.csv"]):
            content = (
                "trade_id,trade_date,settlement_date,trader,counterparty,"
                "asset_class,symbol,side,quantity,price,currency,broker,status\n"
                f"TRD-{i:03d},2026-03-28,2026-04-01,Trader,CP,Equity,SYM,Buy,100,10.0,USD,Broker,Confirmed\n"
            )
            (tmp_path / name).write_text(content)

        result = load_directory(tmp_path, pattern="*.csv")
        assert result.row_count == 2

    def test_source_file_column_added(self, tmp_path):
        content = (
            "trade_id,trade_date,settlement_date,trader,counterparty,"
            "asset_class,symbol,side,quantity,price,currency,broker,status\n"
            "TRD-001,2026-03-28,2026-04-01,Trader,CP,Equity,SYM,Buy,100,10.0,USD,Broker,Confirmed\n"
        )
        (tmp_path / "trades.csv").write_text(content)
        result = load_directory(tmp_path)
        assert "_source_file" in result.dataframe.columns

    def test_no_matching_files_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_directory(tmp_path, pattern="*.csv")

    def test_loads_csv_and_excel_files(self, tmp_path, sample_csv, sample_excel):
        # Copy sample files to tmp_path
        (tmp_path / "trades.csv").write_text(sample_csv.read_text())
        (tmp_path / "trades.xlsx").write_bytes(sample_excel.read_bytes())

        result = load_directory(tmp_path, pattern="*.*")
        assert result.row_count == 4  # 2 from CSV + 2 from Excel

    def test_load_csv2_and_excel2_files(self, tmp_path, sample_csv, sample_excel):
        #copy sample files to tmp_path with different names
        (tmp_path/"bobo1.csv").write_text(sample_csv.read_text())
        (tmp_path/"bobo2.xlsx").write_bytes(sample_excel.read_bytes())
        result = load_directory(tmp_path, pattern="*bobo*.*")
        assert result.row_count == 4  # 2 from CSV + 2 from Excel

# ---------------------------------------------------------------------------
# _check_columns (warning behaviour)
# ---------------------------------------------------------------------------

class TestCheckColumns:
    def test_warns_on_missing_columns(self, missing_column_csv, caplog):
        import logging
        df = pd.read_csv(missing_column_csv)
        with caplog.at_level(logging.WARNING):
            _check_columns(df, source="test")
        assert "missing expected columns" in caplog.text

    def test_no_warning_when_all_columns_present(self, sample_csv, caplog):
        import logging
        df = pd.read_csv(sample_csv)
        with caplog.at_level(logging.WARNING):
            _check_columns(df, source="test")
        assert "missing expected columns" not in caplog.text
    
    def test_extra_columns(self, caplog):
        import logging
        df =  pd.DataFrame([{
            "trade_id": "T1", "trade_date": "2026-01-01", "settlement_date": "2026-01-03",
            "trader": "Ray", "counterparty": "GS", "asset_class": "Equity",
            "symbol": "AAPL", "side": "Buy", "quantity": "100", "price": "10.0",
            "currency": "USD", "broker": "ITG", "status": "Confirmed",
            "notes": "extra column here"   # ← the unexpected column
        }])        
        with caplog.at_level(logging.INFO):
                _check_columns(df, source="test_source.csv")
        assert "extra" in caplog.text
