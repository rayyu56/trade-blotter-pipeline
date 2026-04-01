"""Tests for the silver cleaning layer."""

import datetime

import pandas as pd
import pytest

from trade_blotter.silver.cleaner import clean


def _valid_df(**overrides) -> pd.DataFrame:
    row = {
        "trade_id": "TRD-001",
        "trade_date": "2026-03-28",
        "settlement_date": "2026-04-01",
        "trader": "  Sarah Chen  ",
        "counterparty": "Goldman Sachs",
        "asset_class": "Equity",
        "symbol": "AAPL",
        "side": "Buy",
        "quantity": "5000",
        "price": "213.45",
        "currency": "USD",
        "broker": "ITG",
        "status": "Confirmed",
    }
    row.update(overrides)
    return pd.DataFrame([row])


# ---------------------------------------------------------------------------
# String normalisation
# ---------------------------------------------------------------------------

class TestStripStrings:
    def test_strips_whitespace_from_trader(self):
        df = clean(_valid_df())
        assert df.loc[0, "trader"] == "Sarah Chen"

    def test_strips_whitespace_from_symbol(self):
        df = clean(_valid_df(symbol="  AAPL  "))
        assert df.loc[0, "symbol"] == "AAPL"


# ---------------------------------------------------------------------------
# Side normalisation
# ---------------------------------------------------------------------------

class TestSideNormalisation:
    @pytest.mark.parametrize("raw,expected", [
        ("Buy", "Buy"),
        ("Sell", "Sell"),
    ])
    def test_valid_sides_unchanged(self, raw, expected):
        df = clean(_valid_df(side=raw))
        assert df.loc[0, "side"] == expected


# ---------------------------------------------------------------------------
# Numeric casting
# ---------------------------------------------------------------------------

class TestNumericCasting:
    def test_quantity_cast_to_int(self):
        df = clean(_valid_df(quantity="5000"))
        assert pd.api.types.is_integer_dtype(df["quantity"])
        assert df.loc[0, "quantity"] == 5000

    def test_price_cast_to_float(self):
        df = clean(_valid_df(price="213.45"))
        assert pd.api.types.is_float_dtype(df["price"])
        assert df.loc[0, "price"] == pytest.approx(213.45)

    def test_missing_price_is_null(self):
        df = clean(_valid_df(price=""))
        assert pd.isna(df.loc[0, "price"])


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

class TestDateParsing:
    def test_trade_date_parsed_to_date(self):
        df = clean(_valid_df())
        assert isinstance(df.loc[0, "trade_date"], datetime.date)
        assert df.loc[0, "trade_date"] == datetime.date(2026, 3, 28)

    def test_settlement_date_parsed_to_date(self):
        df = clean(_valid_df())
        assert isinstance(df.loc[0, "settlement_date"], datetime.date)
        assert df.loc[0, "settlement_date"] == datetime.date(2026, 4, 1)

    def test_missing_settlement_date_is_null(self):
        df = clean(_valid_df(settlement_date=""))
        assert pd.isna(df.loc[0, "settlement_date"])


# ---------------------------------------------------------------------------
# Derived fields
# ---------------------------------------------------------------------------

class TestDerivedFields:
    def test_notional_computed(self):
        df = clean(_valid_df(quantity="5000", price="213.45"))
        assert df.loc[0, "notional"] == pytest.approx(5000 * 213.45)

    def test_notional_null_when_price_missing(self):
        df = clean(_valid_df(price=""))
        assert pd.isna(df.loc[0, "notional"])

    def test_ingested_at_column_added(self):
        df = clean(_valid_df())
        assert "ingested_at" in df.columns
        assert df.loc[0, "ingested_at"] != ""
