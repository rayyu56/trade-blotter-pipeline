"""Tests for the gold P&L aggregation layer."""

import datetime

import pandas as pd
import pytest

from trade_blotter.gold.pnl import (
    compute_trade_pnl,
    compute_pnl_by_symbol,
    compute_pnl_by_trader,
    compute_pnl_summary,
)


def _silver_df(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal cleaned silver DataFrame."""
    defaults = {
        "trade_id": "TRD-001",
        "trade_date": datetime.date(2026, 3, 28),
        "settlement_date": datetime.date(2026, 4, 1),
        "trader": "Sarah Chen",
        "counterparty": "Goldman Sachs",
        "asset_class": "Equity",
        "symbol": "AAPL",
        "side": "Buy",
        "quantity": 5000,
        "price": 213.45,
        "currency": "USD",
        "broker": "ITG",
        "status": "Confirmed",
        "notional": 5000 * 213.45,
        "ingested_at": "2026-04-01T00:00:00+00:00",
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


# ---------------------------------------------------------------------------
# compute_trade_pnl
# ---------------------------------------------------------------------------

class TestComputeTradePnl:
    def test_buy_gross_pnl_is_positive(self):
        df = _silver_df([{"side": "Buy", "notional": 10_000.0}])
        result = compute_trade_pnl(df)
        assert result.loc[0, "gross_pnl"] == pytest.approx(10_000.0)

    def test_sell_gross_pnl_is_negative(self):
        df = _silver_df([{"side": "Sell", "notional": 10_000.0}])
        result = compute_trade_pnl(df)
        assert result.loc[0, "gross_pnl"] == pytest.approx(-10_000.0)

    def test_gross_pnl_column_added(self):
        df = _silver_df([{}])
        result = compute_trade_pnl(df)
        assert "gross_pnl" in result.columns

    def test_original_df_not_mutated(self):
        df = _silver_df([{}])
        compute_trade_pnl(df)
        assert "gross_pnl" not in df.columns


# ---------------------------------------------------------------------------
# compute_pnl_by_symbol
# ---------------------------------------------------------------------------

class TestComputePnlBySymbol:
    def test_returns_one_row_per_symbol(self):
        df = _silver_df([
            {"symbol": "AAPL", "side": "Buy",  "notional": 100.0},
            {"symbol": "AAPL", "side": "Sell", "notional": 120.0, "trade_id": "TRD-002"},
            {"symbol": "MSFT", "side": "Buy",  "notional": 200.0, "trade_id": "TRD-003"},
        ])
        result = compute_pnl_by_symbol(df)
        assert set(result["symbol"]) == {"AAPL", "MSFT"}

    def test_net_pnl_is_sell_minus_buy(self):
        df = _silver_df([
            {"symbol": "AAPL", "side": "Buy",  "notional": 100.0},
            {"symbol": "AAPL", "side": "Sell", "notional": 150.0, "trade_id": "TRD-002"},
        ])
        result = compute_pnl_by_symbol(df)
        row = result[result["symbol"] == "AAPL"].iloc[0]
        assert row["net_pnl"] == pytest.approx(50.0)

    def test_buy_only_symbol_has_zero_sell_notional(self):
        df = _silver_df([{"side": "Buy", "notional": 500.0}])
        result = compute_pnl_by_symbol(df)
        assert result.loc[0, "sell_notional"] == pytest.approx(0.0)

    def test_trade_count_correct(self):
        df = _silver_df([
            {"side": "Buy", "notional": 100.0},
            {"side": "Buy", "notional": 200.0, "trade_id": "TRD-002"},
        ])
        result = compute_pnl_by_symbol(df)
        assert result.loc[0, "trade_count"] == 2

    def test_required_columns_present(self):
        df = _silver_df([{}])
        result = compute_pnl_by_symbol(df)
        for col in ["symbol", "asset_class", "currency", "buy_notional",
                    "sell_notional", "total_notional", "net_pnl", "trade_count"]:
            assert col in result.columns


# ---------------------------------------------------------------------------
# compute_pnl_by_trader
# ---------------------------------------------------------------------------

class TestComputePnlByTrader:
    def test_returns_one_row_per_trader(self):
        df = _silver_df([
            {"trader": "Alice", "side": "Buy",  "notional": 100.0},
            {"trader": "Bob",   "side": "Sell", "notional": 200.0, "trade_id": "TRD-002"},
        ])
        result = compute_pnl_by_trader(df)
        assert set(result["trader"]) == {"Alice", "Bob"}

    def test_buy_trader_net_pnl_is_positive(self):
        df = _silver_df([{"trader": "Alice", "side": "Buy", "notional": 500.0}])
        result = compute_pnl_by_trader(df)
        assert result.loc[0, "net_pnl"] == pytest.approx(500.0)

    def test_sell_trader_net_pnl_is_negative(self):
        df = _silver_df([{"trader": "Bob", "side": "Sell", "notional": 500.0,
                          "trade_id": "TRD-002"}])
        result = compute_pnl_by_trader(df)
        assert result.loc[0, "net_pnl"] == pytest.approx(-500.0)


# ---------------------------------------------------------------------------
# compute_pnl_summary
# ---------------------------------------------------------------------------

class TestComputePnlSummary:
    def test_groups_by_asset_class_and_currency(self):
        df = _silver_df([
            {"asset_class": "Equity",       "currency": "USD", "side": "Buy",  "notional": 100.0},
            {"asset_class": "Fixed Income", "currency": "USD", "side": "Sell", "notional": 200.0,
             "trade_id": "TRD-002"},
        ])
        result = compute_pnl_summary(df)
        assert set(result["asset_class"]) == {"Equity", "Fixed Income"}

    def test_trade_count_per_group(self):
        df = _silver_df([
            {"asset_class": "FX", "side": "Buy",  "notional": 100.0},
            {"asset_class": "FX", "side": "Sell", "notional": 80.0, "trade_id": "TRD-002"},
        ])
        result = compute_pnl_summary(df)
        assert result.loc[0, "trade_count"] == 2
