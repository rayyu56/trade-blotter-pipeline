"""Tests for the gold positions layer."""

import datetime

import pandas as pd
import pytest

from trade_blotter.gold.positions import (
    compute_positions,
    compute_net_positions,
    compute_position_snapshot,
)


def _silver_df(rows: list[dict]) -> pd.DataFrame:
    defaults = {
        "trade_id": "TRD-001",
        "trade_date": datetime.date(2026, 3, 28),
        "trader": "Sarah Chen",
        "asset_class": "Equity",
        "symbol": "AAPL",
        "side": "Buy",
        "quantity": 1000,
        "price": 213.45,
        "currency": "USD",
        "notional": 213_450.0,
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


# ---------------------------------------------------------------------------
# compute_positions
# ---------------------------------------------------------------------------

class TestComputePositions:
    def test_net_long_position(self):
        df = _silver_df([
            {"side": "Buy",  "quantity": 1000},
            {"side": "Buy",  "quantity": 500,  "trade_id": "TRD-002"},
        ])
        result = compute_positions(df)
        assert result.loc[0, "net_position"] == 1500

    def test_net_short_after_sell(self):
        df = _silver_df([
            {"side": "Buy",  "quantity": 500},
            {"side": "Sell", "quantity": 800, "trade_id": "TRD-002"},
        ])
        result = compute_positions(df)
        assert result.loc[0, "net_position"] == -300

    def test_flat_position_when_buy_equals_sell(self):
        df = _silver_df([
            {"side": "Buy",  "quantity": 1000},
            {"side": "Sell", "quantity": 1000, "trade_id": "TRD-002"},
        ])
        result = compute_positions(df)
        assert result.loc[0, "net_position"] == 0

    def test_separate_rows_per_trader(self):
        df = _silver_df([
            {"trader": "Alice", "side": "Buy", "quantity": 100},
            {"trader": "Bob",   "side": "Buy", "quantity": 200, "trade_id": "TRD-002"},
        ])
        result = compute_positions(df)
        assert len(result) == 2

    def test_avg_buy_price_is_quantity_weighted(self):
        df = _silver_df([
            {"side": "Buy", "quantity": 100, "price": 100.0, "notional": 10_000.0},
            {"side": "Buy", "quantity": 200, "price": 200.0, "notional": 40_000.0,
             "trade_id": "TRD-002"},
        ])
        result = compute_positions(df)
        # Weighted avg = (100*100 + 200*200) / 300 = 50000/300 ≈ 166.67
        assert result.loc[0, "avg_buy_price"] == pytest.approx(50_000 / 300)

    def test_required_columns_present(self):
        df = _silver_df([{}])
        result = compute_positions(df)
        for col in ["symbol", "asset_class", "currency", "trader",
                    "buy_qty", "sell_qty", "net_position",
                    "avg_buy_price", "avg_sell_price", "trade_count"]:
            assert col in result.columns

    def test_buy_only_has_null_avg_sell_price(self):
        df = _silver_df([{"side": "Buy", "quantity": 500}])
        result = compute_positions(df)
        assert pd.isna(result.loc[0, "avg_sell_price"])


# ---------------------------------------------------------------------------
# compute_net_positions
# ---------------------------------------------------------------------------

class TestComputeNetPositions:
    def test_collapses_traders(self):
        df = _silver_df([
            {"trader": "Alice", "side": "Buy",  "quantity": 300},
            {"trader": "Bob",   "side": "Buy",  "quantity": 200, "trade_id": "TRD-002"},
            {"trader": "Alice", "side": "Sell", "quantity": 100, "trade_id": "TRD-003"},
        ])
        result = compute_net_positions(df)
        assert len(result) == 1
        assert result.loc[0, "net_position"] == 400

    def test_separate_rows_per_symbol(self):
        df = _silver_df([
            {"symbol": "AAPL", "side": "Buy", "quantity": 100},
            {"symbol": "MSFT", "side": "Buy", "quantity": 200, "trade_id": "TRD-002"},
        ])
        result = compute_net_positions(df)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# compute_position_snapshot
# ---------------------------------------------------------------------------

class TestComputePositionSnapshot:
    def test_as_of_date_column_added(self):
        df = _silver_df([{}])
        result = compute_position_snapshot(df)
        assert "as_of_date" in result.columns

    def test_explicit_as_of_date(self):
        df = _silver_df([{}])
        snap_date = datetime.date(2026, 4, 1)
        result = compute_position_snapshot(df, as_of_date=snap_date)
        assert result.loc[0, "as_of_date"] == snap_date

    def test_defaults_to_latest_trade_date(self):
        df = _silver_df([
            {"trade_date": datetime.date(2026, 3, 28)},
            {"trade_date": datetime.date(2026, 4, 1), "trade_id": "TRD-002",
             "side": "Sell", "quantity": 100},
        ])
        result = compute_position_snapshot(df)
        assert result.loc[0, "as_of_date"] == datetime.date(2026, 4, 1)
