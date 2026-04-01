"""Tests for the silver validation layer."""

import pandas as pd
import pytest

from trade_blotter.silver.validator import validate
from trade_blotter.models.trade import ValidationResult


def _make_row(**overrides) -> dict:
    """Return a valid bronze row, with optional field overrides."""
    base = {
        "trade_id": "TRD-001",
        "trade_date": "2026-03-28",
        "settlement_date": "2026-04-01",
        "trader": "Sarah Chen",
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
    base.update(overrides)
    return base


def _df(*rows) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


# ---------------------------------------------------------------------------
# Return type
# ---------------------------------------------------------------------------

class TestReturnType:
    def test_returns_validation_result(self):
        result = validate(_df(_make_row()))
        assert isinstance(result, ValidationResult)

    def test_valid_row_passes(self):
        result = validate(_df(_make_row()))
        assert result.valid_count == 1
        assert result.rejected_count == 0


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------

class TestDuplicates:
    def test_exact_duplicate_row_is_dropped(self):
        row = _make_row()
        result = validate(_df(row, row))
        # Only one row survives — the exact duplicate is silently dropped before
        # trade_id deduplication, so we get 1 valid row with no rejection.
        assert result.valid_count == 1
        assert result.rejected_count == 0

    def test_duplicate_trade_id_different_data_is_rejected(self):
        r1 = _make_row(trade_id="TRD-001", price="213.45")
        r2 = _make_row(trade_id="TRD-001", price="215.00")
        result = validate(_df(r1, r2))
        assert result.valid_count == 1
        assert result.rejected_count == 1
        assert "duplicate_trade_id" in result.rejected.iloc[0]["_validation_issues"]


# ---------------------------------------------------------------------------
# Required field checks
# ---------------------------------------------------------------------------

class TestRequiredFields:
    @pytest.mark.parametrize("field", [
        "trade_id", "trade_date", "trader", "counterparty",
        "symbol", "quantity", "price", "currency",
    ])
    def test_missing_required_field_rejects_row(self, field):
        result = validate(_df(_make_row(**{field: ""})))
        assert result.rejected_count == 1
        assert f"missing_{field}" in result.rejected.iloc[0]["_validation_issues"]

    def test_valid_row_with_missing_broker_is_warned_not_rejected(self):
        result = validate(_df(_make_row(broker="")))
        assert result.rejected_count == 0
        assert result.warning_count == 1

    def test_valid_row_with_missing_settlement_date_is_warned(self):
        result = validate(_df(_make_row(settlement_date="")))
        assert result.rejected_count == 0
        assert result.warning_count == 1


# ---------------------------------------------------------------------------
# Side validation
# ---------------------------------------------------------------------------

class TestSideValidation:
    @pytest.mark.parametrize("side", ["Buy", "Sell"])
    def test_valid_sides_pass(self, side):
        result = validate(_df(_make_row(side=side)))
        assert result.valid_count == 1

    @pytest.mark.parametrize("side", ["buy", "SELL", "BUY", "sell"])
    def test_non_title_case_sides_are_rejected(self, side):
        result = validate(_df(_make_row(side=side)))
        assert result.rejected_count == 1
        assert "invalid_side" in result.rejected.iloc[0]["_validation_issues"]

    def test_gibberish_side_is_rejected(self):
        result = validate(_df(_make_row(side="SHORT")))
        assert result.rejected_count == 1


# ---------------------------------------------------------------------------
# Numeric validation
# ---------------------------------------------------------------------------

class TestNumericValidation:
    def test_non_numeric_quantity_rejected(self):
        result = validate(_df(_make_row(quantity="abc")))
        assert result.rejected_count == 1
        assert "non_numeric_quantity" in result.rejected.iloc[0]["_validation_issues"]

    def test_non_numeric_price_rejected(self):
        result = validate(_df(_make_row(price="N/A")))
        assert result.rejected_count == 1
        assert "non_numeric_price" in result.rejected.iloc[0]["_validation_issues"]

    def test_negative_quantity_rejected(self):
        result = validate(_df(_make_row(quantity="-500")))
        assert result.rejected_count == 1
        assert "non_positive_quantity" in result.rejected.iloc[0]["_validation_issues"]

    def test_zero_quantity_rejected(self):
        result = validate(_df(_make_row(quantity="0")))
        assert result.rejected_count == 1

    def test_valid_numeric_values_pass(self):
        result = validate(_df(_make_row(quantity="1000000", price="0.6321")))
        assert result.valid_count == 1


# ---------------------------------------------------------------------------
# Date validation
# ---------------------------------------------------------------------------

class TestDateValidation:
    def test_settlement_before_trade_date_rejected(self):
        result = validate(_df(_make_row(trade_date="2026-04-01", settlement_date="2026-03-28")))
        assert result.rejected_count == 1
        assert "settlement_before_trade_date" in result.rejected.iloc[0]["_validation_issues"]

    def test_same_day_settlement_passes(self):
        result = validate(_df(_make_row(trade_date="2026-04-01", settlement_date="2026-04-01")))
        assert result.valid_count == 1


# ---------------------------------------------------------------------------
# Summary errors list
# ---------------------------------------------------------------------------

class TestSummary:
    def test_errors_list_populated_on_rejections(self):
        result = validate(_df(_make_row(quantity="-100")))
        assert any("non_positive_quantity" in e for e in result.errors)

    def test_errors_list_empty_on_clean_data(self):
        result = validate(_df(_make_row()))
        assert result.errors == []
