"""Silver layer – validation: enforce schema and business rules on bronze data.

Hard failures (row rejected):
- Duplicate trade_id
- Missing required field (trade_id, trade_date, trader, counterparty,
  asset_class, symbol, side, quantity, price, currency)
- Non-numeric quantity or price
- Negative or zero quantity
- Unrecognised side value (after normalisation to title-case)
- settlement_date before trade_date

Soft warnings (row flagged but kept):
- Missing broker
- Missing settlement_date
- Unrecognised asset_class
"""

from __future__ import annotations

import pandas as pd

from trade_blotter.models.trade import (
    REQUIRED_FIELDS,
    VALID_ASSET_CLASSES,
    VALID_SIDES,
    ValidationResult,
)
from trade_blotter.utils.logger import get_logger

logger = get_logger(__name__)

# Internal column added to carry rejection/warning reasons through the pipeline.
_REASON_COL = "_validation_issues"


def validate(df: pd.DataFrame) -> ValidationResult:
    """Validate a bronze DataFrame and split it into valid, rejected, and warned rows.

    Args:
        df: Raw bronze DataFrame with all columns as strings.

    Returns:
        ValidationResult with valid, rejected, and warnings DataFrames.
    """
    df = df.copy()
    df[_REASON_COL] = ""

    df = _flag_exact_duplicates(df)
    df = _flag_duplicate_trade_ids(df)
    df = _flag_missing_required_fields(df)
    df = _flag_invalid_side(df)
    df = _flag_non_numeric(df)
    df = _flag_non_positive_quantity(df)
    df = _flag_settlement_before_trade(df)

    warnings_mask = _build_warnings_mask(df)
    rejected_mask = df[_REASON_COL].str.contains("REJECT", na=False)

    # Rows are valid if they have no rejection reasons; warnings are separate.
    valid_mask = ~rejected_mask
    valid = df[valid_mask].drop(columns=[_REASON_COL]).reset_index(drop=True)
    rejected = df[rejected_mask].reset_index(drop=True)
    warnings = df[warnings_mask & ~rejected_mask].reset_index(drop=True)

    errors = _summarise(df)
    _log_summary(valid, rejected, warnings, errors)

    return ValidationResult(valid=valid, rejected=rejected, warnings=warnings, errors=errors)


# ---------------------------------------------------------------------------
# Rule implementations
# ---------------------------------------------------------------------------

def _flag_exact_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Flag exact duplicate rows for rejection rather than silently dropping them.

    Keeping duplicates in the rejected set ensures every bronze row is
    accounted for in silver and the reconciler never sees an unaccounted row.
    The first occurrence is kept; subsequent exact copies are rejected.
    """
    dupes = df.duplicated(keep="first")
    _append_reason(df, dupes, "REJECT:exact_duplicate")
    if dupes.any():
        logger.warning(
            "Found %d exact duplicate row(s) — flagged for rejection (previously silently dropped)",
            dupes.sum(),
        )
    return df


def _flag_duplicate_trade_ids(df: pd.DataFrame) -> pd.DataFrame:
    dupes = df["trade_id"].duplicated(keep="first")
    _append_reason(df, dupes, "REJECT:duplicate_trade_id")
    if dupes.any():
        ids = df.loc[dupes, "trade_id"].tolist()
        logger.warning("Duplicate trade_id(s) (keeping first): %s", ids)
    return df


def _flag_missing_required_fields(df: pd.DataFrame) -> pd.DataFrame:
    for col in REQUIRED_FIELDS:
        if col not in df.columns:
            logger.warning("Column '%s' not present in DataFrame — all rows flagged", col)
            df[_REASON_COL] += f"|REJECT:missing_column_{col}"
            continue
        missing = df[col].str.strip() == ""
        _append_reason(df, missing, f"REJECT:missing_{col}")
    return df


def _flag_invalid_side(df: pd.DataFrame) -> pd.DataFrame:
    if "side" not in df.columns:
        return df
    # Check raw stripped value — case must be exact (Buy / Sell).
    # cleaner.py normalises side only for rows that pass validation.
    stripped = df["side"].str.strip()
    invalid = ~stripped.isin(VALID_SIDES) & (stripped != "")
    _append_reason(df, invalid, "REJECT:invalid_side")
    return df


def _flag_non_numeric(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("quantity", "price"):
        if col not in df.columns:
            continue
        non_empty = df[col].str.strip() != ""
        numeric = pd.to_numeric(df[col].str.strip(), errors="coerce").notna()
        bad = non_empty & ~numeric
        _append_reason(df, bad, f"REJECT:non_numeric_{col}")
    return df


def _flag_non_positive_quantity(df: pd.DataFrame) -> pd.DataFrame:
    if "quantity" not in df.columns:
        return df
    numeric_qty = pd.to_numeric(df["quantity"].str.strip(), errors="coerce")
    bad = numeric_qty.notna() & (numeric_qty <= 0)
    _append_reason(df, bad, "REJECT:non_positive_quantity")
    return df


def _flag_settlement_before_trade(df: pd.DataFrame) -> pd.DataFrame:
    if "trade_date" not in df.columns or "settlement_date" not in df.columns:
        return df
    trade_dt = pd.to_datetime(df["trade_date"].str.strip(), errors="coerce")
    settle_dt = pd.to_datetime(df["settlement_date"].str.strip(), errors="coerce")
    both_present = trade_dt.notna() & settle_dt.notna()
    bad = both_present & (settle_dt < trade_dt)
    _append_reason(df, bad, "REJECT:settlement_before_trade_date")
    return df


def _build_warnings_mask(df: pd.DataFrame) -> pd.Series:
    """Flag rows with non-fatal data quality issues."""
    mask = pd.Series(False, index=df.index)

    if "broker" in df.columns:
        mask |= df["broker"].str.strip() == ""

    if "settlement_date" in df.columns:
        mask |= df["settlement_date"].str.strip() == ""

    if "asset_class" in df.columns:
        non_empty = df["asset_class"].str.strip() != ""
        mask |= non_empty & ~df["asset_class"].str.strip().isin(VALID_ASSET_CLASSES)

    return mask


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _append_reason(df: pd.DataFrame, mask: pd.Series, reason: str) -> None:
    df.loc[mask, _REASON_COL] = df.loc[mask, _REASON_COL] + f"|{reason}"


def _summarise(df: pd.DataFrame) -> list[str]:
    reasons: list[str] = []
    all_reasons = df[_REASON_COL].str.cat(sep="|")
    for tag in set(r for r in all_reasons.split("|") if r.startswith("REJECT:")):
        count = df[_REASON_COL].str.contains(tag, regex=False).sum()
        reasons.append(f"{tag}: {count} row(s)")
    return sorted(reasons)


def _log_summary(valid, rejected, warnings, errors) -> None:
    logger.info(
        "Validation complete — valid: %d, rejected: %d, warnings: %d",
        len(valid), len(rejected), len(warnings),
    )
    for e in errors:
        logger.warning("  %s", e)
