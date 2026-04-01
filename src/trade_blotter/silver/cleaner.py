"""Silver layer – cleaning: normalize and standardize validated bronze data.

Applied to the valid DataFrame produced by validator.validate():
- Normalise side to title-case  (buy/BUY → Buy, sell/SELL → Sell)
- Cast quantity to int, price to float
- Parse trade_date and settlement_date to datetime.date
- Strip leading/trailing whitespace from all string columns
- Derive notional = quantity * price
- Add ingested_at timestamp

Output is the cleaned silver DataFrame ready for gold aggregations.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from trade_blotter.utils.logger import get_logger

logger = get_logger(__name__)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize and enrich a validated bronze DataFrame.

    Args:
        df: The valid DataFrame from ValidationResult.valid.

    Returns:
        A cleaned DataFrame with correct types and derived fields.
    """
    df = df.copy()

    df = _strip_strings(df)
    df = _normalise_side(df)
    df = _cast_numerics(df)
    df = _parse_dates(df)
    df = _derive_notional(df)
    df = _add_ingested_at(df)

    logger.info("Cleaning complete — %d rows, %d columns", len(df), len(df.columns))
    return df


# ---------------------------------------------------------------------------
# Step implementations
# ---------------------------------------------------------------------------

def _strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())
    return df


def _normalise_side(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise side to title-case: Buy or Sell."""
    df["side"] = df["side"].str.title()
    return df


def _cast_numerics(df: pd.DataFrame) -> pd.DataFrame:
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    df["price"] = pd.to_numeric(df["price"], errors="coerce").astype(float)
    return df


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    if "settlement_date" in df.columns:
        df["settlement_date"] = pd.to_datetime(df["settlement_date"], errors="coerce").dt.date
    return df


def _derive_notional(df: pd.DataFrame) -> pd.DataFrame:
    """Compute notional = quantity * price. Null when either input is missing."""
    df["notional"] = df["quantity"] * df["price"]
    return df


def _add_ingested_at(df: pd.DataFrame) -> pd.DataFrame:
    df["ingested_at"] = datetime.now(tz=timezone.utc).isoformat()
    return df
