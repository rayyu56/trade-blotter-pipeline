"""Gold layer: compute P&L from silver trade data.

Terminology
-----------
- Gross P&L per trade  = signed_notional = notional * direction
  where direction = +1 for Buy (cash out, position in), -1 for Sell (cash in)
- Realised P&L per symbol = sum of signed_notional across all trades
  A positive value means net cash outflow (long position cost basis).
  A negative value means net cash inflow (realised proceeds exceed cost).
- P&L by trader / desk / asset_class are simple groupby aggregations
  of trade-level gross P&L.

Note: this model assumes a blotter-level mark-to-notional approach.
True realised/unrealised P&L requires a separate market-data feed
(current prices) which is outside the scope of this pipeline.

Output columns
--------------
compute_trade_pnl()     → one row per trade, adds gross_pnl
compute_pnl_by_symbol() → symbol, asset_class, currency, total_notional,
                          buy_notional, sell_notional, net_pnl, trade_count
compute_pnl_by_trader() → trader, total_notional, net_pnl, trade_count
compute_pnl_summary()   → asset_class, currency, net_pnl, trade_count
"""

from __future__ import annotations

import pandas as pd

from trade_blotter.utils.logger import get_logger

logger = get_logger(__name__)

_DIRECTION = {"Buy": 1, "Sell": -1}


def compute_trade_pnl(df: pd.DataFrame) -> pd.DataFrame:
    """Add a gross_pnl column to a cleaned silver DataFrame.

    gross_pnl = notional * direction  (+ve = Buy cost, -ve = Sell proceeds)

    Args:
        df: Cleaned silver DataFrame with notional (float) and side columns.

    Returns:
        DataFrame with a new gross_pnl column.
    """
    df = df.copy()
    direction = df["side"].map(_DIRECTION)
    df["gross_pnl"] = df["notional"] * direction
    logger.info("Computed trade-level gross P&L for %d rows", len(df))
    return df


def compute_pnl_by_symbol(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate P&L by symbol.

    Returns one row per (symbol, asset_class, currency) with:
    - buy_notional:  total notional of Buy trades
    - sell_notional: total notional of Sell trades (positive value)
    - net_pnl:       sell_notional - buy_notional
                     (positive = net cash inflow / realised gain)
    - trade_count:   number of trades
    """
    df = compute_trade_pnl(df)

    buy = (
        df[df["side"] == "Buy"]
        .groupby(["symbol", "asset_class", "currency"])["notional"]
        .sum()
        .rename("buy_notional")
    )
    sell = (
        df[df["side"] == "Sell"]
        .groupby(["symbol", "asset_class", "currency"])["notional"]
        .sum()
        .rename("sell_notional")
    )
    count = (
        df.groupby(["symbol", "asset_class", "currency"])["trade_id"]
        .count()
        .rename("trade_count")
    )
    total = (
        df.groupby(["symbol", "asset_class", "currency"])["notional"]
        .sum()
        .rename("total_notional")
    )

    result = (
        pd.concat([buy, sell, total, count], axis=1)
        .fillna(0)
        .reset_index()
    )
    result["net_pnl"] = result["sell_notional"] - result["buy_notional"]
    result["trade_count"] = result["trade_count"].astype(int)

    logger.info("P&L by symbol: %d symbols", len(result))
    return result[["symbol", "asset_class", "currency",
                   "buy_notional", "sell_notional", "total_notional",
                   "net_pnl", "trade_count"]]


def compute_pnl_by_trader(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate gross P&L by trader.

    Returns one row per trader with total_notional, net_pnl, trade_count.
    """
    df = compute_trade_pnl(df)
    result = (
        df.groupby("trader")
        .agg(
            total_notional=("notional", "sum"),
            net_pnl=("gross_pnl", "sum"),
            trade_count=("trade_id", "count"),
        )
        .reset_index()
    )
    result["trade_count"] = result["trade_count"].astype(int)
    logger.info("P&L by trader: %d traders", len(result))
    return result


def compute_pnl_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate gross P&L by asset class and currency.

    Useful as a top-level dashboard summary across the blotter.
    """
    df = compute_trade_pnl(df)
    result = (
        df.groupby(["asset_class", "currency"])
        .agg(
            total_notional=("notional", "sum"),
            net_pnl=("gross_pnl", "sum"),
            trade_count=("trade_id", "count"),
        )
        .reset_index()
    )
    result["trade_count"] = result["trade_count"].astype(int)
    logger.info("P&L summary: %d asset-class/currency groups", len(result))
    return result
