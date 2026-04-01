"""Gold layer: compute net positions from silver trade data.

Position model
--------------
- signed_quantity = quantity * direction
  where direction = +1 for Buy, -1 for Sell
- net_position per symbol = sum(signed_quantity)
  Positive = net long, Negative = net short

Output columns
--------------
compute_positions()          → one row per (symbol, asset_class, currency, trader)
                               with buy_qty, sell_qty, net_position, trade_count,
                               avg_buy_price, avg_sell_price
compute_net_positions()      → one row per (symbol, asset_class, currency)
                               collapsed across all traders
compute_position_snapshot()  → net_positions enriched with as_of_date
"""

from __future__ import annotations

import datetime

import pandas as pd

from trade_blotter.utils.logger import get_logger

logger = get_logger(__name__)

_DIRECTION = {"Buy": 1, "Sell": -1}


def compute_positions(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-trader positions by symbol.

    Returns one row per (symbol, asset_class, currency, trader) with:
    - buy_qty / sell_qty:    total quantities on each side
    - net_position:          buy_qty - sell_qty  (+ve = long, -ve = short)
    - avg_buy_price:         quantity-weighted average buy price
    - avg_sell_price:        quantity-weighted average sell price
    - trade_count:           number of trades
    """
    df = df.copy()
    df["signed_qty"] = df["quantity"] * df["side"].map(_DIRECTION)

    keys = ["symbol", "asset_class", "currency", "trader"]

    buy_df = df[df["side"] == "Buy"]
    sell_df = df[df["side"] == "Sell"]

    buy_qty = buy_df.groupby(keys)["quantity"].sum().rename("buy_qty")
    sell_qty = sell_df.groupby(keys)["quantity"].sum().rename("sell_qty")
    trade_count = df.groupby(keys)["trade_id"].count().rename("trade_count")
    net_pos = df.groupby(keys)["signed_qty"].sum().rename("net_position")

    # Quantity-weighted average prices
    buy_df = buy_df.copy()
    sell_df = sell_df.copy()
    buy_df["notional_qty"] = buy_df["quantity"] * buy_df["price"]
    sell_df["notional_qty"] = sell_df["quantity"] * sell_df["price"]

    avg_buy = (
        buy_df.groupby(keys)["notional_qty"].sum()
        / buy_df.groupby(keys)["quantity"].sum()
    ).rename("avg_buy_price")

    avg_sell = (
        sell_df.groupby(keys)["notional_qty"].sum()
        / sell_df.groupby(keys)["quantity"].sum()
    ).rename("avg_sell_price")

    result = (
        pd.concat([buy_qty, sell_qty, net_pos, avg_buy, avg_sell, trade_count], axis=1)
        .fillna({"buy_qty": 0, "sell_qty": 0})
        .reset_index()
    )
    result["trade_count"] = result["trade_count"].astype(int)

    logger.info("Positions by trader/symbol: %d rows", len(result))
    return result[keys + ["buy_qty", "sell_qty", "net_position",
                          "avg_buy_price", "avg_sell_price", "trade_count"]]


def compute_net_positions(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse per-trader positions into firm-wide net positions per symbol."""
    per_trader = compute_positions(df)
    keys = ["symbol", "asset_class", "currency"]

    result = (
        per_trader.groupby(keys)
        .agg(
            buy_qty=("buy_qty", "sum"),
            sell_qty=("sell_qty", "sum"),
            net_position=("net_position", "sum"),
            trade_count=("trade_count", "sum"),
        )
        .reset_index()
    )
    result["trade_count"] = result["trade_count"].astype(int)
    logger.info("Net positions: %d symbols", len(result))
    return result


def compute_position_snapshot(
    df: pd.DataFrame,
    as_of_date: datetime.date | None = None,
) -> pd.DataFrame:
    """Net positions enriched with an as_of_date for point-in-time reporting.

    Args:
        df:          Cleaned silver DataFrame.
        as_of_date:  Snapshot date. Defaults to the latest trade_date in df.
    """
    if as_of_date is None:
        as_of_date = df["trade_date"].max()

    result = compute_net_positions(df)
    result["as_of_date"] = as_of_date
    logger.info("Position snapshot as of %s: %d rows", as_of_date, len(result))
    return result
