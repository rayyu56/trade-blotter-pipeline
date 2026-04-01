"""Data models and schemas for trade blotter records."""

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd


# Expected columns and their pandas dtype for bronze ingestion.
# All loaded as object (string) at the bronze layer — type coercion happens in silver.
BRONZE_SCHEMA: dict[str, str] = {
    "trade_id":        "object",
    "trade_date":      "object",
    "settlement_date": "object",
    "trader":          "object",
    "counterparty":    "object",
    "asset_class":     "object",
    "symbol":          "object",
    "side":            "object",
    "quantity":        "object",
    "price":           "object",
    "currency":        "object",
    "broker":          "object",
    "status":          "object",
}

REQUIRED_COLUMNS: list[str] = list(BRONZE_SCHEMA.keys())


@dataclass(frozen=True)
class IngestResult:
    """Outcome of a single bronze ingestion run."""
    source: str
    row_count: int
    columns: list[str]
    dataframe: pd.DataFrame

    # Excluded from frozen dataclass comparison — mutable type
    _df_field: ClassVar = "dataframe"
