"""Data models and schemas for trade blotter records."""

from dataclasses import dataclass, field
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

# Fields that must be non-empty for a trade to be valid.
REQUIRED_FIELDS: list[str] = [
    "trade_id", "trade_date", "trader", "counterparty",
    "asset_class", "symbol", "side", "quantity", "price", "currency",
]

# Canonical side values after normalisation.
VALID_SIDES: set[str] = {"Buy", "Sell"}

# Canonical asset classes.
VALID_ASSET_CLASSES: set[str] = {"Equity", "FX", "Fixed Income"}


@dataclass(frozen=True)
class IngestResult:
    """Outcome of a single bronze ingestion run."""
    source: str
    row_count: int
    columns: list[str]
    dataframe: pd.DataFrame

    # Excluded from frozen dataclass comparison — mutable type
    _df_field: ClassVar = "dataframe"


@dataclass
class ValidationResult:
    """Outcome of the silver validation step."""
    valid: pd.DataFrame
    rejected: pd.DataFrame          # rows that failed hard rules
    warnings: pd.DataFrame          # rows with non-fatal issues (e.g. missing broker)
    errors: list[str] = field(default_factory=list)   # summary messages

    @property
    def valid_count(self) -> int:
        return len(self.valid)

    @property
    def rejected_count(self) -> int:
        return len(self.rejected)

    @property
    def warning_count(self) -> int:
        return len(self.warnings)
