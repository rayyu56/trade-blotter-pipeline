"""Silver layer: persist validation outputs to Parquet files.

Writes three datasets to data/silver/ after each pipeline run:

  valid_trades.parquet   — cleaned, type-cast rows ready for gold
  rejected_trades.parquet — raw rows that failed hard validation rules,
                            with _validation_issues column explaining why
  warnings.parquet       — valid rows with non-fatal data quality issues
                            (missing broker, missing settlement_date, etc.)
                            also carries _validation_issues for traceability
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from trade_blotter.utils.logger import get_logger

logger = get_logger(__name__)

# Dataset names — used as Parquet file stems.
VALID_DATASET = "valid_trades"
REJECTED_DATASET = "rejected_trades"
WARNINGS_DATASET = "warnings"


def write_silver_outputs(
    valid: pd.DataFrame,
    rejected: pd.DataFrame,
    warnings: pd.DataFrame,
    output_dir: str | Path,
) -> dict[str, Path]:
    """Write valid, rejected, and warnings DataFrames to Parquet files.

    Args:
        valid:      Cleaned DataFrame from cleaner.clean().
        rejected:   Rows that failed hard validation rules.
        warnings:   Valid rows with non-fatal data quality issues.
        output_dir: Directory to write into (data/silver/).

    Returns:
        Mapping of dataset name → absolute Path of the written file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    paths: dict[str, Path] = {}
    for name, df in [
        (VALID_DATASET, valid),
        (REJECTED_DATASET, rejected),
        (WARNINGS_DATASET, warnings),
    ]:
        path = output_dir / f"{name}.parquet"
        df.to_parquet(path, index=False)
        logger.info("[Silver] Wrote %d rows to %s", len(df), path)
        paths[name] = path

    return paths
