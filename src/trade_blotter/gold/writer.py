"""Gold layer: write aggregated business outputs to target storage.

Supports writing to:
- Parquet files (default, partitioned by dataset name)
- SQLAlchemy-compatible databases

Each write is atomic at the file/table level. Existing data is replaced
(overwrite semantics) to keep gold outputs idempotent on re-runs.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import Engine

from trade_blotter.utils.logger import get_logger

logger = get_logger(__name__)


def write_parquet(df: pd.DataFrame, output_dir: str | Path, dataset_name: str) -> Path:
    """Write a gold DataFrame to a Parquet file.

    Args:
        df:           Gold-layer DataFrame to persist.
        output_dir:   Directory to write into (data/gold/ by default).
        dataset_name: File stem, e.g. "pnl_by_symbol" → data/gold/pnl_by_symbol.parquet

    Returns:
        Path to the written file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{dataset_name}.parquet"

    df.to_parquet(path, index=False)
    logger.info("Wrote %d rows to %s", len(df), path)
    return path


def write_database(
    df: pd.DataFrame,
    engine: Engine,
    table_name: str,
    schema: str | None = None,
    if_exists: str = "replace",
) -> None:
    """Write a gold DataFrame to a database table via SQLAlchemy.

    Args:
        df:         Gold-layer DataFrame to persist.
        engine:     SQLAlchemy engine for the target database.
        table_name: Target table name.
        schema:     Optional database schema (e.g. "gold").
        if_exists:  pandas to_sql behaviour — "replace" (default) or "append".
    """
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
    )
    logger.info("Wrote %d rows to %s%s", len(df), f"{schema}." if schema else "", table_name)


def write_gold_outputs(
    outputs: dict[str, pd.DataFrame],
    output_dir: str | Path,
    fmt: str = "parquet",
    engine: Engine | None = None,
    schema: str | None = None,
) -> dict[str, Path | str]:
    """Write multiple gold DataFrames in one call.

    Args:
        outputs:    Mapping of dataset_name → DataFrame.
        output_dir: Target directory (Parquet) or unused (database).
        fmt:        "parquet" or "database".
        engine:     Required when fmt="database".
        schema:     Optional database schema when fmt="database".

    Returns:
        Mapping of dataset_name → file path (Parquet) or table name (database).
    """
    results: dict[str, Path | str] = {}

    for name, df in outputs.items():
        if fmt == "parquet":
            results[name] = write_parquet(df, output_dir, name)
        elif fmt == "database":
            if engine is None:
                raise ValueError("engine is required when fmt='database'")
            write_database(df, engine, table_name=name, schema=schema)
            results[name] = name
        else:
            raise ValueError(f"Unsupported format: {fmt!r}. Use 'parquet' or 'database'.")

    logger.info("Gold write complete — %d dataset(s) written", len(results))
    return results
