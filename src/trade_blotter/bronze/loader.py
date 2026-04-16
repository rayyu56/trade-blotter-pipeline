"""Bronze layer: ingest raw trade blotter data as-is from source files or databases.

No transformations or validation are applied here. Data is loaded into a
DataFrame and written to data/bronze/ preserving the original structure.
Supported sources: Excel (.xlsx), CSV, SQLAlchemy-compatible databases.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, text

from trade_blotter.models.trade import BRONZE_SCHEMA, REQUIRED_COLUMNS, IngestResult
from trade_blotter.utils.logger import get_logger

logger = get_logger(__name__)


def load_csv(path: str | Path) -> IngestResult:
    """Load a CSV file into a DataFrame without any transformation.

    All columns are read as strings to preserve raw values exactly as received.
    """ 
    path = Path(path)
    logger.info("Loading CSV: %s", path)

    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    _check_columns(df, source=str(path))

    logger.info("Loaded %d rows, %d columns from %s", len(df), len(df.columns), path.name)
    return IngestResult(source=str(path), row_count=len(df), columns=list(df.columns), dataframe=df)


def load_excel(path: str | Path, sheet_name: str | int = 0) -> IngestResult:
    """Load an Excel worksheet into a DataFrame without any transformation.

    All columns are read as strings to preserve raw values exactly as received.
    """
    path = Path(path)
    logger.info("Loading Excel: %s (sheet=%s)", path, sheet_name)

    df = pd.read_excel(path, sheet_name=sheet_name, dtype=str, keep_default_na=False)
    _check_columns(df, source=str(path))

    logger.info("Loaded %d rows, %d columns from %s", len(df), len(df.columns), path.name)
    return IngestResult(source=str(path), row_count=len(df), columns=list(df.columns), dataframe=df)


def load_database(engine: Engine, query: str) -> IngestResult:
    """Execute a SQL query and return results as a DataFrame without transformation.

    Args:
        engine: A SQLAlchemy engine connected to the source database.
        query:  A SELECT statement returning trade blotter records.
    """
    logger.info("Loading from database: %.120s", query)

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, dtype=str)

    _check_columns(df, source=query[:60])
    logger.info("Loaded %d rows, %d columns from database", len(df), len(df.columns))
    return IngestResult(source=query[:60], row_count=len(df), columns=list(df.columns), dataframe=df)


def load_directory(path: str | Path, pattern: str = "*.csv") -> IngestResult:
    """Load and concatenate all matching files in a directory.

    Adds a `_source_file` column to each row so the origin file is traceable.
    """
    path = Path(path)
    files = sorted(path.glob(pattern))

    if not files:
        raise FileNotFoundError(f"No files matching '{pattern}' found in {path}")

    logger.info("Found %d file(s) matching '%s' in %s", len(files), pattern, path)

    frames: list[pd.DataFrame] = []
    for f in files:
        result = load_csv(f) if f.suffix == ".csv" else load_excel(f)
        result.dataframe["_source_file"] = f.name
        frames.append(result.dataframe)

    df = pd.concat(frames, ignore_index=True)
    logger.info("Combined %d rows from %d file(s)", len(df), len(files))
    return IngestResult(source=str(path), row_count=len(df), columns=list(df.columns), dataframe=df)


def _check_columns(df: pd.DataFrame, source: str) -> None:
    """Warn if expected columns are missing from the ingested file.

    Bronze does not reject data on missing columns — it logs warnings so the
    silver validator can handle enforcement with full context.
    """
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    extra = [col for col in df.columns if col not in REQUIRED_COLUMNS and not col.startswith("_")]

    if missing:
        logger.warning("Source '%s' is missing expected columns: %s", source, missing)
    if extra:
        logger.info("Source '%s' has extra columns (will be carried through): %s", source, extra)
