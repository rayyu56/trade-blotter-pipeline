"""Top-level pipeline orchestration: bronze → silver → gold.

Entry points
------------
run(config_path)  — load config and execute the full pipeline end-to-end
run_bronze(cfg)   — ingest raw source data
run_silver(df)    — validate and clean bronze output
run_gold(df, cfg) — compute P&L / positions and write outputs

Config (pipeline.yaml)
----------------------
ingest:
  source_type: excel | csv | database
  source_path: data/bronze/

silver:
  fail_on_validation_error: true
  output_path: data/silver/

gold:
  outputs: [pnl, positions]         # which gold datasets to produce
  target_type: parquet | database
  output_path: data/gold/
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from trade_blotter.bronze.loader import load_csv, load_directory, load_excel
from trade_blotter.gold.pnl import (
    compute_pnl_by_symbol,
    compute_pnl_by_trader,
    compute_pnl_summary,
    compute_trade_pnl,
)
from trade_blotter.gold.positions import compute_net_positions, compute_position_snapshot
from trade_blotter.gold.writer import write_gold_outputs
from trade_blotter.models.trade import ValidationResult
from trade_blotter.silver.cleaner import clean
from trade_blotter.silver.validator import validate
from trade_blotter.utils.logger import get_logger

logger = get_logger(__name__)

_DEFAULT_CONFIG = Path(__file__).parent.parent.parent / "config" / "pipeline.yaml"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@dataclass
class PipelineResult:
    """Summary of a completed pipeline run."""
    source: str
    bronze_row_count: int
    silver_valid_count: int
    silver_rejected_count: int
    silver_warning_count: int
    gold_datasets: dict[str, int]        # dataset_name → row count
    validation_errors: list[str]


def run(config_path: str | Path = _DEFAULT_CONFIG) -> PipelineResult:
    """Execute the full bronze → silver → gold pipeline.

    Args:
        config_path: Path to pipeline.yaml. Defaults to config/pipeline.yaml.

    Returns:
        PipelineResult with counts and dataset summaries.

    Raises:
        SystemExit: if silver validation fails and fail_on_validation_error is true.
    """
    cfg = _load_config(config_path)
    logger.info("=== Trade Blotter Pipeline starting ===")
    logger.info("Config: %s", config_path)

    # ── Bronze ──────────────────────────────────────────────────────────────
    ingest_result = run_bronze(cfg)
    bronze_df = ingest_result.dataframe

    # ── Silver ──────────────────────────────────────────────────────────────
    validation_result, silver_df = run_silver(bronze_df, cfg)

    # ── Gold ─────────────────────────────────────────────────────────────────
    gold_counts = run_gold(silver_df, cfg)

    result = PipelineResult(
        source=ingest_result.source,
        bronze_row_count=ingest_result.row_count,
        silver_valid_count=validation_result.valid_count,
        silver_rejected_count=validation_result.rejected_count,
        silver_warning_count=validation_result.warning_count,
        gold_datasets=gold_counts,
        validation_errors=validation_result.errors,
    )
    _log_summary(result)
    logger.info("=== Pipeline complete ===")
    return result


def run_bronze(cfg: dict[str, Any]):
    """Ingest raw data according to config and return an IngestResult."""
    ingest_cfg = cfg.get("ingest", {})
    source_type = ingest_cfg.get("source_type", "csv")
    source_path = Path(ingest_cfg.get("source_path", "data/bronze/"))

    logger.info("[Bronze] source_type=%s  path=%s", source_type, source_path)

    if source_type == "csv":
        if source_path.is_dir():
            return load_directory(source_path, pattern="*.csv")
        return load_csv(source_path)

    if source_type == "excel":
        if source_path.is_dir():
            return load_directory(source_path, pattern="*.xlsx")
        return load_excel(source_path)

    raise ValueError(
        f"Unsupported source_type: {source_type!r}. Use 'csv' or 'excel'."
    )


def run_silver(
    bronze_df: pd.DataFrame,
    cfg: dict[str, Any],
) -> tuple[ValidationResult, pd.DataFrame]:
    """Validate and clean a bronze DataFrame.

    Returns (ValidationResult, cleaned_silver_df).
    Exits the process if fail_on_validation_error is true and rows were rejected.
    """
    silver_cfg = cfg.get("silver", {})
    fail_on_error = silver_cfg.get("fail_on_validation_error", True)

    logger.info("[Silver] Validating %d bronze rows", len(bronze_df))
    validation_result = validate(bronze_df)

    if validation_result.rejected_count > 0 and fail_on_error:
        logger.error(
            "[Silver] %d row(s) rejected and fail_on_validation_error=true — aborting.",
            validation_result.rejected_count,
        )
        for err in validation_result.errors:
            logger.error("  %s", err)
        sys.exit(1)

    logger.info("[Silver] Cleaning %d valid rows", validation_result.valid_count)
    silver_df = clean(validation_result.valid)
    return validation_result, silver_df


def run_gold(
    silver_df: pd.DataFrame,
    cfg: dict[str, Any],
) -> dict[str, int]:
    """Compute gold outputs and write them. Returns dataset_name → row count."""
    gold_cfg = cfg.get("gold", {})
    requested = set(gold_cfg.get("outputs", ["pnl", "positions"]))
    target_type = gold_cfg.get("target_type", "parquet")
    output_path = Path(gold_cfg.get("output_path", "data/gold/"))

    logger.info("[Gold] Computing outputs: %s", sorted(requested))

    outputs: dict[str, pd.DataFrame] = {}

    if "pnl" in requested:
        outputs["pnl_by_symbol"] = compute_pnl_by_symbol(silver_df)
        outputs["pnl_by_trader"] = compute_pnl_by_trader(silver_df)
        outputs["pnl_summary"] = compute_pnl_summary(silver_df)

    if "positions" in requested:
        outputs["net_positions"] = compute_net_positions(silver_df)
        outputs["position_snapshot"] = compute_position_snapshot(silver_df)

    write_gold_outputs(outputs, output_path, fmt=target_type)

    return {name: len(df) for name, df in outputs.items()}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_config(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    if not path.exists():
        logger.warning("Config not found at %s — using defaults", path)
        return {}
    with open(path) as f:
        cfg = yaml.safe_load(f) or {}
    logger.info("Loaded config from %s", path)
    return cfg


def _log_summary(result: PipelineResult) -> None:
    logger.info("-- Pipeline Summary ------------------------------------------")
    logger.info("  Source:           %s", result.source)
    logger.info("  Bronze rows:      %d", result.bronze_row_count)
    logger.info("  Silver valid:     %d", result.silver_valid_count)
    logger.info("  Silver rejected:  %d", result.silver_rejected_count)
    logger.info("  Silver warnings:  %d", result.silver_warning_count)
    for name, count in result.gold_datasets.items():
        logger.info("  Gold %-20s %d rows", name + ":", count)
    logger.info("-------------------------------------------------------------")
