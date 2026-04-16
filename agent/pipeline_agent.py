"""Main DataOps agent loop for the trade-blotter pipeline.

Usage::

    python agent/pipeline_agent.py                        # process all new files
    python agent/pipeline_agent.py --file path/to/file.csv  # process one specific file
    python agent/pipeline_agent.py --dry-run              # scan only, no Databricks calls
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import yaml
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Bootstrap — resolve repo root and load .env before importing tools
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

load_dotenv(_REPO_ROOT / ".env", override=True)

from agent.tools.file_monitor import scan_for_new_files          # noqa: E402
from agent.tools.file_uploader import upload_file_to_workspace   # noqa: E402
from agent.tools.job_monitor import poll_run                      # noqa: E402
from agent.tools.job_trigger import trigger_notebook_run          # noqa: E402
from agent.tools.quality_checker import check_table_counts        # noqa: E402
from agent.tools.reconciler import reconcile                      # noqa: E402
from agent.tools.reporter import generate_eod_summary            # noqa: E402


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _load_config(config_path: Path) -> Dict[str, Any]:
    with config_path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _load_history(memory_path: Path) -> List[Dict[str, Any]]:
    if memory_path.exists():
        return json.loads(memory_path.read_text(encoding="utf-8"))
    return []


def _save_history(memory_path: Path, history: List[Dict[str, Any]]) -> None:
    memory_path.parent.mkdir(parents=True, exist_ok=True)
    memory_path.write_text(json.dumps(history, indent=2, default=str), encoding="utf-8")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Single-file pipeline run
# ---------------------------------------------------------------------------

def run_pipeline_for_file(
    source_file: str,
    cfg: Dict[str, Any],
    host: str,
    token: str,
    anthropic_key: str,
    dry_run: bool,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """Execute the full agent loop for one source CSV file.

    Steps
    -----
    1. Upload CSV to Databricks workspace bronze directory.
    2. Trigger Databricks notebook run.
    3. Poll until terminal state.
    4. Run quality checks (table row counts).
    5. Reconcile bronze vs silver.
    6. Generate EOD summary via Claude.
    7. Return the completed run record.
    """
    db_cfg = cfg["databricks"]
    pipeline_cfg = cfg["pipeline"]
    tables_cfg = cfg["tables"]

    record: Dict[str, Any] = {
        "source_file": source_file,
        "triggered_at": _now(),
        "dry_run": dry_run,
        "workspace_path": None,
        "run_id": None,
        "run_url": None,
        "status": "pending",
        "quality_report": None,
        "reconciliation": None,
        "eod_summary": None,
        "error": None,
        "completed_at": None,
    }

    try:
        # ---- 1. Upload --------------------------------------------------------
        if dry_run:
            logger.info("[DRY-RUN] Would upload %s and trigger notebook", source_file)
            record["status"] = "dry_run"
            record["completed_at"] = _now()
            return record

        logger.info("Uploading %s to Databricks workspace …", source_file)
        upload = upload_file_to_workspace(
            host=host,
            token=token,
            local_path=source_file,
            workspace_dir=db_cfg["upload_path"],
        )
        record["workspace_path"] = upload.workspace_path
        logger.info(
            "Uploaded %s bytes → %s",
            f"{upload.size_bytes:,}",
            upload.workspace_path,
        )

        # ---- 2. Trigger -------------------------------------------------------
        logger.info("Triggering notebook for %s …", source_file)
        trigger = trigger_notebook_run(
            host=host,
            token=token,
            notebook_path=db_cfg["notebook_path"],
            cluster_id=db_cfg.get("cluster_id") or None,
            run_name=f"trade-blotter-{Path(source_file).stem}",
        )
        record["run_id"] = trigger.run_id
        record["run_url"] = trigger.run_url
        logger.info("Run %s submitted — %s", trigger.run_id, trigger.run_url)

        # ---- 2. Poll ----------------------------------------------------------
        logger.info("Polling run %s …", trigger.run_id)
        monitor = poll_run(
            host=host,
            token=token,
            run_id=trigger.run_id,
            poll_interval_seconds=pipeline_cfg["poll_interval_seconds"],
            max_attempts=pipeline_cfg["max_poll_attempts"],
        )
        record["status"] = "success" if monitor.succeeded else "failed"
        logger.info(
            "Run %s finished: %s / %s — %s",
            trigger.run_id,
            monitor.life_cycle_state,
            monitor.result_state,
            monitor.state_message,
        )

        if not monitor.succeeded:
            record["error"] = monitor.state_message
            record["completed_at"] = _now()
            return record

        # ---- 3. Quality check -------------------------------------------------
        logger.info("Running quality checks …")
        qr = check_table_counts(
            host=host,
            token=token,
            http_path=db_cfg["sql_http_path"],
            tables=tables_cfg,
            catalog=db_cfg.get("catalog", "hive_metastore"),
            schema=db_cfg.get("schema", "default"),
        )
        record["quality_report"] = {
            "table_counts": qr.table_counts,
            "empty_tables": qr.empty_tables,
            "passed": qr.passed,
        }
        if not qr.passed:
            logger.warning("Quality check FAILED — empty tables: %s", qr.empty_tables)
        else:
            logger.info("Quality check PASSED — all tables populated.")

        # ---- 4. Reconciliation ------------------------------------------------
        logger.info("Reconciling bronze vs silver …")
        recon = reconcile(
            bronze_count=qr.table_counts.get("bronze_trades", 0),
            silver_valid_count=qr.table_counts.get("silver_valid", 0),
            silver_rejected_count=qr.table_counts.get("silver_rejected", 0),
        )
        record["reconciliation"] = {
            "bronze_count": recon.bronze_count,
            "silver_valid_count": recon.silver_valid_count,
            "silver_rejected_count": recon.silver_rejected_count,
            "unaccounted": recon.unaccounted,
            "passed": recon.passed,
            "message": recon.message,
        }
        logger.info(recon.message)

        # ---- 5. EOD summary ---------------------------------------------------
        logger.info("Generating EOD summary via Claude …")
        summary = generate_eod_summary(api_key=anthropic_key, run_record=record)
        record["eod_summary"] = summary
        logger.info("EOD summary:\n%s", summary)

    except Exception as exc:  # noqa: BLE001
        logger.exception("Unhandled error processing %s", source_file)
        record["status"] = "error"
        record["error"] = str(exc)

    finally:
        record["completed_at"] = _now()

    return record


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Trade Blotter DataOps Agent")
    parser.add_argument("--file", help="Process a specific file instead of scanning bronze/")
    parser.add_argument("--dry-run", action="store_true", help="Scan only; no Databricks calls")
    parser.add_argument(
        "--config",
        default=str(_REPO_ROOT / "agent" / "config" / "agent_config.yaml"),
        help="Path to agent_config.yaml",
    )
    args = parser.parse_args()

    cfg = _load_config(Path(args.config))

    log_level = cfg.get("agent", {}).get("log_level", "INFO")
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logger = logging.getLogger("pipeline_agent")

    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    anthropic_key = os.environ["ANTHROPIC_API_KEY"]

    memory_path = _REPO_ROOT / cfg["agent"]["memory_path"]
    history = _load_history(memory_path)

    # Determine which files to process
    if args.file:
        files_to_process = [str(Path(args.file).resolve())]
    else:
        monitor_result = scan_for_new_files(
            bronze_path=str(_REPO_ROOT / cfg["pipeline"]["bronze_source_path"]),
            file_pattern=cfg["pipeline"]["file_pattern"],
            memory_path=str(memory_path),
        )
        files_to_process = monitor_result.unprocessed
        logger.info(
            "Found %d unprocessed file(s), %d already processed.",
            len(monitor_result.unprocessed),
            len(monitor_result.already_processed),
        )

    if not files_to_process:
        logger.info("Nothing to do — no new files found.")
        return

    for source_file in files_to_process:
        logger.info("=== Processing: %s ===", source_file)
        record = run_pipeline_for_file(
            source_file=source_file,
            cfg=cfg,
            host=host,
            token=token,
            anthropic_key=anthropic_key,
            dry_run=args.dry_run,
            logger=logger,
        )
        history.append(record)
        _save_history(memory_path, history)
        logger.info("Run record saved to %s", memory_path)

    logger.info("Agent finished. Processed %d file(s).", len(files_to_process))


if __name__ == "__main__":
    main()
