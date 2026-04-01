# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`trade-blotter-pipeline` is a Python data engineering project that processes capital markets trade blotter data using a **medallion architecture** (bronze → silver → gold).

## Setup

```bash
pip install -r requirements-dev.txt
```

## Common Commands

```bash
# Run all tests
pytest

# Run a single test
pytest tests/silver/test_validator.py::test_missing_trade_id

# Run the pipeline
python scripts/run_pipeline.py
```

## Architecture

The pipeline follows the medallion pattern orchestrated by `src/trade_blotter/pipeline.py`:

```
bronze → silver → gold
```

| Layer | Modules | Responsibility |
|---|---|---|
| **Bronze** | `bronze/loader.py` | Ingest raw data as-is from Excel, CSV, or database into `data/bronze/` — no transformations |
| **Silver** | `silver/validator.py`, `silver/cleaner.py` | Validate schema and business rules; normalize fields, types, and identifiers into `data/silver/` |
| **Gold** | `gold/pnl.py`, `gold/positions.py`, `gold/writer.py` | Aggregate clean silver data into business-ready outputs (P&L, positions) in `data/gold/` |

**Supporting modules:**
- `models/trade.py` — trade record schemas shared across layers
- `utils/logger.py` — shared logging configuration
- `config/pipeline.yaml` — runtime configuration (source type, paths, gold outputs)

**Data directories** (not committed — `.gitkeep` placeholders only):
- `data/bronze/` — raw source data, preserved as ingested
- `data/silver/` — validated and cleaned trades
- `data/gold/` — aggregated P&L and position outputs

**Tests** mirror the source layout: `tests/bronze/`, `tests/silver/`, `tests/gold/`.
