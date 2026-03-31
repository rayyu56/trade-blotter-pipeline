# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`trade-blotter-pipeline` is a Python data engineering project that ingests, validates, transforms, and stores capital markets trade blotter data.

## Setup

```bash
pip install -r requirements-dev.txt
```

## Common Commands

```bash
# Run all tests
pytest

# Run a single test
pytest tests/path/to/test_file.py::test_function_name

# Run the pipeline
python scripts/run_pipeline.py
```

## Architecture

The pipeline follows a linear four-stage flow orchestrated by `src/trade_blotter/pipeline.py`:

```
ingest → validate → transform → store
```

| Stage | Module | Responsibility |
|---|---|---|
| Ingest | `ingest/loader.py` | Load raw trade data from Excel, CSV, or database into a DataFrame |
| Validate | `validate/validator.py` | Enforce schema and business rules; reject or flag bad records |
| Transform | `transform/transformer.py` | Normalize fields, compute derived values, aggregate |
| Store | `store/writer.py` | Write output to a database or Parquet files |

**Supporting modules:**
- `models/trade.py` — data models and schemas for trade records
- `utils/logger.py` — shared logging configuration
- `config/pipeline.yaml` — runtime configuration (source type, paths, target)

**Data directories** (not committed, only `.gitkeep` placeholders):
- `data/raw/` — source files dropped here for ingestion
- `data/interim/` — intermediate outputs between stages
- `data/processed/` — final output

**Tests** mirror the source layout under `tests/` with one subdirectory per pipeline stage.
