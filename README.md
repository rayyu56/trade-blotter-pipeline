# Trade Blotter Pipeline

A Python data engineering pipeline for ingesting, validating, transforming, and storing capital markets trade blotter data.

---

## Overview

Trade blotter data records every buy and sell order executed by a trading desk — including instrument details, counterparties, quantities, prices, and timestamps. This pipeline automates the end-to-end processing of that data from raw source files through to a clean, queryable output, supporting downstream risk, compliance, and reporting workflows.

---

## Architecture

The pipeline follows a linear four-stage flow, orchestrated by `src/trade_blotter/pipeline.py`:

```
Raw Source Files
      │
      ▼
┌─────────────┐
│   Ingest    │  Load Excel / CSV / database → pandas DataFrame
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Validate   │  Schema checks, business rule enforcement, error flagging
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Transform  │  Field normalization, derived calculations, aggregations
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Store    │  Write to database or Parquet files
└─────────────┘
```

| Stage | Module | Responsibility |
|---|---|---|
| Ingest | `ingest/loader.py` | Load raw trade data from Excel, CSV, or a database source |
| Validate | `validate/validator.py` | Enforce schema and business rules; reject or flag bad records |
| Transform | `transform/transformer.py` | Normalize fields, compute derived values, aggregate |
| Store | `store/writer.py` | Write transformed data to a target database or Parquet files |

**Supporting modules:**

| Module | Responsibility |
|---|---|
| `models/trade.py` | Data models and schemas for trade records |
| `utils/logger.py` | Shared logging configuration |
| `config/pipeline.yaml` | Runtime configuration (source type, paths, output target) |

---

## Project Structure

```
trade-blotter-pipeline/
├── src/
│   └── trade_blotter/
│       ├── pipeline.py          # Top-level orchestration
│       ├── ingest/
│       │   └── loader.py
│       ├── validate/
│       │   └── validator.py
│       ├── transform/
│       │   └── transformer.py
│       ├── store/
│       │   └── writer.py
│       ├── models/
│       │   └── trade.py
│       └── utils/
│           └── logger.py
├── tests/
│   ├── ingest/
│   ├── validate/
│   ├── transform/
│   └── store/
├── config/
│   └── pipeline.yaml
├── data/
│   ├── raw/                     # Drop source files here
│   ├── interim/                 # Between-stage scratch space
│   └── processed/               # Final output
├── notebooks/                   # Exploratory analysis
├── scripts/
│   └── run_pipeline.py          # Pipeline entry point
├── requirements.txt
└── requirements-dev.txt
```

---

## Setup

**1. Clone the repository**

```bash
git clone https://github.com/rayyu56/trade-blotter-pipeline.git
cd trade-blotter-pipeline
```

**2. Create and activate a virtual environment**

```bash
python -m venv .venv
source .venv/bin/activate        # macOS/Linux
.venv\Scripts\activate           # Windows
```

**3. Install dependencies**

```bash
# Runtime only
pip install -r requirements.txt

# Runtime + dev/test tools
pip install -r requirements-dev.txt
```

**4. Configure the pipeline**

Edit `config/pipeline.yaml` to set your source type, input path, and output target:

```yaml
ingest:
  source_type: excel  # excel | csv | database
  source_path: data/raw/

store:
  target_type: database  # database | parquet
  output_path: data/processed/
```

---

## Usage

Place raw trade blotter files in `data/raw/`, then run:

```bash
python scripts/run_pipeline.py
```

---

## Testing

```bash
# Run all tests
pytest

# Run a single test
pytest tests/validate/test_validator.py::test_missing_trade_id
```

---

## Dependencies

| Package | Purpose |
|---|---|
| `pandas` | DataFrame-based data processing |
| `sqlalchemy` | Database connectivity |
| `openpyxl` | Excel file reading |
| `pytest` | Testing (dev) |
