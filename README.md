# Trade Blotter Pipeline

A Python data engineering pipeline for processing capital markets trade blotter data using a **medallion architecture** (bronze → silver → gold).

---

## Overview

Trade blotter data records every buy and sell order executed by a trading desk — including instrument details, counterparties, quantities, prices, and timestamps. This pipeline automates end-to-end processing from raw source files through to business-ready outputs (P&L, positions), supporting downstream risk, compliance, and reporting workflows.

---

## Architecture

The pipeline implements the **medallion architecture** with three data quality layers, orchestrated by `src/trade_blotter/pipeline.py`:

```
Raw Source Files
      │
      ▼
┌─────────────────────────────────────────┐
│  BRONZE  –  Raw Ingestion               │
│  loader.py                              │
│  Load Excel / CSV / DB → data/bronze/   │
│  No transformations. Data preserved     │
│  exactly as received.                   │
└──────────────────────┬──────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────┐
│  SILVER  –  Validated & Cleaned Trades  │
│  validator.py  →  cleaner.py            │
│  Schema checks, business rule           │
│  enforcement, normalization,            │
│  type standardization → data/silver/    │
└──────────────────────┬──────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────┐
│  GOLD  –  Business-Ready Outputs        │
│  pnl.py  │  positions.py  │  writer.py  │
│  Realized/unrealized P&L, net           │
│  positions by instrument & desk         │
│  → data/gold/ (DB or Parquet)           │
└─────────────────────────────────────────┘
```

| Layer | Modules | Output |
|---|---|---|
| **Bronze** | `bronze/loader.py` | Raw data in `data/bronze/`, unchanged from source |
| **Silver** | `silver/validator.py`, `silver/cleaner.py` | Validated, normalized trades in `data/silver/` |
| **Gold** | `gold/pnl.py`, `gold/positions.py`, `gold/writer.py` | Aggregated P&L and positions in `data/gold/` |

---

## Project Structure

```
trade-blotter-pipeline/
├── src/
│   └── trade_blotter/
│       ├── pipeline.py              # Top-level orchestration
│       ├── bronze/
│       │   └── loader.py            # Raw ingestion
│       ├── silver/
│       │   ├── validator.py         # Schema & business rule validation
│       │   └── cleaner.py           # Normalization & standardization
│       ├── gold/
│       │   ├── pnl.py               # P&L aggregation
│       │   ├── positions.py         # Net position calculation
│       │   └── writer.py            # Output to DB or Parquet
│       ├── models/
│       │   └── trade.py             # Shared trade record schemas
│       └── utils/
│           └── logger.py
├── tests/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── config/
│   └── pipeline.yaml
├── data/
│   ├── bronze/                      # Raw source data (not committed)
│   ├── silver/                      # Cleaned trades (not committed)
│   └── gold/                        # Aggregated outputs (not committed)
├── notebooks/                       # Exploratory analysis
├── scripts/
│   └── run_pipeline.py              # Pipeline entry point
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

Edit `config/pipeline.yaml` to set your source type, input path, and output targets:

```yaml
ingest:
  source_type: excel  # excel | csv | database
  source_path: data/bronze/

silver:
  fail_on_validation_error: true

gold:
  outputs:
    - pnl
    - positions
  target_type: database  # database | parquet
```

---

## Usage

Place raw trade blotter files in `data/bronze/`, then run:

```bash
python scripts/run_pipeline.py
```

---

## Testing

```bash
# Run all tests
pytest

# Run a single test
pytest tests/silver/test_validator.py::test_missing_trade_id
```

---

## Dependencies

| Package | Purpose |
|---|---|
| `pandas` | DataFrame-based data processing across all layers |
| `sqlalchemy` | Database connectivity for bronze ingestion and gold output |
| `openpyxl` | Excel file reading in the bronze layer |
| `pytest` | Testing (dev) |
