# Databricks notebook source
# Trade Blotter Pipeline — Production Notebook
#
# Uses the trade-blotter-pipeline package installed directly from GitHub.
# Each cell delegates to the real package modules rather than inlining logic,
# so this notebook always reflects the behaviour of the canonical Python code.
#
# Cells:
#   1   Install package from GitHub
#   2   Bronze  — ingest raw data         (trade_blotter.bronze.loader)
#   3   Silver  — validate                (trade_blotter.silver.validator)
#   4   Silver  — clean                   (trade_blotter.silver.cleaner)
#   5   Gold    — aggregate P&L & positions (trade_blotter.gold.pnl / positions)
#   6   Gold    — write Delta tables      (mergeSchema)
#   7   Verification                      (SQL row-count + spot-check queries)
#
# Tested against: Databricks Runtime 14.x (Spark 3.5, Delta 3.x)
# Catalog model:  Unity Catalog  (catalog.schema.table)

# COMMAND ----------

# MAGIC %md
# MAGIC # Trade Blotter Pipeline
# MAGIC **Bronze → Silver → Gold** medallion architecture using the
# MAGIC `trade-blotter-pipeline` package and Delta Lake.
# MAGIC
# MAGIC | Layer | What happens | Delta tables written |
# MAGIC |-------|-------------|----------------------|
# MAGIC | Bronze | Ingest raw CSV/Excel as strings, no transformations | `bronze.trades` |
# MAGIC | Silver | Validate business rules; clean and type-cast valid rows | `silver.trades_valid`, `silver.trades_rejected`, `silver.trades_warnings` |
# MAGIC | Gold | Aggregate P&L and positions | `gold.pnl_by_symbol`, `gold.pnl_by_trader`, `gold.pnl_summary`, `gold.net_positions`, `gold.position_snapshot` |

# COMMAND ----------

# Configuration — edit before running.
# Table names use schema.table format (compatible with Community Edition /
# hive_metastore; no catalog prefix required).

SOURCE_PATH   = "dbfs:/FileStore/trade-blotter/bronze/trades_20260401.csv"
SOURCE_FORMAT = "csv"   # "csv" or "excel"

BRONZE_TRADES          = "bronze.trades"
SILVER_VALID           = "silver.trades_valid"
SILVER_REJECTED        = "silver.trades_rejected"
SILVER_WARNINGS        = "silver.trades_warnings"
GOLD_PNL_BY_SYMBOL     = "gold.pnl_by_symbol"
GOLD_PNL_BY_TRADER     = "gold.pnl_by_trader"
GOLD_PNL_SUMMARY       = "gold.pnl_summary"
GOLD_NET_POSITIONS     = "gold.net_positions"
GOLD_POSITION_SNAPSHOT = "gold.position_snapshot"

# If True, the pipeline aborts when any rows are rejected in the silver layer.
FAIL_ON_VALIDATION_ERROR = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Install package from GitHub

# COMMAND ----------

# MAGIC %pip install git+https://github.com/rayyu56/trade-blotter-pipeline.git

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Bronze — Ingest raw data
# MAGIC
# MAGIC **Module:** `trade_blotter.bronze.loader`
# MAGIC
# MAGIC All columns are ingested as strings — no type coercion or validation.
# MAGIC The raw DataFrame is written to the bronze Delta table for traceability.

# COMMAND ----------

from trade_blotter.bronze.loader import load_csv, load_excel


def _dbfs_local(path: str) -> str:
    """Convert a dbfs:/… URI to the /dbfs/… mount path for local filesystem access."""
    return path.replace("dbfs:/", "/dbfs/", 1) if path.startswith("dbfs:/") else path


if SOURCE_FORMAT == "csv":
    ingest_result = load_csv(_dbfs_local(SOURCE_PATH))
elif SOURCE_FORMAT == "excel":
    ingest_result = load_excel(_dbfs_local(SOURCE_PATH))
else:
    raise ValueError(f"Unsupported SOURCE_FORMAT: {SOURCE_FORMAT!r}")

df_bronze_pd = ingest_result.dataframe
df_bronze_pd["_source_file"] = ingest_result.source

print(f"Bronze: loaded {ingest_result.row_count:,} rows from {ingest_result.source}")
print(f"        columns: {ingest_result.columns}")

# COMMAND ----------

# Write raw data to the bronze Delta table.
# mergeSchema allows the table to evolve if the source gains new columns.
(
    spark.createDataFrame(df_bronze_pd)
    .write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(BRONZE_TRADES)
)
print(f"Bronze: wrote {ingest_result.row_count:,} rows to {BRONZE_TRADES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Silver — Validation
# MAGIC
# MAGIC **Module:** `trade_blotter.silver.validator`
# MAGIC
# MAGIC Reads from the persisted bronze Delta table so validation is always
# MAGIC reproducible regardless of how the session started.
# MAGIC
# MAGIC | Rule | Disposition |
# MAGIC |------|------------|
# MAGIC | Duplicate trade_id (after first occurrence) | REJECT |
# MAGIC | Missing required field | REJECT |
# MAGIC | Non-numeric quantity or price | REJECT |
# MAGIC | Non-positive (≤ 0) quantity | REJECT |
# MAGIC | Invalid side (not Buy / Sell) | REJECT |
# MAGIC | settlement_date before trade_date | REJECT |
# MAGIC | Missing broker | WARN |
# MAGIC | Missing settlement_date | WARN |
# MAGIC | Unrecognised asset_class | WARN |

# COMMAND ----------

from trade_blotter.silver.validator import validate

df_bronze_pd = spark.table(BRONZE_TRADES).toPandas()
validation_result = validate(df_bronze_pd)

valid_count    = len(validation_result.valid)
rejected_count = len(validation_result.rejected)
warning_count  = len(validation_result.warnings)

print(f"Validation — valid: {valid_count:,}, rejected: {rejected_count:,}, warnings: {warning_count:,}")
for err in validation_result.errors:
    print(f"  {err}")

if FAIL_ON_VALIDATION_ERROR and rejected_count > 0:
    print(validation_result.rejected[["trade_id", "_validation_issues"]].to_string())
    raise RuntimeError(
        f"Pipeline aborted: {rejected_count} row(s) rejected and "
        "FAIL_ON_VALIDATION_ERROR=True"
    )

# COMMAND ----------

# Persist rejected and warned rows for audit / remediation.
for table_name, df_pd in [
    (SILVER_REJECTED, validation_result.rejected),
    (SILVER_WARNINGS, validation_result.warnings),
]:
    (
        spark.createDataFrame(df_pd)
        .write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Silver: wrote {len(df_pd):,} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Silver — Cleaning
# MAGIC
# MAGIC **Module:** `trade_blotter.silver.cleaner`
# MAGIC
# MAGIC Applied only to the valid DataFrame from the validation step:
# MAGIC 1. Strip whitespace from all string columns
# MAGIC 2. Normalise `side` to title-case (`buy` / `BUY` → `Buy`)
# MAGIC 3. Cast `quantity` to `Int64`, `price` to `float`
# MAGIC 4. Parse `trade_date` and `settlement_date` to `date`
# MAGIC 5. Derive `notional = quantity × price`
# MAGIC 6. Stamp `ingested_at` with the current UTC timestamp

# COMMAND ----------

from trade_blotter.silver.cleaner import clean

df_clean_pd = clean(validation_result.valid)
print(f"Cleaning complete — {len(df_clean_pd):,} rows, {len(df_clean_pd.columns)} columns")

(
    spark.createDataFrame(df_clean_pd)
    .write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(SILVER_VALID)
)
print(f"Silver: wrote {len(df_clean_pd):,} cleaned rows to {SILVER_VALID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Gold — P&L and Position Aggregation
# MAGIC
# MAGIC **Modules:** `trade_blotter.gold.pnl`, `trade_blotter.gold.positions`
# MAGIC
# MAGIC Reads from the persisted silver Delta table so gold aggregations are
# MAGIC always independently reproducible.

# COMMAND ----------

from trade_blotter.gold.pnl import (
    compute_pnl_by_symbol,
    compute_pnl_by_trader,
    compute_pnl_summary,
)
from trade_blotter.gold.positions import (
    compute_net_positions,
    compute_position_snapshot,
)

df_silver_pd = spark.table(SILVER_VALID).toPandas()

pnl_by_symbol     = compute_pnl_by_symbol(df_silver_pd)
pnl_by_trader     = compute_pnl_by_trader(df_silver_pd)
pnl_summary       = compute_pnl_summary(df_silver_pd)
net_positions     = compute_net_positions(df_silver_pd)
position_snapshot = compute_position_snapshot(df_silver_pd)

print(f"pnl_by_symbol:     {len(pnl_by_symbol):,} rows")
print(f"pnl_by_trader:     {len(pnl_by_trader):,} rows")
print(f"pnl_summary:       {len(pnl_summary):,} rows")
print(f"net_positions:     {len(net_positions):,} rows")
print(f"position_snapshot: {len(position_snapshot):,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Write Gold Tables to Delta

# COMMAND ----------

# Write all gold outputs with mergeSchema so the tables can evolve as the
# package adds new columns without requiring a schema-breaking overwrite.
gold_outputs = {
    GOLD_PNL_BY_SYMBOL:     pnl_by_symbol,
    GOLD_PNL_BY_TRADER:     pnl_by_trader,
    GOLD_PNL_SUMMARY:       pnl_summary,
    GOLD_NET_POSITIONS:     net_positions,
    GOLD_POSITION_SNAPSHOT: position_snapshot,
}

for table_name, df_pd in gold_outputs.items():
    (
        spark.createDataFrame(df_pd)
        .write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Gold: wrote {len(df_pd):,} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Verification

# COMMAND ----------

# Row-count check across every pipeline table.
all_tables = {
    "bronze.trades":           BRONZE_TRADES,
    "silver.trades_valid":     SILVER_VALID,
    "silver.trades_rejected":  SILVER_REJECTED,
    "silver.trades_warnings":  SILVER_WARNINGS,
    "gold.pnl_by_symbol":      GOLD_PNL_BY_SYMBOL,
    "gold.pnl_by_trader":      GOLD_PNL_BY_TRADER,
    "gold.pnl_summary":        GOLD_PNL_SUMMARY,
    "gold.net_positions":      GOLD_NET_POSITIONS,
    "gold.position_snapshot":  GOLD_POSITION_SNAPSHOT,
}

print("=" * 60)
print("  Pipeline Verification")
print("=" * 60)
for label, table_name in all_tables.items():
    try:
        count = spark.sql(f"SELECT COUNT(*) AS n FROM {table_name}").collect()[0]["n"]
        print(f"  {label:<35} {count:>8,} rows")
    except Exception as e:
        print(f"  {label:<35}   ERROR: {e}")
print("=" * 60)

# COMMAND ----------

# Spot-check: top 5 symbols by absolute net P&L.
print("Top 5 symbols by |net_pnl|:")
spark.sql(f"""
    SELECT symbol, asset_class, currency, net_pnl, trade_count
    FROM {GOLD_PNL_BY_SYMBOL}
    ORDER BY ABS(net_pnl) DESC
    LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# Spot-check: net positions as of snapshot date.
print("Net positions (position_snapshot):")
spark.sql(f"""
    SELECT symbol, asset_class, currency, net_position, as_of_date
    FROM {GOLD_POSITION_SNAPSHOT}
    ORDER BY ABS(net_position) DESC
""").show(truncate=False)
