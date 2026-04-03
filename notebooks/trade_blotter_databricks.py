# Databricks notebook source
# Trade Blotter Pipeline — Bronze → Silver → Gold (PySpark / Delta Lake)
#
# This notebook is a PySpark port of the trade-blotter-pipeline package
# (src/trade_blotter/). Each section maps directly to a module in that package.
# The logic — validation rules, cleaning steps, aggregation formulas — is
# identical to the pandas implementation; only the DataFrame API differs.
#
# Tested against: Databricks Runtime 14.x (Spark 3.5, Delta 3.x)
# Catalog model:  Unity Catalog  (catalog.schema.table)

# COMMAND ----------

# MAGIC %md
# MAGIC # Trade Blotter Pipeline
# MAGIC **Bronze → Silver → Gold** medallion architecture using PySpark and Delta Lake.
# MAGIC
# MAGIC | Layer | What happens | Delta tables written |
# MAGIC |-------|-------------|----------------------|
# MAGIC | Bronze | Ingest raw CSV/Excel as strings, no transformations | `bronze.trades` |
# MAGIC | Silver | Validate business rules; clean and type-cast valid rows | `silver.trades_valid`, `silver.trades_rejected`, `silver.trades_warnings` |
# MAGIC | Gold | Aggregate P&L and positions | `gold.pnl_by_symbol`, `gold.pnl_by_trader`, `gold.pnl_summary`, `gold.net_positions`, `gold.position_snapshot` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Configuration

# COMMAND ----------

# All paths and table names are defined here — change these to match your
# Unity Catalog setup and storage location before running.

# Unity Catalog: catalog that owns the three schemas below
CATALOG = "main"

# Cloud storage path to the source file (DBFS, ADLS, or S3)
# e.g. "abfss://container@account.dfs.core.windows.net/trade-blotter/bronze/"
SOURCE_PATH = "dbfs:/FileStore/trade-blotter/bronze/trades_20260401.csv"
SOURCE_FORMAT = "csv"   # "csv" or "excel" (see bronze section for Excel variant)

# Schema / database names within the catalog
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

# Fully-qualified table names
BRONZE_TRADES         = f"{CATALOG}.{BRONZE_SCHEMA}.trades"
SILVER_VALID          = f"{CATALOG}.{SILVER_SCHEMA}.trades_valid"
SILVER_REJECTED       = f"{CATALOG}.{SILVER_SCHEMA}.trades_rejected"
SILVER_WARNINGS       = f"{CATALOG}.{SILVER_SCHEMA}.trades_warnings"
GOLD_PNL_BY_SYMBOL    = f"{CATALOG}.{GOLD_SCHEMA}.pnl_by_symbol"
GOLD_PNL_BY_TRADER    = f"{CATALOG}.{GOLD_SCHEMA}.pnl_by_trader"
GOLD_PNL_SUMMARY      = f"{CATALOG}.{GOLD_SCHEMA}.pnl_summary"
GOLD_NET_POSITIONS    = f"{CATALOG}.{GOLD_SCHEMA}.net_positions"
GOLD_POSITION_SNAPSHOT = f"{CATALOG}.{GOLD_SCHEMA}.position_snapshot"

# If True, the pipeline aborts when any rows are rejected in the silver layer.
FAIL_ON_VALIDATION_ERROR = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0.1 · Ensure schemas exist

# COMMAND ----------

# Create the three schemas if they don't already exist.
# In Unity Catalog these are databases scoped to the catalog.
for schema in (BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"Schema ready: {CATALOG}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Bronze — Ingest raw data
# MAGIC
# MAGIC **Mirrors:** `src/trade_blotter/bronze/loader.py`
# MAGIC
# MAGIC Bronze ingests the source file as-is: all columns are read as strings,
# MAGIC no type coercion or validation takes place. The goal is a faithful,
# MAGIC traceable copy of the source data.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Bronze schema — all columns as strings, matching BRONZE_SCHEMA in models/trade.py
# ---------------------------------------------------------------------------
bronze_schema = StructType([
    StructField("trade_id",        StringType(), True),
    StructField("trade_date",      StringType(), True),
    StructField("settlement_date", StringType(), True),
    StructField("trader",          StringType(), True),
    StructField("counterparty",    StringType(), True),
    StructField("asset_class",     StringType(), True),
    StructField("symbol",          StringType(), True),
    StructField("side",            StringType(), True),
    StructField("quantity",        StringType(), True),
    StructField("price",           StringType(), True),
    StructField("currency",        StringType(), True),
    StructField("broker",          StringType(), True),
    StructField("status",          StringType(), True),
])

# COMMAND ----------

# Read the source file.
# Enforcing the schema (rather than inferSchema) keeps bronze predictable:
# no silent type promotions even if Spark could guess the types.
if SOURCE_FORMAT == "csv":
    df_bronze = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("enforceSchema", "false")   # warn on schema drift rather than fail
        .schema(bronze_schema)
        .load(SOURCE_PATH)
    )
elif SOURCE_FORMAT == "excel":
    # com.crealytics:spark-excel library required on the cluster
    df_bronze = (
        spark.read
        .format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(bronze_schema)
        .load(SOURCE_PATH)
    )
else:
    raise ValueError(f"Unsupported SOURCE_FORMAT: {SOURCE_FORMAT!r}")

# Add a provenance column so downstream layers can trace every row back to
# the file it came from — equivalent to the _source_file column added by
# load_directory() in the pandas pipeline.
df_bronze = df_bronze.withColumn("_source_file", F.input_file_name())

print(f"Bronze: loaded {df_bronze.count():,} rows from {SOURCE_PATH}")
df_bronze.printSchema()

# COMMAND ----------

# Write the raw data to the bronze Delta table.
# mode="overwrite" + replaceWhere makes reruns idempotent for the same source file.
(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TRADES)
)
print(f"Bronze: wrote to {BRONZE_TRADES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Silver — Validation
# MAGIC
# MAGIC **Mirrors:** `src/trade_blotter/silver/validator.py`
# MAGIC
# MAGIC Each rule appends a tagged reason string to the `_validation_issues` array
# MAGIC column. Rows with any `REJECT:*` tag are quarantined; rows with only
# MAGIC `WARN:*` tags are flagged but kept as valid.
# MAGIC
# MAGIC | Rule | Disposition |
# MAGIC |------|------------|
# MAGIC | Duplicate trade_id (after the first occurrence) | REJECT |
# MAGIC | Missing required field | REJECT |
# MAGIC | Non-numeric quantity or price | REJECT |
# MAGIC | Non-positive (≤ 0) quantity | REJECT |
# MAGIC | Invalid side (not Buy / Sell) | REJECT |
# MAGIC | settlement_date before trade_date | REJECT |
# MAGIC | Missing broker | WARN |
# MAGIC | Missing settlement_date | WARN |
# MAGIC | Unrecognised asset_class | WARN |

# COMMAND ----------

# Required fields — a row is rejected if any of these is null or blank.
# Mirrors REQUIRED_FIELDS in models/trade.py.
REQUIRED_FIELDS = [
    "trade_id", "trade_date", "trader", "counterparty",
    "asset_class", "symbol", "side", "quantity", "price", "currency",
]

VALID_SIDES         = {"Buy", "Sell"}
VALID_ASSET_CLASSES = {"Equity", "FX", "Fixed Income"}

# COMMAND ----------

# Read from the bronze Delta table so validation always runs on persisted data.
df = spark.table(BRONZE_TRADES)

# Initialise the issues accumulator as an empty array of strings.
df = df.withColumn("_validation_issues", F.array().cast("array<string>"))

# COMMAND ----------

# ---------------------------------------------------------------------------
# Rule 1: Drop exact duplicate rows (all columns identical).
# Mirrors _drop_exact_duplicates() — these are silently removed before any
# per-rule flagging, matching the pandas behaviour.
# ---------------------------------------------------------------------------
before = df.count()
df = df.dropDuplicates()
dropped = before - df.count()
if dropped:
    print(f"Dropped {dropped:,} exact duplicate row(s)")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Rule 2: Flag duplicate trade_ids (keep first occurrence, reject the rest).
# Mirrors _flag_duplicate_trade_ids().
# ---------------------------------------------------------------------------
dedup_window = Window.partitionBy("trade_id").orderBy(F.monotonically_increasing_id())

df = df.withColumn("_row_num", F.row_number().over(dedup_window))
df = df.withColumn(
    "_validation_issues",
    F.when(
        F.col("_row_num") > 1,
        F.array_union(
            F.col("_validation_issues"),
            F.array(F.lit("REJECT:duplicate_trade_id")),
        ),
    ).otherwise(F.col("_validation_issues")),
).drop("_row_num")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Rule 3: Flag missing required fields.
# Mirrors _flag_missing_required_fields() — null OR empty-string is treated
# as missing, consistent with the pandas str.strip() == "" check.
# ---------------------------------------------------------------------------
for field_name in REQUIRED_FIELDS:
    df = df.withColumn(
        "_validation_issues",
        F.when(
            F.col(field_name).isNull() | (F.trim(F.col(field_name)) == ""),
            F.array_union(
                F.col("_validation_issues"),
                F.array(F.lit(f"REJECT:missing_{field_name}")),
            ),
        ).otherwise(F.col("_validation_issues")),
    )

# COMMAND ----------

# ---------------------------------------------------------------------------
# Rule 4: Flag invalid side values.
# Mirrors _flag_invalid_side() — expects exactly "Buy" or "Sell" (case-sensitive).
# cleaner.py normalises case only after validation passes.
# ---------------------------------------------------------------------------
df = df.withColumn(
    "_validation_issues",
    F.when(
        ~F.trim(F.col("side")).isin(*VALID_SIDES) & (F.trim(F.col("side")) != ""),
        F.array_union(
            F.col("_validation_issues"),
            F.array(F.lit("REJECT:invalid_side")),
        ),
    ).otherwise(F.col("_validation_issues")),
)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Rule 5: Flag non-numeric quantity and price.
# Mirrors _flag_non_numeric() — .cast("double") returns null on failure,
# which is the PySpark equivalent of pd.to_numeric(errors="coerce").
# ---------------------------------------------------------------------------
for col_name in ("quantity", "price"):
    df = df.withColumn(
        "_validation_issues",
        F.when(
            (F.trim(F.col(col_name)) != "")
            & F.col(col_name).cast("double").isNull(),
            F.array_union(
                F.col("_validation_issues"),
                F.array(F.lit(f"REJECT:non_numeric_{col_name}")),
            ),
        ).otherwise(F.col("_validation_issues")),
    )

# COMMAND ----------

# ---------------------------------------------------------------------------
# Rule 6: Flag non-positive quantity (zero or negative).
# Mirrors _flag_non_positive_quantity().
# ---------------------------------------------------------------------------
df = df.withColumn(
    "_validation_issues",
    F.when(
        F.col("quantity").cast("double").isNotNull()
        & (F.col("quantity").cast("double") <= 0),
        F.array_union(
            F.col("_validation_issues"),
            F.array(F.lit("REJECT:non_positive_quantity")),
        ),
    ).otherwise(F.col("_validation_issues")),
)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Rule 7: Flag settlement_date before trade_date.
# Mirrors _flag_settlement_before_trade() — only checked when both dates
# are parseable (non-null after to_date cast).
# ---------------------------------------------------------------------------
df = df.withColumn("_trade_dt",  F.to_date(F.trim(F.col("trade_date"))))
df = df.withColumn("_settle_dt", F.to_date(F.trim(F.col("settlement_date"))))

df = df.withColumn(
    "_validation_issues",
    F.when(
        F.col("_trade_dt").isNotNull()
        & F.col("_settle_dt").isNotNull()
        & (F.col("_settle_dt") < F.col("_trade_dt")),
        F.array_union(
            F.col("_validation_issues"),
            F.array(F.lit("REJECT:settlement_before_trade_date")),
        ),
    ).otherwise(F.col("_validation_issues")),
).drop("_trade_dt", "_settle_dt")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Soft warnings — row is kept as valid but flagged.
# Mirrors _build_warnings_mask() in validator.py.
# ---------------------------------------------------------------------------

# Warn: missing broker
df = df.withColumn(
    "_validation_issues",
    F.when(
        F.col("broker").isNull() | (F.trim(F.col("broker")) == ""),
        F.array_union(
            F.col("_validation_issues"),
            F.array(F.lit("WARN:missing_broker")),
        ),
    ).otherwise(F.col("_validation_issues")),
)

# Warn: missing settlement_date
df = df.withColumn(
    "_validation_issues",
    F.when(
        F.col("settlement_date").isNull() | (F.trim(F.col("settlement_date")) == ""),
        F.array_union(
            F.col("_validation_issues"),
            F.array(F.lit("WARN:missing_settlement_date")),
        ),
    ).otherwise(F.col("_validation_issues")),
)

# Warn: unrecognised asset_class (non-empty but not in the canonical set)
df = df.withColumn(
    "_validation_issues",
    F.when(
        (F.trim(F.col("asset_class")) != "")
        & ~F.trim(F.col("asset_class")).isin(*VALID_ASSET_CLASSES),
        F.array_union(
            F.col("_validation_issues"),
            F.array(F.lit("WARN:unrecognised_asset_class")),
        ),
    ).otherwise(F.col("_validation_issues")),
)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Split into valid / rejected / warnings DataFrames.
# A row is rejected if its issues array contains any element starting with
# "REJECT:". Warnings are valid rows that have at least one "WARN:" tag.
# ---------------------------------------------------------------------------

# Helper: true if the issues array contains a REJECT tag
has_reject = F.exists("_validation_issues", lambda x: x.startswith("REJECT:"))
has_warn   = F.exists("_validation_issues", lambda x: x.startswith("WARN:"))

df_rejected  = df.filter(has_reject)
df_valid_raw = df.filter(~has_reject)
df_warnings  = df_valid_raw.filter(has_warn)

# Drop the internal issues column from the clean valid set
df_silver_valid = df_valid_raw.drop("_validation_issues")

# Summary
rejected_count = df_rejected.count()
valid_count     = df_silver_valid.count()
warning_count   = df_warnings.count()

print(f"Validation complete — valid: {valid_count:,}, rejected: {rejected_count:,}, warnings: {warning_count:,}")

if FAIL_ON_VALIDATION_ERROR and rejected_count > 0:
    # Surface the rejection reasons before aborting
    df_rejected.select(
        "trade_id",
        F.concat_ws(", ", F.col("_validation_issues")).alias("issues"),
    ).show(truncate=False)
    raise RuntimeError(
        f"Pipeline aborted: {rejected_count} row(s) rejected and "
        "FAIL_ON_VALIDATION_ERROR=True"
    )

# COMMAND ----------

# Persist rejected rows for audit / remediation.
(
    df_rejected.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_REJECTED)
)

# Persist warned rows for review (these are also present in trades_valid).
(
    df_warnings.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_WARNINGS)
)

print(f"Silver: wrote {rejected_count:,} rejected rows to {SILVER_REJECTED}")
print(f"Silver: wrote {warning_count:,} warned rows to {SILVER_WARNINGS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Silver — Cleaning
# MAGIC
# MAGIC **Mirrors:** `src/trade_blotter/silver/cleaner.py`
# MAGIC
# MAGIC Applied only to the valid DataFrame. Steps in order:
# MAGIC 1. Strip leading/trailing whitespace from all string columns
# MAGIC 2. Normalise `side` to title-case (`buy` / `BUY` → `Buy`)
# MAGIC 3. Cast `quantity` to `BIGINT`, `price` to `DOUBLE`
# MAGIC 4. Parse `trade_date` and `settlement_date` to `DATE`
# MAGIC 5. Derive `notional = quantity * price`
# MAGIC 6. Stamp `ingested_at` with the current UTC timestamp

# COMMAND ----------

from pyspark.sql.types import LongType, DoubleType, DateType, TimestampType

df_clean = df_silver_valid

# Step 1: Strip whitespace from every string column.
# Mirrors _strip_strings() — applied before any casting so that "  123  "
# parses correctly rather than failing the numeric cast.
string_cols = [f.name for f in df_clean.schema.fields if isinstance(f.dataType, StringType)]
for col_name in string_cols:
    df_clean = df_clean.withColumn(col_name, F.trim(F.col(col_name)))

# COMMAND ----------

# Step 2: Normalise side to title-case.
# Mirrors _normalise_side() — initcap() on a two-letter token produces the
# same result as str.title() in Python: "buy" → "Buy", "SELL" → "Sell".
df_clean = df_clean.withColumn("side", F.initcap(F.col("side")))

# COMMAND ----------

# Step 3: Cast quantity (Int64) and price (float64).
# Mirrors _cast_numerics() — invalid values become null (equivalent to
# pandas coerce behaviour; those rows were already rejected in silver).
df_clean = (
    df_clean
    .withColumn("quantity", F.col("quantity").cast(LongType()))
    .withColumn("price",    F.col("price").cast(DoubleType()))
)

# COMMAND ----------

# Step 4: Parse dates.
# Mirrors _parse_dates() — to_date() returns null on unparseable input
# (those rows were quarantined in validation before reaching this step).
df_clean = df_clean.withColumn("trade_date", F.to_date(F.col("trade_date")))
df_clean = df_clean.withColumn("settlement_date", F.to_date(F.col("settlement_date")))

# COMMAND ----------

# Step 5: Derive notional = quantity * price.
# Mirrors _derive_notional() — null when either input is null.
df_clean = df_clean.withColumn(
    "notional",
    F.col("quantity").cast(DoubleType()) * F.col("price"),
)

# COMMAND ----------

# Step 6: Stamp ingested_at with the current UTC timestamp.
# Mirrors _add_ingested_at().
df_clean = df_clean.withColumn("ingested_at", F.current_timestamp())

# COMMAND ----------

# Persist the cleaned silver DataFrame.
(
    df_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_VALID)
)
print(f"Silver: wrote {df_clean.count():,} cleaned rows to {SILVER_VALID}")
df_clean.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Gold — P&L Aggregations
# MAGIC
# MAGIC **Mirrors:** `src/trade_blotter/gold/pnl.py`
# MAGIC
# MAGIC P&L model: **blotter-level mark-to-notional**.
# MAGIC - `gross_pnl per trade = notional × direction`  (+1 Buy, −1 Sell)
# MAGIC - `net_pnl per group = Σ(sell notional) − Σ(buy notional)`
# MAGIC   Positive = net cash inflow; negative = net cost/outflow.
# MAGIC
# MAGIC True realised/unrealised P&L requires a live market-data feed and is
# MAGIC outside the scope of this pipeline.

# COMMAND ----------

# Read from the persisted silver table so gold is always reproducible
# independently of whether cleaning ran in the same session.
df_silver = spark.table(SILVER_VALID)

# Trade-level direction: +1 for Buy, -1 for Sell.
# Mirrors the _DIRECTION dict in pnl.py.
direction_expr = F.when(F.col("side") == "Buy", F.lit(1.0)).otherwise(F.lit(-1.0))

df_trade_pnl = df_silver.withColumn("gross_pnl", F.col("notional") * direction_expr)

# COMMAND ----------

# ---------------------------------------------------------------------------
# pnl_by_symbol — one row per (symbol, asset_class, currency)
# Mirrors compute_pnl_by_symbol().
# ---------------------------------------------------------------------------
# Buy and sell notionals are computed separately then joined, because a symbol
# may have only buys or only sells and we need 0 rather than null in that case.

buy_notional = (
    df_trade_pnl.filter(F.col("side") == "Buy")
    .groupBy("symbol", "asset_class", "currency")
    .agg(F.sum("notional").alias("buy_notional"))
)

sell_notional = (
    df_trade_pnl.filter(F.col("side") == "Sell")
    .groupBy("symbol", "asset_class", "currency")
    .agg(F.sum("notional").alias("sell_notional"))
)

pnl_totals = (
    df_trade_pnl
    .groupBy("symbol", "asset_class", "currency")
    .agg(
        F.sum("notional").alias("total_notional"),
        F.sum("gross_pnl").alias("net_pnl"),
        F.count("trade_id").alias("trade_count"),
    )
)

pnl_by_symbol = (
    pnl_totals
    .join(buy_notional,  on=["symbol", "asset_class", "currency"], how="left")
    .join(sell_notional, on=["symbol", "asset_class", "currency"], how="left")
    .fillna(0.0, subset=["buy_notional", "sell_notional"])
    .select(
        "symbol", "asset_class", "currency",
        "buy_notional", "sell_notional", "total_notional",
        "net_pnl", "trade_count",
    )
)

print(f"pnl_by_symbol: {pnl_by_symbol.count()} symbols")
pnl_by_symbol.show()

# COMMAND ----------

# ---------------------------------------------------------------------------
# pnl_by_trader — one row per trader
# Mirrors compute_pnl_by_trader().
# ---------------------------------------------------------------------------
pnl_by_trader = (
    df_trade_pnl
    .groupBy("trader")
    .agg(
        F.sum("notional").alias("total_notional"),
        F.sum("gross_pnl").alias("net_pnl"),
        F.count("trade_id").alias("trade_count"),
    )
)

print(f"pnl_by_trader: {pnl_by_trader.count()} traders")
pnl_by_trader.show()

# COMMAND ----------

# ---------------------------------------------------------------------------
# pnl_summary — one row per (asset_class, currency)
# Mirrors compute_pnl_summary().  Useful as a top-level dashboard rollup.
# ---------------------------------------------------------------------------
pnl_summary = (
    df_trade_pnl
    .groupBy("asset_class", "currency")
    .agg(
        F.sum("notional").alias("total_notional"),
        F.sum("gross_pnl").alias("net_pnl"),
        F.count("trade_id").alias("trade_count"),
    )
)

print(f"pnl_summary: {pnl_summary.count()} asset-class/currency groups")
pnl_summary.show()

# COMMAND ----------

# Write all three P&L tables to Delta.
for table_name, df in [
    (GOLD_PNL_BY_SYMBOL, pnl_by_symbol),
    (GOLD_PNL_BY_TRADER, pnl_by_trader),
    (GOLD_PNL_SUMMARY,   pnl_summary),
]:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Gold: wrote to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Gold — Position Aggregations
# MAGIC
# MAGIC **Mirrors:** `src/trade_blotter/gold/positions.py`
# MAGIC
# MAGIC Position model:
# MAGIC - `signed_quantity = quantity × direction`  (+1 Buy, −1 Sell)
# MAGIC - `net_position per symbol = Σ(signed_quantity)`  (+ve = long, −ve = short)
# MAGIC - Weighted-average prices are computed separately per side.
# MAGIC
# MAGIC Three outputs:
# MAGIC - `positions` — per-trader breakdown (intermediate, not written to Delta)
# MAGIC - `net_positions` — firm-wide, collapsed across traders
# MAGIC - `position_snapshot` — net_positions with an `as_of_date` column

# COMMAND ----------

trader_keys = ["symbol", "asset_class", "currency", "trader"]
symbol_keys = ["symbol", "asset_class", "currency"]

# Signed quantity for net position calculation.
# Mirrors the signed_qty column in compute_positions().
df_pos = df_silver.withColumn(
    "signed_qty",
    F.col("quantity").cast(DoubleType()) * direction_expr,
)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Per-trader positions (intermediate)
# Mirrors compute_positions().
# ---------------------------------------------------------------------------

# Buy and sell quantities — split before aggregating so each side is summed
# independently (a trader with only buys should have sell_qty = 0, not null).
buy_qty = (
    df_pos.filter(F.col("side") == "Buy")
    .groupBy(trader_keys)
    .agg(F.sum("quantity").cast(DoubleType()).alias("buy_qty"))
)

sell_qty = (
    df_pos.filter(F.col("side") == "Sell")
    .groupBy(trader_keys)
    .agg(F.sum("quantity").cast(DoubleType()).alias("sell_qty"))
)

net_and_count = (
    df_pos
    .groupBy(trader_keys)
    .agg(
        F.sum("signed_qty").alias("net_position"),
        F.count("trade_id").alias("trade_count"),
    )
)

# Quantity-weighted average prices.
# avg_buy_price  = Σ(quantity × price) / Σ(quantity)  for Buy trades
# avg_sell_price = Σ(quantity × price) / Σ(quantity)  for Sell trades
# Mirrors the notional_qty divide in compute_positions().
avg_buy_price = (
    df_pos.filter(F.col("side") == "Buy")
    .groupBy(trader_keys)
    .agg(
        (
            F.sum(F.col("quantity").cast(DoubleType()) * F.col("price"))
            / F.sum(F.col("quantity").cast(DoubleType()))
        ).alias("avg_buy_price")
    )
)

avg_sell_price = (
    df_pos.filter(F.col("side") == "Sell")
    .groupBy(trader_keys)
    .agg(
        (
            F.sum(F.col("quantity").cast(DoubleType()) * F.col("price"))
            / F.sum(F.col("quantity").cast(DoubleType()))
        ).alias("avg_sell_price")
    )
)

positions = (
    net_and_count
    .join(buy_qty,       on=trader_keys, how="left")
    .join(sell_qty,      on=trader_keys, how="left")
    .join(avg_buy_price, on=trader_keys, how="left")
    .join(avg_sell_price, on=trader_keys, how="left")
    .fillna(0.0, subset=["buy_qty", "sell_qty"])
    .select(
        *trader_keys,
        "buy_qty", "sell_qty", "net_position",
        "avg_buy_price", "avg_sell_price", "trade_count",
    )
)

print(f"positions (per-trader): {positions.count()} rows")

# COMMAND ----------

# ---------------------------------------------------------------------------
# net_positions — firm-wide, collapsed across all traders
# Mirrors compute_net_positions().
# ---------------------------------------------------------------------------
net_positions = (
    positions
    .groupBy(symbol_keys)
    .agg(
        F.sum("buy_qty").alias("buy_qty"),
        F.sum("sell_qty").alias("sell_qty"),
        F.sum("net_position").alias("net_position"),
        F.sum("trade_count").alias("trade_count"),
    )
)

print(f"net_positions: {net_positions.count()} symbols")
net_positions.show()

# COMMAND ----------

# ---------------------------------------------------------------------------
# position_snapshot — net_positions with an as_of_date column
# Mirrors compute_position_snapshot().
# as_of_date defaults to the latest trade_date in the silver table.
# ---------------------------------------------------------------------------
as_of_date = df_silver.agg(F.max("trade_date")).collect()[0][0]

position_snapshot = net_positions.withColumn("as_of_date", F.lit(as_of_date))

print(f"position_snapshot as of {as_of_date}: {position_snapshot.count()} rows")
position_snapshot.show()

# COMMAND ----------

# Write net_positions and position_snapshot to Delta.
for table_name, df in [
    (GOLD_NET_POSITIONS,    net_positions),
    (GOLD_POSITION_SNAPSHOT, position_snapshot),
]:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Gold: wrote to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Pipeline Summary

# COMMAND ----------

# Print a final row-count summary across all layers — equivalent to the
# _log_summary() output in pipeline.py.

summary = {
    "bronze.trades":            BRONZE_TRADES,
    "silver.trades_valid":      SILVER_VALID,
    "silver.trades_rejected":   SILVER_REJECTED,
    "silver.trades_warnings":   SILVER_WARNINGS,
    "gold.pnl_by_symbol":       GOLD_PNL_BY_SYMBOL,
    "gold.pnl_by_trader":       GOLD_PNL_BY_TRADER,
    "gold.pnl_summary":         GOLD_PNL_SUMMARY,
    "gold.net_positions":       GOLD_NET_POSITIONS,
    "gold.position_snapshot":   GOLD_POSITION_SNAPSHOT,
}

print("=" * 60)
print("  Pipeline Summary")
print("=" * 60)
for label, table_name in summary.items():
    try:
        count = spark.table(table_name).count()
        print(f"  {label:<35} {count:>8,} rows  →  {table_name}")
    except Exception as e:
        print(f"  {label:<35}   ERROR: {e}")
print("=" * 60)
