# Databricks notebook source

# COMMAND ----------

# MAGIC %pip install git+https://github.com/rayyu56/trade-blotter-pipeline.git

# COMMAND ----------

from trade_blotter.bronze.loader import load_csv
from trade_blotter.silver.validator import validate
from trade_blotter.silver.cleaner import clean
from trade_blotter.gold.pnl import compute_pnl_by_symbol, compute_pnl_by_trader, compute_pnl_summary
from trade_blotter.gold.positions import compute_net_positions, compute_position_snapshot

# COMMAND ----------

ingest_result = load_csv("/Workspace/Users/rayyu56@hotmail.com/trade-blotter-pipeline-repo/data/bronze/trades_20260401.csv")
df_bronze = ingest_result.dataframe
print(f"Loaded {ingest_result.row_count:,} rows")

# COMMAND ----------

validation_result = validate(df_bronze)
df_clean = clean(validation_result.valid)

print(f"Valid:    {len(validation_result.valid):,}")
print(f"Rejected: {len(validation_result.rejected):,}")
print(f"Warnings: {len(validation_result.warnings):,}")

# COMMAND ----------

pnl_by_symbol     = compute_pnl_by_symbol(df_clean)
pnl_by_trader     = compute_pnl_by_trader(df_clean)
pnl_summary       = compute_pnl_summary(df_clean)
net_positions     = compute_net_positions(df_clean)
position_snapshot = compute_position_snapshot(df_clean)

print(f"pnl_by_symbol:     {len(pnl_by_symbol):,} rows")
print(f"pnl_by_trader:     {len(pnl_by_trader):,} rows")
print(f"pnl_summary:       {len(pnl_summary):,} rows")
print(f"net_positions:     {len(net_positions):,} rows")
print(f"position_snapshot: {len(position_snapshot):,} rows")

# COMMAND ----------

tables = {
    "bronze_trades":         df_bronze,
    "silver_valid":          df_clean,
    "silver_rejected":       validation_result.rejected,
    "silver_warnings":       validation_result.warnings,
    "gold_pnl_by_symbol":    pnl_by_symbol,
    "gold_pnl_by_trader":    pnl_by_trader,
    "gold_pnl_summary":      pnl_summary,
    "gold_net_positions":    net_positions,
    "gold_position_snapshot": position_snapshot,
}

for table_name, df_pd in tables.items():
    (
        spark.createDataFrame(df_pd)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Wrote {len(df_pd):,} rows to {table_name}")

# COMMAND ----------

for table_name in tables:
    count = spark.sql(f"SELECT COUNT(*) AS n FROM {table_name}").collect()[0]["n"]
    print(f"  {table_name:<30} {count:>8,} rows")

spark.sql("SELECT symbol, net_pnl, trade_count FROM gold_pnl_by_symbol ORDER BY ABS(net_pnl) DESC LIMIT 5").show()
spark.sql("SELECT symbol, net_position, as_of_date FROM gold_position_snapshot ORDER BY ABS(net_position) DESC").show()
