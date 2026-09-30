# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # PRICE DAILY TARGET
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** daily mean day-ahead price by market area and local date. Each year has
# MAGIC one 23-hour and one 25-hour day, kept as the day.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "smard"
COMPONENT = "ml/energy/targets/price_daily"
TABLE = "target_price_daily"
DAY = "local_day_europe_berlin"
MARKET_AREA = "de_lu"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Read the daily prices
candidates = (
    read_gold("market_price", source=SOURCE)
    .filter(
        (F.col("interval_reference") == DAY)
        & (F.col("market_area_code") == MARKET_AREA)
    )
    .groupBy("market_area_code", "local_date")
    .agg(
        F.avg("price_eur_per_mwh").alias("target_price_eur_per_mwh"),
        F.first("interval_seconds").alias("interval_seconds"),
    )
)

# COMMAND ----------

# DBTITLE 1,Keep valid targets
out = (
    candidates.filter(F.col("target_price_eur_per_mwh").isNotNull())
    .withColumn("regime_year", F.year("local_date"))
    .withColumn("provenance_tier", F.lit("sourced"))
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Record the dropped rows
_dropped = candidates.count() - out.count()
drop_block = ("dropped_rows", markdown_table(["rows_dropped"], [(_dropped,)]))
print(f"rows dropped as invalid: {_dropped}")

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["market_area_code", "local_date"]
assert_unique_grain(out, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid)
assert_no_forbidden_columns(out, component=COMPONENT, source=SOURCE, rid=rid)
check(
    COMPONENT,
    SOURCE,
    "non_empty",
    out.limit(1).count() > 0,
    detail="no rows produced; check the input filters",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_ml(out, TABLE, ecosystem=ECO, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=_GRAIN
)
write_ml_findings(ECO, "targets__" + TABLE, TABLE, _blocks)

# COMMAND ----------

# DBTITLE 1,Add the drop count to the findings
write_ml_findings(ECO, "targets__" + TABLE + "__drops", TABLE + " drops", [drop_block])
