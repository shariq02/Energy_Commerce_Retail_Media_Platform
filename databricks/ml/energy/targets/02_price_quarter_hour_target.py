# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # PRICE QUARTER-HOUR TARGET
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** quarter-hour day-ahead price from 2024-09-08 and the negative-price flag.
# MAGIC Never pooled with the daily series.

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
COMPONENT = "ml/energy/targets/price_quarter_hour"
TABLE = "target_price_quarter_hour"
MARKET_AREA = "de_lu"
QH_FROM = SPLIT_CALENDARS["quarter_hour"]["train"][0]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Read the quarter-hour prices
candidates = read_gold("market_price", source=SOURCE).filter(
    (F.col("interval_reference") == "clock")
    & (F.col("interval_seconds") == 900)
    & (F.col("market_area_code") == MARKET_AREA)
    & (F.col("local_date") >= QH_FROM)
)

# COMMAND ----------

# DBTITLE 1,Keep valid targets
out = (
    candidates.filter(F.col("price_eur_per_mwh").isNotNull())
    .select(
        "market_area_code",
        F.col("observation_timestamp_utc").alias("interval_start_utc"),
        "local_date",
        F.col("price_eur_per_mwh").alias("target_price_eur_per_mwh"),
        (F.col("price_eur_per_mwh") < 0).alias("target_is_negative_price"),
    )
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
_GRAIN = ["market_area_code", "interval_start_utc"]
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
