# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ZONE GENERATION TARGET
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** regional daily generation of onshore wind, offshore wind and photovoltaic
# MAGIC for the four control-zone market areas.

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
COMPONENT = "ml/energy/targets/zone_generation"
TABLE = "target_zone_generation"
DAY = "local_day_europe_berlin"
ZONES = ["fifty_hertz", "amprion", "tennet_de", "transnetbw"]
CARRIERS = ["onshore_wind", "offshore_wind", "photovoltaic"]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Read the regional generation
candidates = read_gold("energy_balance_component", source=SOURCE).filter(
    (F.col("component_kind") == "generation")
    & F.col("carrier_code").isin(*CARRIERS)
    & F.col("market_area_code").isin(*ZONES)
    & (F.col("interval_reference") == DAY)
)

# COMMAND ----------

# DBTITLE 1,Aggregate to the day
out = (
    candidates.groupBy(
        "market_area_code", "local_date", F.col("carrier_code").alias("carrier")
    )
    .agg(F.sum("energy_mwh").alias("target_generation_mwh"))
    .filter(F.col("target_generation_mwh").isNotNull())
    .withColumn("provenance_tier", F.lit("sourced"))
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Record the dropped rows
_dropped = (
    candidates.groupBy("market_area_code", "local_date", "carrier_code").count().count()
    - out.count()
)
drop_block = ("dropped_rows", markdown_table(["rows_dropped"], [(_dropped,)]))
print(f"rows dropped as invalid: {_dropped}")

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["market_area_code", "local_date", "carrier"]
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
