# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ZONE CAPACITY AS-OF
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** wind capacity per control zone and carrier for every date: units
# MAGIC commissioned on or before the date and not yet finally decommissioned. The
# MAGIC clock is the commissioning date.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "mastr"
COMPONENT = "ml/energy/features/zone_capacity_asof"
TABLE = "features_zone_capacity_asof"
DATE_FROM = SPLIT_CALENDARS["energy_daily"]["train"][0]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Wind units that have a commissioning date
units = wind_units(read_gold("generation_unit", source=SOURCE)).filter(
    F.col("lifecycle_state").isin(*OPERATING_STATES)
    & F.col("commissioning_date").isNotNull()
    & F.col("capacity_net_kw").isNotNull()
)

# COMMAND ----------

# DBTITLE 1,Zone chain: unit, location, connection point, control zone
zmap = unit_zone_map(
    units,
    read_gold("grid_connection_point", source=SOURCE),
    read_ml("zone_code_map", ecosystem=ECO),
)
zoned = (
    units.join(zmap, "unit_id")
    .filter(F.col("zone_status") == "zoned")
    .withColumn(
        "carrier",
        F.when(F.col("is_offshore"), "wind_offshore").otherwise("wind_onshore"),
    )
)

# COMMAND ----------

# DBTITLE 1,Capacity changes per zone, carrier and date
_add = zoned.select(
    "market_area_code",
    "carrier",
    F.to_date("commissioning_date").alias("local_date"),
    (F.col("capacity_net_kw") / 1000).alias("delta_mw"),
    F.lit(1).alias("delta_units"),
)
_sub = zoned.filter(F.col("final_decommissioning_date").isNotNull()).select(
    "market_area_code",
    "carrier",
    F.to_date("final_decommissioning_date").alias("local_date"),
    (-F.col("capacity_net_kw") / 1000).alias("delta_mw"),
    F.lit(-1).alias("delta_units"),
)
changes = _add.unionByName(_sub)

# COMMAND ----------

# DBTITLE 1,Date spine to the last market date
_end = read_gold("market_price", source="smard").agg(F.max("local_date")).first()[0]
spine = (
    changes.select("market_area_code", "carrier")
    .distinct()
    .crossJoin(
        spark.range(1).select(
            F.explode(F.sequence(F.lit(DATE_FROM).cast("date"), F.lit(_end))).alias(
                "local_date"
            )
        )
    )
    .withColumn("delta_mw", F.lit(0.0))
    .withColumn("delta_units", F.lit(0))
)

# COMMAND ----------

# DBTITLE 1,Cumulative capacity as of each date
_keys = ["market_area_code", "carrier"]
_daily = (
    changes.unionByName(spine)
    .groupBy(*_keys, "local_date")
    .agg(F.sum("delta_mw").alias("delta_mw"), F.sum("delta_units").alias("delta_units"))
)
_w = (
    Window.partitionBy(*_keys)
    .orderBy("local_date")
    .rowsBetween(Window.unboundedPreceding, 0)
)
out = (
    _daily.withColumn("capacity_net_mw", F.sum("delta_mw").over(_w))
    .withColumn("unit_count", F.sum("delta_units").over(_w))
    .filter(F.col("local_date") >= F.lit(DATE_FROM).cast("date"))
    .select(*_keys, "local_date", "capacity_net_mw", "unit_count")
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Report the units left out of the zone chain
_total = units.agg(F.sum("capacity_net_kw")).first()[0] or 0.0
_rows = [
    (
        r["zone_status"],
        r["n"],
        round((r["kw"] or 0.0) / 1000, 1),
        round(100 * (r["kw"] or 0.0) / _total, 2),
    )
    for r in units.join(zmap, "unit_id")
    .groupBy("zone_status")
    .agg(F.count("*").alias("n"), F.sum("capacity_net_kw").alias("kw"))
    .collect()
]
exclusion_block = (
    "zone_chain_exclusions",
    markdown_table(["zone_status", "units", "capacity_mw", "share_percent"], _rows),
)
for _r in _rows:
    print(_r)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["market_area_code", "carrier", "local_date"]
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
write_ml_findings(ECO, "features__" + TABLE, TABLE, _blocks)

# COMMAND ----------

# DBTITLE 1,Add the exclusion report to the findings
write_ml_findings(
    ECO,
    "features__" + TABLE + "__exclusions",
    TABLE + " zone chain exclusions",
    [exclusion_block],
)
