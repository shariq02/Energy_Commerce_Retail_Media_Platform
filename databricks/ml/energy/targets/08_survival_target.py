# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # UNIT SURVIVAL TARGET
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** time from commissioning to final decommissioning for wind units at risk
# MAGIC from the window start, with left truncation and right censoring. Fully
# MAGIC deregistered units are absent from the registry, so survivorship is
# MAGIC declared.

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
COMPONENT = "ml/energy/targets/survival"
TABLE = "target_unit_survival"
WINDOW_START = SURVIVAL_WINDOW_START
CENSOR_DATE = SURVIVAL_CENSOR_DATE

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Wind units with a commissioning date
candidates = wind_units(read_gold("generation_unit", source=SOURCE)).filter(
    F.col("lifecycle_state").isin(*OPERATING_STATES)
    & F.col("commissioning_date").isNotNull()
)

# COMMAND ----------

# DBTITLE 1,Units at risk in the window
_w0, _c = F.lit(WINDOW_START).cast("date"), F.lit(CENSOR_DATE).cast("date")
_comm = F.to_date("commissioning_date")
_dec = F.to_date("final_decommissioning_date")
at_risk = candidates.filter((_comm <= _c) & (_dec.isNull() | (_dec >= _w0)))

# COMMAND ----------

# DBTITLE 1,Duration, entry time and event indicator in years
_event = _dec.isNotNull() & (_dec <= _c)
_stop = F.when(_event, _dec).otherwise(_c)
out = at_risk.select(
    "unit_id",
    _comm.alias("commissioning_date"),
    F.greatest(_comm, _w0).alias("entry_date"),
    _stop.alias("stop_date"),
    _event.alias("target_event"),
    (F.datediff(F.greatest(_comm, _w0), _comm) / 365.25).alias("entry_years"),
    (F.datediff(_stop, _comm) / 365.25).alias("target_duration_years"),
    (_comm < _w0).alias("left_truncated"),
).withColumn("provenance_tier", F.lit("rule_derived"))
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Record the units and events outside the window
_before = candidates.filter(_dec.isNotNull() & (_dec < _w0))
_by_year = [
    (r["y"], r["n"])
    for r in _before.groupBy(F.year(_dec).alias("y"))
    .agg(F.count("*").alias("n"))
    .orderBy("y")
    .collect()
]
drop_block = (
    "excluded_before_window",
    markdown_table(["decommissioning_year", "units"], _by_year),
)
print("events before the window by year:", _by_year)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["unit_id"]
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
