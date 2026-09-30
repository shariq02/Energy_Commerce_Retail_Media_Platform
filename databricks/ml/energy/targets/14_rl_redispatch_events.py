# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # REDISPATCH EVENT TRAJECTORIES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** episode by step records for the old redispatch regime: one episode per
# MAGIC operator and day, one step per intervention, reward minus the energy
# MAGIC volume. Steps are irregular events, not fixed intervals.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "redispatch"
COMPONENT = "ml/energy/targets/rl_redispatch_events"
TABLE = "target_rl_redispatch_events"
REGIME_START = SPLIT_CALENDARS["redispatch"]["train"][0]
REGIME_END = SPLIT_CALENDARS["redispatch"]["test"][1]
TSOS = ["fifty_hertz", "amprion", "tennet_de", "transnetbw"]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Read the events
candidates = read_gold("grid_intervention_event", source=SOURCE).filter(
    F.to_date("event_start_project").between(REGIME_START, REGIME_END)
)

# COMMAND ----------

# DBTITLE 1,Steps within each operator-day episode
_valid = candidates.filter(
    F.col("instructing_market_area_code").isin(*TSOS)
    & (F.col("duration_hours") >= 0)
    & F.col("energy_mwh").isNotNull()
)
_w = Window.partitionBy(
    "instructing_market_area_code", F.to_date("event_start_project")
).orderBy("event_start_utc", "event_key")
out = (
    _valid.select(
        "event_key",
        F.col("instructing_market_area_code").alias("market_area_code"),
        F.to_date("event_start_project").alias("local_date"),
        "event_start_utc",
        F.row_number().over(_w).alias("step"),
        F.col("direction").alias("action_direction"),
        F.col("reason").alias("action_reason"),
        F.col("energy_mwh").alias("action_energy_mwh"),
        (-F.col("energy_mwh")).alias("reward"),
    )
    .withColumn(
        "episode_id",
        F.concat_ws("|", "market_area_code", F.col("local_date").cast("string")),
    )
    .withColumn("provenance_tier", F.lit("rule_derived"))
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Record the dropped rows
_dropped = candidates.count() - out.count()
drop_block = ("dropped_rows", markdown_table(["rows_dropped"], [(_dropped,)]))
print(f"rows dropped as invalid: {_dropped}")

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["event_key"]
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
