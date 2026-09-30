# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # REDISPATCH TARGET
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** two-part target per operator and day for the old regime: whether any
# MAGIC intervention occurred and the log energy given one. Negative-duration rows
# MAGIC and non-German operators are excluded.

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
COMPONENT = "ml/energy/targets/redispatch"
TABLE = "target_redispatch_daily"
REGIME_START = SPLIT_CALENDARS["redispatch"]["train"][0]
REGIME_END = SPLIT_CALENDARS["redispatch"]["test"][1]
TSOS = ["fifty_hertz", "amprion", "tennet_de", "transnetbw"]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Read the events
all_events = read_gold("grid_intervention_event", source=SOURCE)
candidates = all_events.filter(
    F.to_date("event_start_project").between(REGIME_START, REGIME_END)
)

# COMMAND ----------

# DBTITLE 1,Valid events per operator and start day
valid = (
    candidates.filter(
        F.col("instructing_market_area_code").isin(*TSOS)
        & (F.col("duration_hours") >= 0)
        & F.col("energy_mwh").isNotNull()
    )
    .withColumn("local_date", F.to_date("event_start_project"))
    .groupBy(
        F.col("instructing_market_area_code").alias("market_area_code"), "local_date"
    )
    .agg(
        F.count("*").alias("event_count"), F.sum("energy_mwh").alias("event_energy_mwh")
    )
)

# COMMAND ----------

# DBTITLE 1,Dense grid with the two-part target
_dates = spark.range(1).select(
    F.explode(
        F.sequence(F.lit(REGIME_START).cast("date"), F.lit(REGIME_END).cast("date"))
    ).alias("local_date")
)
_grid = _dates.crossJoin(
    spark.createDataFrame([(t,) for t in TSOS], "market_area_code string")
)
out = (
    _grid.join(valid, ["market_area_code", "local_date"], "left")
    .withColumn("event_count", F.coalesce(F.col("event_count"), F.lit(0)))
    .withColumn("target_any_event", F.col("event_count") > 0)
    .withColumn(
        "target_event_energy_mwh",
        F.when(F.col("target_any_event"), F.col("event_energy_mwh")),
    )
    .withColumn(
        "target_log_event_energy_mwh",
        F.when(
            F.col("target_any_event") & (F.col("event_energy_mwh") > 0),
            F.log1p(F.col("event_energy_mwh")),
        ),
    )
    .drop("event_energy_mwh")
    .withColumn("provenance_tier", F.lit("sourced"))
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Record the excluded events
_neg = candidates.filter(F.col("duration_hours") < 0).count()
_other = candidates.filter(
    ~F.col("instructing_market_area_code").isin(*TSOS)
    | F.col("instructing_market_area_code").isNull()
).count()
drop_block = (
    "excluded_events",
    markdown_table(
        ["reason", "events"],
        [("negative_duration", _neg), ("other_or_missing_operator", _other)],
    ),
)
print(f"excluded events: negative duration {_neg}, other operator {_other}")

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
