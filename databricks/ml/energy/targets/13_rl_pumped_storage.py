# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # PUMPED STORAGE TRAJECTORIES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** episode by step records for pumped-storage arbitrage: state price and
# MAGIC residual load, action net pumped output, reward price times net energy.
# MAGIC The action is the aggregate of many plants.

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
COMPONENT = "ml/energy/targets/rl_pumped_storage"
TABLE = "target_rl_pumped_storage"
MARKET_AREA = "de_lu"
QH_FROM = SPLIT_CALENDARS["quarter_hour"]["train"][0]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Session time zone
set_utc_session()

# COMMAND ----------

# DBTITLE 1,Quarter-hour series
_b = read_gold("energy_balance_component", source=SOURCE).filter(
    (F.col("market_area_code") == MARKET_AREA)
    & (F.col("interval_reference") == "clock")
    & (F.col("interval_seconds") == 900)
    & (F.col("local_date") >= QH_FROM)
)
generation = (
    _b.filter(
        (F.col("component_kind") == "generation")
        & (F.col("carrier_code") == "pumped_storage")
    )
    .groupBy("interval_start_utc", "local_date")
    .agg(F.sum("energy_mwh").alias("action_generation_mwh"))
)
consumption = (
    _b.filter(F.col("component_kind") == "pumped_storage_consumption")
    .groupBy("interval_start_utc")
    .agg(F.sum("energy_mwh").alias("action_consumption_mwh"))
)
residual = (
    _b.filter(F.col("component_kind") == "residual_load")
    .groupBy("interval_start_utc")
    .agg(F.sum("energy_mwh").alias("state_residual_load_mwh"))
)
price = (
    read_gold("market_price", source=SOURCE)
    .filter(
        (F.col("market_area_code") == MARKET_AREA)
        & (F.col("interval_reference") == "clock")
        & (F.col("interval_seconds") == 900)
    )
    .select(
        F.col("observation_timestamp_utc").alias("interval_start_utc"),
        F.col("price_eur_per_mwh").alias("state_price_eur_per_mwh"),
    )
)

# COMMAND ----------

# DBTITLE 1,Join into steps
candidates = (
    generation.join(consumption, "interval_start_utc", "full")
    .join(residual, "interval_start_utc", "left")
    .join(price, "interval_start_utc", "left")
)
candidates = candidates.withColumn(
    "local_date",
    F.coalesce(
        F.col("local_date"),
        F.to_date(F.from_utc_timestamp("interval_start_utc", PROJECT_TIMEZONE)),
    ),
)

# COMMAND ----------

# DBTITLE 1,Episode, step, net action and reward
_start = local_day_start_utc("local_date")
out = (
    candidates.filter(
        F.col("state_price_eur_per_mwh").isNotNull()
        & (
            F.col("action_generation_mwh").isNotNull()
            | F.col("action_consumption_mwh").isNotNull()
        )
    )
    .withColumn("episode_id", F.col("local_date").cast("string"))
    .withColumn(
        "step",
        ((F.col("interval_start_utc").cast("long") - _start.cast("long")) / 900).cast(
            "int"
        ),
    )
    .withColumn(
        "action_net_mwh",
        F.coalesce(F.col("action_generation_mwh"), F.lit(0.0))
        - F.coalesce(F.col("action_consumption_mwh"), F.lit(0.0)),
    )
    .withColumn(
        "reward_eur", F.col("state_price_eur_per_mwh") * F.col("action_net_mwh")
    )
    .withColumn("provenance_tier", F.lit("constructed"))
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Record the dropped rows
_dropped = candidates.count() - out.count()
drop_block = ("dropped_rows", markdown_table(["rows_dropped"], [(_dropped,)]))
print(f"rows dropped as invalid: {_dropped}")

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["episode_id", "step"]
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
