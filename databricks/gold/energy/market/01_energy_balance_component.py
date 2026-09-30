# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- ENERGY_BALANCE_COMPONENT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.energy_balance_component` -- Silver
# MAGIC `electricity_balance`'s nested `components` array exploded to one row
# MAGIC per component, plus `carrier_key` (resolved via
# MAGIC `ref_energy_carrier_map`). Grain: place x interval start x interval x
# MAGIC interval reference x component x source.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Gold inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "smard"
COMPONENT = "gold/energy/market/energy_balance_component"
TABLE = "energy_balance_component"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
electricity_balance = read_silver("electricity_balance")
carrier_map = read_gold(
    "ref_energy_carrier_map", source=SOURCE, schema=SHARED_CONFORMED_SCHEMA
)

# COMMAND ----------

# DBTITLE 1,Explode components -- one row per element
# origin_source_system preserves the real per-row Silver source (smard/honda_iot);
# add_gold_provenance below overwrites a column literally named source_system with
# this table's own SOURCE constant, which would otherwise erase the distinction.
energy_balance_component = (
    electricity_balance.select(
        "location_key",
        "source_location_id",
        "market_area_code",
        "observation_timestamp_native",
        "time_basis",
        "utc_offset_hours",
        F.col("observation_timestamp_utc").alias("interval_start_utc"),
        "observation_timestamp_project",
        "local_date",
        "interval_seconds",
        "interval_reference",
        F.explode("components").alias("_component"),
        "sign_convention",
        "quality_flags",
        "measurement_basis",
        F.col("source_system").alias("origin_source_system"),
        "source_dataset",
        "source_record_id",
    )
    .select(
        "*",
        F.col("_component.component_kind").alias("component_kind"),
        F.col("_component.carrier_code").alias("carrier_code"),
        F.col("_component.native_label").alias("native_label"),
        F.col("_component.energy_mwh").alias("energy_mwh"),
        F.col("_component.power_w").alias("power_w"),
        F.col("_component.source_series").alias("component_source_series"),
        F.col("_component.source_filter_id").alias("source_filter_id"),
    )
    .drop("_component")
)

# COMMAND ----------

# DBTITLE 1,Resolve carrier_key
_carrier = carrier_map.filter(
    F.col("source_vocabulary") == "electricity_balance.components.carrier_code"
).select(
    F.col("source_value").alias("carrier_code"),
    "origin_source_system",
    "carrier_key",
)
energy_balance_component = energy_balance_component.join(
    _carrier, ["carrier_code", "origin_source_system"], "left"
)

# COMMAND ----------

# DBTITLE 1,Build the Gold row
energy_balance_component = add_gold_provenance(energy_balance_component, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate
_GRAIN = [
    "location_key",
    "market_area_code",
    "interval_start_utc",
    "interval_seconds",
    "interval_reference",
    "component_kind",
    "carrier_code",
    "native_label",
    "source_filter_id",
    "origin_source_system",
]
assert_unique_grain(
    energy_balance_component, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(energy_balance_component, TABLE, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    energy_balance_component,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=_GRAIN,
)
write_gold_findings(SOURCE, f"market__{TABLE}", TABLE, _blocks)
