# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- PUBLISH SHARED_CONFORMED.DIM_WEATHER_CONTEXT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per (`ags_code`, `date_key`).
# MAGIC
# MAGIC **Sources:** `fact_weather_air_temperature`, `fact_weather_precipitation`
# MAGIC (Gold, energy_gold).
# MAGIC
# MAGIC **Serves use case:** the cross-ecosystem conformed weather-context
# MAGIC dimension `shared_conformed.dim_weather_context` -- a coarse daily
# MAGIC (Bundesland, day) summary, not the full observation-level `fact_weather`,
# MAGIC which stays energy-local per `03_create_shared_conformed.py`'s own
# MAGIC comment ("STRUCTURE ONLY -- published later from the energy weather Gold
# MAGIC layer").
# MAGIC
# MAGIC **Purpose:** the specific handoff `databricks/setup/03_create_shared_conformed.py`
# MAGIC already commits to -- aggregate `fact_weather_air_temperature` (mean) and
# MAGIC `fact_weather_precipitation` (sum) to (ags_code, date), write into
# MAGIC `shared_conformed.dim_weather_context`. `weather_regime` is left NULL --
# MAGIC no defensible derivation rule yet, never fabricated.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "gold/energy/weather/03_publish_shared_geography"
RID = gold_run_id()
GOLD_TABLE = "dim_weather_context"

# COMMAND ----------

# DBTITLE 1,Read Gold -- fact_weather_air_temperature
_air_temperature = read_gold("fact_weather_air_temperature", source="dwd")

# COMMAND ----------

# DBTITLE 1,Read Gold -- fact_weather_precipitation
_precipitation = read_gold("fact_weather_precipitation", source="dwd")

# COMMAND ----------

# DBTITLE 1,Transform -- daily average temperature by ags_code
_temp_daily = (
    _air_temperature.filter(
        F.col("ags_code").isNotNull() & F.col("air_temperature_2m").isNotNull()
    )
    .withColumn(
        "date_key", F.date_format(F.col("observation_ts"), "yyyyMMdd").cast("int")
    )
    .groupBy("ags_code", "date_key")
    .agg(F.avg("air_temperature_2m").alias("avg_temperature_c"))
)

# COMMAND ----------

# DBTITLE 1,Transform -- daily total precipitation by ags_code
_precip_daily = (
    _precipitation.filter(
        F.col("ags_code").isNotNull() & F.col("precipitation_hourly_total").isNotNull()
    )
    .withColumn(
        "date_key", F.date_format(F.col("observation_ts"), "yyyyMMdd").cast("int")
    )
    .groupBy("ags_code", "date_key")
    .agg(F.sum("precipitation_hourly_total").alias("total_precipitation_mm"))
)

# COMMAND ----------

# DBTITLE 1,Transform -- dim_weather_context
dim = _temp_daily.join(_precip_daily, ["ags_code", "date_key"], "full_outer")
dim = (
    dim.withColumn("weather_regime", F.lit(None).cast("string"))
    .withColumn("source_system", F.lit(SOURCE))
    .withColumn("published_from", F.lit(COMPONENT))
    .withColumn("created_at", F.current_timestamp())
    .withColumn(
        "weather_context_key",
        surrogate_key("ags_code", F.col("date_key").cast("string")),
    )
)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- dim_weather_context, one row per (ags_code, date_key)
assert_unique_grain(
    dim, ["ags_code", "date_key"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write shared_conformed -- dim_weather_context
write_gold(
    dim,
    GOLD_TABLE,
    schema=SHARED_CONFORMED_SCHEMA,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect dim_weather_context + export findings
_findings_blocks = inspect_gold_table(
    dim,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    schema=SHARED_CONFORMED_SCHEMA,
    key_cols=["ags_code", "date_key"],
)
write_gold_findings(
    SOURCE, f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}", GOLD_TABLE, _findings_blocks
)
