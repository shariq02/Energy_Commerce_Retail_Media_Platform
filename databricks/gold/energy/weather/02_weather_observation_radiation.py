# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- WEATHER_OBSERVATION_RADIATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.weather_observation_radiation` -- the 3 solar-
# MAGIC file families, kept apart from `weather_observation` because they carry
# MAGIC an extra context column (`true_solar_time_native`, DWD only) and a
# MAGIC distinct `interval_reference` tag; the instant grid itself is not
# MAGIC shifted and can coincide with `weather_observation`'s. Same generic
# MAGIC union-then-left-join build as `01_weather_observation.py`.

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
SOURCE = "dwd"
COMPONENT = "gold/energy/weather/weather_observation_radiation"
TABLE = "weather_observation_radiation"

FAMILIES = [
    "weather_solar_radiation",
    "weather_longwave_radiation",
    "weather_solar_geometry",
]
GRAIN_KEY = [
    "location_key",
    "observation_timestamp_utc",
    "interval_seconds",
    "interval_reference",
]
SHARED_CONTEXT = [
    "location_key",
    "source_location_id",
    "observation_timestamp_native",
    "time_basis",
    "utc_offset_hours",
    "observation_timestamp_utc",
    "observation_timestamp_project",
    "local_date",
    "interval_seconds",
    "interval_reference",
    "true_solar_time_native",
    # real per-row origin (dwd/honda_iot/accuweather -- location_key is source-
    # specific, so this is uniform per grain key); add_gold_provenance below
    # would otherwise overwrite a bare source_system with the constant "dwd"
    # and mislabel every Honda/AccuWeather-origin row.
    "source_system",
]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read every family, split into context and prefixed value columns
_family_dfs = {
    f: read_silver(f).drop("observation_key", "_silver_loaded_at", "_silver_run_id")
    for f in FAMILIES
}
_context_frames = [df.select(*SHARED_CONTEXT) for df in _family_dfs.values()]
_value_frames = {}
for family, df in _family_dfs.items():
    short = family.removeprefix("weather_")
    value_cols = [c for c in df.columns if c not in SHARED_CONTEXT]
    _value_frames[short] = df.select(
        *GRAIN_KEY, *[F.col(c).alias(f"{short}__{c}") for c in value_cols]
    )

# COMMAND ----------

# DBTITLE 1,Canonical context -- one row per distinct grain key
context = _context_frames[0]
for cf in _context_frames[1:]:
    context = context.unionByName(cf)
context = context.dropDuplicates(GRAIN_KEY)

# COMMAND ----------

# DBTITLE 1,Join every family's own columns onto the canonical context
weather_observation_radiation = context
for short, vf in _value_frames.items():
    weather_observation_radiation = weather_observation_radiation.join(
        vf, GRAIN_KEY, "left"
    )

# COMMAND ----------

# DBTITLE 1,Derive families_observed
_presence_flags = [
    F.when(F.col(f"{short}__quality_code").isNotNull(), F.lit(short))
    for short in _value_frames
]
weather_observation_radiation = weather_observation_radiation.withColumn(
    "families_observed",
    F.filter(F.array(*_presence_flags), lambda x: x.isNotNull()),
)

# COMMAND ----------

# DBTITLE 1,Build the Gold row
weather_observation_radiation = weather_observation_radiation.withColumnRenamed(
    "source_system", "origin_source_system"
)
weather_observation_radiation = add_gold_provenance(
    weather_observation_radiation, SOURCE, rid
)

# COMMAND ----------

# DBTITLE 1,Grain gate
assert_unique_grain(
    weather_observation_radiation,
    GRAIN_KEY,
    component=COMPONENT,
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(
    weather_observation_radiation, TABLE, source=SOURCE, component=COMPONENT, rid=rid
)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    weather_observation_radiation,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=GRAIN_KEY,
)
write_gold_findings(SOURCE, f"weather__{TABLE}", TABLE, _blocks)
