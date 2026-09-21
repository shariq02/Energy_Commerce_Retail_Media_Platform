# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- WEATHER OBSERVATION (HONDA SITE)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Honda on-site air temperature and irradiance into
# MAGIC `weather_observation`, all three sampling frequencies kept.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../_semantic_common

# COMMAND ----------

# DBTITLE 1,Weather specifications
# MAGIC %run ./_weather_specs

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "honda_iot"
COMPONENT = "silver/semantic/weather/03_weather_observation_honda"
RID = run_id()
BRONZE_TABLE = "honda_iot_weather"
SITE_ID = "honda_site"

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Bronze -- honda_iot_weather
bronze_honda_weather = read_bronze(BRONZE_TABLE)

# COMMAND ----------

# DBTITLE 1,Transform -- long rows
honda_long = long_from_spec(
    bronze_honda_weather, HONDA_SPECS, ["frequency", "datetime_utc"]
)

# COMMAND ----------

# DBTITLE 1,Transform -- time, place, provenance
_interval = F.create_map(
    [F.lit(x) for kv in HONDA_INTERVAL_SECONDS.items() for x in kv]
)
honda_obs = (
    honda_long.withColumn("interval_seconds", _interval[F.col("frequency")].cast("int"))
    .withColumn("source_location_id", F.lit(SITE_ID))
    .withColumn("location_key", location_key("honda_iot", "source_location_id"))
    .withColumn("observation_ts_native", F.col("datetime_utc"))
    .withColumn("time_basis", F.lit("utc"))
    .withColumn("observation_ts_utc", F.col("datetime_utc").cast("timestamp"))
    .withColumn("measurement_basis", F.lit("site_sensor"))
    .withColumn(
        "source_record_id", sha_key(F.lit(BRONZE_TABLE), "frequency", "datetime_utc")
    )
    .withColumn(
        "observation_key",
        sha_key(F.lit(BRONZE_TABLE), "frequency", "datetime_utc", "source_column"),
    )
)
honda_obs = add_project_time(honda_obs)
honda_obs = add_semantic_provenance(honda_obs, SOURCE, BRONZE_TABLE, RID)
honda_obs = conform(honda_obs, WEATHER_OBSERVATION_COLUMNS)

# COMMAND ----------

# DBTITLE 1,Write Silver -- weather_observation (honda_iot_weather)
write_semantic(
    honda_obs,
    "weather_observation",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    replace_where=f"source_dataset = '{BRONZE_TABLE}'",
)

# COMMAND ----------

# DBTITLE 1,Inspect -- weather_observation (honda)
written = spark.table(semantic_table("weather_observation")).filter(
    F.col("source_dataset") == BRONZE_TABLE
)
findings_blocks = inspect_table(
    written,
    "weather_observation",
    source=FINDINGS_SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["observation_key"],
    extra_checks=structure_extra_checks(written),
)

# COMMAND ----------

# DBTITLE 1,Export findings -- weather_observation (honda)
write_silver_findings(
    FINDINGS_SOURCE,
    f"{COMPONENT.split('/')[-1]}__{BRONZE_TABLE}",
    f"weather_observation -- {BRONZE_TABLE}",
    findings_blocks,
)
