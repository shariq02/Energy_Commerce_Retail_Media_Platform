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
# MAGIC **Purpose:** Honda on-site air temperature and irradiance into the shared
# MAGIC `weather_temperature` and `weather_solar_radiation` structures alongside
# MAGIC the other sources, all three sampling frequencies kept.

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

# COMMAND ----------

# DBTITLE 1,Write Silver -- family structures (honda_iot_weather)
HONDA_FAMILIES = sorted({s["family"] for s in HONDA_SPECS})
write_semantic_families(
    honda_obs,
    HONDA_FAMILIES,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    replace_where_fn=lambda _fam: f"source_dataset = '{BRONZE_TABLE}'",
)

# COMMAND ----------

# DBTITLE 1,Inspect -- each family structure (honda)
findings_blocks = {}
for fam in HONDA_FAMILIES:
    written = spark.table(semantic_table(fam)).filter(
        F.col("source_dataset") == BRONZE_TABLE
    )
    findings_blocks[fam] = inspect_table(
        written,
        fam,
        source=FINDINGS_SOURCE,
        component=COMPONENT,
        rid=RID,
        key_cols=["observation_key"],
        extra_checks=structure_extra_checks(written),
    )

# COMMAND ----------

# DBTITLE 1,Export findings -- each family structure (honda)
for fam, blocks in findings_blocks.items():
    write_silver_findings(
        FINDINGS_SOURCE,
        f"{COMPONENT.split('/')[-1]}__{BRONZE_TABLE}__{fam}",
        f"{fam} -- {BRONZE_TABLE}",
        blocks,
    )
