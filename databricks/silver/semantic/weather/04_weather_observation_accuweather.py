# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- WEATHER OBSERVATION (ACCUWEATHER HISTORICAL HOURLY)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** AccuWeather historical hourly (metric) into the shared
# MAGIC per-parameter weather structures alongside DWD and Honda; the imperial
# MAGIC variant is the same record in other units and is not loaded. Provider
# MAGIC values, so `measurement_basis` differs from station sources.

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
SOURCE = "accuweather"
COMPONENT = "silver/semantic/weather/04_weather_observation_accuweather"
RID = run_id()
DATASET = "historical_hourly_metric"
SAMPLES_TABLE = f"samples.accuweather.{DATASET}"

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Samples -- historical_hourly_metric
aw_hourly_src = spark.table(SAMPLES_TABLE)

# COMMAND ----------

# DBTITLE 1,Transform -- long rows
aw_long = long_from_spec(
    aw_hourly_src, AW_SPECS, ["city_name", "datetime_valid_local", "gmt_offset"]
)

# COMMAND ----------

# DBTITLE 1,Transform -- time, place, provenance
aw_obs = (
    aw_long.withColumn("source_location_id", F.col("city_name"))
    .withColumn("location_key", location_key(SOURCE, "source_location_id"))
    .withColumn("observation_ts_native", F.col("datetime_valid_local").cast("string"))
    .withColumn("time_basis", F.lit("local_with_offset"))
    .withColumn("utc_offset_hours", F.col("gmt_offset").cast("double"))
    .withColumn(
        "observation_ts_utc",
        local_time_to_utc("datetime_valid_local", "utc_offset_hours"),
    )
    .withColumn("measurement_basis", F.lit("provider_historical"))
    .withColumn(
        "source_record_id", sha_key(F.lit(DATASET), "city_name", "datetime_valid_local")
    )
    .withColumn(
        "observation_key",
        sha_key(F.lit(DATASET), "city_name", "datetime_valid_local", "source_column"),
    )
)
aw_obs = add_project_time(aw_obs)
aw_obs = add_semantic_provenance(aw_obs, SOURCE, DATASET, RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- family structures (accuweather)
AW_FAMILIES = sorted({s["family"] for s in AW_SPECS})
write_semantic_families(
    aw_obs,
    AW_FAMILIES,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    replace_where_fn=lambda _fam: f"source_dataset = '{DATASET}'",
)

# COMMAND ----------

# DBTITLE 1,Inspect -- each family structure (aw)
findings_blocks = {}
for fam in AW_FAMILIES:
    written = spark.table(semantic_table(fam)).filter(
        F.col("source_dataset") == DATASET
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

# DBTITLE 1,Export findings -- each family structure (aw)
for fam, blocks in findings_blocks.items():
    write_silver_findings(
        FINDINGS_SOURCE,
        f"{COMPONENT.split('/')[-1]}__{DATASET}__{fam}",
        f"{fam} -- {DATASET}",
        blocks,
    )
