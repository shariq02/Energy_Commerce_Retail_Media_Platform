# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- HONDA SITE WEATHER
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `honda_iot_weather` into source-scoped Silver at its
# MAGIC (frequency, datetime_utc) grain. Two on-site sensors renamed to their
# MAGIC English business meaning (`air_temperature_2m`, `global_irradiance`), and
# MAGIC a single synthetic `weather_location = 'honda_site'` -- there is one
# MAGIC physical site and no source-provided location field.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "honda_iot"
COMPONENT = "silver/energy/weather/03_honda_weather"
RID = run_id()
BT = "honda_iot_weather"
SILVER_TABLE = "honda_weather"

RENAMES = {
    "WeatherStation_Weather_Ta": "air_temperature_2m",
    "WeatherStation_Weather_Igm": "global_irradiance",
}

# COMMAND ----------

# DBTITLE 1,honda_iot_weather -> Silver
df = read_bronze(BT)
for src, tgt in RENAMES.items():
    df = df.withColumnRenamed(src, tgt)
df = (
    df.withColumn("air_temperature_2m", F.col("air_temperature_2m").cast("double"))
    .withColumn("global_irradiance", F.col("global_irradiance").cast("double"))
    .withColumn("datetime_utc", F.col("datetime_utc").cast("timestamp"))
    .withColumn("weather_location", F.lit("honda_site"))
    .withColumn("_srid", sha_key(F.lit(BT), "frequency", "datetime_utc"))
)
df = add_provenance(df, SOURCE, "_srid", RID)
write_silver(df, SILVER_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print(f"HONDA SITE WEATHER -- COMPLETE  (run_id {RID})")
print("=" * 70)
