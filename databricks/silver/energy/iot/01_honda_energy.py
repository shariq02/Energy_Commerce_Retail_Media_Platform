# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- HONDA ENERGY (ELECTRICITY / HEATING / COOLING)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the six Honda IoT power (`_p`) and energy (`_w`) Bronze
# MAGIC tables into source-scoped Silver at their (frequency, datetime_utc) grain.
# MAGIC The channel columns keep their source names and their sign -- a negative
# MAGIC value is on-site generation / export, not an error.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "honda_iot"
COMPONENT = "silver/energy/iot/01_honda_energy"
RID = run_id()

# Bronze table -> Silver table (the `_iot` infix is dropped).
TABLE_MAP = {
    "honda_iot_electricity_p": "honda_electricity_p",
    "honda_iot_electricity_w": "honda_electricity_w",
    "honda_iot_heating_p": "honda_heating_p",
    "honda_iot_heating_w": "honda_heating_w",
    "honda_iot_cooling_p": "honda_cooling_p",
    "honda_iot_cooling_w": "honda_cooling_w",
}

# COMMAND ----------

# DBTITLE 1,One Honda energy table -> Silver


def process(bt: str, silver_table: str) -> None:
    df = read_bronze(bt).withColumn(
        "datetime_utc", F.col("datetime_utc").cast("timestamp")
    )
    for c in df.columns:
        if c not in ("frequency", "datetime_utc"):
            df = df.withColumn(c, F.col(c).cast("double"))
    df = df.withColumn("_srid", sha_key(F.lit(bt), "frequency", "datetime_utc"))
    df = add_provenance(df, SOURCE, "_srid", RID)
    write_silver(df, silver_table, source=SOURCE, component=COMPONENT, rid=RID)


for _bt, _st in TABLE_MAP.items():
    process(_bt, _st)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print(f"HONDA ENERGY -- COMPLETE  (run_id {RID})")
print("=" * 70)
