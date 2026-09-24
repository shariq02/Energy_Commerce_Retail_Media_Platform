# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- WEATHER MISSINGNESS RECONCILIATION (DWD)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** cross-check REPORTED gaps (`weather_missing_value_period`)
# MAGIC against OBSERVED missing values (NULL or sentinel) per station and DWD
# MAGIC parameter code in the 14 hourly Bronze products. Flags disagreement only
# MAGIC -- never deletes or corrects either signal.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "silver/energy/weather/09_weather_missingness_reconciliation"
RID = run_id()
FINDINGS = "weather"
TABLE = "weather_missingness_reconciliation"
MEASUREMENT_TABLES = (
    "dwd_air_temperature",
    "dwd_cloudiness",
    "dwd_moisture",
    "dwd_precipitation",
    "dwd_pressure",
    "dwd_sun",
    "dwd_wind",
    "dwd_dew_point",
    "dwd_soil_temperature",
    "dwd_visibility",
    "dwd_cloud_type",
    "dwd_wind_synop",
    "dwd_extreme_wind",
    "dwd_weather_phenomena",
)

# COMMAND ----------

# DBTITLE 1,Read Silver -- weather_parameter_catalog (the DWD parameter codes)
CODES = {
    r["parameter_source_code"]
    for r in spark.table(semantic_table("weather_parameter_catalog"))
    .select("parameter_source_code")
    .collect()
}

# COMMAND ----------

# DBTITLE 1,Transform -- OBSERVED from the Bronze products
# Bronze column names are the DWD parameter codes REPORTED uses.
observed = None
for _t in MEASUREMENT_TABLES:
    _df = read_bronze(_t)
    _value_cols = [c for c in _df.columns if c in CODES]
    if not _value_cols:
        continue
    _part = (
        strip_sentinels(_df, _value_cols)
        .select(
            strip_float_suffix("STATIONS_ID").alias("source_location_id"),
            F.explode(
                F.array(*[F.when(F.col(c).isNull(), F.lit(c)) for c in _value_cols])
            ).alias("parameter_source_code"),
        )
        .filter(F.col("parameter_source_code").isNotNull())
        .distinct()
    )
    observed = _part if observed is None else observed.unionByName(_part)
observed = observed.distinct().withColumn("observed", F.lit(True))

# COMMAND ----------

# DBTITLE 1,Read Silver -- weather_missing_value_period (REPORTED)
reported = (
    spark.table(semantic_table("weather_missing_value_period"))
    .select("source_location_id", "parameter_source_code")
    .distinct()
    .withColumn("reported", F.lit(True))
)

# COMMAND ----------

# DBTITLE 1,Transform -- join REPORTED and OBSERVED
_key = ["source_location_id", "parameter_source_code"]
recon = (
    observed.join(reported, _key, "full_outer")
    .na.fill(False, subset=["observed", "reported"])
    .withColumn(
        "reconciliation_status",
        F.when(F.col("observed") & F.col("reported"), F.lit("matched"))
        .when(F.col("observed"), F.lit("observed_only"))
        .otherwise(F.lit("reported_only")),
    )
    .withColumn("location_key", location_key(SOURCE, "source_location_id"))
    .withColumn("_srid", sha_key(*_key))
)
recon = add_semantic_provenance(recon, SOURCE, "dwd_hourly_bronze", RID, "_srid")

# COMMAND ----------

# DBTITLE 1,Write Silver -- weather_missingness_reconciliation
write_semantic(
    conform(recon, SEMANTIC_STRUCTURES[TABLE]),
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- weather_missingness_reconciliation
written = spark.table(semantic_table(TABLE))
findings_blocks = inspect_table(
    written,
    TABLE,
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=_key,
    extra_checks={
        "rows_by_status": {
            r["reconciliation_status"]: r["count"]
            for r in written.groupBy("reconciliation_status").count().collect()
        }
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- weather_missingness_reconciliation
write_silver_findings(
    FINDINGS, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, findings_blocks
)
