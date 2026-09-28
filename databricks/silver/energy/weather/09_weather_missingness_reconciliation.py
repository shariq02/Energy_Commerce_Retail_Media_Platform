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
# MAGIC **Purpose:** one row per (station, DWD parameter code) actually present
# MAGIC in the 14 hourly Bronze products, carrying its observed-value count and
# MAGIC time span, plus a cross-check of REPORTED gaps
# MAGIC (`weather_missing_value_period`) against OBSERVED missing values (NULL
# MAGIC or sentinel). Flags disagreement only -- never deletes or corrects
# MAGIC either signal.

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

# DBTITLE 1,Transform -- coverage (observed_count, time span) and OBSERVED-missing, from Bronze
# Bronze column names are the DWD parameter codes REPORTED uses. One pass per
# table: `_clean` (sentinels stripped, timestamp parsed) feeds both the
# per-column coverage aggregate (base grain -- every station x parameter code
# structurally present) and the observed-missing explode (gap signal).
_missing_parts = []
_coverage_parts = []
for _t in MEASUREMENT_TABLES:
    _df = read_bronze(_t)
    _value_cols = [c for c in _df.columns if c in CODES]
    if not _value_cols:
        continue
    _clean = (
        strip_sentinels(_df, _value_cols)
        .withColumn("source_location_id", strip_float_suffix("STATIONS_ID"))
        .withColumn("_ts", parse_mess_datum("MESS_DATUM"))
    )
    _missing_parts.append(
        _clean.select(
            "source_location_id",
            F.explode(
                F.array(*[F.when(F.col(c).isNull(), F.lit(c)) for c in _value_cols])
            ).alias("parameter_source_code"),
        )
        .filter(F.col("parameter_source_code").isNotNull())
        .distinct()
    )
    for _c in _value_cols:
        _coverage_parts.append(
            _clean.groupBy("source_location_id")
            .agg(
                F.count(F.col(_c)).alias("observed_count"),
                F.min(F.when(F.col(_c).isNotNull(), F.col("_ts"))).alias(
                    "time_span_start_utc"
                ),
                F.max(F.when(F.col(_c).isNotNull(), F.col("_ts"))).alias(
                    "time_span_end_utc"
                ),
            )
            .withColumn("parameter_source_code", F.lit(_c))
        )

observed = _missing_parts[0]
for _p in _missing_parts[1:]:
    observed = observed.unionByName(_p)
observed = observed.distinct().withColumn("observed", F.lit(True))

_coverage_union = _coverage_parts[0]
for _p in _coverage_parts[1:]:
    _coverage_union = _coverage_union.unionByName(_p)
# A handful of codes are reported by more than one Bronze table (the shared
# QN_8 quality column; V_N/V_N_I, genuinely duplicated between dwd_cloudiness
# and dwd_cloud_type) -- combine their per-table aggregates onto one row per
# (station, code) instead of leaving one row per contributing table.
coverage = _coverage_union.groupBy("source_location_id", "parameter_source_code").agg(
    F.sum("observed_count").alias("observed_count"),
    F.min("time_span_start_utc").alias("time_span_start_utc"),
    F.max("time_span_end_utc").alias("time_span_end_utc"),
)

# COMMAND ----------

# DBTITLE 1,Read Silver -- weather_missing_value_period (REPORTED)
reported = (
    spark.table(semantic_table("weather_missing_value_period"))
    .select("source_location_id", "parameter_source_code")
    .distinct()
    .withColumn("reported", F.lit(True))
)

# COMMAND ----------

# DBTITLE 1,Transform -- join coverage (base grain) with REPORTED and OBSERVED-missing
_key = ["source_location_id", "parameter_source_code"]
recon = (
    coverage.join(observed, _key, "left")
    .join(reported, _key, "left")
    .na.fill(False, subset=["observed", "reported"])
    .withColumn(
        "reconciliation_status",
        F.when(F.col("observed") & F.col("reported"), F.lit("matched"))
        .when(F.col("observed"), F.lit("observed_only"))
        .when(F.col("reported"), F.lit("reported_only"))
        .otherwise(F.lit("no_gap_signal")),
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