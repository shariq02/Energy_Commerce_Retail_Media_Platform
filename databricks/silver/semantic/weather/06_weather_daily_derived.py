# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- WEATHER DAILY (DERIVED FROM HOURLY)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** daily min, max and mean air temperature per DWD station over
# MAGIC the Europe/Berlin local day, derived from `weather_observation` and marked
# MAGIC `derived` in `weather_daily`.

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

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "silver/semantic/weather/06_weather_daily_derived"
RID = run_id()
DERIVED_FROM = "weather_observation"
VARIABLE = "air_temperature"
STATISTICS = {"min": F.min, "max": F.max, "mean": F.avg}

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Silver -- primary hourly air temperature
hourly_temperature = (
    spark.table(semantic_table("weather_observation"))
    .filter(
        (F.col("source_system") == SOURCE)
        & (F.col("variable") == VARIABLE)
        & F.col("is_primary")
        & F.col("value").isNotNull()
        & F.col("local_date").isNotNull()
    )
    .select("location_key", "source_location_id", "local_date", "value")
)

# COMMAND ----------

# DBTITLE 1,Transform -- daily aggregates
daily_agg = hourly_temperature.groupBy(
    "location_key", "source_location_id", "local_date"
).agg(
    *[fn("value").alias(f"agg_{name}") for name, fn in STATISTICS.items()],
    F.count("*").alias("n_observations"),
)

# COMMAND ----------

# DBTITLE 1,Transform -- long derived rows
_stack = ", ".join(f"'{name}', agg_{name}" for name in STATISTICS)
derived_daily = (
    daily_agg.select(
        "location_key",
        "source_location_id",
        "local_date",
        "n_observations",
        F.expr(f"stack({len(STATISTICS)}, {_stack}) as (statistic, value)"),
    )
    .withColumn("variable", F.lit(VARIABLE))
    .withColumn("is_primary", F.lit(True))
    .withColumn("unit", F.lit("degC"))
    .withColumn("value_origin", F.lit("derived"))
    .withColumn(
        "derivation_rule",
        F.lit(
            "min/max/mean of hourly air_temperature over the Europe/Berlin local day"
        ),
    )
    .withColumn("time_basis", F.lit("utc_project_day"))
    .withColumn("date_native", F.col("local_date").cast("string"))
    .withColumn("measurement_basis", F.lit("derived_from_station_observation"))
    .withColumn("source_column", F.lit(None).cast("string"))
    .withColumn(
        "source_record_id",
        sha_key("location_key", "local_date", "variable", "statistic"),
    )
    .withColumn(
        "daily_key", sha_key("location_key", "local_date", "variable", "statistic")
    )
)
derived_daily = add_semantic_provenance(derived_daily, SOURCE, DERIVED_FROM, RID)
derived_daily = conform(derived_daily, WEATHER_DAILY_COLUMNS)

# COMMAND ----------

# DBTITLE 1,Write Silver -- weather_daily (derived, dwd)
write_semantic(
    derived_daily,
    "weather_daily",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    replace_where=f"source_system = '{SOURCE}' AND source_dataset = '{DERIVED_FROM}'",
)

# COMMAND ----------

# DBTITLE 1,Inspect -- weather_daily (derived)
written = spark.table(semantic_table("weather_daily")).filter(
    (F.col("source_system") == SOURCE) & (F.col("source_dataset") == DERIVED_FROM)
)
findings_blocks = inspect_table(
    written,
    "weather_daily",
    source=FINDINGS_SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["daily_key"],
    extra_checks=structure_extra_checks(written),
)

# COMMAND ----------

# DBTITLE 1,Export findings -- weather_daily (derived)
write_silver_findings(
    FINDINGS_SOURCE,
    f"{COMPONENT.split('/')[-1]}__weather_daily_derived_{SOURCE}",
    f"weather_daily -- derived from {DERIVED_FROM} ({SOURCE})",
    findings_blocks,
)
