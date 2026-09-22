# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- WEATHER DAILY (SEATTLE)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Seattle daily high and low (degF) into `weather_daily` as
# MAGIC degC max/min; date-only, so no instant or project time is derived.

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
SOURCE = "weather_seattle"
COMPONENT = "silver/semantic/weather/05_weather_daily_seattle"
RID = run_id()
ROOT = "/Volumes/samples/databricks/datasets/weather"
SITE_ID = "seattle"
FILE_STATISTIC = {"high_temps": "max", "low_temps": "min"}

# COMMAND ----------

# DBTITLE 1,Read Samples -- high_temps and low_temps
seattle_src = (
    spark.read.format("csv")
    .option("header", True)
    .load([f"{ROOT}/{name}" for name in FILE_STATISTIC])
    .select("date", "temp", F.col("_metadata.file_name").alias("_file"))
)

# COMMAND ----------

# DBTITLE 1,Dedupe -- collapse identical rows, quarantine same-key value conflicts
_seattle_kept, _seattle_q = resolve_conflicts(
    seattle_src, ["_file", "date"], ["temp"], bronze_table="weather_seattle"
)
write_quarantine(_seattle_q.withColumn("source_system", F.lit(SOURCE)), RID)
RECONCILIATION = {
    "weather_seattle": reconciliation_stats(seattle_src, _seattle_kept, _seattle_q)
}
seattle_src = _seattle_kept

# COMMAND ----------

# DBTITLE 1,Transform -- rows with role, unit and standardised value
_stat = F.create_map([F.lit(x) for kv in FILE_STATISTIC.items() for x in kv])
seattle_daily = (
    seattle_src.withColumn("statistic", _stat[F.col("_file")])
    .withColumn("value_native", F.col("temp").cast("double"))
    .withColumn("date_native", F.trim(F.col("date")))
    .withColumn("local_date", F.to_date(F.col("date_native"), "yyyy-MM-dd"))
    .withColumn("variable", F.lit("air_temperature"))
    .withColumn("is_primary", F.lit(True))
    .withColumn("unit_native", F.lit("degF"))
    .withColumn("unit", F.lit("degC"))
    .withColumn("value", (F.col("value_native") - 32.0) * 5.0 / 9.0)
    .withColumn("value_origin", F.lit("converted"))
    .withColumn("derivation_rule", F.lit("degF -> degC ((F - 32) x 5/9)"))
    .withColumn("time_basis", F.lit("date_only"))
    .withColumn("measurement_basis", F.lit("observed_daily_summary"))
    .withColumn("source_location_id", F.lit(SITE_ID))
    .withColumn("location_key", location_key(SOURCE, "source_location_id"))
    .withColumn("source_column", F.col("_file"))
    .withColumn("source_record_id", sha_key("_file", "date_native"))
    .withColumn("daily_key", sha_key("_file", "date_native", "variable", "statistic"))
)
seattle_daily = add_semantic_provenance(seattle_daily, SOURCE, "weather", RID)
seattle_daily = conform(seattle_daily, WEATHER_DAILY_COLUMNS)

# COMMAND ----------

# DBTITLE 1,Write Silver -- weather_daily (weather_seattle)
write_semantic(
    seattle_daily,
    "weather_daily",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    replace_where="source_system = 'weather_seattle'",
)

# COMMAND ----------

# DBTITLE 1,Inspect -- weather_daily (seattle)
written = spark.table(semantic_table("weather_daily")).filter(
    F.col("source_system") == SOURCE
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

# DBTITLE 1,Export findings -- weather_daily (seattle)
write_silver_findings(
    FINDINGS_SOURCE,
    f"{COMPONENT.split('/')[-1]}__weather_daily_{SOURCE}",
    f"weather_daily -- {SOURCE}",
    findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Export findings -- dedup/conflict reconciliation proof
write_silver_findings(
    FINDINGS_SOURCE,
    f"{COMPONENT.split('/')[-1]}__weather_seattle__reconciliation",
    "dedup/conflict reconciliation -- weather_seattle",
    [
        (
            "Bronze -> exact duplicates collapsed -> conflicts quarantined -> kept",
            dict_to_markdown_row(RECONCILIATION["weather_seattle"]),
        )
    ],
)