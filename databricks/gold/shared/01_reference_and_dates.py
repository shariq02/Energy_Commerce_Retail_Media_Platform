# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GOLD SHARED -- REFERENCE AND DATES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the `shared_conformed` tables: the carrier map and carriers,
# MAGIC the reference code list, the date dimension (widened to the oldest Gold date),
# MAGIC the places, the weather series coverage and `metric_definition` (one row per
# MAGIC energy and commerce metric, read from `analytics_rules.yml`).

# COMMAND ----------

# DBTITLE 1,Gold library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "gold/shared/01_reference_and_dates"
RID = gold_run_id()
GATE = {"source": SOURCE, "component": COMPONENT, "rid": RID}
COMMON = {**GATE, "schema": SHARED_CONFORMED_SCHEMA}
DATE_HORIZON_END = _dt.date(2031, 12, 31)
METRIC_COLUMNS = [
    "ecosystem",
    "metric_key",
    "definition",
    "formula",
    "source_mart",
    "grain",
    "source_system",
]
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Read Silver
carrier_map = read_silver("energy_carrier_map")
code_list = read_silver("reference_code_list")
weather_location = read_silver("weather_location")
station_period = read_silver("weather_station_period")
reconciliation = read_silver("weather_missingness_reconciliation")
missing_period = read_silver("weather_missing_value_period")

# COMMAND ----------

# DBTITLE 1,Transform -- ref_energy_carrier_map and ref_energy_carrier
TABLES["ref_energy_carrier_map"] = (
    add_provenance(from_silver(carrier_map), SOURCE, RID),
    ["source_system", "source_vocabulary", "source_value"],
)
TABLES["ref_energy_carrier"] = (
    add_provenance(
        carrier_map.filter(F.col("carrier_key").isNotNull())
        .groupBy("carrier_key")
        .agg(F.min("source_value").alias("display_label")),
        SOURCE,
        RID,
    ),
    ["carrier_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- ref_measure (DWD parameters and MaStR code lists)
TABLES["ref_measure"] = (
    add_provenance(from_silver(code_list), SOURCE, RID),
    ["catalog_kind", "code_id"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- dim_date (widened when a Silver date is older than its range)
dim_date_table = f"{CATALOG}.{SHARED_CONFORMED_SCHEMA}.dim_date"
current = spark.table(dim_date_table).agg(
    F.min("calendar_date").alias("first"), F.max("calendar_date").alias("last")
)
current = current.first()
needed = [
    read_silver("weather_daily").agg(F.min("local_date")).first()[0],
    read_silver("generation_unit")
    .agg(F.least(F.min("commissioning_date"), F.min("registration_date")))
    .first()[0],
]
needed = [d.date() if hasattr(d, "date") else d for d in needed if d is not None]
range_start = min([current["first"], *needed])
range_end = max(current["last"], DATE_HORIZON_END)
dim_date = (
    spark.sql(
        f"SELECT explode(sequence(DATE'{range_start}', DATE'{range_end}')) AS calendar_date"
    )
    .withColumn("date_key", F.date_format("calendar_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("calendar_date").cast("short"))
    .withColumn("quarter", F.quarter("calendar_date").cast("short"))
    .withColumn("month", F.month("calendar_date").cast("short"))
    .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
    .withColumn("day_of_month", F.dayofmonth("calendar_date").cast("short"))
    .withColumn("day_of_year", F.dayofyear("calendar_date").cast("short"))
    .withColumn("iso_week", F.weekofyear("calendar_date").cast("short"))
    .withColumn("day_of_week", F.expr("weekday(calendar_date) + 1").cast("short"))
    .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
    .withColumn("is_weekend", F.expr("weekday(calendar_date) >= 5"))
    .select(*spark.table(dim_date_table).columns)
)
print(
    f"dim_date: {current['first']} .. {current['last']} -> {range_start} .. {range_end}"
)
TABLES["dim_date"] = (dim_date, ["date_key"])

# COMMAND ----------

# DBTITLE 1,Transform -- dim_place (weather_location plus the open location window)
latest_window = (
    station_period.filter(F.col("period_kind") == "location")
    .groupBy("location_key")
    .agg(F.max_by("valid_to", F.col("valid_from")).alias("_latest_valid_to"))
    .withColumn("is_current", F.col("_latest_valid_to").isNull())
    .drop("_latest_valid_to")
)
TABLES["dim_place"] = (
    add_provenance(
        from_silver(weather_location).join(latest_window, "location_key", "left"),
        SOURCE,
        RID,
    ),
    ["location_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- series_coverage (counts, gaps and station periods, by kind)
coverage_parts = {
    "reconciliation": reconciliation,
    "missing_value_period": missing_period,
    "station_period": station_period,
}
series_coverage = None
for kind, frame in coverage_parts.items():
    part = from_silver(frame).withColumn("record_kind", F.lit(kind))
    series_coverage = (
        part
        if series_coverage is None
        else series_coverage.unionByName(part, allowMissingColumns=True)
    )
TABLES["series_coverage"] = (
    add_provenance(series_coverage, SOURCE, RID),
    ["record_kind", "source_record_id"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- metric_definition (one row per metric, all ecosystems)
metrics = load_yaml("src/schemas/mappings/analytics_rules.yml")["metrics"]
TABLES["metric_definition"] = (
    add_provenance(
        spark.createDataFrame(
            [tuple(m.get(c) for c in METRIC_COLUMNS) for m in metrics],
            ", ".join(f"{c} string" for c in METRIC_COLUMNS),
        ),
        SOURCE,
        RID,
    ),
    ["ecosystem", "metric_key"],
)

# COMMAND ----------

# DBTITLE 1,Grain gate -- every table
gate_tables(TABLES, **GATE)

# COMMAND ----------

# DBTITLE 1,Write -- every table
write_tables(TABLES, **COMMON)

# COMMAND ----------

# DBTITLE 1,Inspect and export -- every table
inspect_tables(TABLES, findings="shared", **COMMON)

# COMMAND ----------

# DBTITLE 1,Validation -- every table exists and has rows
validate_tables(TABLES, **COMMON)
