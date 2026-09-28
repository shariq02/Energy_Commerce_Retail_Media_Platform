# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- SERIES_COVERAGE + DATA_GAP_PERIOD
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `shared_conformed.series_coverage` (from
# MAGIC `weather_missingness_reconciliation`, which already carries
# MAGIC observed-value count and time span per station/parameter, computed
# MAGIC against Bronze) and `data_gap_period` (from `weather_missing_value_period`,
# MAGIC place key already resolved). `gap_origin` is `declared` for every row.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Gold inspection library
# MAGIC %run ../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "gold/shared/series_coverage"
COVERAGE_TABLE = "series_coverage"
GAP_TABLE = "data_gap_period"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
weather_missingness_reconciliation = read_silver("weather_missingness_reconciliation")
weather_missing_value_period = read_silver("weather_missing_value_period")

# COMMAND ----------

# DBTITLE 1,Build series_coverage
series_coverage = weather_missingness_reconciliation.select(
    F.lit("weather_observation").alias("structure"),
    F.col("location_key").alias("place_key"),
    F.col("parameter_source_code").alias("measure"),
    "reconciliation_status",
    "observed_count",
    F.col("time_span_start_utc").alias("time_span_start"),
    F.col("time_span_end_utc").alias("time_span_end"),
    "source_system",
    "source_dataset",
    "source_record_id",
)
series_coverage = add_gold_provenance(series_coverage, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Build data_gap_period
data_gap_period = weather_missing_value_period.select(
    F.col("location_key").alias("place_key"),
    F.col("parameter_source_code").alias("measure"),
    "gap_start_timestamp",
    "gap_end_timestamp",
    "record_ordinal",
    "missing_value_count",
    "gap_description",
    F.lit("declared").alias("gap_origin"),
    "source_system",
    "source_dataset",
    "source_record_id",
)
data_gap_period = add_gold_provenance(data_gap_period, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gates
assert_unique_grain(
    series_coverage,
    ["structure", "place_key", "measure"],
    component=COMPONENT,
    source=SOURCE,
    rid=rid,
)
assert_unique_grain(
    data_gap_period,
    [
        "place_key",
        "measure",
        "gap_start_timestamp",
        "gap_end_timestamp",
        "record_ordinal",
    ],
    component=COMPONENT,
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(
    series_coverage,
    COVERAGE_TABLE,
    schema=SHARED_CONFORMED_SCHEMA,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
)
write_gold(
    data_gap_period,
    GAP_TABLE,
    schema=SHARED_CONFORMED_SCHEMA,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    series_coverage,
    COVERAGE_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    schema=SHARED_CONFORMED_SCHEMA,
    key_cols=["structure", "place_key", "measure"],
    df_before=weather_missingness_reconciliation,
)
_blocks += inspect_gold_table(
    data_gap_period,
    GAP_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    schema=SHARED_CONFORMED_SCHEMA,
    key_cols=[
        "place_key",
        "measure",
        "gap_start_timestamp",
        "gap_end_timestamp",
        "record_ordinal",
    ],
    df_before=weather_missing_value_period,
)
write_gold_findings(
    SOURCE, "shared__series_coverage", "series_coverage / data_gap_period", _blocks
)
