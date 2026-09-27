# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- DIM_PLACE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `shared_conformed.dim_place` (from `weather_location`) and
# MAGIC `dim_place_validity` (`weather_location_validity` +
# MAGIC `weather_station_name_history`, unioned by a `validity_kind`
# MAGIC discriminator since they carry different fields). Grain: `dim_place` =
# MAGIC place key; `dim_place_validity` = place x validity window x record
# MAGIC ordinal.

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

# DBTITLE 1,Imports
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "gold/shared/dim_place"
PLACE_TABLE = "dim_place"
VALIDITY_TABLE = "dim_place_validity"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver
weather_location = read_silver("weather_location")
weather_location_validity = read_silver("weather_location_validity")
weather_station_name_history = read_silver("weather_station_name_history")

# COMMAND ----------

# DBTITLE 1,Build dim_place_validity -- union of the two validity-window sources
_geometry_windows = weather_location_validity.select(
    F.col("location_key").alias("place_key"),
    "source_location_id",
    "valid_from",
    "valid_to",
    "record_ordinal",
    F.lit("geometry").alias("validity_kind"),
    "name",
    "latitude",
    "longitude",
    "elevation_m",
    "quality_flags",
    "source_system",
    "source_dataset",
    "source_record_id",
)
_name_windows = weather_station_name_history.select(
    F.col("location_key").alias("place_key"),
    "source_location_id",
    "valid_from",
    "valid_to",
    "record_ordinal",
    F.lit("name").alias("validity_kind"),
    "name",
    F.lit(None).cast("double").alias("latitude"),
    F.lit(None).cast("double").alias("longitude"),
    F.lit(None).cast("double").alias("elevation_m"),
    F.lit(None).cast("array<string>").alias("quality_flags"),
    "source_system",
    "source_dataset",
    "source_record_id",
)
dim_place_validity = _geometry_windows.unionByName(_name_windows)
dim_place_validity = add_gold_provenance(dim_place_validity, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Derive is_current -- open (valid_to IS NULL) latest geometry window per place
_w = Window.partitionBy("place_key").orderBy(F.col("valid_from").desc_nulls_last())
_is_current = (
    dim_place_validity.filter(F.col("validity_kind") == "geometry")
    .withColumn("_rn", F.row_number().over(_w))
    .filter(F.col("_rn") == 1)
    .select("place_key", F.col("valid_to").isNull().alias("is_current"))
)

# COMMAND ----------

# DBTITLE 1,Build dim_place
dim_place = weather_location.select(
    F.col("location_key").alias("place_key"),
    "source_location_id",
    F.col("location_role").alias("place_kind"),
    "name",
    "latitude",
    "longitude",
    "elevation_m",
    "continent",
    "country_code",
    "region",
    "city",
    "official_municipality_key",
    "geography_basis",
    "source_system",
    "source_dataset",
    "source_record_id",
).join(_is_current, "place_key", "left")
dim_place = dim_place.fillna({"is_current": True})
dim_place = add_gold_provenance(dim_place, SOURCE, rid)

# COMMAND ----------

# DBTITLE 1,Grain gates
assert_unique_grain(
    dim_place, ["place_key"], component=COMPONENT, source=SOURCE, rid=rid
)
assert_unique_grain(
    dim_place_validity,
    ["place_key", "validity_kind", "record_ordinal"],
    component=COMPONENT,
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_gold(
    dim_place,
    PLACE_TABLE,
    schema=SHARED_CONFORMED_SCHEMA,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
)
write_gold(
    dim_place_validity,
    VALIDITY_TABLE,
    schema=SHARED_CONFORMED_SCHEMA,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    dim_place,
    PLACE_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    schema=SHARED_CONFORMED_SCHEMA,
    key_cols=["place_key"],
    df_before=weather_location,
)
_blocks += inspect_gold_table(
    dim_place_validity,
    VALIDITY_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    schema=SHARED_CONFORMED_SCHEMA,
    key_cols=["place_key", "validity_kind", "record_ordinal"],
)
write_gold_findings(SOURCE, "shared__dim_place", "dim_place(_validity)", _blocks)
