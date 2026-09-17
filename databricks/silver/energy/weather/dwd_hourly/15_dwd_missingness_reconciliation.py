# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DWD MISSINGNESS RECONCILIATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** cross-check REPORTED gaps (`dwd_missing_value_periods`)
# MAGIC against OBSERVED nulls in the 14 measurement tables this folder's other
# MAGIC 14 notebooks write. Flags disagreement only -- never deletes or corrects
# MAGIC either signal. Runs after all 14 `NN_dwd_*.py` notebooks in this folder.
# MAGIC
# MAGIC OBSERVED is recomputed here directly from each already-written Silver
# MAGIC table (via `read_silver`), not accumulated during those notebooks' own
# MAGIC runs -- each notebook is now a separate process, so no in-memory state
# MAGIC survives across them. `dwd_parameter_catalog`'s own
# MAGIC `parameter_business_name` -> `parameter_source_code` mapping translates a
# MAGIC Silver value column back to the raw code REPORTED uses.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "silver/energy/weather/dwd_hourly/15_dwd_missingness_reconciliation"
RID = run_id()

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

# DBTITLE 1,Read Silver -- dwd_parameter_catalog (business name -> source code)
_catalog = read_silver("dwd_parameter_catalog").select(
    "parameter_business_name", "parameter_source_code"
)
_business_to_code = {
    r["parameter_business_name"]: r["parameter_source_code"]
    for r in _catalog.collect()
    if r["parameter_business_name"] is not None
}

# COMMAND ----------

# DBTITLE 1,Transform -- build OBSERVED (recomputed from each already-written table)
# REPORTED = dwd_missing_value_periods; OBSERVED = per-station null counts in
# the 14 already-written measurement tables. Reading N already-Silver tables
# for one derived cross-table aggregate -- same pattern as the coordinate-
# conflict / commissioning-date checks in the MaStR grid/change-log notebooks.
_observed = None
for _t in MEASUREMENT_TABLES:
    _df = read_silver(_t)
    _value_cols = [c for c in _df.columns if c in _business_to_code]
    if not _value_cols:
        continue
    _part = (
        _df.select(
            F.col("STATIONS_ID").cast("string").alias("station_id"),
            *[
                F.when(F.col(c).isNull(), F.lit(_business_to_code[c])).alias(
                    f"_missing_{c}"
                )
                for c in _value_cols
            ],
        )
        .select(
            "station_id",
            F.explode(F.array(*[F.col(f"_missing_{c}") for c in _value_cols])).alias(
                "parameter_source_code"
            ),
        )
        .filter(F.col("parameter_source_code").isNotNull())
        .distinct()
    )
    _observed = _part if _observed is None else _observed.unionByName(_part)

_observed = _observed.withColumn("observed", F.lit(True))

# COMMAND ----------

# DBTITLE 1,Read Silver -- dwd_missing_value_periods (REPORTED)
_reported = (
    read_silver("dwd_missing_value_periods")
    .select("station_id", "parameter_source_code")
    .distinct()
    .withColumn("reported", F.lit(True))
)

# COMMAND ----------

# DBTITLE 1,Transform -- join REPORTED and OBSERVED
_recon = (
    _observed.join(_reported, ["station_id", "parameter_source_code"], "full_outer")
    .na.fill(False, subset=["observed", "reported"])
    .withColumn(
        "_missingness_reconciliation_status",
        F.when(F.col("observed") & F.col("reported"), F.lit("matched"))
        .when(F.col("observed") & ~F.col("reported"), F.lit("observed_only"))
        .otherwise(F.lit("reported_only")),
    )
    .select("station_id", "parameter_source_code", "_missingness_reconciliation_status")
)
_recon = _recon.withColumn("_srid", sha_key("station_id", "parameter_source_code"))
_recon = add_provenance(_recon, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_missingness_reconciliation
write_silver(
    _recon,
    "dwd_missingness_reconciliation",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_missingness_reconciliation
_findings_blocks = inspect_table(
    _recon,
    "dwd_missingness_reconciliation",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["station_id", "parameter_source_code"],
    extra_checks={
        "observed_only_count": _recon.filter(
            F.col("_missingness_reconciliation_status") == "observed_only"
        ).count(),
        "reported_only_count": _recon.filter(
            F.col("_missingness_reconciliation_status") == "reported_only"
        ).count(),
    },
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_missingness_reconciliation",
    "dwd_missingness_reconciliation",
    _findings_blocks,
)
