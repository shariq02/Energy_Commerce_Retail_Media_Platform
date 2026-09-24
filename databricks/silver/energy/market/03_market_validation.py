# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MARKET VALIDATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** read-only checks of `electricity_balance`, `electricity_price` and
# MAGIC `electricity_generation_forecast`: keys, grain, places and the SMARD
# MAGIC relationships the EDA measured.

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
COMPONENT = "silver/energy/market/03_market_validation"
FINDINGS = "energy"
# Families checked for keys, grain and places; other tables read for checks.
FAMILIES = [
    "electricity_balance",
    "electricity_price",
    "electricity_generation_forecast",
]
_ALL = [
    *FAMILIES,
]

# COMMAND ----------

# DBTITLE 1,Read Silver -- the structures this notebook checks
T = {
    t: spark.table(semantic_table(t))
    for t in _ALL
    if spark.catalog.tableExists(semantic_table(t))
}
for t in _ALL:
    if t not in T:
        report(f"{t} table exists", True, "not written yet", status="SKIP")

# COMMAND ----------

# DBTITLE 1,Check -- observation_key and grain key unique per family
for fam in FAMILIES:
    if fam not in T:
        continue
    _dup_key = T[fam].groupBy("observation_key").count().filter("count > 1").count()
    _dup_grain = T[fam].groupBy(*ENERGY_GRAIN).count().filter("count > 1").count()
    report(f"{fam} observation_key unique", _dup_key == 0, f"duplicates: {_dup_key}")
    report(f"{fam} one row per grain key", _dup_grain == 0, f"groups >1: {_dup_grain}")

# COMMAND ----------

# DBTITLE 1,Check -- places: market-area vocabulary and site locations
_codes = set(MARKET_AREA_CODES.values())
_loc = spark.table(semantic_table("weather_location")).select("location_key")
for fam in [f for f in FAMILIES if f in T]:
    _df = T[fam]
    _bad_code = _df.filter(
        F.col("market_area_code").isNotNull() & ~F.col("market_area_code").isin(_codes)
    ).count()
    _no_place = _df.filter(
        F.col("market_area_code").isNull() & F.col("location_key").isNull()
    ).count()
    _orphan = (
        _df.select("location_key")
        .dropna()
        .distinct()
        .join(_loc, "location_key", "left_anti")
        .count()
    )
    report(
        f"{fam} places resolve",
        _bad_code == 0 and _no_place == 0 and _orphan == 0,
        f"unknown codes {_bad_code}, no place {_no_place}, orphans {_orphan}",
    )

# COMMAND ----------

# DBTITLE 1,Helper -- SMARD balance components as one column per metric


def smard_components(metrics: list):
    return (
        T["electricity_balance"]
        .filter(F.col("source_system") == "smard")
        .select(
            "observation_key",
            "market_area_code",
            "interval_reference",
            "local_date",
            F.explode("components").alias("c"),
        )
        .groupBy(
            "observation_key", "market_area_code", "interval_reference", "local_date"
        )
        .pivot("c.native_label", metrics)
        .agg(F.first("c.energy_mwh"))
    )


# COMMAND ----------

# DBTITLE 1,Check -- SMARD residual load = load - onshore - offshore - PV (within 2%)
if "electricity_balance" in T:
    _r = smard_components(
        [
            "residual_load",
            "total_power_consumption",
            "generation_onshore_wind",
            "generation_offshore_wind",
            "generation_photovoltaic",
        ]
    ).dropna()
    _implied = (
        F.col("total_power_consumption")
        - F.col("generation_onshore_wind")
        - F.col("generation_offshore_wind")
        - F.col("generation_photovoltaic")
    )
    _viol = _r.filter(
        F.abs(F.col("residual_load") - _implied) > 0.02 * F.abs(F.col("residual_load"))
    ).count()
    report(
        "smard residual-load identity",
        _viol == 0,
        f"violations {_viol} of {_r.count()}",
    )

# COMMAND ----------

# DBTITLE 1,Check -- SMARD quarter-hour values sum to the day value (MWh, not MW)
if "electricity_balance" in T:
    _c = smard_components(["total_power_consumption"]).filter(
        F.col("market_area_code") == "de_lu"
    )
    _qh = (
        _c.filter(F.col("interval_reference") == "clock")
        .groupBy("local_date")
        .agg(F.sum("total_power_consumption").alias("qh_sum"), F.count("*").alias("n"))
    )
    _day = _c.filter(F.col("interval_reference") == "local_day_europe_berlin").select(
        "local_date", F.col("total_power_consumption").alias("day_value")
    )
    _ratio = (
        _qh.filter(F.col("n") >= 92)
        .join(_day, "local_date")
        .select((F.col("day_value") / F.col("qh_sum")).alias("ratio"))
        .agg(
            F.percentile_approx("ratio", 0.5).alias("median"),
            F.count("*").alias("days"),
        )
        .first()
    )
    _median = _ratio["median"]
    report(
        "smard quarter-hour unit is MWh per interval",
        _median is not None and abs(_median - 1) < 0.02,
        f"median day/sum(qh) = {_median} over {_ratio['days']} days (MW would give ~4)",
    )

# COMMAND ----------

# DBTITLE 1,Check -- SMARD forecast PV is still the sign mirror of wind+PV (flag stays valid)
if "electricity_generation_forecast" in T:
    _f = (
        T["electricity_generation_forecast"]
        .select("observation_key", F.explode("components").alias("c"))
        .groupBy("observation_key")
        .pivot("c.forecast_scope", ["photovoltaic", "wind_and_photovoltaic"])
        .agg(F.first("c.energy_mwh"))
        .dropna()
    )
    _share = _f.agg(
        F.avg(
            (F.abs(F.col("photovoltaic") + F.col("wind_and_photovoltaic")) < 1e-6).cast(
                "int"
            )
        )
    ).first()[0]
    report(
        "smard forecast pv mirror still holds",
        True,
        f"share mirrored {_share}",
        status="INFO",
    )

# COMMAND ----------

# DBTITLE 1,Build findings -- check results table
findings_blocks = checks_blocks()

# COMMAND ----------

# DBTITLE 1,Export findings -- market validation
write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__validation",
    "market validation",
    findings_blocks,
)
