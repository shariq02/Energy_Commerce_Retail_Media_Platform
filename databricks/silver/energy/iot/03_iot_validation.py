# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- IOT VALIDATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** read-only checks of `thermal_energy`, `energy_meter_reading` and
# MAGIC `device_telemetry_snapshot`: keys, grain, places, Honda P against W and
# MAGIC the Samples IoT label rule.

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
COMPONENT = "silver/energy/iot/03_iot_validation"
FINDINGS = "energy"
# Families checked for keys, grain and places; other tables read for checks.
FAMILIES = [
    "thermal_energy",
    "energy_meter_reading",
]
_ALL = [
    *FAMILIES,
    "electricity_balance",
    "device_telemetry_snapshot",
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

# DBTITLE 1,Check -- Honda P against the forward W increment x1000 (1 h rows)
if {"electricity_balance", "thermal_energy", "energy_meter_reading"} <= set(T):
    _key = ["observation_timestamp_utc", "interval_seconds"]
    _p_bal = (
        T["electricity_balance"]
        .filter(
            (F.col("source_system") == "honda_iot")
            & (F.col("interval_seconds") == 3600)
        )
        .select(*_key, F.explode("components").alias("c"))
        .groupBy(*_key)
        .pivot("c.native_label")
        .agg(F.first("c.power_w"))
    )
    _p = _p_bal.join(
        T["thermal_energy"].filter(F.col("interval_seconds") == 3600), _key
    ).join(T["energy_meter_reading"].filter(F.col("interval_seconds") == 3600), _key)
    _pairs = {
        "electricity.PV": "electricity_solar_photovoltaic",
        "electricity.CHP": "electricity_combined_heat_and_power",
        "electricity.total": "electricity_total",
        "cooling.cool_elec": "cooling_electricity",
        "heating_total_w": "heating_total",
        "heating_combined_heat_and_power_heat_w": "heating_combined_heat_and_power_heat",
        "cooling_total_w": "cooling_total",
    }
    keep(
        "Honda P vs forward W increment x1000, correlation at 1 h",
        _p.select(
            *[
                F.corr(F.col(f"`{p}`"), F.col(f"{w}_increment_kwh") * 1000).alias(p)
                for p, w in _pairs.items()
            ]
        ),
    )

# COMMAND ----------

# DBTITLE 1,Check -- Honda W register equal across frequencies at shared instants
if "energy_meter_reading" in T:
    _m = T["energy_meter_reading"]
    _cols = [f"{ch}_kwh" for ch in HONDA_METER_CHANNELS]
    _one = _m.filter(F.col("interval_seconds") == 60).select(
        "observation_timestamp_utc", *[F.col(c).alias(f"m1_{c}") for c in _cols]
    )
    _hour = _m.filter(F.col("interval_seconds") == 3600).select(
        "observation_timestamp_utc", *_cols
    )
    keep(
        "Honda W: share equal between 1 min and 1 h rows at the same instant",
        _one.join(_hour, "observation_timestamp_utc").select(
            *[F.avg((F.col(c) == F.col(f"m1_{c}")).cast("int")).alias(c) for c in _cols]
        ),
    )

# COMMAND ----------

# DBTITLE 1,Check -- Samples IoT lcd label by co2 range
if "device_telemetry_snapshot" in T:
    keep(
        "iot lcd label by co2 range (derived by the source, ranges must not overlap)",
        T["device_telemetry_snapshot"]
        .groupBy("lcd_label")
        .agg(F.min("co2_level").alias("min_co2"), F.max("co2_level").alias("max_co2"))
        .orderBy("min_co2"),
    )

# COMMAND ----------

# DBTITLE 1,Build findings -- check results table
findings_blocks = checks_blocks()

# COMMAND ----------

# DBTITLE 1,Export findings -- iot validation
write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__validation",
    "iot validation",
    findings_blocks,
)
