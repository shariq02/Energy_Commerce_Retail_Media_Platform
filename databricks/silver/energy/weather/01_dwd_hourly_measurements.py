# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DWD HOURLY MEASUREMENTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the 14 DWD hourly-historical measurement Bronze tables into
# MAGIC source-scoped Silver at their (station, hour) grain -- one config-driven
# MAGIC pass over the contract. `dwd_solar` (10-minute grid) is a separate
# MAGIC notebook.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "dwd"
COMPONENT = "silver/energy/weather/01_dwd_hourly_measurements"
RID = run_id()

CONTRACT = load_contract(SOURCE)
MAPPING = load_mapping(SOURCE)
TABLES = contract_tables(CONTRACT)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

MEASUREMENT_TABLES = [
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
]

# COMMAND ----------

# DBTITLE 1,One measurement table -> Silver


def process(bt: str) -> None:
    cols = [c["name"] for c in TABLES[bt]["columns"]]
    qn_col = next(c for c in cols if c.startswith("QN_"))
    value_cols = [
        c
        for c in cols
        if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor")
        and not c.startswith("QN_")
    ]

    df = strip_sentinels(read_bronze(bt), [*value_cols, qn_col])
    df, q = resolve_conflicts(
        df,
        ["STATIONS_ID", "MESS_DATUM"],
        value_cols,
        qn_col=qn_col,
        bronze_table=bt,
    )
    q = q.withColumn("source_system", F.lit(SOURCE))
    write_quarantine(q, RID)

    df = cast_logical(df, TABLES[bt]["columns"])
    df = decode_qn(df, qn_col)

    for raw, en_map in CODED.items():
        if raw in df.columns:
            pref = NAME_MAP.get(raw, raw)
            df = decode_via_labeled_map(df, raw, pref, en_map).drop(raw)

    df = apply_renames(df, NAME_MAP)
    df = df.withColumn("_srid", sha_key(F.lit(bt), "STATIONS_ID", "MESS_DATUM"))
    df = df.withColumn("observation_ts", parse_mess_datum("MESS_DATUM")).drop(
        "MESS_DATUM"
    )
    df = attach_city_ags(df, "city")
    df = add_provenance(df, SOURCE, "_srid", RID)
    write_silver(df, bt, source=SOURCE, component=COMPONENT, rid=RID)


for _bt in MEASUREMENT_TABLES:
    process(_bt)

# COMMAND ----------

# DBTITLE 1,Summary
audit(
    COMPONENT,
    SOURCE,
    "measurement_tables_written",
    float(len(MEASUREMENT_TABLES)),
    status="PASS",
    rid=RID,
)
print("=" * 70)
print(f"DWD HOURLY MEASUREMENTS -- COMPLETE  (run_id {RID})")
print("=" * 70)
