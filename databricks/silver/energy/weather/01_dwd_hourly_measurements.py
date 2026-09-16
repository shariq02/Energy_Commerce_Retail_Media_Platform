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

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

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

# DWD-1: (station, parameter_source_code, has_observed_missing) accumulated
# across tables, captured pre-rename so codes match parameter_source_code.
_observed_rows: list[tuple] = []


def process(bt: str) -> None:
    cols = [c["name"] for c in TABLES[bt]["columns"]]
    qn_col = next(c for c in cols if c.startswith("QN_"))
    value_cols = [
        c
        for c in cols
        if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor")
        and not c.startswith("QN_")
    ]

    bronze_df = read_bronze(bt)
    df = strip_sentinels(bronze_df, [*value_cols, qn_col])

    for r in (
        df.groupBy("STATIONS_ID")
        .agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in value_cols])
        .collect()
    ):
        sid = str(r["STATIONS_ID"]).strip()
        for c in value_cols:
            _observed_rows.append((sid, c, (r[c] or 0) > 0))

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
    inspect_table(
        df,
        bt,
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
        key_cols=["station_id", "observation_ts"],
        df_before=bronze_df,
    )


for _bt in MEASUREMENT_TABLES:
    process(_bt)

# COMMAND ----------

# DBTITLE 1,DWD-1 -- missingness reconciliation (REPORTED vs OBSERVED)
# REPORTED = dwd_missing_value_periods; OBSERVED = captured above. Flags
# disagreement only -- never deletes or corrects either signal.

_observed = (
    spark.createDataFrame(
        _observed_rows,
        "station_id string, parameter_source_code string, has_observed_missing boolean",
    )
    .filter("has_observed_missing")
    .select("station_id", "parameter_source_code")
    .distinct()
    .withColumn("observed", F.lit(True))
)

_reported = (
    read_silver("dwd_missing_value_periods")
    .select("station_id", "parameter_source_code")
    .distinct()
    .withColumn("reported", F.lit(True))
)

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
write_silver(
    _recon,
    "dwd_missingness_reconciliation",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)
inspect_table(
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
