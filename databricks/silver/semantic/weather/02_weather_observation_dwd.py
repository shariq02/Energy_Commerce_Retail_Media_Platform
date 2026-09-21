# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- WEATHER OBSERVATION (DWD)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** DWD measurement tables into long `weather_observation` rows
# MAGIC (one per station, instant, variable), native value/unit kept beside the
# MAGIC standardised value. Loads the tables listed in `LOAD_TABLES`.

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

# DBTITLE 1,Weather specifications
# MAGIC %run ./_weather_specs

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "silver/semantic/weather/02_weather_observation_dwd"
RID = run_id()

# All DWD measurement tables; narrow the list to load a subset.
LOAD_TABLES = list(DWD_TABLES)

STATION_IDS = [
    str(s) for s in load_contract(SOURCE)["conventions"]["station_set"]["ids"]
]
_KEY_COLS = ("STATIONS_ID", "city", "MESS_DATUM", "eor")

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Helper -- Bronze measurement table to long weather_observation rows


def build_dwd_observations(bronze_df, table: str):
    meta = DWD_TABLES[table]
    qn = meta["qn"]
    data_cols = [c for c in bronze_df.columns if c not in _KEY_COLS and c != qn]

    df = bronze_df.withColumn(
        "STATIONS_ID", F.regexp_replace(F.trim(F.col("STATIONS_ID")), r"\.0$", "")
    ).filter(F.col("STATIONS_ID").isin(STATION_IDS))
    df = strip_sentinels(df, [*data_cols, qn])
    df, _conflicts = resolve_conflicts(
        df, ["STATIONS_ID", "MESS_DATUM"], data_cols, qn_col=qn, bronze_table=table
    )

    long = long_from_spec(
        df, meta["specs"], ["STATIONS_ID", "MESS_DATUM", qn, "_had_key_conflict"]
    )

    parse = parse_mess_datum if meta["time"] == "hourly" else parse_mess_datum_10min
    qn_code = F.regexp_replace(F.trim(F.col(qn)), r"\.0$", "")
    qn_labels = F.create_map([F.lit(x) for kv in DWD_QN_LABELS.items() for x in kv])
    methods = F.create_map([F.lit(x) for kv in METHOD_LABELS.items() for x in kv])

    long = (
        long.withColumn("source_location_id", F.col("STATIONS_ID"))
        .withColumn("location_key", location_key(SOURCE, "source_location_id"))
        .withColumn("observation_ts_native", F.trim(F.col("MESS_DATUM")))
        .withColumn("time_basis", F.lit("utc"))
        .withColumn("observation_ts_utc", parse("MESS_DATUM"))
        .withColumn("quality_code", qn_code)
        .withColumn("quality_label", qn_labels[qn_code])
        .withColumn(
            "quality_flag",
            F.when(F.col("_had_key_conflict"), "key_conflict_resolved").when(
                qn_code.isNull(), "qn_missing"
            ),
        )
        .withColumn(
            "observation_method",
            F.coalesce(
                methods[F.col("observation_method")], F.col("observation_method")
            ),
        )
        .withColumn("measurement_basis", F.lit("station_observation"))
        .withColumn(
            "source_record_id", sha_key(F.lit(table), "STATIONS_ID", "MESS_DATUM")
        )
        .withColumn(
            "observation_key",
            sha_key(F.lit(table), "STATIONS_ID", "MESS_DATUM", "source_column"),
        )
    )
    long = add_project_time(long)
    long = add_semantic_provenance(long, SOURCE, table, RID)
    return conform(long, WEATHER_OBSERVATION_COLUMNS)


# COMMAND ----------

# DBTITLE 1,Read Bronze -- tables in LOAD_TABLES
bronze_frames = {table: read_bronze(table) for table in LOAD_TABLES}

# COMMAND ----------

# DBTITLE 1,Transform -- long weather_observation frames
observation_frames = {
    table: build_dwd_observations(df, table) for table, df in bronze_frames.items()
}

# COMMAND ----------

# DBTITLE 1,Write Silver -- weather_observation (one table at a time)
for table, df in observation_frames.items():
    write_semantic(
        df,
        "weather_observation",
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
        replace_where=f"source_dataset = '{table}'",
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- weather_observation per loaded table
findings_blocks = {}
for table in LOAD_TABLES:
    written = spark.table(semantic_table("weather_observation")).filter(
        F.col("source_dataset") == table
    )
    findings_blocks[table] = inspect_table(
        written,
        "weather_observation",
        source=FINDINGS_SOURCE,
        component=COMPONENT,
        rid=RID,
        key_cols=["observation_key"],
        extra_checks=structure_extra_checks(written),
    )

# COMMAND ----------

# DBTITLE 1,Export findings -- weather_observation per loaded table
for table, blocks in findings_blocks.items():
    write_silver_findings(
        FINDINGS_SOURCE,
        f"{COMPONENT.split('/')[-1]}__{table}",
        f"weather_observation -- {table}",
        blocks,
    )
