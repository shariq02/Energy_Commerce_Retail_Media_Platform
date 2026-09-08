# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DWD SOLAR (10-MINUTE GRID)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `dwd_solar` into source-scoped Silver at its (station,
# MAGIC 10-minute) grain -- separate from the hourly notebook because the time
# MAGIC grid differs and `MESS_DATUM_WOZ` (true local solar time) is retained.
# MAGIC Same treatment otherwise: sentinels to NULL, same-key conflict
# MAGIC resolution, the DWD quality-level triple, English business renames,
# MAGIC station city to Bundesland AGS, `observation_ts` (UTC) from MESS_DATUM.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "dwd"
COMPONENT = "silver/energy/weather/02_dwd_solar"
RID = run_id()
BT = "dwd_solar"

CONTRACT = load_contract(SOURCE)
MAPPING = load_mapping(SOURCE)
TABLES = contract_tables(CONTRACT)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)

COLS = [c["name"] for c in TABLES[BT]["columns"]]
QN_COL = next(c for c in COLS if c.startswith("QN_"))
VALUE_COLS = [
    c
    for c in COLS
    if c not in ("STATIONS_ID", "city", "MESS_DATUM", "MESS_DATUM_WOZ", "eor")
    and not c.startswith("QN_")
]

# COMMAND ----------

# DBTITLE 1,dwd_solar -> Silver
df = strip_sentinels(read_bronze(BT), [*VALUE_COLS, QN_COL])
df, q = resolve_conflicts(
    df, ["STATIONS_ID", "MESS_DATUM"], VALUE_COLS, qn_col=QN_COL, bronze_table=BT
)
write_quarantine(q.withColumn("source_system", F.lit(SOURCE)), RID)

df = cast_logical(df, TABLES[BT]["columns"])
df = decode_qn(df, QN_COL)
df = apply_renames(df, NAME_MAP)
df = df.withColumn("_srid", sha_key(F.lit(BT), "STATIONS_ID", "MESS_DATUM"))
df = df.withColumn("observation_ts", parse_mess_datum_10min("MESS_DATUM")).drop(
    "MESS_DATUM"
)
df = df.withColumn("observation_woz", parse_mess_datum_10min("MESS_DATUM_WOZ", "UTC"))
df = attach_city_ags(df, "city")
df = add_provenance(df, SOURCE, "_srid", RID)
write_silver(df, BT, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print(f"DWD SOLAR -- COMPLETE  (run_id {RID})")
print("=" * 70)
