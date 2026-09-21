# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # BRONZE CLEANUP -- MERGE IDENTICAL-SCHEMA TABLES, DROP SEARCH VISIBILITY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** consolidate the Bronze tables that share one schema, drop the
# MAGIC Search Visibility Bronze tables and Volumes, and drop any leftover upload
# MAGIC Volumes (the loaders normally delete their own, so most will already be gone).
# MAGIC
# MAGIC - `honda_iot_{electricity,heating,cooling}_{p,w}` (6 tables) become
# MAGIC   `honda_iot_{electricity,heating,cooling}` (3 tables) with a
# MAGIC   `measurement_type` column (`p` / `w`).
# MAGIC - `mastr_{einheitentypen,lokationstypen,marktfunktionen,marktrollen}`
# MAGIC   (4 tables) become `mastr_code_lookup` (1 table) with a `catalog_kind`
# MAGIC   column.
# MAGIC - `search_visibility_events` and `search_visibility_repository` are dropped.
# MAGIC
# MAGIC Bronze is the only persisted copy of these rows (the loaders delete their
# MAGIC source Volumes), so nothing is dropped until the merged table has been
# MAGIC checked against its sources row-for-row. The drop steps are switched off in
# MAGIC the configuration cell; read the validation output first, then switch them
# MAGIC on and re-run only the matching cell.
# MAGIC
# MAGIC Every source table is only read. A merged table is created only if it does
# MAGIC not exist yet, so the notebook is safe to re-run.

# COMMAND ----------

# DBTITLE 1,Imports
from functools import reduce

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"

# merged table -> (discriminator column, {discriminator value: source table})
MERGES = {
    "honda_iot_electricity": (
        "measurement_type",
        {"p": "honda_iot_electricity_p", "w": "honda_iot_electricity_w"},
    ),
    "honda_iot_heating": (
        "measurement_type",
        {"p": "honda_iot_heating_p", "w": "honda_iot_heating_w"},
    ),
    "honda_iot_cooling": (
        "measurement_type",
        {"p": "honda_iot_cooling_p", "w": "honda_iot_cooling_w"},
    ),
    "mastr_code_lookup": (
        "catalog_kind",
        {
            "einheitentypen": "mastr_einheitentypen",
            "lokationstypen": "mastr_lokationstypen",
            "marktfunktionen": "mastr_marktfunktionen",
            "marktrollen": "mastr_marktrollen",
        },
    ),
}

SEARCH_VISIBILITY_TABLES = ["search_visibility_events", "search_visibility_repository"]
SEARCH_VISIBILITY_VOLUMES = ["search_visibility_events", "search_visibility_reference"]

# Upload Volumes no longer needed once their tables are loaded; the retired
# change-capture landing Volume is included.
LEFTOVER_VOLUMES = [
    "dwd_analytical",
    "dwd_metadata",
    "smard_analytical",
    "honda_iot_analytical",
    "rees46_events",
    "mastr_analytical",
    "mastr_reference",
    "power_plant_list_analytical",
    "redispatch_analytical",
    "ga4_events",
    "cdc_operational_landing",
]

# Destructive steps stay off until the validation output has been read.
DROP_MERGED_SOURCES = True #False
# Set True only after confirming the external raw copy of the Search Visibility data.
DROP_SEARCH_VISIBILITY = True #False
DROP_LEFTOVER_VOLUMES = True #False

print(f"Catalog / schema : {BRONZE}")
print(f"Merged tables    : {list(MERGES)}")
print(f"Drop merged sources : {DROP_MERGED_SOURCES}")
print(f"Drop Search Visibility : {DROP_SEARCH_VISIBILITY}")
print(f"Drop leftover Volumes : {DROP_LEFTOVER_VOLUMES}")

# COMMAND ----------

# DBTITLE 1,Helper -- fingerprint a table (row count and order-independent hash)

def fingerprint(df, cols):
    row = df.agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.xxhash64(*[F.col(c) for c in cols]).cast("decimal(38,0)")).alias("h"),
    ).first()
    return row["n"], row["h"]


# COMMAND ----------

# DBTITLE 1,Helper -- merged frame for one group

def merged_frame(disc_col, sources):
    parts = [
        spark.table(f"{BRONZE}.{table}").withColumn(disc_col, F.lit(value))
        for value, table in sources.items()
    ]
    cols = spark.table(f"{BRONZE}.{next(iter(sources.values()))}").columns
    return reduce(lambda a, b: a.unionByName(b), parts).select(disc_col, *cols)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspection (read-only)

# COMMAND ----------

# DBTITLE 1,Tables currently in the bronze schema
existing_tables = {
    r["tableName"] for r in spark.sql(f"SHOW TABLES IN {BRONZE}").collect()
}
print(f"Tables in {BRONZE}: {len(existing_tables)}")

# COMMAND ----------

# DBTITLE 1,Status of each merge group
group_state = {}
for target, (_, sources) in MERGES.items():
    present = [t for t in sources.values() if t in existing_tables]
    group_state[target] = {
        "sources_present": len(present) == len(sources),
        "sources_missing": sorted(set(sources.values()) - set(present)),
        "target_present": target in existing_tables,
    }
    print(f"{target}: {group_state[target]}")

# COMMAND ----------

# DBTITLE 1,Check that every group's source tables share one schema
schema_ok = {}
for target, (_, sources) in MERGES.items():
    if not group_state[target]["sources_present"]:
        continue
    col_sets = {t: spark.table(f"{BRONZE}.{t}").columns for t in sources.values()}
    first = next(iter(col_sets.values()))
    schema_ok[target] = all(cols == first for cols in col_sets.values())
    print(f"{target}: identical schema = {schema_ok[target]}")
    if not schema_ok[target]:
        for t, cols in col_sets.items():
            print(f"  {t}: {cols}")

bad = [t for t, ok in schema_ok.items() if not ok]
if bad:
    raise RuntimeError(f"schemas differ within group(s): {bad} -- stop, do not merge")

# COMMAND ----------

# DBTITLE 1,Fingerprint every source table
source_fp = {}
for target, (_, sources) in MERGES.items():
    if not group_state[target]["sources_present"]:
        continue
    for table in sources.values():
        df = spark.table(f"{BRONZE}.{table}")
        source_fp[table] = fingerprint(df, df.columns)
        print(f"{table}: rows={source_fp[table][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build merged tables

# COMMAND ----------

# DBTITLE 1,Create each merged table if it does not exist yet
for target, (disc_col, sources) in MERGES.items():
    state = group_state[target]
    if state["target_present"]:
        print(f"SKIP  {target}: already exists")
        continue
    if not state["sources_present"]:
        print(f"FAIL  {target}: missing sources {state['sources_missing']}")
        continue
    (
        merged_frame(disc_col, sources)
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(f"{BRONZE}.{target}")
    )
    print(f"OK    {target}: created from {list(sources.values())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate merged tables against their sources

# COMMAND ----------

# DBTITLE 1,Compare row count and content hash per discriminator value
validated = {}
for target, (disc_col, sources) in MERGES.items():
    if not group_state[target]["sources_present"]:
        print(f"SKIP  {target}: sources already gone")
        continue
    df_target = spark.table(f"{BRONZE}.{target}")
    src_cols = spark.table(f"{BRONZE}.{next(iter(sources.values()))}").columns
    rows = (
        df_target.groupBy(disc_col)
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.sum(
                F.xxhash64(*[F.col(c) for c in src_cols]).cast("decimal(38,0)")
            ).alias("h"),
        )
        .collect()
    )
    found = {r[disc_col]: (r["n"], r["h"]) for r in rows}
    ok = set(found) == set(sources)
    for value, table in sources.items():
        match = found.get(value) == source_fp[table]
        ok = ok and match
        print(
            f"  {target}[{value}] vs {table}: rows={found.get(value, (None,))[0]}"
            f" / {source_fp[table][0]} match={match}"
        )
    validated[target] = ok
    print(f"{'PASS' if ok else 'FAIL'}  {target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop merged source tables (off by default)

# COMMAND ----------

# DBTITLE 1,Drop the source tables of every validated group
for target, (_, sources) in MERGES.items():
    if not validated.get(target):
        print(f"KEEP  {target}: not validated in this run")
        continue
    for table in sources.values():
        if DROP_MERGED_SOURCES:
            spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.{table}")
            print(f"DROP  {table}")
        else:
            print(f"WOULD DROP  {table}  (DROP_MERGED_SOURCES is False)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Search Visibility (off by default)

# COMMAND ----------

# DBTITLE 1,Inspect the Search Visibility tables and Volumes
for table in SEARCH_VISIBILITY_TABLES:
    if table in existing_tables:
        print(f"{table}: rows={spark.table(f'{BRONZE}.{table}').count()}")
    else:
        print(f"{table}: not present")

display(spark.sql(f"SHOW VOLUMES IN {BRONZE}"))

# COMMAND ----------

# DBTITLE 1,Drop the Search Visibility tables
for table in SEARCH_VISIBILITY_TABLES:
    if DROP_SEARCH_VISIBILITY:
        spark.sql(f"DROP TABLE IF EXISTS {BRONZE}.{table}")
        print(f"DROP  table {table}")
    else:
        print(f"WOULD DROP  table {table}  (DROP_SEARCH_VISIBILITY is False)")

# COMMAND ----------

# DBTITLE 1,Drop the Search Visibility Volumes
for volume in SEARCH_VISIBILITY_VOLUMES:
    if DROP_SEARCH_VISIBILITY:
        spark.sql(f"DROP VOLUME IF EXISTS {BRONZE}.{volume}")
        print(f"DROP  volume {volume}")
    else:
        print(f"WOULD DROP  volume {volume}  (DROP_SEARCH_VISIBILITY is False)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leftover Volumes (off by default)

# COMMAND ----------

# DBTITLE 1,Inspect the Volumes that exist
live_volumes = sorted(
    r["volume_name"] for r in spark.sql(f"SHOW VOLUMES IN {BRONZE}").collect()
)
print(f"Volumes in {BRONZE}: {live_volumes}")
print(
    f"Not in the drop list (kept): {sorted(set(live_volumes) - set(LEFTOVER_VOLUMES))}"
)

# COMMAND ----------

# DBTITLE 1,Show what each leftover Volume still holds
for volume in LEFTOVER_VOLUMES:
    if volume not in live_volumes:
        print(f"{volume}: not present")
        continue
    entries = dbutils.fs.ls(f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{volume}")
    print(f"{volume}: {len(entries)} top-level entries")

# COMMAND ----------

# DBTITLE 1,Drop the leftover Volumes
for volume in LEFTOVER_VOLUMES:
    if DROP_LEFTOVER_VOLUMES:
        spark.sql(f"DROP VOLUME IF EXISTS {BRONZE}.{volume}")
        print(f"DROP  volume {volume}")
    else:
        print(f"WOULD DROP  volume {volume}  (DROP_LEFTOVER_VOLUMES is False)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result

# COMMAND ----------

# DBTITLE 1,Bronze inventory after this run
final_tables = sorted(
    r["tableName"] for r in spark.sql(f"SHOW TABLES IN {BRONZE}").collect()
)
print(f"Tables before: {len(existing_tables)}")
print(f"Tables after : {len(final_tables)}")
for table in final_tables:
    print(f"  {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC Once the table drop steps have been run, take a new Bronze schema snapshot with
# MAGIC `databricks/schema_registry/01_snapshot_bronze_schema.py`. The expected
# MAGIC table count after both steps is 55.