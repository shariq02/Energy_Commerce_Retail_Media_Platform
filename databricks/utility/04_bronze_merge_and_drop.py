# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # BRONZE CLEANUP -- MERGE SAME-SHAPE TABLES, DROP LOADED UPLOAD VOLUMES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** for every Bronze schema found in the catalog, (1) find the tables of
# MAGIC identical columns whose names differ in one part and merge them into one table
# MAGIC with a `source_variant` column, (2) drop the upload Volumes whose files were all
# MAGIC read by a load, (3) list the tables and Volumes of every Bronze schema against
# MAGIC the Unity Catalog limits. Nothing is named in this notebook: tables, Volumes and
# MAGIC schemas come from the catalog.
# MAGIC
# MAGIC A source table is dropped only after a row-for-row check of the merged table. A
# MAGIC Volume is dropped only when the lineage shows that every file in it was read.
# MAGIC The destructive steps are switches in the configuration cell. Safe to re-run.

# COMMAND ----------

# DBTITLE 1,Imports
from collections import defaultdict
from functools import reduce

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
DISCRIMINATOR = "source_variant"

# Unity Catalog limits: tables per schema (guard below the warning mail at 90) and
# Volumes per metastore (warning at 40, limit 50).
TABLE_GUARD = 90
VOLUME_WARNING = 40
VOLUME_LIMIT = 50

# Destructive steps stay off until the report has been read.
MERGE_SAME_SHAPE = False  # True
DROP_MERGED_SOURCES = False  # True
DROP_UPLOAD_VOLUMES = True  # False

print(f"Catalog : {CATALOG}")
print(f"Merge same-shape tables : {MERGE_SAME_SHAPE}")
print(f"Drop merged source tables : {DROP_MERGED_SOURCES}")
print(f"Drop upload Volumes : {DROP_UPLOAD_VOLUMES}")

# COMMAND ----------

# DBTITLE 1,Helper -- fingerprint a table (row count and order-independent hash)


def fingerprint(df, cols):
    row = df.agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.xxhash64(*[F.col(c) for c in cols]).cast("decimal(38,0)")).alias("h"),
    ).first()
    return row["n"], row["h"]


# COMMAND ----------

# DBTITLE 1,Helper -- merged name and value of each table, from the words the names share


def name_parts(names):
    """(merged name, {table: value}) from the words that the table names share at the
    start and at the end; None when the names share no word."""
    words = [name.split("_") for name in names]
    shortest = min(len(w) for w in words)
    head = 0
    while head < shortest and len({w[head] for w in words}) == 1:
        head += 1
    tail = 0
    while tail < shortest - head and len({w[-1 - tail] for w in words}) == 1:
        tail += 1
    # every table needs a non-empty value between the shared words
    while any(len(w) - head - tail < 1 for w in words) and (head or tail):
        if tail:
            tail -= 1
        else:
            head -= 1
    if not (head or tail):
        return None
    values = {
        name: "_".join(w[head : len(w) - tail])
        for name, w in zip(names, words, strict=True)
    }
    merged = "_".join(words[0][:head] + words[0][len(words[0]) - tail :])
    return merged, values


# COMMAND ----------

# DBTITLE 1,Helper -- walk a Volume for its files


def volume_files(path):
    files = []
    for entry in dbutils.fs.ls(path):
        if entry.isDir():
            files += volume_files(entry.path)
        else:
            files.append(entry.path.removeprefix("dbfs:"))
    return files


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze schemas

# COMMAND ----------

# DBTITLE 1,Find the Bronze schemas and count their tables
bronze_schemas = sorted(
    r["databaseName"]
    for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()
    if r["databaseName"].endswith("bronze")
)
tables_before = {}
for schema in bronze_schemas:
    tables_before[schema] = {
        r["tableName"]
        for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()
    }
    print(f"{schema}: {len(tables_before[schema])} tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Same-shape tables

# COMMAND ----------

# DBTITLE 1,Read the columns of every Bronze table
schema_list = ", ".join(f"'{schema}'" for schema in bronze_schemas)
column_rows = spark.sql(
    f"""
    SELECT table_schema, table_name, column_name, data_type
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema IN ({schema_list})
    """
).collect()
shapes = defaultdict(set)
for r in column_rows:
    shapes[(r["table_schema"], r["table_name"])].add((r["column_name"], r["data_type"]))
print(f"Tables read: {len(shapes)}")

# COMMAND ----------

# DBTITLE 1,Find the groups of tables with identical columns
by_shape = defaultdict(list)
for (schema, table), shape in shapes.items():
    by_shape[(schema, frozenset(shape))].append(table)

candidates = []
for (schema, _shape), tables in sorted(by_shape.items(), key=lambda item: item[0][0]):
    if len(tables) < 2:
        continue
    parts = name_parts(sorted(tables))
    if parts is None:
        print(f"SKIP  {schema}: {sorted(tables)} share no word in their names")
        continue
    merged, values = parts
    if merged in tables_before[schema]:
        print(f"SKIP  {schema}.{merged}: a table of this name exists")
        continue
    candidates.append({"schema": schema, "merged": merged, "values": values})
    print(f"FOUND {schema}: {values} -> {merged}")
if not candidates:
    print("No same-shape tables found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge (off by default)

# COMMAND ----------

# DBTITLE 1,Create the merged tables
source_fp = {}
created = []
for c in candidates:
    schema, merged, values = c["schema"], c["merged"], c["values"]
    if not MERGE_SAME_SHAPE:
        print(
            f"WOULD MERGE  {schema}: {sorted(values)} -> {merged}  "
            "(MERGE_SAME_SHAPE is False)"
        )
        continue
    columns = spark.table(f"{CATALOG}.{schema}.{next(iter(values))}").columns
    parts = []
    for table, value in values.items():
        frame = spark.table(f"{CATALOG}.{schema}.{table}")
        source_fp[(schema, table)] = fingerprint(frame, sorted(columns))
        parts.append(frame.withColumn(DISCRIMINATOR, F.lit(value)))
    (
        reduce(lambda a, b: a.unionByName(b), parts)
        .select(DISCRIMINATOR, *columns)
        .write.format("delta")
        .saveAsTable(f"{CATALOG}.{schema}.{merged}")
    )
    created.append(c)
    print(f"OK    {schema}.{merged}: created from {sorted(values)}")

# COMMAND ----------

# DBTITLE 1,Compare row count and content hash per value
validated = []
for c in created:
    schema, merged, values = c["schema"], c["merged"], c["values"]
    columns = [
        name
        for name in spark.table(f"{CATALOG}.{schema}.{merged}").columns
        if name != DISCRIMINATOR
    ]
    rows = (
        spark.table(f"{CATALOG}.{schema}.{merged}")
        .groupBy(DISCRIMINATOR)
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.sum(
                F.xxhash64(*[F.col(name) for name in sorted(columns)]).cast(
                    "decimal(38,0)"
                )
            ).alias("h"),
        )
        .collect()
    )
    found = {r[DISCRIMINATOR]: (r["n"], r["h"]) for r in rows}
    ok = set(found) == set(values.values())
    for table, value in values.items():
        match = found.get(value) == source_fp[(schema, table)]
        ok = ok and match
        print(
            f"  {merged}[{value}] vs {table}: rows={found.get(value, (None,))[0]}"
            f" / {source_fp[(schema, table)][0]} match={match}"
        )
    print(f"{'PASS' if ok else 'FAIL'}  {schema}.{merged}")
    if ok:
        validated.append(c)

# COMMAND ----------

# DBTITLE 1,Drop the source tables of every validated group
for c in validated:
    for table in c["values"]:
        if DROP_MERGED_SOURCES:
            spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{c['schema']}.{table}")
            print(f"DROP  {c['schema']}.{table}")
        else:
            print(f"WOULD DROP  {c['schema']}.{table}  (DROP_MERGED_SOURCES is False)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload Volumes (off by default)

# COMMAND ----------

# DBTITLE 1,Read the lineage of the loads (which paths were read to write a table)
try:
    read_paths = [
        (r["source_path"].removeprefix("dbfs:"), r["target_table_full_name"])
        for r in spark.sql(
            f"""
            SELECT DISTINCT source_path, target_table_full_name
            FROM system.access.table_lineage
            WHERE source_path LIKE '%/Volumes/{CATALOG}/%'
              AND target_table_full_name IS NOT NULL
            """
        ).collect()
    ]
except Exception as exc:
    read_paths = []
    print(f"Lineage not available: {str(exc)[:150]}")
print(f"Lineage rows for Volume paths: {len(read_paths)}")

# COMMAND ----------

# DBTITLE 1,Decide for every Volume whether all its files were read
volume_plan = {}
for schema in bronze_schemas:
    for r in spark.sql(f"SHOW VOLUMES IN {CATALOG}.{schema}").collect():
        volume = r["volume_name"]
        root = f"/Volumes/{CATALOG}/{schema}/{volume}"
        files = volume_files(root)
        read = {
            path.rstrip("/"): table
            for path, table in read_paths
            if path == root or path.startswith(root + "/")
        }
        unread = [
            f for f in files if not any(f == p or f.startswith(p + "/") for p in read)
        ]
        if not files:
            decision = "empty"
        elif not read:
            decision = "no lineage rows for this Volume"
        elif unread:
            decision = f"{len(unread)} of {len(files)} files not read by a load"
        else:
            decision = ""
        volume_plan[(schema, volume)] = decision
        fed = sorted(set(read.values()))
        print(f"{schema}.{volume}: {len(files)} files, fed {len(fed)} tables")

# COMMAND ----------

# DBTITLE 1,Drop the Volumes that are empty or fully read
for (schema, volume), decision in volume_plan.items():
    if decision not in ("", "empty"):
        print(f"KEEP  volume {schema}.{volume}: {decision}")
    elif DROP_UPLOAD_VOLUMES:
        spark.sql(f"DROP VOLUME IF EXISTS {CATALOG}.{schema}.{volume}")
        print(f"DROP  volume {schema}.{volume}  ({decision or 'every file was read'})")
    else:
        print(f"WOULD DROP  volume {schema}.{volume}  (DROP_UPLOAD_VOLUMES is False)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result

# COMMAND ----------

# DBTITLE 1,Tables and Volumes of every Bronze schema after this run
for schema in bronze_schemas:
    tables_after = sorted(
        r["tableName"]
        for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()
    )
    volumes_after = sorted(
        r["volume_name"]
        for r in spark.sql(f"SHOW VOLUMES IN {CATALOG}.{schema}").collect()
    )
    over = "  OVER THE GUARD" if len(tables_after) > TABLE_GUARD else ""
    empty = "  EMPTY SCHEMA" if not (tables_after or volumes_after) else ""
    print(
        f"{schema}: tables {len(tables_before[schema])} -> {len(tables_after)} "
        f"(guard {TABLE_GUARD}){over}; Volumes left {len(volumes_after)}{empty}"
    )
    for table in tables_after:
        print(f"  {table}")
    if volumes_after:
        print(f"  Volumes: {volumes_after}")

# COMMAND ----------

# DBTITLE 1,Volumes in the metastore
try:
    metastore_volumes = spark.sql(
        "SELECT count(*) AS n FROM system.information_schema.volumes"
    ).first()["n"]
except Exception as exc:
    print(f"Metastore Volume count not available: {str(exc)[:150]}")
else:
    status = "OVER THE LIMIT" if metastore_volumes > VOLUME_LIMIT else "ok"
    if status == "ok" and metastore_volumes >= VOLUME_WARNING:
        status = "at or above the warning"
    print(
        f"Volumes in the metastore: {metastore_volumes} "
        f"(warning {VOLUME_WARNING}, limit {VOLUME_LIMIT}): {status}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Once the table drop steps have been run, take a new Bronze schema snapshot with
# MAGIC `databricks/schema_registry/01_snapshot_bronze_schema.py`. It covers every
# MAGIC Bronze schema.
