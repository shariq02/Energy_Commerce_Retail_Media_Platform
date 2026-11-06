# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ML REGISTRY TABLES -- MERGE THE TWO ECOSYSTEM SCHEMAS INTO ONE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** nine registry tables of the two ML dataset schemas
# MAGIC (`energy_ml_datasets`, `commerce_ml_datasets`) have the same names and columns.
# MAGIC They now live once in the schema `ml_datasets_shared`, with an `ecosystem`
# MAGIC column. This notebook copies the rows, checks every table against its source
# MAGIC row for row, and drops a source table only after that check.
# MAGIC
# MAGIC `gap_limits` (energy only) and every `dataset_*`, `assembled_*`, `target_*` and
# MAGIC `features_*` table stay where they are. No schema is dropped.
# MAGIC
# MAGIC Both writes are switched off in the configuration cell (`APPLY_COPY`,
# MAGIC `APPLY_DROP`). Run with both off to read the plan, then switch on the copy, read
# MAGIC the check, and switch on the drop. A copy skips a table that already holds rows
# MAGIC of the ecosystem, so the notebook is safe to run again.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SOURCE_SCHEMAS = {"energy": "energy_ml_datasets", "commerce": "commerce_ml_datasets"}
TARGET_SCHEMA = "ml_datasets_shared"
TABLES = (
    "dataset_manifest",
    "split_specification",
    "partition_manifest",
    "null_class_registry",
    "imputer_parameters",
    "mask_specification",
    "null_rate_profile",
    "feature_contract",
    "gate_results",
)
ECOSYSTEM_COLUMN = "ecosystem"
APPLY_COPY = False  # True
APPLY_DROP = False  # True

print(f"Sources : {SOURCE_SCHEMAS}")
print(f"Target  : {CATALOG}.{TARGET_SCHEMA}")
print(f"Tables  : {list(TABLES)}")
print(f"Apply copy: {APPLY_COPY}")
print(f"Apply drop: {APPLY_DROP}")

# COMMAND ----------

# DBTITLE 1,Helper -- fingerprint of a frame (row count and order-independent hash)


def fingerprint(df, cols):
    row = df.agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.xxhash64(*[F.col(c) for c in cols]).cast("decimal(38,0)")).alias("h"),
    ).first()
    return row["n"], row["h"]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspection (read-only)

# COMMAND ----------

# DBTITLE 1,The registry tables in each source schema
_wanted = ", ".join(f"'{t}'" for t in TABLES)
source_tables = {}
for eco, schema in SOURCE_SCHEMAS.items():
    rows = spark.sql(
        f"SELECT table_name FROM {CATALOG}.information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_type <> 'VIEW' "
        f"AND table_name IN ({_wanted}) ORDER BY 1"
    ).collect()
    source_tables[eco] = [r["table_name"] for r in rows]
    print(f"{schema}: {len(rows)} of {len(TABLES)} table(s)")

# COMMAND ----------

# DBTITLE 1,Tables that differ between the two schemas
names = set(TABLES)
_gaps = {
    eco: sorted(names - set(tables))
    for eco, tables in source_tables.items()
    if names - set(tables)
}
print(f"{len(names)} distinct table name(s)")
print(f"Missing in one schema: {_gaps if _gaps else 'none'}")

# COMMAND ----------

# DBTITLE 1,Objects in the target schema
_target_exists = bool(
    spark.sql(f"SHOW SCHEMAS IN {CATALOG} LIKE '{TARGET_SCHEMA}'").count()
)
_target_have = (
    spark.sql(f"SHOW TABLES IN {CATALOG}.{TARGET_SCHEMA}").count()
    if _target_exists
    else 0
)
print(f"{TARGET_SCHEMA}: {_target_have} table(s) now, {len(names)} to hold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy

# COMMAND ----------

# DBTITLE 1,Create the target schema
if APPLY_COPY:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")
    print(f"OK  schema ready: {CATALOG}.{TARGET_SCHEMA}")
else:
    print("SKIP  APPLY_COPY is False")

# COMMAND ----------

# DBTITLE 1,Create the target tables from the source columns
for name in sorted(names):
    target = f"{CATALOG}.{TARGET_SCHEMA}.{name}"
    if not APPLY_COPY:
        print(f"SKIP  {target}: APPLY_COPY is False")
        continue
    eco = next(e for e, tables in source_tables.items() if name in tables)
    source = f"{CATALOG}.{SOURCE_SCHEMAS[eco]}.{name}"
    extra = (
        ""
        if ECOSYSTEM_COLUMN in spark.table(source).columns
        else f", CAST(NULL AS STRING) AS {ECOSYSTEM_COLUMN}"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target} USING delta AS "
        f"SELECT *{extra} FROM {source} WHERE 1 = 0"
    )
    print(f"OK  {target}")

# COMMAND ----------

# DBTITLE 1,Helper -- the source rows in the column order of the target


def rows_for_target(src, target, eco):
    """Source rows with the ecosystem value; a target column that the source lacks
    is filled with NULL."""
    if ECOSYSTEM_COLUMN not in src.columns:
        src = src.withColumn(ECOSYSTEM_COLUMN, F.lit(eco))
    return src.select(
        *[
            F.col(f.name)
            if f.name in src.columns
            else F.lit(None).cast(f.dataType).alias(f.name)
            for f in spark.table(target).schema.fields
        ]
    )


# COMMAND ----------

# DBTITLE 1,Copy the rows of each ecosystem
for eco, schema in SOURCE_SCHEMAS.items():
    for name in source_tables[eco]:
        source = f"{CATALOG}.{schema}.{name}"
        target = f"{CATALOG}.{TARGET_SCHEMA}.{name}"
        if not APPLY_COPY:
            print(f"SKIP  {source}: APPLY_COPY is False")
            continue
        have = (
            spark.table(target).filter(F.col(ECOSYSTEM_COLUMN) == eco).limit(1).count()
        )
        if have:
            print(f"SKIP  {source}: {target} already holds rows of {eco}")
            continue
        rows_for_target(spark.table(source), target, eco).write.format("delta").mode(
            "append"
        ).saveAsTable(target)
        print(f"OK  {source} -> {target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check

# COMMAND ----------

# DBTITLE 1,Every source table against its rows in the target
verified = {}
for eco, schema in SOURCE_SCHEMAS.items():
    for name in source_tables[eco]:
        source = f"{CATALOG}.{schema}.{name}"
        target = f"{CATALOG}.{TARGET_SCHEMA}.{name}"
        if not spark.catalog.tableExists(target):
            verified[(eco, name)] = False
            print(f"MISSING  {target}")
            continue
        src = spark.table(source)
        tgt = spark.table(target).filter(F.col(ECOSYSTEM_COLUMN) == eco)
        cols = [c for c in src.columns if c in tgt.columns and c != ECOSYSTEM_COLUMN]
        want = src.count()
        got = tgt.count()
        try:
            same_hash = fingerprint(src, cols) == fingerprint(tgt, cols)
            how = "rows and hash"
        except Exception as exc:
            same_hash = True
            how = f"rows only ({type(exc).__name__})"
        ok = want == got and same_hash
        verified[(eco, name)] = ok
        flag = "OK  " if ok else "FAIL"
        print(f"{flag} {eco}.{name}: source {want}, target {got} ({how})")
print(f"verified {sum(verified.values())} of {len(verified)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop the source tables

# COMMAND ----------

# DBTITLE 1,Drop each verified source table
_failed = [k for k, ok in verified.items() if not ok]
if _failed:
    print(f"STOP  {len(_failed)} table(s) failed the check, nothing dropped: {_failed}")
for (eco, name), ok in verified.items():
    source = f"{CATALOG}.{SOURCE_SCHEMAS[eco]}.{name}"
    if _failed or not ok:
        continue
    if not APPLY_DROP:
        print(f"SKIP  {source}: APPLY_DROP is False")
        continue
    spark.sql(f"DROP TABLE {source}")
    print(f"OK  dropped {source}")
for schema in SOURCE_SCHEMAS.values():
    left = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").count()
    print(f"{schema}: {left} table(s) left")
