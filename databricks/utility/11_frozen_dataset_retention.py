# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FROZEN DATASET RETENTION AND CLONES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** the model notebooks read every dataset with `VERSION AS OF` its frozen
# MAGIC version. Delta refuses a version older than `delta.deletedFileRetentionDuration`
# MAGIC (168 hours) and drops log entries after `delta.logRetentionDuration` (30 days).
# MAGIC This notebook (1) reports the history of every frozen dataset table, (2) sets both
# MAGIC retention values to a long interval, (3) checks that the frozen version reads, and
# MAGIC (4) deep clones each readable frozen version into its own table.
# MAGIC
# MAGIC Both writes are switched off (`APPLY_RETENTION`, `APPLY_CLONE`). Read the history
# MAGIC report first. Safe to run again: the clone skips tables that exist.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../models/lib/_model_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECOSYSTEMS = ("energy", "commerce")
RETENTION = "interval 3650 days"
APPLY_RETENTION = False  # True
APPLY_CLONE = False  # True
DROP_ECOSYSTEMS = ("energy",)
APPLY_DROP = False  # True

print(f"Retention: {RETENTION}")
print(f"Apply retention: {APPLY_RETENTION}")
print(f"Apply clone: {APPLY_CLONE}")
print(f"Apply drop: {APPLY_DROP} for {DROP_ECOSYSTEMS}")

# COMMAND ----------

# DBTITLE 1,Frozen datasets and their versions
frozen = {}
for eco in ECOSYSTEMS:
    rows = (
        read_ml("dataset_manifest", ecosystem=eco)
        .filter(F.col("freeze_status") == "frozen")
        .select("dataset_id", "frozen_delta_version")
        .collect()
    )
    for r in rows:
        frozen[(eco, r["dataset_id"])] = int(r["frozen_delta_version"])
print(f"OK  {len(frozen)} frozen dataset(s)")

# COMMAND ----------

# DBTITLE 1,History report -- vacuum runs and whether the frozen version is still in the log
report = []
for (eco, ds), version in frozen.items():
    full = ml_fqn(f"dataset_{ds}", eco)
    hist = spark.sql(f"DESCRIBE HISTORY {full}").select(
        "version", "timestamp", "operation"
    )
    rows = hist.collect()
    versions = {r["version"]: r["timestamp"] for r in rows}
    vacuums = [r["timestamp"] for r in rows if r["operation"] == "VACUUM END"]
    report.append(
        (
            eco,
            ds,
            version,
            max(versions),
            versions.get(version),
            min(versions),
            version in versions,
            len(vacuums),
            max(vacuums) if vacuums else None,
        )
    )
display(
    spark.createDataFrame(
        report,
        "ecosystem string, dataset_id string, frozen_version bigint, "
        "latest_version bigint, frozen_at timestamp, log_starts_at bigint, "
        "frozen_in_log boolean, vacuum_runs int, last_vacuum_at timestamp",
    ).orderBy("ecosystem", "dataset_id")
)

# COMMAND ----------

# DBTITLE 1,Retention values before the change
for eco, ds in frozen:
    full = ml_fqn(f"dataset_{ds}", eco)
    props = spark.sql(f"DESCRIBE DETAIL {full}").first()["properties"] or {}
    print(
        f"{full}: files {props.get('delta.deletedFileRetentionDuration', 'default')}, "
        f"log {props.get('delta.logRetentionDuration', 'default')}"
    )

# COMMAND ----------

# DBTITLE 1,Set the retention values
for eco, ds in frozen:
    full = ml_fqn(f"dataset_{ds}", eco)
    if not APPLY_RETENTION:
        print(f"SKIP  {full}: APPLY_RETENTION is False")
        continue
    spark.sql(
        f"ALTER TABLE {full} SET TBLPROPERTIES ("
        f"'delta.deletedFileRetentionDuration' = '{RETENTION}', "
        f"'delta.logRetentionDuration' = '{RETENTION}')"
    )
    print(f"OK  {full}: retention set to {RETENTION}")

# COMMAND ----------

# DBTITLE 1,Does the frozen version read
readable = {}
for (eco, ds), version in frozen.items():
    full = ml_fqn(f"dataset_{ds}", eco)
    try:
        spark.sql(f"SELECT * FROM {full} VERSION AS OF {version} LIMIT 1").collect()
        readable[(eco, ds)] = True
        print(f"OK    {full} version {version}")
    except Exception as exc:
        readable[(eco, ds)] = False
        print(f"FAIL  {full} version {version}: {str(exc).splitlines()[0][:150]}")
print(f"readable {sum(readable.values())} of {len(readable)}")

# COMMAND ----------

# DBTITLE 1,Clone plan and room in each schema
clone_name = {
    (eco, ds): ml_fqn(f"dataset_{ds}_frozen_v{version}", eco)
    for (eco, ds), version in frozen.items()
}
to_clone = {
    key: name
    for key, name in clone_name.items()
    if readable[key] and not spark.catalog.tableExists(name)
}
for eco in ECOSYSTEMS:
    new = sum(1 for (e, _) in to_clone if e == eco)
    have = _schema_object_count(eco)
    print(
        f"{ml_schema_for(eco)}: {have} object(s), {new} clone(s) to add, "
        f"stop at {SCHEMA_OBJECT_STOP} of {SCHEMA_OBJECT_QUOTA}"
    )
    if have + new > SCHEMA_OBJECT_STOP:
        print(f"WARN {ml_schema_for(eco)}: not enough room, the clone cell will stop")

# COMMAND ----------

# DBTITLE 1,Deep clone each readable frozen version
for (eco, ds), name in to_clone.items():
    if not APPLY_CLONE:
        print(f"SKIP  {name}: APPLY_CLONE is False")
        continue
    _quota_guard(name, eco)
    src = ml_fqn(f"dataset_{ds}", eco)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {name} DEEP CLONE {src} VERSION AS OF {frozen[(eco, ds)]}"
    )
    print(f"OK  {name}")

# COMMAND ----------

# DBTITLE 1,Clone row counts against the frozen version
for (eco, ds), name in clone_name.items():
    if not spark.catalog.tableExists(name):
        continue
    src = ml_fqn(f"dataset_{ds}", eco)
    want = spark.sql(
        f"SELECT COUNT(*) AS n FROM {src} VERSION AS OF {frozen[(eco, ds)]}"
    ).first()["n"]
    got = spark.table(name).count()
    print(f"{'OK  ' if want == got else 'FAIL'} {name}: {got} row(s), frozen {want}")

# COMMAND ----------

# DBTITLE 1,Drop the clones of the chosen ecosystems (frees schema objects)
for (eco, ds), name in clone_name.items():
    if eco not in DROP_ECOSYSTEMS or not spark.catalog.tableExists(name):
        continue
    src = ml_fqn(f"dataset_{ds}", eco)
    props = spark.sql(f"DESCRIBE DETAIL {src}").first()["properties"] or {}
    if "delta.deletedFileRetentionDuration" not in props:
        print(f"SKIP  {name}: retention is not set on {src}")
        continue
    if not APPLY_DROP:
        print(f"SKIP  {name}: APPLY_DROP is False")
        continue
    spark.sql(f"DROP TABLE {name}")
    print(f"OK  dropped {name}")
for eco in DROP_ECOSYSTEMS:
    print(f"{ml_schema_for(eco)}: {_schema_object_count(eco)} object(s)")
