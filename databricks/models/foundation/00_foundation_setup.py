# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION SETUP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** create the weights volume and the foundation tables, check the
# MAGIC weight files in the volume against the manifest written by the upload command,
# MAGIC and record each model with its repository, revision, licence and file hashes.
# MAGIC Run on the same compute type as the foundation notebooks.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Foundation time-series library
# MAGIC %run ../lib/_fit_foundation_ts

# COMMAND ----------

# DBTITLE 1,Foundation tabular library
# MAGIC %run ../lib/_fit_foundation_tab

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/foundation/00_foundation_setup"
SOURCE = "models"
FOUNDATION_ECOSYSTEMS = ("energy", "commerce")
LIBRARY_NAMES = (
    "chronos",
    "timesfm",
    "tsfm_public",
    "tabpfn",
    "tabicl",
    "momentfm",
    "toto2",
    "peft",
    "sksurv",
)

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Create the weights volume
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{VOLUME_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{VOLUME_SCHEMA}.{FOUNDATION_VOLUME}")
print(f"OK  volume ready: {foundation_root()}")

# COMMAND ----------

# DBTITLE 1,Create the foundation tables
for eco in FOUNDATION_ECOSYSTEMS:
    for name, ddl in FOUNDATION_DDL.items():
        print(f"OK  foundation table ready: {create_model_table(name, ddl, eco)}")

# COMMAND ----------

# DBTITLE 1,Schema headroom
for eco in FOUNDATION_ECOSYSTEMS:
    n = spark.sql(f"SHOW TABLES IN {CATALOG}.{model_schema_for(eco)}").count()
    check(
        COMPONENT,
        SOURCE,
        f"schema_headroom:{eco}",
        n < SCHEMA_OBJECT_WARN,
        detail=f"{n} objects of {SCHEMA_OBJECT_QUOTA}",
        metric_value=float(n),
        rid=rid,
    )

# COMMAND ----------

# DBTITLE 1,Read the manifest and check every file against its hash
manifest = read_foundation_manifest()
entries = [model_entry(key) for key in sorted(manifest["models"])]
for e in entries:
    print(
        f"OK  {e['model_key']}: {e['repo_id']} @ {e['revision'][:12]}, {e['licence']}"
    )

# COMMAND ----------

# DBTITLE 1,Every model has a repository, revision, licence and file hashes
_incomplete = [
    e["model_key"]
    for e in entries
    if not (e["repo_id"] and e["revision"] and e["licence"] and e["files"])
]
check(
    COMPONENT,
    SOURCE,
    "models_have_revision_and_licence",
    not _incomplete,
    detail=f"incomplete: {_incomplete}",
    metric_value=float(len(_incomplete)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Record the models
_rows = [
    (
        e["model_key"],
        e["repo_id"],
        e["revision"],
        e["licence"],
        e.get("licence_restriction") or "none",
        e.get("pretraining_data") or "",
        json.dumps(e["files"], sort_keys=True),
        rid,
        now_utc(),
    )
    for e in entries
]
for eco in FOUNDATION_ECOSYSTEMS:
    replace_model_rows(
        spark.createDataFrame(_rows, FOUNDATION_DDL[FOUNDATION_REGISTER]),
        FOUNDATION_REGISTER,
        ecosystem=eco,
        predicate="model_key IS NOT NULL",
    )

# COMMAND ----------

# DBTITLE 1,Libraries and device of this compute
for lib in LIBRARY_NAMES:
    print(
        f"{'OK  ' if library_available(lib) else 'MISS'} {lib:12s} "
        f"{library_version(lib) or '-'}"
    )
print("processor type:", _platform.machine(), "| library folder:", library_folder())
