# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION REFERENCES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** for every registered foundation model, record the repository,
# MAGIC revision, file hashes, adapter, licence and how it ran, beside the registry
# MAGIC version, in `foundation_registry_entry`. The registry table itself is not
# MAGIC changed. A version is recorded once.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Approval rules library
# MAGIC %run ../lib/_approval_rules

# COMMAND ----------

# DBTITLE 1,Registry library
# MAGIC %run ../lib/_registry_rules

# COMMAND ----------

# DBTITLE 1,Foundation time-series library
# MAGIC %run ../lib/_fit_foundation_ts

# COMMAND ----------

# DBTITLE 1,Imports
import ast as _ast

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/register/03_foundation_references"
SOURCE = "models"
ECOSYSTEMS = ("energy", "commerce")

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Create the reference table
for eco in ECOSYSTEMS:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {model_fqn(FOUNDATION_REGISTRY, eco)} "
        f"({FOUNDATION_DDL[FOUNDATION_REGISTRY]}) USING delta"
    )

# COMMAND ----------

# DBTITLE 1,Registered foundation models and what was recorded for them
manifest = read_foundation_manifest()["models"]
planned = {}
for eco in ECOSYSTEMS:
    entries = [
        e
        for e in latest_versions(registry_rows(eco)).values()
        if is_foundation_model(e["model_name"])
    ]
    params = {}
    if spark.catalog.tableExists(model_fqn(FOUNDATION_RESULTS, eco)):
        params = {
            (r["task_id"], r["model_name"]): _ast.literal_eval(r["params"] or "{}")
            for r in read_model(FOUNDATION_RESULTS, ecosystem=eco)
            .filter(~F.col("smoke") & (F.col("status") == "ok"))
            .select("task_id", "model_name", "params")
            .distinct()
            .collect()
        }
    known = {
        (r["task_id"], r["model_name"], r["role"], r["registry_version"])
        for r in read_model(FOUNDATION_REGISTRY, ecosystem=eco).collect()
    }
    planned[eco] = (entries, params, known)
    print(f"{eco}: {len(entries)} registered foundation entry(ies)")

# COMMAND ----------

# DBTITLE 1,Build the reference rows
now = now_utc()
rows = {eco: [] for eco in planned}
for eco, (entries, params, known) in planned.items():
    for e in entries:
        key = (e["task_id"], e["model_name"], e["role"], int(e["version"]))
        if key in known:
            continue
        info = params.get((e["task_id"], e["model_name"]), {})
        item = manifest.get(info.get("model_key"), {})
        rows[eco].append(
            (
                e["task_id"],
                e["model_name"],
                e["role"],
                int(e["version"]),
                info.get("model_key"),
                item.get("repo_id") or info.get("repo_id"),
                item.get("revision") or info.get("revision"),
                json.dumps(item.get("files", {}), sort_keys=True),
                info.get("adapter_dir") or None,
                item.get("licence") or info.get("licence"),
                item.get("licence_restriction"),
                info.get("mode"),
                json.dumps(info, sort_keys=True, default=str),
                rid,
                now,
            )
        )
print({eco: len(r) for eco, r in rows.items()})

# COMMAND ----------

# DBTITLE 1,Write the new reference rows
for eco, items in rows.items():
    if not items:
        print(f"OK  {eco}: nothing new")
        continue
    (
        spark.createDataFrame(items, FOUNDATION_DDL[FOUNDATION_REGISTRY])
        .write.format("delta")
        .mode("append")
        .saveAsTable(model_fqn(FOUNDATION_REGISTRY, eco))
    )
    print(f"OK  {eco}: {len(items)} row(s) appended")

# COMMAND ----------

# DBTITLE 1,Every registered foundation model has a complete reference
_incomplete = []
for eco, (entries, _params, _known) in planned.items():
    have = {
        (r["task_id"], r["model_name"], r["role"], r["registry_version"]): r
        for r in read_model(FOUNDATION_REGISTRY, ecosystem=eco).collect()
    }
    for e in entries:
        r = have.get((e["task_id"], e["model_name"], e["role"], int(e["version"])))
        if not (
            r
            and r["repo_id"]
            and r["revision"]
            and r["licence"]
            and r["weights_files"] not in (None, "{}")
        ):
            _incomplete.append((e["task_id"], e["model_name"], e["version"]))
check(
    COMPONENT,
    SOURCE,
    "foundation_entries_have_a_complete_reference",
    not _incomplete,
    detail=f"incomplete: {_incomplete}",
    metric_value=float(len(_incomplete)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_model_findings(
    ("energy", "commerce"),
    "register__03_foundation_references",
    "Foundation References",
    ["foundation_registry_entry"],
    rid,
)
