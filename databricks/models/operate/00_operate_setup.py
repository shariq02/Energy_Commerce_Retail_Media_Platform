# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # OPERATE SETUP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** create the prediction, reference, monitoring and trigger tables and record the
# MAGIC monitoring settings before the first window is read. Once a monitoring result
# MAGIC exists the settings cannot change.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Registry library
# MAGIC %run ../lib/_registry_rules

# COMMAND ----------

# DBTITLE 1,Operate rules library
# MAGIC %run ../lib/_operate_rules

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/operate/00_operate_setup"
SOURCE = "models"
OPERATE_TABLES = (
    "monitoring_spec",
    "reference_profile",
    "model_predictions",
    "monitoring_results",
    "monitoring_flags",
    "monitoring_triggers",
)

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Create the operate tables
for eco in ("energy", "commerce"):
    for name in OPERATE_TABLES:
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_fqn(name, eco)} "
            f"({MODEL_DDL[name]}) USING delta"
        )
        print(f"OK  operate table ready: {model_fqn(name, eco)}")

# COMMAND ----------

# DBTITLE 1,Settings recorded so far
recorded = {}
for eco in ("energy", "commerce"):
    rows = read_model("monitoring_spec", ecosystem=eco).collect()
    recorded[eco] = {r["parameter"]: r["parameter_value"] for r in rows}
    print(f"{eco}: {len(rows)} setting row(s) recorded")

# COMMAND ----------

# DBTITLE 1,Settings cannot change once a monitoring result exists
WANTED = {p: v for p, v, _d in OPERATE_DEFAULTS}
for eco, have in recorded.items():
    changed = [k for k, v in WANTED.items() if k in have and have[k] != v]
    started = read_model("monitoring_results", ecosystem=eco).limit(1).count()
    check(
        COMPONENT,
        SOURCE,
        f"settings_unchanged_after_first_monitoring:{eco}",
        not (changed and started),
        detail=f"settings differ from the defaults after monitoring started: {changed}",
        metric_value=float(len(changed) if started else 0),
        rid=rid,
    )

# COMMAND ----------

# DBTITLE 1,Record the settings
for eco, have in recorded.items():
    if have == WANTED:
        print(f"OK  {eco}: settings already recorded, kept")
        continue
    now = now_utc()
    rows = [
        {
            "parameter": p,
            "parameter_value": v,
            "description": d,
            "run_id": rid,
            "recorded_at": now,
        }
        for p, v, d in OPERATE_DEFAULTS
    ]
    replace_rows(rows, "monitoring_spec", eco, "parameter IS NOT NULL")
    print(f"OK  {len(rows)} setting row(s) recorded for {eco}")

# COMMAND ----------

# DBTITLE 1,The recorded settings load
for eco in ("energy", "commerce"):
    rows = read_model("monitoring_spec", ecosystem=eco).collect()
    print(eco, operate_spec_from_rows([r.asDict() for r in rows]))
