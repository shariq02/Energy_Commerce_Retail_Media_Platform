# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # APPROVAL SETUP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** create the approval tables and record the approval rule table
# MAGIC before the first recommendation. Once a recommendation exists the settings
# MAGIC cannot change.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Approval rules library
# MAGIC %run ../lib/_approval_rules

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Create the approval tables
for eco in ("energy", "commerce"):
    for name in ("approval_spec", "model_approval"):
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_fqn(name, eco)} "
            f"({MODEL_DDL[name]}) USING delta"
        )
        print(f"OK  approval table ready: {model_fqn(name, eco)}")

# COMMAND ----------

# DBTITLE 1,Settings recorded so far
recorded = {}
for eco in ("energy", "commerce"):
    rows = read_model("approval_spec", ecosystem=eco).collect()
    recorded[eco] = {(r["rule_id"], r["parameter"]): r["parameter_value"] for r in rows}
    print(f"{eco}: {len(rows)} setting row(s) recorded")

# COMMAND ----------

# DBTITLE 1,Settings cannot change once a recommendation exists
WANTED = {(r, p): v for r, _c, _d, p, v in APPROVAL_RULES}
for eco, have in recorded.items():
    changed = [k for k, v in WANTED.items() if k in have and have[k] != v]
    started = read_model("model_approval", ecosystem=eco).limit(1).count()
    check(
        "models/approve/00_approval_setup",
        "models",
        f"settings_unchanged_after_first_recommendation:{eco}",
        not (changed and started),
        detail=f"settings differ from the defaults after a recommendation: {changed}",
        metric_value=float(len(changed) if started else 0),
        rid=rid,
    )

# COMMAND ----------

# DBTITLE 1,Record the rule table
for eco, have in recorded.items():
    if have == WANTED:
        print(f"OK  {eco}: rule table already recorded, kept")
        continue
    rows = [(r, c, d, p, v, rid, now_utc()) for r, c, d, p, v in APPROVAL_RULES]
    replace_model_rows(
        spark.createDataFrame(rows, MODEL_DDL["approval_spec"]),
        "approval_spec",
        ecosystem=eco,
        predicate="rule_id IS NOT NULL",
    )
    print(f"OK  {len(rows)} setting row(s) recorded for {eco}")

# COMMAND ----------

# DBTITLE 1,The recorded settings load
for eco in ("energy", "commerce"):
    print(eco, spec_from_rows(read_model("approval_spec", ecosystem=eco).collect()))
