# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # APPROVAL DECISIONS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** record the owner's decision for every model. A row keeps the rule
# MAGIC recommendation unless an override is listed; an override needs a reason. Running
# MAGIC the notebook with no override confirms the recommendations as the decisions.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Approval rules library
# MAGIC %run ../lib/_approval_rules

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/approve/02_record_decisions"
SOURCE = "models"
# (task_id, model_name): {"decision": ..., "restriction": ..., "reason": ...}
OWNER_DECISIONS = {}

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the recommendations
rows = {
    eco: [r.asDict() for r in read_model("model_approval", ecosystem=eco).collect()]
    for eco in ("energy", "commerce")
}
print({eco: len(v) for eco, v in rows.items()})

# COMMAND ----------

# DBTITLE 1,The overrides name existing models and carry a reason
_known = {(r["task_id"], r["model_name"]) for v in rows.values() for r in v}
_unknown = [k for k in OWNER_DECISIONS if k not in _known]
_bad = [
    k
    for k, o in OWNER_DECISIONS.items()
    if o.get("decision") not in DECISIONS
    or o.get("restriction") not in (None, *RESTRICTIONS)
    or not str(o.get("reason") or "").strip()
]
if _unknown or _bad:
    raise RuntimeError(f"invalid overrides: unknown {_unknown}, incomplete {_bad}")
print(f"{len(OWNER_DECISIONS)} override(s)")

# COMMAND ----------

# DBTITLE 1,Apply the decisions
now = now_utc()
decided = {}
for eco, items in rows.items():
    out = []
    for r in items:
        o = OWNER_DECISIONS.get((r["task_id"], r["model_name"]))
        row = dict(r)
        row["decision"] = r["recommended_decision"]
        row["restriction"] = r["recommended_restriction"]
        row["overridden"], row["override_reason"] = False, None
        if o:
            row["decision"], row["restriction"] = o["decision"], o.get("restriction")
            same = (row["decision"], row["restriction"]) == (
                r["recommended_decision"],
                r["recommended_restriction"],
            )
            row["overridden"] = not same
            row["override_reason"] = str(o["reason"]).strip()
        row["decided_by"], row["decided_at"], row["run_id"] = DECIDER_OWNER, now, rid
        out.append(row)
    decided[eco] = out
print({eco: sum(r["overridden"] for r in v) for eco, v in decided.items()})

# COMMAND ----------

# DBTITLE 1,Write the decisions
for eco, items in decided.items():
    cols = read_model("model_approval", ecosystem=eco).columns
    tuples = [tuple(r[c] for c in cols) for r in items]
    replace_model_rows(
        spark.createDataFrame(tuples, MODEL_DDL["model_approval"]),
        "model_approval",
        ecosystem=eco,
        predicate="task_id IS NOT NULL",
    )

# COMMAND ----------

# DBTITLE 1,Every model of every task carries the owner's decision
_open = [
    (eco, r["task_id"], r["model_name"])
    for eco, items in decided.items()
    for r in items
    if r["decided_by"] != DECIDER_OWNER
]
check(
    COMPONENT,
    SOURCE,
    "every_model_decided_by_the_owner",
    not _open,
    detail=f"not decided by the owner: {_open[:10]}",
    metric_value=float(len(_open)),
    rid=rid,
)
