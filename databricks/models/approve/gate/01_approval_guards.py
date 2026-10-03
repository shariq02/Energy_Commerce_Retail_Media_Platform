# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # APPROVAL GUARDS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** fail the run when a task has no decision, a decision is not the owner's, an override has no reason, a blocking rule is passed without an override, or an approved model lacks its evaluation at the frozen version or its stored artifact.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Approval rules library
# MAGIC %run ../../lib/_approval_rules

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/approve/gate/01_approval_guards"
SOURCE = "models"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the decisions and the evaluation record
record = {}
for eco in ("energy", "commerce"):
    approvals = [
        r.asDict() for r in read_model("model_approval", ecosystem=eco).collect()
    ]
    evaluated = {
        (r["task_id"], r["model_name"]): r["frozen_delta_version"]
        for r in read_model("evaluation_results", ecosystem=eco)
        .filter(~F.col("smoke") & (F.col("status") == "ok"))
        .select("task_id", "model_name", "frozen_delta_version")
        .distinct()
        .collect()
    }
    spec = (
        read_model("approval_spec", ecosystem=eco).agg(F.min("recorded_at")).first()[0]
    )
    record[eco] = (approvals, evaluated, spec)
    print(f"{eco}: {len(approvals)} decision row(s)")

# COMMAND ----------

# DBTITLE 1,Every task has a selected model and a baseline fallback
_none = []
for t in TASKS:
    rows = record[t["ecosystem"]][0]
    roles = {r["role"] for r in rows if r["task_id"] == t["task_id"]}
    if not {"selected", "baseline_fallback"} <= roles:
        _none.append(t["task_id"])
check(
    COMPONENT,
    SOURCE,
    "every_task_has_a_decision",
    not _none,
    detail=f"no decision: {_none}",
    metric_value=float(len(_none)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Decision values are valid
_invalid = [
    (r["task_id"], r["model_name"], r["decision"], r["restriction"])
    for rows, _e, _s in record.values()
    for r in rows
    if r["decision"] not in DECISIONS
    or r["role"] not in ROLES
    or r["restriction"] not in (None, *RESTRICTIONS)
]
check(
    COMPONENT,
    SOURCE,
    "decision_values_valid",
    not _invalid,
    detail=f"invalid values: {_invalid[:10]}",
    metric_value=float(len(_invalid)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Every decision is the owner's
_not_owner = [
    (r["task_id"], r["model_name"])
    for rows, _e, _s in record.values()
    for r in rows
    if r["decided_by"] != DECIDER_OWNER
]
check(
    COMPONENT,
    SOURCE,
    "every_decision_is_the_owners",
    not _not_owner,
    detail=f"decided by the rule engine only: {_not_owner[:10]}",
    metric_value=float(len(_not_owner)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,An override carries a reason
_no_reason = [
    (r["task_id"], r["model_name"])
    for rows, _e, _s in record.values()
    for r in rows
    if r["overridden"] and not (r["override_reason"] or "").strip()
]
check(
    COMPONENT,
    SOURCE,
    "override_has_a_reason",
    not _no_reason,
    detail=f"override without a reason: {_no_reason}",
    metric_value=float(len(_no_reason)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,A blocking rule is passed only by an override
_passed = []
for rows, _e, _s in record.values():
    for r in rows:
        rules = _json.loads(r["rules"] or "[]")
        blocked = any(
            x["rule"] in BLOCKING_RULES and x["outcome"] == "blocking" for x in rules
        )
        justified = r["overridden"] and (r["override_reason"] or "").strip()
        approved = r["role"] == "selected" and r["decision"] in APPROVED
        if approved and blocked and not justified:
            _passed.append((r["task_id"], r["model_name"]))
check(
    COMPONENT,
    SOURCE,
    "blocking_rule_passed_only_by_override",
    not _passed,
    detail=f"approved against a blocking rule: {_passed}",
    metric_value=float(len(_passed)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,An approved model has an evaluation at the frozen version and an artifact
_frozen = {t["task_id"]: frozen_version(t["dataset_id"], t["ecosystem"]) for t in TASKS}
_unsupported = []
for rows, evaluated, _s in record.values():
    for r in rows:
        if r["role"] != "selected" or r["decision"] not in APPROVED:
            continue
        key = (r["task_id"], r["model_name"])
        ok = evaluated.get(key) == _frozen[r["task_id"]] and r["mlflow_run_id"]
        if not ok:
            _unsupported.append(key)
check(
    COMPONENT,
    SOURCE,
    "approved_model_evaluated_at_frozen_version_with_artifact",
    not _unsupported,
    detail=f"approved without evaluation or artifact: {_unsupported}",
    metric_value=float(len(_unsupported)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Rules were recorded before the first recommendation
_late = []
for eco, (rows, _e, recorded) in record.items():
    first = min((r["recommended_at"] for r in rows), default=None)
    if recorded is None or (first is not None and recorded > first):
        _late.append(eco)
check(
    COMPONENT,
    SOURCE,
    "rules_recorded_before_first_recommendation",
    not _late,
    detail=f"rules recorded after a recommendation: {_late}",
    metric_value=float(len(_late)),
    rid=rid,
)
