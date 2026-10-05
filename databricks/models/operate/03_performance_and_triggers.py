# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # PERFORMANCE AND TRIGGERS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** record the performance check and the retraining triggers of each registered
# MAGIC model. The performance check compares the recorded held-out value with the recorded
# MAGIC validation value against the recorded degradation limit. A trigger is a record: it
# MAGIC never changes a decision, a status or a model. Reads recorded results only.

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
COMPONENT = "models/operate/03_performance_and_triggers"
SOURCE = "models"
PERFORMANCE_WINDOW = "held_out"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the registry, the decisions, the limit and the results
evidence = {}
for eco in ("energy", "commerce"):
    approvals = {
        (r["task_id"], r["model_name"]): r.asDict()
        for r in read_model("model_approval", ecosystem=eco).collect()
        if r["role"] == "selected"
    }
    limit = None
    for r in read_model("approval_spec", ecosystem=eco).collect():
        if r["parameter"] == "degradation_relative":
            limit = float(r["parameter_value"])
    results = [
        r.asDict()
        for r in read_model("monitoring_results", ecosystem=eco)
        .filter(F.col("check_kind") != "performance")
        .collect()
    ]
    evidence[eco] = (operate_entries(eco), approvals, limit, results)
    print(
        f"{eco}: {len(evidence[eco][0])} registered model(s), {len(results)} result(s)"
    )

# COMMAND ----------

# DBTITLE 1,The degradation limit is recorded
_no_limit = [eco for eco, (_e, _a, limit, _r) in evidence.items() if limit is None]
check(
    COMPONENT,
    SOURCE,
    "degradation_limit_recorded",
    not _no_limit,
    detail=f"no degradation limit in approval_spec: {_no_limit}",
    metric_value=float(len(_no_limit)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Check the performance and build the triggers
now = now_utc()
checked = {}
for eco, (entries, approvals, limit, results) in evidence.items():
    performance, triggers, flags = [], [], []
    for entry in entries:
        approval = approvals[(entry["task_id"], entry["model_name"])]
        found = performance_result(approval, limit)
        rows = result_rows([found], entry, PERFORMANCE_WINDOW, rid, now)
        mine = [
            r
            for r in results
            if (r["task_id"], r["model_name"])
            == (entry["task_id"], entry["model_name"])
        ]
        current = frozen_version(entry["dataset_id"], eco)
        built = build_triggers(approval, [*mine, *rows], current, limit)
        performance.append((entry, rows))
        flags += flag_rows(rows, rid, now)
        triggers.append((entry, trigger_rows(built, entry, rid, now)))
    checked[eco] = (performance, triggers, flags)
    print(
        f"{eco}: {len(performance)} performance result(s), "
        f"{sum(len(t) for _e, t in triggers)} trigger(s)"
    )

# COMMAND ----------

# DBTITLE 1,Write the performance results, the flags and the triggers
for eco, (performance, triggers, flags) in checked.items():
    for entry, rows in performance:
        where = entry_predicate(entry, " AND check_kind = 'performance'")
        replace_rows(rows, "monitoring_results", eco, where)
        replace_rows(
            [f for f in flags if f["task_id"] == entry["task_id"]],
            "monitoring_flags",
            eco,
            where,
        )
    for entry, rows in triggers:
        replace_rows(rows, "monitoring_triggers", eco, entry_predicate(entry))

# COMMAND ----------

# DBTITLE 1,Every registered model has a performance result
_missing = [
    (eco, entry["task_id"])
    for eco, (performance, _t, _f) in checked.items()
    for entry, rows in performance
    if not rows
]
check(
    COMPONENT,
    SOURCE,
    "every_registered_model_has_a_performance_result",
    not _missing,
    detail=f"no performance result: {_missing}",
    metric_value=float(len(_missing)),
    rid=rid,
)