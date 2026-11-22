# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # PERFORMANCE AND TRIGGERS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** record the performance check and the retraining triggers of each
# MAGIC registered model. The performance check compares the recorded held-out value with
# MAGIC the recorded validation value against the recorded degradation limit. A trigger
# MAGIC is a record: it never changes a decision, a status or a model. Reads recorded
# MAGIC results only.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model governance library
# MAGIC %run ../lib/_model_govern

# COMMAND ----------

# DBTITLE 1,Model operate library
# MAGIC %run ../lib/_model_operate

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/operate/02_performance_and_triggers"
PERFORMANCE_WINDOW = "held_out"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the registry, the decisions, the limit and the results
evidence = {}
for eco in ECOSYSTEMS:
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
guard(
    COMPONENT,
    rid,
    "degradation_limit_recorded",
    [eco for eco, (_e, _a, limit, _r) in evidence.items() if limit is None],
    "no degradation limit in approval_spec",
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
guard(
    COMPONENT,
    rid,
    "every_registered_model_has_a_performance_result",
    [
        (eco, entry["task_id"])
        for eco, (performance, _t, _f) in checked.items()
        for entry, rows in performance
        if not rows
    ],
    "no performance result",
)

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_model_findings(
    ECOSYSTEMS,
    "operate__02_performance_and_triggers",
    "Performance And Triggers",
    ["monitoring_results", "monitoring_flags", "monitoring_triggers"],
    rid,
)
