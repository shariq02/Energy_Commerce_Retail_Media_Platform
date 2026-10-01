# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CANDIDATE GUARDS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** after the model notebooks have run, check the candidate record. Hard
# MAGIC failures: a task without a baseline, results at a version other than the frozen
# MAGIC one, a task without a recorded candidate, a regression candidate without skill
# MAGIC against its baseline. Failed and skipped candidates are reported, not failed.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../_model_common

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/gate/01_candidate_guards"
SOURCE = "models"
dbutils.widgets.text("use_smoke_results", "false")
USE_SMOKE = dbutils.widgets.get("use_smoke_results").lower() == "true"
SKILL_TASK_TYPES = ("regression", "regression_price", "regression_count")

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the candidate record (full runs, or smoke runs when use_smoke_results is true)
results = {
    eco: read_model("candidate_results", ecosystem=eco).filter(
        F.col("smoke") == USE_SMOKE
    )
    for eco in ("energy", "commerce")
}
rows = [
    (eco, r)
    for eco, df in results.items()
    for r in df.select(
        "task_id", "model_name", "stage", "status", "metric", "frozen_delta_version"
    ).collect()
]
print(f"{len(rows)} candidate result row(s)")

# COMMAND ----------

# DBTITLE 1,Every task has a baseline that ran
_by_task = {}
for _eco, r in rows:
    _by_task.setdefault(r["task_id"], []).append(r)
_no_baseline = [
    t["task_id"]
    for t in TASKS
    if not any(
        x["stage"] == "baseline" and x["status"] == "ok"
        for x in _by_task.get(t["task_id"], [])
    )
]
check(
    COMPONENT,
    SOURCE,
    "every_task_has_a_baseline",
    not _no_baseline,
    detail=f"no baseline: {_no_baseline}",
    metric_value=float(len(_no_baseline)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Every task recorded at least one candidate row
_no_candidate = [
    t["task_id"]
    for t in TASKS
    if not any(x["stage"] == "candidate" for x in _by_task.get(t["task_id"], []))
]
check(
    COMPONENT,
    SOURCE,
    "every_task_has_a_candidate_row",
    not _no_candidate,
    detail=f"no candidate rows: {_no_candidate}",
    metric_value=float(len(_no_candidate)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Results were produced at the frozen version
_mismatch = []
for t in TASKS:
    want = frozen_version(t["dataset_id"], t["ecosystem"])
    got = {x["frozen_delta_version"] for x in _by_task.get(t["task_id"], [])}
    if got and got != {want}:
        _mismatch.append((t["task_id"], sorted(got), want))
check(
    COMPONENT,
    SOURCE,
    "results_at_frozen_version",
    not _mismatch,
    detail=f"version mismatch: {_mismatch}",
    metric_value=float(len(_mismatch)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Regression candidates carry skill against their baseline
_no_skill = []
for t in TASKS:
    if t["task_type"] not in SKILL_TASK_TYPES:
        continue
    ok_models = {
        x["model_name"]
        for x in _by_task.get(t["task_id"], [])
        if x["stage"] == "candidate" and x["status"] == "ok"
    }
    with_skill = {
        x["model_name"]
        for x in _by_task.get(t["task_id"], [])
        if x["metric"] in ("skill_mae", "skill_pinball_q50")
    }
    _no_skill += [(t["task_id"], m) for m in sorted(ok_models - with_skill)]
check(
    COMPONENT,
    SOURCE,
    "regression_candidates_carry_skill",
    not _no_skill,
    detail=f"no skill metric: {_no_skill}",
    metric_value=float(len(_no_skill)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Status summary per task (failed and skipped candidates are reported)
_summary = {}
for t in TASKS:
    counts = {}
    seen = set()
    for x in _by_task.get(t["task_id"], []):
        key = (x["model_name"], x["status"])
        if key not in seen:
            seen.add(key)
            counts[x["status"]] = counts.get(x["status"], 0) + 1
    _summary[t["task_id"]] = counts
    print(f"{t['task_id']:45s} {counts}")
_only_baseline = [
    k for k, v in _summary.items() if v and set(v) == {"ok"} and v["ok"] == 1
]
print("tasks with only the baseline as a successful model:", _only_baseline)

# COMMAND ----------

# DBTITLE 1,Evaluation splits are group-disjoint
for eco, ds in (("energy", "weak_supervision"), ("energy", "redispatch_matching")):
    shared = (
        read_model("evaluation_split_manifest", ecosystem=eco)
        .filter(F.col("dataset_id") == ds)
        .groupBy("group_key")
        .agg(F.countDistinct("partition").alias("n"))
        .filter(F.col("n") > 1)
        .count()
    )
    check(
        COMPONENT,
        SOURCE,
        f"groups_in_one_partition:{ds}",
        shared == 0,
        detail=f"groups in several partitions: {shared}",
        metric_value=float(shared),
        rid=rid,
    )
