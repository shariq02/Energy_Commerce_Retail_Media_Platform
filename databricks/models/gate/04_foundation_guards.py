# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION GUARDS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** after the foundation notebooks have run, check the foundation record.
# MAGIC Hard failures: a foundation task without a recorded row, results at a version
# MAGIC other than the frozen one, a candidate scored on other rows than the accepted
# MAGIC baseline, a broken forecast rule (a context that reaches past the time minus the
# MAGIC gap), a regression candidate without skill metric, a model without revision and
# MAGIC licence in the register. Failed and skipped candidates are reported, not failed.

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

# DBTITLE 1,Foundation anomaly and reconstruction library
# MAGIC %run ../lib/_fit_foundation_anomaly

# COMMAND ----------

# DBTITLE 1,Foundation survival library
# MAGIC %run ../lib/_fit_foundation_survival

# COMMAND ----------

# DBTITLE 1,Foundation ranking library
# MAGIC %run ../lib/_fit_foundation_rank

# COMMAND ----------

# DBTITLE 1,Foundation offline policy library
# MAGIC %run ../lib/_fit_foundation_rl

# COMMAND ----------

# DBTITLE 1,Foundation stacking library
# MAGIC %run ../lib/_fit_foundation_stack

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/gate/04_foundation_guards"
SOURCE = "models"
ECOSYSTEMS = ("energy", "commerce")
SKILL_TASK_TYPES = ("regression", "regression_price", "regression_count")
TASK_IDS = foundation_task_ids()
MODEL_KEYS_OF = {name: [backend] for name, backend, _, _ in CANDIDATES}
MODEL_KEYS_OF.update(
    {
        name: sorted({key for key, _ in TAB_WEIGHTS[backend].values()})
        for name, backend, _ in TAB_CANDIDATES
    }
)
MODEL_KEYS_OF.update({name: [backend] for name, backend, _ in RECON_CANDIDATES})
MODEL_KEYS_OF["survpfn_in_context"] = ["survpfn_weights", "survpfn_code"]
MODEL_KEYS_OF["tabicl_discrete_in_context"] = ["tabicl_v2"]
MODEL_KEYS_OF.update({name: [STACK_MODEL] for name, _, _ in STACK_CANDIDATES})

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the foundation record
rows = [
    r.asDict()
    for eco in ECOSYSTEMS
    for r in read_model(FOUNDATION_RESULTS, ecosystem=eco)
    .filter(~F.col("smoke"))
    .select(
        "task_id",
        "model_name",
        "stage",
        "status",
        "metric",
        "value",
        "n_rows",
        "frozen_delta_version",
        "artifact_status",
    )
    .collect()
]
by_task = {}
for r in rows:
    by_task.setdefault(r["task_id"], []).append(r)
print(f"{len(rows)} foundation result row(s) for {len(by_task)} task(s)")

# COMMAND ----------

# DBTITLE 1,Read the baseline rows of the accepted candidates
baseline_rows = {
    r["task_id"]: int(r["n_rows"])
    for eco in ECOSYSTEMS
    for r in read_model("candidate_results", ecosystem=eco)
    .filter(
        ~F.col("smoke") & (F.col("stage") == "baseline") & (F.col("status") == "ok")
    )
    .select("task_id", "n_rows")
    .distinct()
    .collect()
}
print(f"{len(baseline_rows)} task(s) with an accepted baseline")

# COMMAND ----------

# DBTITLE 1,Every foundation task has a foundation row
_missing = [t for t in TASK_IDS if not by_task.get(t)]
check(
    COMPONENT,
    SOURCE,
    "every_foundation_task_has_a_row",
    not _missing,
    detail=f"no foundation rows: {_missing}",
    metric_value=float(len(_missing)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Results were produced at the frozen version
_mismatch = []
for t in TASK_IDS:
    task = TASK_BY_ID[t]
    want = frozen_version(task["dataset_id"], task["ecosystem"])
    got = {x["frozen_delta_version"] for x in by_task.get(t, [])}
    if got and got != {want}:
        _mismatch.append((t, sorted(got), want))
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

# DBTITLE 1,Candidates are scored on the rows of the accepted baseline
_other_rows = []
for t in TASK_IDS:
    seen = {
        (x["model_name"], x["n_rows"])
        for x in by_task.get(t, [])
        if x["status"] == "ok"
    }
    _other_rows += [(t, m, n) for m, n in sorted(seen) if n != baseline_rows.get(t, n)]
check(
    COMPONENT,
    SOURCE,
    "same_rows_as_the_accepted_baseline",
    not _other_rows,
    detail=f"other row counts: {_other_rows}",
    metric_value=float(len(_other_rows)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,No context reaches past the time minus the gap
_broken = [
    (x["task_id"], x["model_name"], x["value"])
    for x in rows
    if x["status"] == "ok"
    and x["metric"] == "context_rule_violations"
    and x["value"] not in (0, 0.0)
]
check(
    COMPONENT,
    SOURCE,
    "forecast_context_rule_holds",
    not _broken,
    detail=f"rule broken: {_broken}",
    metric_value=float(len(_broken)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Regression candidates carry skill against the accepted baseline
_no_skill = []
for t in TASK_IDS:
    if TASK_BY_ID[t]["task_type"] not in SKILL_TASK_TYPES:
        continue
    ok_models = {x["model_name"] for x in by_task.get(t, []) if x["status"] == "ok"}
    with_skill = {
        x["model_name"]
        for x in by_task.get(t, [])
        if x["metric"] in ("skill_mae", "skill_pinball_q50")
    }
    _no_skill += [(t, m) for m in sorted(ok_models - with_skill)]
check(
    COMPONENT,
    SOURCE,
    "foundation_candidates_carry_skill",
    not _no_skill,
    detail=f"no skill metric: {_no_skill}",
    metric_value=float(len(_no_skill)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Every used model has a revision and a licence in the register
_register = {
    r["model_key"]: r.asDict()
    for eco in ECOSYSTEMS
    for r in read_model(FOUNDATION_REGISTER, ecosystem=eco).collect()
}
_used = {
    key
    for x in rows
    if x["status"] == "ok"
    for key in MODEL_KEYS_OF.get(x["model_name"], [])
}
_unrecorded = sorted(
    k
    for k in _used
    if not (_register.get(k) and _register[k]["revision"] and _register[k]["licence"])
)
check(
    COMPONENT,
    SOURCE,
    "used_models_have_revision_and_licence",
    not _unrecorded,
    detail=f"not recorded: {_unrecorded}",
    metric_value=float(len(_unrecorded)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Status summary per task (failed and skipped candidates are reported)
for t in TASK_IDS:
    counts = {}
    seen = set()
    for x in by_task.get(t, []):
        key = (x["model_name"], x["status"])
        if key not in seen:
            seen.add(key)
            counts[x["status"]] = counts.get(x["status"], 0) + 1
    print(f"{t:45s} {counts}")
