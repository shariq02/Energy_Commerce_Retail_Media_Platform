# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # CANDIDATE GATE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** after the training notebook, check the candidate record, choose per
# MAGIC task the models forwarded for evaluation on the test partition (the best
# MAGIC candidates by the primary validation metric whose fitted artifact was stored,
# MAGIC plus the baseline) and write the candidate record of each ecosystem to the
# MAGIC repository as markdown. Hard failures: a task without a baseline, results at a
# MAGIC version other than the frozen one, a task without a candidate, a regression
# MAGIC candidate without skill, a forwarded candidate without a stored artifact. Failed
# MAGIC and skipped candidates are reported, not failed.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ./lib/_model_common

# COMMAND ----------

# DBTITLE 1,Imports
import os as _os

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/02_candidate_gate"
SKILL_TASK_TYPES = ("regression", "regression_price", "regression_count")
RESULT_FIELDS = (
    "task_id",
    "model_name",
    "stage",
    "status",
    "metric",
    "value",
    "n_rows",
    "detail",
    "frozen_delta_version",
    "artifact_status",
    "run_id",
    "run_at",
)
SELECTION_FIELDS = ("task_id", "model_name", "rank", "primary_value")
FINDINGS_DIR = _findings_dir()
_os.makedirs(FINDINGS_DIR, exist_ok=True)

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the full candidate record
results = {
    eco: read_model("candidate_results", ecosystem=eco).filter(~F.col("smoke"))
    for eco in ECOSYSTEMS
}
rows = [
    r
    for df in results.values()
    for r in df.select(
        "task_id", "model_name", "stage", "status", "metric", "frozen_delta_version"
    ).collect()
]
_by_task = {}
for r in rows:
    _by_task.setdefault(r["task_id"], []).append(r)
print(f"{len(rows)} candidate result row(s)")

# COMMAND ----------

# DBTITLE 1,Every task has a baseline that ran and a candidate row
guard(
    COMPONENT,
    rid,
    "every_task_has_a_baseline",
    [
        t["task_id"]
        for t in TASKS
        if not any(
            x["stage"] == "baseline" and x["status"] == "ok"
            for x in _by_task.get(t["task_id"], [])
        )
    ],
    "no baseline",
)
guard(
    COMPONENT,
    rid,
    "every_task_has_a_candidate_row",
    [
        t["task_id"]
        for t in TASKS
        if not any(x["stage"] == "candidate" for x in _by_task.get(t["task_id"], []))
    ],
    "no candidate rows",
)

# COMMAND ----------

# DBTITLE 1,Results were produced at the frozen version
_mismatch = []
for t in TASKS:
    want = frozen_version(t["dataset_id"], t["ecosystem"])
    got = {x["frozen_delta_version"] for x in _by_task.get(t["task_id"], [])}
    if got and got != {want}:
        _mismatch.append((t["task_id"], sorted(got), want))
guard(COMPONENT, rid, "results_at_frozen_version", _mismatch, "version mismatch")

# COMMAND ----------

# DBTITLE 1,Regression candidates carry skill against their baseline
_no_skill = []
for t in TASKS:
    if t["task_type"] not in SKILL_TASK_TYPES:
        continue
    mine = _by_task.get(t["task_id"], [])
    ok_models = {
        x["model_name"]
        for x in mine
        if x["stage"] == "candidate" and x["status"] == "ok"
    }
    with_skill = {
        x["model_name"]
        for x in mine
        if x["metric"] in ("skill_mae", "skill_pinball_q50")
    }
    _no_skill += [(t["task_id"], m) for m in sorted(ok_models - with_skill)]
guard(COMPONENT, rid, "regression_candidates_carry_skill", _no_skill, "no skill metric")

# COMMAND ----------

# DBTITLE 1,Status summary per task (failed and skipped candidates are reported)
for t in TASKS:
    counts, seen = {}, set()
    for x in _by_task.get(t["task_id"], []):
        key = (x["model_name"], x["status"])
        if key not in seen:
            seen.add(key)
            counts[x["status"]] = counts.get(x["status"], 0) + 1
    print(f"{t['task_id']:45s} {counts}")

# COMMAND ----------

# DBTITLE 1,Primary metric per candidate from the full (non-smoke) record
_records = {}
for t in TASKS:
    df = (
        read_model("candidate_results", ecosystem=t["ecosystem"])
        .filter(
            (F.col("task_id") == t["task_id"])
            & ~F.col("smoke")
            & (F.col("status") == "ok")
            & (F.col("metric") == t["primary_metric"])
        )
        .select(
            "model_name",
            "family",
            "stage",
            "value",
            "mlflow_run_id",
            "artifact_status",
            "frozen_delta_version",
        )
    )
    _records[t["task_id"]] = [r.asDict() for r in df.collect()]
print({k: len(v) for k, v in _records.items()})

# COMMAND ----------

# DBTITLE 1,Rank the candidates and keep the top ones plus the baseline
_selected = []
for t in TASKS:
    recs = [r for r in _records[t["task_id"]] if r["value"] is not None]
    cands = [
        r for r in recs if r["stage"] == "candidate" and r["artifact_status"] == "ok"
    ]
    cands.sort(key=lambda r: r["value"], reverse=bool(t["higher_is_better"]))
    chosen = [(i + 1, r) for i, r in enumerate(cands[:FORWARD_TOP_N])]
    chosen += [(0, r) for r in recs if r["stage"] == "baseline"]
    for rank, r in chosen:
        _selected.append(
            (
                t["task_id"],
                t["dataset_id"],
                r["model_name"],
                r["family"],
                rank,
                t["primary_metric"],
                float(r["value"]),
                r["stage"] == "baseline",
                r["mlflow_run_id"],
                int(r["frozen_delta_version"]),
                rid,
                now_utc(),
            )
        )
print(f"{len(_selected)} selection row(s) for {len(TASKS)} task(s)")

# COMMAND ----------

# DBTITLE 1,Every task forwards its baseline and at least one candidate
guard(
    COMPONENT,
    rid,
    "every_task_forwards_its_baseline",
    [
        t["task_id"]
        for t in TASKS
        if not any(s[0] == t["task_id"] and s[7] for s in _selected)
    ],
    "no baseline selected",
)
guard(
    COMPONENT,
    rid,
    "every_task_forwards_a_candidate",
    [
        t["task_id"]
        for t in TASKS
        if not any(s[0] == t["task_id"] and not s[7] for s in _selected)
    ],
    "no candidate with a stored artifact",
)

# COMMAND ----------

# DBTITLE 1,Write the selection
for eco in ECOSYSTEMS:
    selected = [s for s in _selected if TASK_BY_ID[s[0]]["ecosystem"] == eco]
    replace_model_rows(
        spark.createDataFrame(selected, MODEL_DDL["candidate_selection"]),
        "candidate_selection",
        ecosystem=eco,
        predicate="task_id IS NOT NULL",
    )
    print(f"OK  {len(selected)} row(s) for {eco}")

# COMMAND ----------

# DBTITLE 1,Read the check results of the model notebooks
checks = [
    r.asDict()
    for r in spark.table(AUDIT_TABLE)
    .filter(
        (F.col("stage") == ML_STAGE)
        & F.col("component").like("models/%")
        & ~F.col("component").like("models/evaluate/%")
    )
    .select("component", "metric_name", "status", "error_detail", "recorded_at")
    .collect()
]
print(f"{len(checks)} check row(s)")

# COMMAND ----------

# DBTITLE 1,Write the full-run and smoke-run findings files
stamp = now_utc().strftime("%Y-%m-%dT%H:%MZ")
for eco in ECOSYSTEMS:
    all_rows = [
        {**r.asDict(), "smoke": bool(r["smoke"])}
        for r in read_model("candidate_results", ecosystem=eco)
        .select(*RESULT_FIELDS, "smoke")
        .collect()
    ]
    selection = [
        r.asDict()
        for r in read_model("candidate_selection", ecosystem=eco)
        .select(*SELECTION_FIELDS)
        .collect()
    ]
    context = [
        r.asDict() for r in read_model("task_run_context", ecosystem=eco).collect()
    ]
    for smoke in (False, True):
        mine = [r for r in all_rows if r["smoke"] == smoke]
        if smoke and not mine:
            continue
        other_mode = {}
        for r in all_rows:
            if r["smoke"] != smoke:
                n, last = other_mode.get(r["task_id"], (0, ""))
                other_mode[r["task_id"]] = (n + 1, max(last, str(r["run_at"])[:19]))
        text = render_findings(
            eco,
            mine,
            selection,
            smoke=smoke,
            stamp=stamp,
            context=context,
            other_mode=other_mode,
            checks=checks,
        )
        name = f"{eco}_smoke.md" if smoke else f"{eco}.md"
        with open(_os.path.join(FINDINGS_DIR, name), "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
        print(f"OK  {name}: {len(mine)} row(s)")

# COMMAND ----------

# DBTITLE 1,Merge the findings of the notebooks
merge_notebook_findings()
