# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EVALUATION GATE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** after the evaluation notebook, check the record, raise the diagnostic
# MAGIC flags and write the evaluation record of each ecosystem to the repository as
# MAGIC markdown. Hard failures: a forwarded model without an evaluation, a task
# MAGIC without an evaluation, a result recorded twice, a result at another frozen
# MAGIC version or on another partition, and settings recorded after the first read. A
# MAGIC flag never rejects a model; the approval decides. The number of held-out reads
# MAGIC per task is reported.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model evaluation library
# MAGIC %run ../lib/_model_evaluate

# COMMAND ----------

# DBTITLE 1,Imports
import os as _os

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/evaluate/02_evaluation_gate"
RESULT_FIELDS = (
    "task_id",
    "model_name",
    "family",
    "stage",
    "partition",
    "metric",
    "value",
    "validation_value",
    "n_rows",
    "status",
    "detail",
    "frozen_delta_version",
    "smoke",
    "run_id",
    "run_at",
)
FINDINGS_DIR = _findings_dir()
_os.makedirs(FINDINGS_DIR, exist_ok=True)

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the evaluation record
record = {}
for eco in ECOSYSTEMS:
    results = [
        r.asDict()
        for r in read_model("evaluation_results", ecosystem=eco)
        .select(*RESULT_FIELDS)
        .collect()
    ]
    selection = read_model("candidate_selection", ecosystem=eco).collect()
    context = [
        r.asDict() for r in read_model(EVAL_CONTEXT_TABLE, ecosystem=eco).collect()
    ]
    spec = [
        r.asDict()
        for r in read_model("evaluation_spec", ecosystem=eco)
        .filter(F.col("dataset_id") == EVAL_SPEC_DATASET)
        .collect()
    ]
    record[eco] = (results, selection, context, spec)
    print(f"{eco}: {len(results)} result row(s), {len(selection)} selected model(s)")
_full = {eco: [r for r in rec[0] if not r["smoke"]] for eco, rec in record.items()}

# COMMAND ----------

# DBTITLE 1,Every forwarded model and every task was evaluated
_missing = []
for eco, (_results, selection, _context, _spec) in record.items():
    done = {(r["task_id"], r["model_name"]) for r in _full[eco] if r["status"] == "ok"}
    other = {(r["task_id"], r["model_name"]): r for r in _full[eco]}
    for s in selection:
        key = (s["task_id"], s["model_name"])
        if key not in done:
            row = other.get(key)
            why = f"{row['status']}: {row['detail']}" if row else "no result row"
            _missing.append((*key, why[:300]))
guard(COMPONENT, rid, "every_forwarded_model_evaluated", _missing, "not evaluated")
_seen = {r["task_id"] for rows in _full.values() for r in rows}
guard(
    COMPONENT,
    rid,
    "every_task_evaluated",
    [t["task_id"] for t in (*TASKS, *CHECK_TASKS) if t["task_id"] not in _seen],
    "no evaluation",
)

# COMMAND ----------

# DBTITLE 1,Each result is recorded once
_twice = []
for rows in _full.values():
    counts = {}
    for r in rows:
        key = (r["task_id"], r["model_name"], r["metric"])
        counts[key] = counts.get(key, 0) + 1
    _twice += [k for k, n in counts.items() if n > 1]
guard(COMPONENT, rid, "results_recorded_once", _twice, "recorded more than once")

# COMMAND ----------

# DBTITLE 1,Results are at the frozen version and on the held-out partition
_frozen = {t["task_id"]: frozen_version(t["dataset_id"], t["ecosystem"]) for t in TASKS}
_frozen |= {
    t["task_id"]: frozen_version(t["dataset_id"], t["ecosystem"]) for t in CHECK_TASKS
}
_wrong_version, _wrong_partition = [], []
for rows in _full.values():
    for r in rows:
        want = _frozen[r["task_id"]]
        if r["frozen_delta_version"] != want:
            _wrong_version.append((r["task_id"], r["frozen_delta_version"], want))
        if r["partition"] != TEST_PARTITION:
            _wrong_partition.append((r["task_id"], r["partition"]))
guard(
    COMPONENT,
    rid,
    "results_at_frozen_version",
    sorted(set(_wrong_version)),
    "version mismatch",
)
guard(
    COMPONENT,
    rid,
    "results_on_held_out_partition",
    sorted(set(_wrong_partition)),
    "other partition",
)

# COMMAND ----------

# DBTITLE 1,Settings were recorded before the first read
_late = []
for eco, (_results, _selection, context, spec) in record.items():
    reads = [
        c["recorded_at"] for c in context if c["status"] == "started" and not c["smoke"]
    ]
    if reads and spec and min(s["recorded_at"] for s in spec) > min(reads):
        _late.append(eco)
guard(COMPONENT, rid, "settings_recorded_before_first_read", _late, "recorded late")

# COMMAND ----------

# DBTITLE 1,Held-out reads per task (reported, not a hard failure)
_reads = {}
for eco, (_results, _selection, context, _spec) in record.items():
    for c in context:
        if c["status"] == "data_ready" and not c["smoke"]:
            _reads.setdefault(c["task_id"], set()).add(c["run_id"])
_repeated = {t: len(r) for t, r in _reads.items() if len(r) > 1}
record_check(
    COMPONENT,
    MODEL_STAGE,
    "one_held_out_read_per_task",
    not _repeated,
    detail=f"tasks read more than once: {_repeated}",
    metric_value=float(len(_repeated)),
    rid=rid,
)
print("tasks read more than once:", _repeated or "none")

# COMMAND ----------

# DBTITLE 1,Raise the flags and store them
flags = {}
for eco in ECOSYSTEMS:
    flags[eco] = evaluation_flag_rows(_full[eco], read_thresholds(eco))
    kinds = {}
    for row in flags[eco]:
        kinds[row[3]] = kinds.get(row[3], 0) + 1
    print(eco, kinds or "no flags")
    stamped = [(*row, rid, now_utc()) for row in flags[eco]]
    replace_model_rows(
        spark.createDataFrame(stamped, MODEL_DDL["evaluation_flags"]),
        "evaluation_flags",
        ecosystem=eco,
        predicate="task_id IS NOT NULL",
    )

# COMMAND ----------

# DBTITLE 1,Read the check results of the evaluation notebooks
checks = [
    r.asDict()
    for r in spark.table(AUDIT_TABLE)
    .filter((F.col("stage") == ML_STAGE) & F.col("component").like("models/evaluate/%"))
    .select("component", "metric_name", "status", "error_detail", "recorded_at")
    .collect()
]
print(f"{len(checks)} check row(s)")

# COMMAND ----------

# DBTITLE 1,Write the full-run and smoke-run files
stamp = now_utc().strftime("%Y-%m-%dT%H:%MZ")
for eco, (results, _selection, context, spec) in record.items():
    stored = [
        r.asDict() for r in read_model("evaluation_flags", ecosystem=eco).collect()
    ]
    for smoke in (False, True):
        rows = [r for r in results if r["smoke"] == smoke]
        if smoke and not rows:
            continue
        shown_flags = stored
        if smoke:
            shown_flags = flag_dicts(evaluation_flag_rows(rows, read_thresholds(eco)))
        text = render_evaluation_findings(
            eco, rows, shown_flags, context, checks, spec, smoke=smoke, stamp=stamp
        )
        name = f"{eco}_test_smoke.md" if smoke else f"{eco}_test.md"
        with open(_os.path.join(FINDINGS_DIR, name), "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
        print(f"OK  {name}")

# COMMAND ----------

# DBTITLE 1,Merge the findings of the notebooks
merge_notebook_findings()
