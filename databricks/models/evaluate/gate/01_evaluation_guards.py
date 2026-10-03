# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EVALUATION GUARDS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** after the evaluation notebooks have run, check the record. Hard
# MAGIC failures: a forwarded model without an evaluation, a result recorded twice, a
# MAGIC result at another frozen version or on another partition, and settings recorded
# MAGIC after the first read. The number of held-out reads per task is reported.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../../_model_common

# COMMAND ----------

# DBTITLE 1,Evaluation shared library
# MAGIC %run ../../_eval_common

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/evaluate/gate/01_evaluation_guards"
SOURCE = "models"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the evaluation record
record = {}
for eco in ("energy", "commerce"):
    results = (
        read_model("evaluation_results", ecosystem=eco)
        .filter(~F.col("smoke"))
        .select(
            "task_id",
            "model_name",
            "partition",
            "metric",
            "status",
            "frozen_delta_version",
            "run_id",
        )
        .collect()
    )
    selection = read_model("candidate_selection", ecosystem=eco).collect()
    context = (
        read_model(EVAL_CONTEXT_TABLE, ecosystem=eco)
        .filter(~F.col("smoke"))
        .select("task_id", "run_id", "status", "recorded_at")
        .collect()
    )
    spec = (
        read_model("evaluation_spec", ecosystem=eco)
        .filter(F.col("dataset_id") == EVAL_SPEC_DATASET)
        .select("recorded_at")
        .collect()
    )
    record[eco] = (results, selection, context, spec)
    print(f"{eco}: {len(results)} result row(s), {len(selection)} selected model(s)")

# COMMAND ----------

# DBTITLE 1,Every forwarded model was evaluated
_missing = []
for eco, (results, selection, _context, _spec) in record.items():
    done = {(r["task_id"], r["model_name"]) for r in results if r["status"] == "ok"}
    _missing += [
        (s["task_id"], s["model_name"])
        for s in selection
        if (s["task_id"], s["model_name"]) not in done
    ]
check(
    COMPONENT,
    SOURCE,
    "every_forwarded_model_evaluated",
    not _missing,
    detail=f"not evaluated: {_missing}",
    metric_value=float(len(_missing)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Every task was evaluated
_seen = {r["task_id"] for results, *_ in record.values() for r in results}
_untouched = [t["task_id"] for t in TASKS if t["task_id"] not in _seen]
check(
    COMPONENT,
    SOURCE,
    "every_task_evaluated",
    not _untouched,
    detail=f"no evaluation: {_untouched}",
    metric_value=float(len(_untouched)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Each result is recorded once
_twice = []
for results, *_ in record.values():
    counts = {}
    for r in results:
        key = (r["task_id"], r["model_name"], r["metric"])
        counts[key] = counts.get(key, 0) + 1
    _twice += [k for k, n in counts.items() if n > 1]
check(
    COMPONENT,
    SOURCE,
    "results_recorded_once",
    not _twice,
    detail=f"recorded more than once: {_twice[:10]}",
    metric_value=float(len(_twice)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Results are at the frozen version and on the held-out partition
_frozen = {t["task_id"]: frozen_version(t["dataset_id"], t["ecosystem"]) for t in TASKS}
_wrong_version, _wrong_partition = [], []
for results, *_ in record.values():
    for r in results:
        want = _frozen[r["task_id"]]
        if r["frozen_delta_version"] != want:
            _wrong_version.append((r["task_id"], r["frozen_delta_version"], want))
        if r["partition"] != TEST_PARTITION:
            _wrong_partition.append((r["task_id"], r["partition"]))
check(
    COMPONENT,
    SOURCE,
    "results_at_frozen_version",
    not _wrong_version,
    detail=f"version mismatch: {sorted(set(_wrong_version))[:10]}",
    metric_value=float(len(_wrong_version)),
    rid=rid,
)
check(
    COMPONENT,
    SOURCE,
    "results_on_held_out_partition",
    not _wrong_partition,
    detail=f"other partition: {sorted(set(_wrong_partition))[:10]}",
    metric_value=float(len(_wrong_partition)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Settings were recorded before the first read
_late = []
for eco, (_results, _selection, context, spec) in record.items():
    reads = [c["recorded_at"] for c in context if c["status"] == "started"]
    if reads and spec and min(s["recorded_at"] for s in spec) > min(reads):
        _late.append(eco)
check(
    COMPONENT,
    SOURCE,
    "settings_recorded_before_first_read",
    not _late,
    detail=f"settings recorded after a read: {_late}",
    metric_value=float(len(_late)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Held-out reads per task (reported, not a hard failure)
_reads = {}
for eco, (_results, _selection, context, _spec) in record.items():
    for c in context:
        if c["status"] == "data_ready":
            _reads.setdefault(c["task_id"], set()).add(c["run_id"])
_repeated = {t: len(r) for t, r in _reads.items() if len(r) > 1}
record_check(
    COMPONENT,
    SOURCE,
    "one_held_out_read_per_task",
    not _repeated,
    detail=f"tasks read more than once: {_repeated}",
    metric_value=float(len(_repeated)),
    rid=rid,
)
print("tasks read more than once:", _repeated or "none")
