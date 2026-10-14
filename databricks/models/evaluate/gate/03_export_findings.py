# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EXPORT EVALUATION FINDINGS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** write the evaluation record of each ecosystem to the repository as
# MAGIC markdown (`src/findings/model_findings/`): settings, run context, flags, check
# MAGIC results, and per task the validation and held-out values side by side with every
# MAGIC metric. Smoke runs go to their own files.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model metrics library
# MAGIC %run ../../lib/_model_metrics

# COMMAND ----------

# DBTITLE 1,Evaluation shared library
# MAGIC %run ../../lib/_eval_common

# COMMAND ----------

# DBTITLE 1,Imports
import os as _os

# COMMAND ----------

# DBTITLE 1,Configuration
FINDINGS_SUBDIR = "src/findings/model_findings"
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

# COMMAND ----------

# DBTITLE 1,Repo-root discovery


def _repo_root():
    p = _os.path.abspath(_os.getcwd())
    for _ in range(12):
        if _os.path.isdir(_os.path.join(p, "src", "schemas")) and _os.path.isdir(
            _os.path.join(p, "databricks")
        ):
            return p
        if _os.path.dirname(p) == p:
            break
        p = _os.path.dirname(p)
    try:
        wp = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
    except Exception as exc:
        raise RuntimeError(
            "repo root not found -- run from inside the repo's Databricks Git folder"
        ) from exc
    i = wp.rfind("/databricks/")
    if i > 0:
        for cand in (wp[:i], "/Workspace" + wp[:i]):
            if _os.path.isdir(_os.path.join(cand, "src", "schemas")):
                return cand
    raise RuntimeError(
        "repo root not found -- run from inside the repo's Databricks Git folder"
    )


FINDINGS_DIR = _os.path.join(_repo_root(), FINDINGS_SUBDIR)
_os.makedirs(FINDINGS_DIR, exist_ok=True)
print(f"OK  findings directory: {FINDINGS_DIR}")

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the evaluation record per ecosystem
record = {}
for eco in ("energy", "commerce"):
    results = [
        r.asDict()
        for r in read_model("evaluation_results", ecosystem=eco)
        .select(*RESULT_FIELDS)
        .collect()
    ]
    flags = [
        r.asDict() for r in read_model("evaluation_flags", ecosystem=eco).collect()
    ]
    context = [
        r.asDict() for r in read_model(EVAL_CONTEXT_TABLE, ecosystem=eco).collect()
    ]
    spec = [
        r.asDict()
        for r in read_model("evaluation_spec", ecosystem=eco)
        .filter(F.col("dataset_id") == EVAL_SPEC_DATASET)
        .collect()
    ]
    record[eco] = (results, flags, context, spec)
    print(f"{eco}: {len(results)} result row(s), {len(flags)} flag(s)")

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
for eco, (results, flags, context, spec) in record.items():
    for smoke in (False, True):
        rows = [r for r in results if r["smoke"] == smoke]
        if smoke and not rows:
            continue
        shown_flags = flags
        if smoke:
            shown_flags = flag_dicts(evaluation_flag_rows(rows, read_thresholds(eco)))
        text = render_evaluation_findings(
            eco, rows, shown_flags, context, checks, spec, smoke=smoke, stamp=stamp
        )
        name = f"{eco}_test_smoke.md" if smoke else f"{eco}_test.md"
        with open(_os.path.join(FINDINGS_DIR, name), "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
        print(f"OK  {name}")
