# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EXPORT FINDINGS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** write the candidate record of each ecosystem to the repository as
# MAGIC markdown (`src/schemas/model_findings/`): per task the baseline and every
# MAGIC candidate with status, stored-artifact state and metrics, and the models
# MAGIC forwarded for test evaluation. Smoke runs go to their own files.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../_model_common

# COMMAND ----------

# DBTITLE 1,Imports
import os as _os

# COMMAND ----------

# DBTITLE 1,Configuration
FINDINGS_SUBDIR = "src/schemas/model_findings"
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

# DBTITLE 1,Read the candidate record and the selection per ecosystem
record = {}
for eco in ("energy", "commerce"):
    results = [
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
    try:
        context = [
            r.asDict() for r in read_model("task_run_context", ecosystem=eco).collect()
        ]
    except Exception as exc:
        print(f"WARN no run context for {eco}: {type(exc).__name__}")
        context = []
    record[eco] = (results, selection, context)
    print(
        f"{eco}: {len(results)} result row(s), {len(selection)} selection row(s), "
        f"{len(context)} run-context row(s)"
    )

# COMMAND ----------

# DBTITLE 1,Read the check results of the model notebooks
checks = [
    r.asDict()
    for r in spark.table(AUDIT_TABLE)
    .filter((F.col("stage") == ML_STAGE) & F.col("component").like("models/%"))
    .select("component", "metric_name", "status", "error_detail", "recorded_at")
    .collect()
]
print(f"{len(checks)} check row(s)")

# COMMAND ----------

# DBTITLE 1,Write the full-run and smoke-run files
stamp = now_utc().strftime("%Y-%m-%dT%H:%MZ")
for eco, (results, selection, context) in record.items():
    for smoke in (False, True):
        rows = [r for r in results if r["smoke"] == smoke]
        if smoke and not rows:
            continue
        other_mode = {}
        for r in results:
            if r["smoke"] != smoke:
                n, last = other_mode.get(r["task_id"], (0, ""))
                other_mode[r["task_id"]] = (n + 1, max(last, str(r["run_at"])[:19]))
        text = render_findings(
            eco,
            rows,
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
        print(f"OK  {name}: {len(rows)} row(s)")