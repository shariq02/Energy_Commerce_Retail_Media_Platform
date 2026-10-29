# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION FINDINGS EXPORT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** write the foundation record as markdown into the repository
# MAGIC (`src/findings/model_findings/foundation_<ecosystem>.md` and a smoke-run file): the
# MAGIC models with revision and licence, run context, flags, check results, and every
# MAGIC candidate beside the baseline of the accepted models.

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

# DBTITLE 1,Imports
import os as _os

# COMMAND ----------

# DBTITLE 1,Configuration
FINDINGS_SUBDIR = "src/findings/model_findings"
ECOSYSTEMS = ("energy", "commerce")
RESULT_FIELDS = (
    "task_id",
    "model_name",
    "family",
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
    "smoke",
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

# DBTITLE 1,Read the foundation record and the baseline rows of the accepted models
TASK_IDS = foundation_task_ids()
record = {}
for eco in ECOSYSTEMS:
    foundation = [
        r.asDict()
        for r in read_model(FOUNDATION_RESULTS, ecosystem=eco)
        .select(*RESULT_FIELDS)
        .collect()
    ]
    baseline = [
        r.asDict()
        for r in read_model("candidate_results", ecosystem=eco)
        .filter((F.col("stage") == "baseline") & F.col("task_id").isin(*TASK_IDS))
        .select(*RESULT_FIELDS)
        .collect()
    ]
    selection = [
        r.asDict() for r in read_model(FOUNDATION_SELECTION, ecosystem=eco).collect()
    ]
    context = [
        r.asDict() for r in read_model(FOUNDATION_CONTEXT, ecosystem=eco).collect()
    ]
    register = [
        r.asDict() for r in read_model(FOUNDATION_REGISTER, ecosystem=eco).collect()
    ]
    record[eco] = (foundation, baseline, selection, context, register)
    print(
        f"{eco}: {len(foundation)} foundation row(s), {len(baseline)} baseline "
        f"row(s), {len(selection)} selection row(s), {len(context)} run-context row(s)"
    )

# COMMAND ----------

# DBTITLE 1,Read the check results
checks = [
    r.asDict()
    for r in spark.table(AUDIT_TABLE)
    .filter(
        (F.col("stage") == ML_STAGE)
        & (
            F.col("component").like("models/foundation/%")
            | F.col("component").like("models/gate/04_foundation%")
            | F.col("component").like("models/gate/05_foundation%")
        )
    )
    .select("component", "metric_name", "status", "error_detail", "recorded_at")
    .collect()
]
print(f"{len(checks)} check row(s)")

# COMMAND ----------

# DBTITLE 1,Write the full-run and smoke-run files
stamp = now_utc().strftime("%Y-%m-%dT%H:%MZ")
for eco, (foundation, baseline, selection, context, register) in record.items():
    for smoke in (False, True):
        mine = [r for r in foundation if bool(r["smoke"]) == smoke]
        if not mine:
            continue
        base_rows = [r for r in baseline if bool(r["smoke"]) == smoke]
        text = render_foundation_findings(
            eco,
            [*mine, *base_rows],
            selection if not smoke else [],
            context,
            register,
            checks,
            smoke=smoke,
            stamp=stamp,
        )
        name = f"foundation_{eco}_smoke.md" if smoke else f"foundation_{eco}.md"
        with open(_os.path.join(FINDINGS_DIR, name), "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
        print(f"OK  {name}: {len(mine)} row(s)")

# COMMAND ----------

# DBTITLE 1,Merge the findings of the notebooks
merge_notebook_findings()
