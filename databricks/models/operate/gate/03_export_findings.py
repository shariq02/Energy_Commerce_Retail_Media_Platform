# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EXPORT MONITORING FINDINGS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** write the monitoring record of each ecosystem to
# MAGIC `src/findings/model_findings/<ecosystem>_monitoring.md`. The files are committed,
# MAGIC so private paths and e-mail addresses are removed.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Registry library
# MAGIC %run ../../lib/_registry_rules

# COMMAND ----------

# DBTITLE 1,Operate rules library
# MAGIC %run ../../lib/_operate_rules

# COMMAND ----------

# DBTITLE 1,Imports
import os as _os

# COMMAND ----------

# DBTITLE 1,Configuration
FINDINGS_SUBDIR = "src/findings/model_findings"

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

# DBTITLE 1,Read the check results of the operate notebooks
checks = [
    r.asDict()
    for r in spark.table(AUDIT_TABLE)
    .filter((F.col("stage") == ML_STAGE) & F.col("component").like("models/operate/%"))
    .select("component", "metric_name", "status", "error_detail", "recorded_at")
    .collect()
]
print(f"{len(checks)} check row(s)")

# COMMAND ----------

# DBTITLE 1,Write the monitoring findings
stamp = now_utc().strftime("%Y-%m-%dT%H:%MZ")
for eco in ("energy", "commerce"):
    data = {
        "spec": [
            r.asDict() for r in read_model("monitoring_spec", ecosystem=eco).collect()
        ],
        "reference": [
            r.asDict()
            for r in read_model("reference_profile", ecosystem=eco)
            .select(
                "task_id",
                "model_name",
                "registry_version",
                "frozen_delta_version",
                "subject",
                "n_rows",
            )
            .collect()
        ],
        "predictions": [
            r.asDict()
            for r in read_model("model_predictions", ecosystem=eco)
            .groupBy("task_id", "model_name", "window_id")
            .agg(F.count("*").alias("rows"))
            .collect()
        ],
        "results": [
            r.asDict()
            for r in read_model("monitoring_results", ecosystem=eco).collect()
        ],
        "triggers": [
            r.asDict()
            for r in read_model("monitoring_triggers", ecosystem=eco).collect()
        ],
        "checks": checks,
    }
    text = render_monitoring_findings(eco, data, stamp)
    with open(
        _os.path.join(FINDINGS_DIR, f"{eco}_monitoring.md"), "w", encoding="utf-8"
    ) as fh:
        fh.write(text + "\n")
    print(f"OK  {eco}_monitoring.md")
