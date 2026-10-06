# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EXPORT REGISTRY FINDINGS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** write the registry and the reproducibility record of each ecosystem to
# MAGIC `src/findings/model_findings/<ecosystem>_registry.md`. The files are committed,
# MAGIC so private paths and e-mail addresses are removed.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Approval rules library
# MAGIC %run ../../lib/_approval_rules

# COMMAND ----------

# DBTITLE 1,Registry library
# MAGIC %run ../../lib/_registry_rules

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

# DBTITLE 1,Read the check results of the registry notebooks
results = [
    r.asDict()
    for r in spark.table(AUDIT_TABLE)
    .filter((F.col("stage") == ML_STAGE) & F.col("component").like("models/register/%"))
    .select("component", "metric_name", "status", "error_detail", "recorded_at")
    .collect()
]
print(f"{len(results)} check row(s)")

# COMMAND ----------

# DBTITLE 1,Write the registry findings
stamp = now_utc().strftime("%Y-%m-%dT%H:%MZ")
for eco in ("energy", "commerce"):
    text = render_registry_findings(
        eco, registry_rows(eco), check_rows(eco), results, stamp
    )
    with open(
        _os.path.join(FINDINGS_DIR, f"{eco}_registry.md"), "w", encoding="utf-8"
    ) as fh:
        fh.write(text + "\n")
    print(f"OK  {eco}_registry.md")
