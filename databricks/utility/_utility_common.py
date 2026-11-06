# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # UTILITY LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the two helpers that the utility notebooks share, pulled in with
# MAGIC `%run ./_utility_common`: `find_repo_root()` and `markdown_table(columns, rows)`.

# COMMAND ----------

# DBTITLE 1,Imports
import os

# COMMAND ----------

# DBTITLE 1,Define find_repo_root


def find_repo_root():
    path = os.path.abspath(os.getcwd())
    for _ in range(12):
        if os.path.isdir(os.path.join(path, "src")) and os.path.isdir(
            os.path.join(path, "databricks")
        ):
            return path
        if os.path.dirname(path) == path:
            break
        path = os.path.dirname(path)
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    cut = notebook_path.rfind("/databricks/")
    if cut > 0:
        for candidate in (notebook_path[:cut], "/Workspace" + notebook_path[:cut]):
            if os.path.isdir(os.path.join(candidate, "src")):
                return candidate
    raise RuntimeError(
        "repo root not found -- run from inside the repo's Databricks Git folder"
    )


# COMMAND ----------

# DBTITLE 1,Define markdown_table


def markdown_table(columns, rows):
    def cell(value):
        if value is None:
            return ""
        if isinstance(value, int):
            return f"{value:,}"
        return str(value).replace("|", "/").replace("\n", " ")

    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    lines += ["| " + " | ".join(cell(v) for v in row) + " |" for row in rows]
    return "\n".join(lines)
