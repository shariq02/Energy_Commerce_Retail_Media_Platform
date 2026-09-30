# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD LEGACY TABLE RETIREMENT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** list every table in the Gold schemas, mark each one active
# MAGIC (written by the current `databricks/gold/` notebooks) or legacy, and
# MAGIC drop the legacy tables. Active tables are discovered by scanning the
# MAGIC current notebooks for `write_gold`/`write_gold_view` calls -- Gold has
# MAGIC no central registry the way Silver's `field_class_registry` does, so
# MAGIC this scan is the only source of truth, same mechanism as
# MAGIC `06_silver_legacy_retirement.py`.
# MAGIC
# MAGIC Dropping is switched off (`DROP_LEGACY = False`); read the plan table,
# MAGIC then switch it on and re-run only the drop cell.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../gold/_gold_common

# COMMAND ----------

# DBTITLE 1,Gold inspection library
# MAGIC %run ../gold/_gold_inspect

# COMMAND ----------

# DBTITLE 1,Imports
import sys as _sys
from pathlib import Path as _Path

# COMMAND ----------

# DBTITLE 1,Configuration
DROP_LEGACY = False  # True

SCHEMAS = ["energy_gold", "commerce_gold", "shared_conformed"]

# COMMAND ----------

# DBTITLE 1,Active tables -- scanned from the current Gold notebooks
if repo_root() not in _sys.path:
    _sys.path.insert(0, repo_root())
from src.schemas._silver_notebook_scan import all_written_gold_tables

_gold_root = _Path(repo_root()) / "databricks" / "gold"
ACTIVE_TABLES = all_written_gold_tables(_gold_root)
print(
    f"OK  {len(ACTIVE_TABLES)} active Gold table/view name(s) scanned from databricks/gold/"
)

# COMMAND ----------

# DBTITLE 1,Inventory -- every table, active or legacy, with its drop decision
SCHEMAS = [s for s in SCHEMAS if spark.catalog.databaseExists(f"{CATALOG}.{s}")]
plan = []
for schema in SCHEMAS:
    for t in spark.catalog.listTables(f"{CATALOG}.{schema}"):
        action = "keep" if t.name in ACTIVE_TABLES else "drop"
        status = "active" if t.name in ACTIVE_TABLES else "legacy"
        plan.append((schema, t.name, status, action))
plan_df = spark.createDataFrame(
    plan, "schema string, table_name string, status string, action string"
).orderBy("action", "schema", "table_name")
display(plan_df)

# COMMAND ----------

# DBTITLE 1,Summary -- counts per schema and action
display(plan_df.groupBy("schema", "status", "action").count().orderBy("schema"))

# COMMAND ----------

# DBTITLE 1,Helper -- plan table to Markdown


def _plan_to_markdown(df, limit=1000):
    rows = df.limit(limit).collect()
    lines = ["| schema | table | status | action |", "|---|---|---|---|"]
    lines += [
        f"| {r.schema} | {r.table_name} | {r.status} | {r.action} |" for r in rows
    ]
    return "\n".join(lines)


# COMMAND ----------

# DBTITLE 1,Export findings -- retirement plan
write_gold_findings(
    "retirement",
    "07_gold_legacy_retirement__plan",
    "gold legacy retirement plan",
    [("plan", _plan_to_markdown(plan_df))],
)

# COMMAND ----------

# DBTITLE 1,Drop -- legacy tables marked drop (switched off by default)
if DROP_LEGACY:
    for r in plan_df.filter(F.col("action") == "drop").collect():
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{r['schema']}.{r['table_name']}")
        print(f"DROPPED  {r['schema']}.{r['table_name']}")
    print("Managed tables can be restored with UNDROP TABLE within retention.")
else:
    print("DROP_LEGACY is False -- nothing dropped. Review the plan above first.")

# COMMAND ----------

# DBTITLE 1,Inventory after -- tables per schema
for schema in SCHEMAS:
    names = sorted(t.name for t in spark.catalog.listTables(f"{CATALOG}.{schema}"))
    print(f"{schema}: {len(names)} tables")
