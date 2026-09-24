# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER LEGACY TABLE RETIREMENT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** list every table in the Silver and quality schemas, mark
# MAGIC each one active (written by the current Silver) or legacy, show whether
# MAGIC its parity check passed, and drop the legacy tables that may go.
# MAGIC
# MAGIC Runs before the new Silver: every legacy table is planned for drop.
# MAGIC Dropping is switched off (`DROP_LEGACY = False`); read the plan table,
# MAGIC then switch it on and re-run only the drop cell.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../silver/_silver_inspect

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../silver/_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
import os
import re

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
DROP_LEGACY = False
# Legacy tables are dropped before the new Silver runs (quota), so no parity
# results exist; True would drop only parity-passed or listed tables.
REQUIRE_PARITY = False

SCHEMAS = [
    "energy_silver",
    "energy_silver_reference",
    "commerce_silver",
    "commerce_silver_reference",
    "quality",
]
QUALITY_ACTIVE = {
    "quarantine",
    "field_class_registry",
    "pipeline_watermarks",
    "quality_audit_log",
}
# Legacy tables with no parity counterpart, and why they can go.
NO_PARITY_NEEDED = {
    "energy_silver.field_class_registry": "pre-relocation copy; live copy in quality",
    "energy_silver.quarantine": "pre-relocation copy; live copy in quality",
    "energy_silver.honda_channel_catalog": "wrong-schema copy",
    "energy_silver_reference.honda_channel_catalog": "labels now carried in the Honda energy structures",
    "quality.silver_inspection_log": "removed from code on 2026-09-16",
    "commerce_silver.search_visibility_events": "Search Visibility removed from scope",
    "commerce_silver_reference.search_visibility_repository": "Search Visibility removed from scope",
    "energy_silver.weather_observation": "single weather table, replaced by the weather families",
    "energy_silver.weather_location": "wrong-schema copy; weather_location lives in the reference schema",
    "energy_silver.weather_location_validity": "wrong-schema copy; lives in the reference schema",
}
PARITY_FILE = os.path.join(_findings_dir(), "parity.md")

# COMMAND ----------

# DBTITLE 1,Helper -- is a table active in the current Silver


def is_active(schema: str, table: str) -> bool:
    if schema == "quality":
        return table in QUALITY_ACTIVE
    structures = {*SEMANTIC_STRUCTURES, *SEMANTIC_MEMBER_STRUCTURES}
    return table in structures and semantic_target_schema(table) == schema


# COMMAND ----------

# DBTITLE 1,Read the parity results (check name -> status)
PARITY = []
if os.path.exists(PARITY_FILE):
    with open(PARITY_FILE, encoding="utf-8") as fh:
        for line in fh:
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cells) == 3 and cells[1] in ("PASS", "FAIL", "SKIP", "INFO"):
                PARITY.append((cells[0], cells[1]))
print(f"{len(PARITY)} parity checks read from {PARITY_FILE}")

# COMMAND ----------

# DBTITLE 1,Helper -- parity status of one legacy table


def parity_status(table: str) -> str:
    """'pass' when a check naming the table passed and none failed."""
    word = re.compile(rf"\b{re.escape(table)}\b")
    statuses = {s for name, s in PARITY if word.search(name)}
    if "FAIL" in statuses:
        return "fail"
    return "pass" if "PASS" in statuses else "none"


# COMMAND ----------

# DBTITLE 1,Inventory -- every table, active or legacy, with its drop decision
SCHEMAS = [s for s in SCHEMAS if spark.catalog.databaseExists(f"{CATALOG}.{s}")]
plan = []
for schema in SCHEMAS:
    for t in spark.catalog.listTables(f"{CATALOG}.{schema}"):
        full = f"{schema}.{t.name}"
        rows = spark.table(f"{CATALOG}.{full}").count()
        if is_active(schema, t.name):
            plan.append((schema, t.name, rows, "active", "-", "keep"))
            continue
        reason = NO_PARITY_NEEDED.get(full)
        parity = "not needed" if reason else parity_status(t.name)
        may_drop = bool(reason) or parity == "pass" or not REQUIRE_PARITY
        plan.append(
            (schema, t.name, rows, "legacy", parity, "drop" if may_drop else "hold")
        )
plan_df = spark.createDataFrame(
    plan,
    (
        "schema string, table_name string, rows long, status string, "
        "parity string, action string"
    ),
).orderBy("action", "schema", "table_name")
display(plan_df)

# COMMAND ----------

# DBTITLE 1,Summary -- counts per schema and action
display(plan_df.groupBy("schema", "status", "action").count().orderBy("schema"))

# COMMAND ----------

# DBTITLE 1,Export findings -- retirement plan
write_silver_findings(
    "retirement",
    "06_silver_legacy_retirement__plan",
    "silver legacy retirement plan",
    [("plan", rows_to_markdown(plan_df, limit=1000))],
)

# COMMAND ----------

# DBTITLE 1,Drop -- legacy tables marked drop (switched off by default)
if DROP_LEGACY:
    for r in plan_df.filter(F.col("action") == "drop").collect():
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{r['schema']}.{r['table_name']}")
        print(f"DROPPED  {r['schema']}.{r['table_name']}  ({r['rows']} rows)")
    print("Managed tables can be restored with UNDROP TABLE within retention.")
else:
    print("DROP_LEGACY is False -- nothing dropped. Review the plan above first.")

# COMMAND ----------

# DBTITLE 1,Inventory after -- tables per schema
for schema in SCHEMAS:
    names = sorted(t.name for t in spark.catalog.listTables(f"{CATALOG}.{schema}"))
    print(f"{schema}: {len(names)} tables")
