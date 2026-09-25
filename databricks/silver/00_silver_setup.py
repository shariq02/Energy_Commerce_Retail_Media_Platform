# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER SETUP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** create the four Silver schemas (`energy_silver`,
# MAGIC `energy_silver_reference`, `commerce_silver`,
# MAGIC `commerce_silver_reference`), the shared `quality.quarantine` table, and
# MAGIC compute + load the shared `quality.field_class_registry` directly from
# MAGIC `src/schemas/_generate_field_classes.py`'s `build_rows()` -- no committed
# MAGIC CSV is read or required; a newly added Silver table is picked up the next
# MAGIC time this notebook runs, and setup hard-fails immediately (before any
# MAGIC Silver notebook runs) if a real `write_silver()` call has no declared
# MAGIC classification anywhere. Idempotent -- safe to re-run.
# MAGIC
# MAGIC The `quality.pipeline_watermarks` and `quality.quality_audit_log` tables
# MAGIC already exist (`databricks/setup/00_create_schemas.py`); this notebook
# MAGIC only asserts them.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ./_silver_common

# COMMAND ----------

# DBTITLE 1,Imports
import sys as _sys

# COMMAND ----------

# DBTITLE 1,Configuration
SILVER_SCHEMAS = (
    "energy_silver",
    "energy_silver_reference",
    "commerce_silver",
    "commerce_silver_reference",
)
VALID_TARGET_SCHEMAS = set(SILVER_SCHEMAS)

# COMMAND ----------

# DBTITLE 1,Create the Silver schemas
for schema in SILVER_SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"OK  schema ready: {CATALOG}.{schema}")

# COMMAND ----------

# DBTITLE 1,Create the slim quarantine table (shared, in quality)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
    source_system    STRING,
    bronze_table     STRING,
    source_record_id STRING,
    rule_id          STRING,
    reason           STRING,
    field_name       STRING,
    offending_value  STRING,
    run_id           STRING,
    quarantined_at   TIMESTAMP
)
USING DELTA
COMMENT 'Slim Silver quarantine -- diagnostic fields only, shared across every ecosystem (disambiguated by source_system). Bronze is the authoritative raw record; complete rows are never duplicated here.'
""")
print(f"OK  table ready: {QUARANTINE_TABLE}")

# COMMAND ----------

# DBTITLE 1,Create the central field-class registry (shared, in quality)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FIELD_CLASS_TABLE} (
    table_name       STRING NOT NULL,
    column_name      STRING NOT NULL,
    field_class      STRING NOT NULL,
    derivation_rule  STRING,
    source_reference STRING,
    target_schema    STRING NOT NULL
)
USING DELTA
COMMENT 'One central registry for every Silver table across every ecosystem. Seeded from the source contracts + mappings + the fixed Silver governance column set; target_schema resolves which of the four Silver schemas each table belongs to. Silver notebooks validate against it; they never author it.'
""")
print(f"OK  table ready: {FIELD_CLASS_TABLE}")

# COMMAND ----------

# DBTITLE 1,Generate the field-class registry (no committed CSV required)
if repo_root() not in _sys.path:
    _sys.path.insert(0, repo_root())
from src.schemas._generate_field_classes import (
    _semantic_namespace,
    assert_registry_complete,
    build_rows,
)

_generated_rows = build_rows()
# Hard-fails here, before any Silver notebook runs, if a real write_silver()
# or write_semantic() target has no declared classification -- never guessed,
# never silently defaulted. Semantic structures come from _semantic_common.
assert_registry_complete(_generated_rows)

rows = [
    (
        r["table_name"],
        r["column_name"],
        r["field_class"],
        r.get("derivation_rule") or None,
        r.get("source_reference") or None,
        r["target_schema"],
    )
    for r in _generated_rows
]

bad_class = sorted(
    {c for _, _, c, _, _, _ in rows} - {"source_provided", "derived", "synthetic"}
)
if bad_class:
    raise RuntimeError(f"generator produced unknown field_class value(s): {bad_class}")

bad_schema = sorted({s for *_, s in rows} - VALID_TARGET_SCHEMAS)
if bad_schema:
    raise RuntimeError(
        f"generator produced unknown target_schema value(s): {bad_schema}"
    )

seed_df = spark.createDataFrame(
    rows,
    "table_name string, column_name string, field_class string, "
    "derivation_rule string, source_reference string, target_schema string",
)
seed_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(FIELD_CLASS_TABLE)
print(f"OK  {FIELD_CLASS_TABLE}: loaded {len(rows)} rows (generated in-process)")

# COMMAND ----------

# DBTITLE 1,Check the per-schema table quota (existing plus planned structures)
# Unity Catalog allows 100 tables per schema; stop here rather than mid-run.
SCHEMA_TABLE_QUOTA = 100
_ns = _semantic_namespace()
_planned = {*_ns["SEMANTIC_STRUCTURES"], *_ns["SEMANTIC_MEMBER_STRUCTURES"]}
for _schema in SILVER_SCHEMAS:
    _existing = {t.name for t in spark.catalog.listTables(f"{CATALOG}.{_schema}")}
    _new = {t for t in _planned if _ns["semantic_target_schema"](t) == _schema}
    _total = len(_existing | _new)
    print(f"{_schema}: {_total} tables once every Silver structure exists")
    if _total > SCHEMA_TABLE_QUOTA:
        raise RuntimeError(
            f"{_schema} would hold {_total} tables (quota {SCHEMA_TABLE_QUOTA}) -- "
            "retire legacy or stale tables in it first"
        )

# COMMAND ----------

# DBTITLE 1,Assert the shared quality tables exist
for t in (AUDIT_TABLE, WATERMARK_TABLE):
    if not spark.catalog.tableExists(t):
        raise RuntimeError(
            f"{t} missing -- run databricks/setup/00_create_schemas.py first"
        )
    print(f"OK  present: {t}")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print("SILVER SETUP SUMMARY")
print("=" * 70)
for schema in SILVER_SCHEMAS:
    print(f"Schema           : {CATALOG}.{schema}")
print(f"Quarantine table : {QUARANTINE_TABLE}")
print(f"Field-class rows : {spark.table(FIELD_CLASS_TABLE).count()}")
by_schema = (
    spark.table(FIELD_CLASS_TABLE)
    .select("target_schema", "table_name")
    .distinct()
    .groupBy("target_schema")
    .count()
    .collect()
)
for r in sorted(by_schema, key=lambda r: r["target_schema"]):
    print(f"  {r['target_schema']:<25} {r['count']} table(s)")
print("RESULT: PASS")
print("=" * 70)