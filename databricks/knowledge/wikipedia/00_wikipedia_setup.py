# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # KNOWLEDGE SETUP -- WIKIPEDIA
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** create the `knowledge` schema and check that
# MAGIC `quality.quality_audit_log`, the article source and the selection terms are
# MAGIC in place. Idempotent -- safe to re-run.

# COMMAND ----------

# DBTITLE 1,Text library
# MAGIC %run ./_wikipedia_text

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ./_wikipedia_common

# COMMAND ----------

# DBTITLE 1,Create the schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"OK  schema ready: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Create the model volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{MODEL_VOLUME}")
print(f"OK  volume ready: {CATALOG}.{SCHEMA}.{MODEL_VOLUME}")

# COMMAND ----------

# DBTITLE 1,Check the model files (needed by notebook 05)
missing = [
    name
    for name in ("model.onnx", "tokenizer.json", MODEL_HASHES_FILE)
    if not os.path.isfile(os.path.join(MODEL_DIR, name))
]
if missing:
    print(
        f"WARN {missing} not in {MODEL_DIR}: run "
        "python -m scripts.knowledge.wikipedia_model_upload before notebook 05"
    )
else:
    print(f"OK  model files present in {MODEL_DIR}")

# COMMAND ----------

# DBTITLE 1,Assert the audit log exists
if not spark.catalog.tableExists(AUDIT_TABLE):
    raise RuntimeError(
        f"{AUDIT_TABLE} not found: run databricks/setup/00_create_schemas.py first"
    )
print(f"OK  audit log present: {AUDIT_TABLE}")

# COMMAND ----------

# DBTITLE 1,Check the selection terms
terms = load_selection_terms()
print(
    f"OK  rule version {terms.version}, {len(terms.pairs)} terms, "
    f"cap {terms.cap_per_term} per term"
)

# COMMAND ----------

# DBTITLE 1,Check the article source
files = [name for name in os.listdir(SOURCE_ROOT) if name.endswith(".parquet")]
assert files, f"no parquet files in {SOURCE_ROOT}"
print(f"OK  {len(files)} article parquet files in {SOURCE_ROOT}")
