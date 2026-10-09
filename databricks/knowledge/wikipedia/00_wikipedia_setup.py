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
# MAGIC **Purpose:** create the `knowledge` schema and the `wikipedia_corpus` volume
# MAGIC and check that the article source and the selection terms are readable.
# MAGIC Idempotent -- safe to re-run.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ./_wikipedia_common

# COMMAND ----------

# DBTITLE 1,Create the schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"OK  schema ready: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Create the volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
os.makedirs(REPORT_DIR, exist_ok=True)
print(f"OK  volume ready: {VOLUME_ROOT}")

# COMMAND ----------

# DBTITLE 1,Import the preparation library
wp = load_preparation()
print("OK  preparation library imported")

# COMMAND ----------

# DBTITLE 1,Check the selection terms
terms = load_selection_terms(wp)
print(
    f"OK  rule version {terms.version}, {len(terms.pairs)} terms, "
    f"cap {terms.cap_per_term} per term"
)

# COMMAND ----------

# DBTITLE 1,Check the article source
files = [name for name in os.listdir(SOURCE_ROOT) if name.endswith(".parquet")]
assert files, f"no parquet files in {SOURCE_ROOT}"
print(f"OK  {len(files)} article parquet files in {SOURCE_ROOT}")
