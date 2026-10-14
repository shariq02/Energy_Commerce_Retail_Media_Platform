# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # UPDATE MODEL CARD PATHS IN THE REGISTRY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** the model cards moved from `src/schemas/model_cards/` to
# MAGIC `src/model_cards/`. This notebook changes the old prefix in `card_path` in the
# MAGIC registry tables of both ecosystems. No other column changes.
# MAGIC
# MAGIC **Dry run** with `APPLY = False` (counts rows only). Safe to run again: only
# MAGIC rows with the old prefix change.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../models/lib/_model_common

# COMMAND ----------

# DBTITLE 1,Configuration
OLD_PREFIX = "src/schemas/model_cards/"
NEW_PREFIX = "src/model_cards/"
ECOSYSTEMS = ("energy", "commerce")
APPLY = True  # False

print(f"Old prefix: {OLD_PREFIX}")
print(f"New prefix: {NEW_PREFIX}")
print(f"Apply: {APPLY}")

# COMMAND ----------

# DBTITLE 1,Count rows by prefix
for eco in ECOSYSTEMS:
    table = model_fqn("model_registry", eco)
    counts = spark.sql(
        f"""
        SELECT
          COUNT(*) AS total_rows,
          SUM(CASE WHEN card_path LIKE '{OLD_PREFIX}%' THEN 1 ELSE 0 END) AS old_prefix,
          SUM(CASE WHEN card_path LIKE '{NEW_PREFIX}%' THEN 1 ELSE 0 END) AS new_prefix
        FROM {table}
        """
    ).first()
    print(
        f"{table}: {counts['total_rows']} row(s), "
        f"{counts['old_prefix']} old prefix, {counts['new_prefix']} new prefix"
    )

# COMMAND ----------

# DBTITLE 1,Update the card path prefix
for eco in ECOSYSTEMS:
    table = model_fqn("model_registry", eco)
    if not APPLY:
        print(f"SKIP  {table}: APPLY is False")
        continue
    spark.sql(
        f"""
        UPDATE {table}
        SET card_path = CONCAT('{NEW_PREFIX}', SUBSTRING(card_path, {len(OLD_PREFIX) + 1}))
        WHERE card_path LIKE '{OLD_PREFIX}%'
        """
    )
    print(f"OK  updated {table}")

# COMMAND ----------

# DBTITLE 1,Rows still on the old prefix
for eco in ECOSYSTEMS:
    table = model_fqn("model_registry", eco)
    left = spark.sql(
        f"SELECT COUNT(*) AS n FROM {table} WHERE card_path LIKE '{OLD_PREFIX}%'"
    ).first()["n"]
    print(f"{table}: {left} row(s) still on the old prefix")
