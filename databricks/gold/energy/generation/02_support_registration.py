# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- SUPPORT_REGISTRATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.support_registration` -- a view over Silver
# MAGIC `support_registration`, every column carried through, plus
# MAGIC `linked_unit_ids` resolved from `generation_unit`'s own support-id
# MAGIC columns (aggregated first, so a registration linked from more than one
# MAGIC unit never fans the row out). Grain: support registration.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Gold inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "gold/energy/generation/support_registration"
TABLE = "support_registration"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver as temp views
read_silver("support_registration").createOrReplaceTempView("_sr")
read_silver("generation_unit").select(
    "unit_id", "renewable_energy_act_support_id", "combined_heat_and_power_support_id"
).createOrReplaceTempView("_gu_links")

# COMMAND ----------

# DBTITLE 1,Create the view
write_gold_view(
    """
WITH links AS (
    SELECT renewable_energy_act_support_id AS support_registration_id, unit_id
    FROM _gu_links WHERE renewable_energy_act_support_id IS NOT NULL
    UNION ALL
    SELECT combined_heat_and_power_support_id AS support_registration_id, unit_id
    FROM _gu_links WHERE combined_heat_and_power_support_id IS NOT NULL
),
agg_links AS (
    SELECT support_registration_id, collect_list(unit_id) AS linked_unit_ids
    FROM links GROUP BY support_registration_id
)
SELECT sr.*, al.linked_unit_ids
FROM _sr sr
LEFT JOIN agg_links al ON al.support_registration_id = sr.support_registration_id
""",
    TABLE,
    source=SOURCE,
)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    read_gold(TABLE, source=SOURCE),
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["support_registration_id"],
)
write_gold_findings(SOURCE, f"generation__{TABLE}", TABLE, _blocks)
