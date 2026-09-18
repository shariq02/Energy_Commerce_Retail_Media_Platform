# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- GA4 TRANSACTIONS (ADDITIVE, TRANSACTION-BEARING EVENT GRAIN)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** flatten `ga4_events.ecommerce` into a source-scoped,
# MAGIC additive transaction-grain table, mirroring `03_ga4_items.py`'s
# MAGIC decomposition of `items`. A refund shares `transaction_id` with its
# MAGIC purchase event but is a separate row (`event_name` disambiguates) --
# MAGIC collapsing them is Gold's `fact_order`, not this table.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "ga4"
COMPONENT = "silver/commerce/journey/04_ga4_transactions"
RID = run_id()

BT = "ga4_events"
KEY_COLS = ["event_date", "event_timestamp", "user_pseudo_id", "event_name"]

# COMMAND ----------

# DBTITLE 1,Read Bronze -- ga4_events
bronze_df = read_bronze(BT)

# COMMAND ----------

# DBTITLE 1,Transform -- ga4_transactions (filter + flatten)
# Re-applies 02_ga4_events.py's sentinel normalisation independently -- reads
# Bronze directly, not 02's Silver output, no Silver-to-Silver dependency.
_UNSET = ("(not set)", "(none)", "")
transactions_df = (
    bronze_df.select(
        "event_date",
        "event_timestamp",
        "user_pseudo_id",
        "event_name",
        F.col("ecommerce.transaction_id").alias("transaction_id"),
        F.col("ecommerce.purchase_revenue").alias("purchase_revenue"),
        F.col("ecommerce.unique_items").alias("unique_items"),
        F.col("ecommerce.total_item_quantity").alias("total_item_quantity"),
    )
    .withColumn(
        "transaction_id",
        F.when(F.col("transaction_id").isin(*_UNSET), F.lit(None)).otherwise(
            F.col("transaction_id")
        ),
    )
    .filter(F.col("transaction_id").isNotNull())
)

# COMMAND ----------

# DBTITLE 1,Transform -- ga4_transactions (provenance)
transactions_df = transactions_df.withColumn("_srid", sha_key(*KEY_COLS))
transactions_df = add_provenance(transactions_df, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- ga4_transactions
write_silver(
    transactions_df, "ga4_transactions", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect ga4_transactions + export findings
_findings_blocks = inspect_table(
    transactions_df,
    "ga4_transactions",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=KEY_COLS,
    extra_checks={
        "distinct_transaction_id_count": transactions_df.select("transaction_id")
        .distinct()
        .count(),
    },
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__ga4_transactions",
    "ga4_transactions",
    _findings_blocks,
)
