# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- FACT ORDER
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Grain:** one row per `source_record_id` (one transaction-bearing GA4
# MAGIC event). Append-only fact, not an SCD2 dimension -- a refund is a later,
# MAGIC distinct event row sharing `transaction_id`, never an in-place update to
# MAGIC the row that placed the order.
# MAGIC
# MAGIC **Sources:** `ga4_transactions` (Silver, commerce_silver); `fact_web_event`
# MAGIC (Gold, this run, for the parent-event FK via the shared `source_record_id`).
# MAGIC
# MAGIC **Serves use case:** GA4 order/transaction identity. REES46 has no
# MAGIC counterpart -- no transaction/order identifier at source.
# MAGIC
# MAGIC **Purpose:** the first Gold representation of a GA4 order as its own
# MAGIC entity. Line items are `fact_ecommerce_item` rows sharing this row's
# MAGIC `web_event_key`.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "ga4"
COMPONENT = "gold/commerce/journey/06_fact_order"
RID = gold_run_id()
GOLD_TABLE = "fact_order"

# COMMAND ----------

# DBTITLE 1,Read Silver -- ga4_transactions
_transactions_silver = read_silver("ga4_transactions")

# COMMAND ----------

# DBTITLE 1,Read Gold -- fact_web_event
_web_event = read_gold("fact_web_event", source=SOURCE)

# COMMAND ----------

# DBTITLE 1,Transform -- resolve the parent web-event key
fact = resolve_fk(
    _transactions_silver,
    _web_event,
    fact_key_cols=["source_record_id"],
    dim_key_cols=["source_record_id"],
    dim_surrogate_col="web_event_key",
    output_col="web_event_key",
)

# COMMAND ----------

# DBTITLE 1,Transform -- surrogate key + Gold provenance
fact = fact.withColumn("order_key", surrogate_key("source_record_id"))
fact = add_gold_provenance(fact, SOURCE, RID)

# COMMAND ----------

# DBTITLE 1,Grain assertion -- one row per source_record_id
assert_unique_grain(
    fact, ["source_record_id"], component=COMPONENT, source=SOURCE, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- fact_order
write_gold(fact, GOLD_TABLE, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect fact_order + export findings
_findings_blocks = inspect_gold_table(
    fact,
    GOLD_TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    df_before=_transactions_silver,
    extra_checks={
        "unmatched_web_event_fk": fact.filter(F.col("web_event_key").isNull()).count(),
    },
)
write_gold_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{GOLD_TABLE}",
    GOLD_TABLE,
    _findings_blocks,
)
