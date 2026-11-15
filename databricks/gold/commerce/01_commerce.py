# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GOLD COMMERCE -- ENTITIES AND EVENTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the 7 `commerce_gold` tables: sessions, users and products (the
# MAGIC Silver aggregates) and the events split by meaning (REES46 view, cart and
# MAGIC purchase, GA4 web events), all with the conformed session, user and product
# MAGIC keys. The last cell checks that every Silver event is in one Gold table.

# COMMAND ----------

# DBTITLE 1,Gold library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "rees46"
COMPONENT = "gold/commerce/01_commerce"
RID = gold_run_id()
GATE = {"source": SOURCE, "component": COMPONENT, "rid": RID}
COMMON = {**GATE, "schema": gold_schema_for(SOURCE)}
ENTITY_KEYS = {
    "commerce_session": "session_key",
    "commerce_user": "user_key",
    "commerce_product": "product_key",
}
# table -> (source_system, REES46 event type or None for every event of the source)
EVENT_TABLES = {
    "commerce_view_event": ("rees46", "view"),
    "commerce_cart_event": ("rees46", "cart"),
    "commerce_purchase_event": ("rees46", "purchase"),
    "commerce_web_event": ("ga4", None),
}
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Read Silver
entities = {table: from_silver(read_silver(table)) for table in ENTITY_KEYS}
commerce_event = from_silver(read_silver("commerce_event"))

# COMMAND ----------

# DBTITLE 1,Transform -- commerce_session, commerce_user, commerce_product
for table, key in ENTITY_KEYS.items():
    TABLES[table] = (add_provenance(entities[table], SOURCE, RID), [key])

# COMMAND ----------

# DBTITLE 1,Transform -- the four event tables (keys as in Silver commerce)
user_id = F.coalesce(F.col("user_id"), F.col("user_pseudo_id"))
session_id = F.coalesce(F.col("user_session"), F.col("session_id").cast("string"))
events = (
    commerce_event.withColumn(
        "session_key", sha_key("source_system", user_id, session_id)
    )
    .withColumn("user_key", sha_key("source_system", user_id))
    .withColumn(
        "product_key",
        F.when(F.col("product_id").isNotNull(), sha_key("source_system", "product_id")),
    )
)
for table, (source_system, event_type) in EVENT_TABLES.items():
    rows = events.filter(F.col("source_system") == source_system)
    if event_type is not None:
        rows = rows.filter(F.col("event_type") == event_type)
    TABLES[table] = (add_provenance(rows, source_system, RID), ["event_key"])

# COMMAND ----------

# DBTITLE 1,Grain gate -- every table
gate_tables(TABLES, **GATE)

# COMMAND ----------

# DBTITLE 1,Write -- every table
write_tables(TABLES, **COMMON)

# COMMAND ----------

# DBTITLE 1,Inspect and export -- every table
inspect_tables(TABLES, findings="commerce", **COMMON)

# COMMAND ----------

# DBTITLE 1,Validation -- tables have rows and together hold every Silver event
validate_tables(TABLES, **COMMON)
gold_rows = sum(read_gold(t, source=SOURCE).count() for t in EVENT_TABLES)
silver_rows = commerce_event.count()
check(
    COMPONENT,
    SOURCE,
    "every_commerce_event_in_one_gold_table",
    gold_rows == silver_rows,
    detail=f"gold={gold_rows} silver={silver_rows}",
    metric_value=float(gold_rows - silver_rows),
    rid=RID,
)
