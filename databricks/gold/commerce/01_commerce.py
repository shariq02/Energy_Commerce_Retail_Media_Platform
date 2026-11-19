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
# MAGIC **Purpose:** the 3 `commerce_gold` tables: sessions (with the columns of their
# MAGIC user), products and `commerce_event` (REES46 view, cart and purchase events
# MAGIC and GA4 web events), all with the conformed session, user and product keys.
# MAGIC The last cell checks that every Silver event is in the Gold table.

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
ENTITY_KEYS = {"commerce_session": "session_key", "commerce_product": "product_key"}
# table -> (parent table, join column, column prefix): the parent columns are joined in
PARENTS = {"commerce_session": ("commerce_user", "user_key", "user_")}
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Read Silver
entities = {
    table: from_silver(read_silver(table))
    for table in (*ENTITY_KEYS, *(p[0] for p in PARENTS.values()))
}
commerce_event = from_silver(read_silver("commerce_event"))

# COMMAND ----------

# DBTITLE 1,Transform -- commerce_session (with its user) and commerce_product
for table, key in ENTITY_KEYS.items():
    frame = entities[table]
    if table in PARENTS:
        parent, on, prefix = PARENTS[table]
        frame = with_parent(frame, entities[parent], on=on, prefix=prefix)
    TABLES[table] = (add_provenance(frame, SOURCE, RID), [key])

# COMMAND ----------

# DBTITLE 1,Transform -- commerce_event (keys as in Silver commerce)
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
TABLES["commerce_event"] = (add_provenance(events, SOURCE, RID), ["event_key"])

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

# DBTITLE 1,Validation -- tables have rows and the Gold table holds every Silver event
validate_tables(TABLES, **COMMON)
gold_rows = read_gold("commerce_event", source=SOURCE).count()
silver_rows = commerce_event.count()
check(
    COMPONENT,
    SOURCE,
    "every_commerce_event_in_the_gold_table",
    gold_rows == silver_rows,
    detail=f"gold={gold_rows} silver={silver_rows}",
    metric_value=float(gold_rows - silver_rows),
    rid=RID,
)
