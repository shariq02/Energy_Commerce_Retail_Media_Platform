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
# MAGIC **Purpose:** the 5 `commerce_gold` tables: sessions (with the columns of their
# MAGIC user), products, `commerce_event` (REES46 view, cart and purchase events
# MAGIC and GA4 web events) and `commerce_event_item` (one row per item of a GA4
# MAGIC event), all with the conformed session, user and product keys, and
# MAGIC `retail_trade_index` (the German retail trade volume, one row per month, with
# MAGIC the date key and calendar attributes of its month).
# MAGIC The last cells check that every Silver event, item and retail row is in a Gold
# MAGIC table and that every Silver commerce table feeds a Gold table.

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
# Gold table -> the Silver tables it is built from
GOLD_FEEDS = {
    "commerce_session": ("commerce_session", "commerce_user"),
    "commerce_product": ("commerce_product",),
    "commerce_event": ("commerce_event",),
    "commerce_event_item": ("ga4_event_item",),
    "retail_trade_index": ("retail_trade_index",),
}
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Read Silver
entities = {
    table: from_silver(read_silver(table))
    for table in (*ENTITY_KEYS, *(p[0] for p in PARENTS.values()))
}
commerce_event = from_silver(read_silver("commerce_event"))
event_item = from_silver(read_silver("ga4_event_item"))
retail_trade = from_silver(read_silver("retail_trade_index"))
dim_date = read_shared_conformed("dim_date").select(
    "calendar_date", "date_key", "year", "quarter", "month", "month_name"
)

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

# DBTITLE 1,Transform -- commerce_event_item (the keys of its event, the product key)
# item_id is a campaign id on promotions: only a product item has a product key
items = event_item.join(
    events.select("event_key", "session_key", "user_key"), "event_key", "left"
).withColumn(
    "product_key",
    F.when(
        (F.col("item_context") == "product") & F.col("item_id").isNotNull(),
        sha_key("source_system", "item_id"),
    ),
)
TABLES["commerce_event_item"] = (
    add_provenance(items, SOURCE, RID),
    ["event_key", "item_ordinal"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- retail_trade_index (with the calendar attributes of its month)
TABLES["retail_trade_index"] = (
    add_provenance(
        with_parent(
            retail_trade,
            dim_date,
            on="observation_month",
            parent_key="calendar_date",
            prefix="",
        ),
        "eurostat",
        RID,
    ),
    ["source_record_id"],
)

# COMMAND ----------

# DBTITLE 1,Write -- every table
write_tables(TABLES, **COMMON)

# COMMAND ----------

# DBTITLE 1,Grain gate -- every written table
gate_tables(TABLES, **COMMON)

# COMMAND ----------

# DBTITLE 1,Inspect and export -- every table
inspect_tables(TABLES, findings="commerce", **COMMON)

# COMMAND ----------

# DBTITLE 1,Validation -- tables have rows and the Gold tables hold every Silver row
validate_tables(TABLES, **COMMON)
for gold_table, silver_frame, name in (
    ("commerce_event", commerce_event, "every_commerce_event_in_the_gold_table"),
    ("commerce_event_item", event_item, "every_ga4_event_item_in_the_gold_table"),
    ("retail_trade_index", retail_trade, "every_retail_trade_row_in_the_gold_table"),
):
    gold_rows = read_gold(gold_table, source=SOURCE).count()
    silver_rows = silver_frame.count()
    check(
        COMPONENT,
        SOURCE,
        name,
        gold_rows == silver_rows,
        detail=f"gold={gold_rows} silver={silver_rows}",
        metric_value=float(gold_rows - silver_rows),
        rid=RID,
    )

# COMMAND ----------

# DBTITLE 1,Validation -- every Silver commerce table feeds a Gold table
unfed = unfed_tables(GOLD_FEEDS, silver_tables(COMMERCE_SILVER_SCHEMAS))
check(
    COMPONENT,
    SOURCE,
    "every_silver_table_feeds_a_gold_table",
    not unfed,
    detail=", ".join(unfed),
    metric_value=float(len(unfed)),
    rid=RID,
)
flush_logs()
