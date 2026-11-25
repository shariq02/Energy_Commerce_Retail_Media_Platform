# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- COMMERCE (GOLD)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** exploratory analysis of the commerce data on the Gold tables, run
# MAGIC after Gold and before ML. Each section profiles one group of tables and writes
# MAGIC its findings to `src/schemas/profiling/<source>.md` (one section key per group).

# COMMAND ----------

# MAGIC %run ./_eda_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- EVENT, SESSION, PRODUCT AND RETAIL TRADE TABLES
# MAGIC
# MAGIC **Purpose:** Profile the commerce Gold tables in two groups: the event tables
# MAGIC (`commerce_event`, `commerce_event_item`) and `commerce_session`,
# MAGIC `commerce_product` and `retail_trade_index`: nulls, constant columns, key, rates
# MAGIC per column kind, coverage by source (GA4 and REES46), the fill of the link
# MAGIC columns and the date-like columns -- as evidence.

# COMMAND ----------

# DBTITLE 1,Standard EDA of the table groups
run_groups(
    "energy_commerce_retail_media",
    "commerce",
    [
        {
            "schema": "commerce_gold",
            "nb_key": "01_events",
            "title": "Event and event item tables",
            "tables": ["commerce_event", "commerce_event_item"],
        },
        {
            "schema": "commerce_gold",
            "nb_key": "02_sessions_products_and_trade",
            "title": "Session, product and retail trade tables",
            "tables": ["commerce_session", "commerce_product", "retail_trade_index"],
        },
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- COMMERCE LINKS AND COVERAGE BIAS
# MAGIC
# MAGIC **Purpose:** Check that the session, product and event keys between the commerce
# MAGIC tables resolve, and measure how the item rows spread over the products.

# COMMAND ----------

# DBTITLE 1,Link checks, item rows per product and export
CATALOG = "energy_commerce_retail_media"
GOLD = "commerce_gold"
LINKS = [
    (GOLD, "commerce_event", "session_key", GOLD, "commerce_session", "session_key"),
    (GOLD, "commerce_event", "product_key", GOLD, "commerce_product", "product_key"),
    (GOLD, "commerce_event_item", "event_key", GOLD, "commerce_event", "event_key"),
    (
        GOLD,
        "commerce_event_item",
        "product_key",
        GOLD,
        "commerce_product",
        "product_key",
    ),
]
BLOCKS = link_bias_blocks(
    CATALOG,
    LINKS,
    [("commerce_event_item per product", GOLD, "commerce_event_item", "product_key")],
    "Links between commerce tables",
    "Item rows per product",
)
write_profiling(
    "commerce", "03_links_and_bias", "Commerce links and coverage bias", BLOCKS
)
