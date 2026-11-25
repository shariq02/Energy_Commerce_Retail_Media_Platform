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
# MAGIC ## EDA -- EVENT AND EVENT ITEM TABLES
# MAGIC
# MAGIC **Purpose:** Profile the event tables of commerce Gold (`commerce_event`,
# MAGIC `commerce_event_item`): nulls, constant columns, key, rates per column kind,
# MAGIC coverage by source (GA4 and REES46), the fill of the link columns and the
# MAGIC date-like columns -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "commerce_gold"
SOURCE = "commerce"
NB_KEY = "01_events"
SECTION_TITLE = "Event and event item tables"
TABLES = ["commerce_event", "commerce_event_item"]

# COMMAND ----------

# DBTITLE 1,Read tables
FRAMES = read_tables(CATALOG, SCHEMA, TABLES)

# COMMAND ----------

# DBTITLE 1,Standard EDA -- profile, keys, rates, coverage, links, dates and export
standard_group_eda(
    SOURCE, NB_KEY, SECTION_TITLE, FRAMES, TABLES, "commerce_01_rows.png"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- SESSION, PRODUCT AND RETAIL TRADE TABLES
# MAGIC
# MAGIC **Purpose:** Profile `commerce_session`, `commerce_product` and `retail_trade_index`:
# MAGIC nulls, constant columns, key, rates per column kind, coverage by source,
# MAGIC the fill of the link columns and the date-like columns -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "commerce_gold"
SOURCE = "commerce"
NB_KEY = "02_sessions_products_and_trade"
SECTION_TITLE = "Session, product and retail trade tables"
TABLES = ["commerce_session", "commerce_product", "retail_trade_index"]

# COMMAND ----------

# DBTITLE 1,Read tables
FRAMES = read_tables(CATALOG, SCHEMA, TABLES)

# COMMAND ----------

# DBTITLE 1,Standard EDA -- profile, keys, rates, coverage, links, dates and export
standard_group_eda(
    SOURCE, NB_KEY, SECTION_TITLE, FRAMES, TABLES, "commerce_02_rows.png"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- COMMERCE LINKS AND COVERAGE BIAS
# MAGIC
# MAGIC **Purpose:** Check that the session, product and event keys between the commerce
# MAGIC tables resolve, and measure how the item rows spread over the products.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SOURCE = "commerce"
NB_KEY = "03_links_and_bias"
SECTION_TITLE = "Commerce links and coverage bias"
LINKS = [
    (
        "commerce_gold",
        "commerce_event",
        "session_key",
        "commerce_gold",
        "commerce_session",
        "session_key",
    ),
    (
        "commerce_gold",
        "commerce_event",
        "product_key",
        "commerce_gold",
        "commerce_product",
        "product_key",
    ),
    (
        "commerce_gold",
        "commerce_event_item",
        "event_key",
        "commerce_gold",
        "commerce_event",
        "event_key",
    ),
    (
        "commerce_gold",
        "commerce_event_item",
        "product_key",
        "commerce_gold",
        "commerce_product",
        "product_key",
    ),
]

# COMMAND ----------

# DBTITLE 1,Link checks and item rows per product
LINK_RESULTS = link_checks(CATALOG, LINKS)
print(links_block(LINK_RESULTS))
ITEMS = spark.table(f"{CATALOG}.commerce_gold.commerce_event_item")
BIAS = {
    "commerce_event_item per product": coverage_bias(rows_per_key(ITEMS, "product_key"))
}
print(bias_block(BIAS))

# COMMAND ----------

# DBTITLE 1,Export findings
write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    [
        ("Links between commerce tables", links_block(LINK_RESULTS)),
        ("Item rows per product", bias_block(BIAS)),
    ],
)
