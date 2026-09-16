# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- GA4 EVENTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `ga4_events` into source-scoped Silver at its event grain.
# MAGIC The candidate key is confirmed unique on Bronze (0 collisions, exact
# MAGIC check) -- conflict resolution here is a defensive re-run safeguard, not
# MAGIC a known problem. `event_params` / `ecommerce` / `items` stay nested
# MAGIC (Delta STRUCT/ARRAY), same decision as Bronze. GA4's own `"(not set)"` /
# MAGIC `"(none)"` sentinel (a real, non-null string) is normalised to NULL on
# MAGIC `items.item_id` / `item_name` / `item_category` and
# MAGIC `ecommerce.transaction_id` -- everywhere else stays as Bronze has it.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "ga4"
COMPONENT = "silver/commerce/journey/02_ga4_events"
RID = run_id()

BT = "ga4_events"
KEY_COLS = ["event_date", "event_timestamp", "user_pseudo_id", "event_name"]
CONTENT_COLS = ["event_params", "ecommerce", "items", "geo_country"]

UNSET = ("(not set)", "(none)", "")

# COMMAND ----------

# DBTITLE 1,Sentinel normalisation (nested fields)


def _clean_str(col: F.Column) -> F.Column:
    return F.when(col.isin(*UNSET), F.lit(None)).otherwise(col)


def _clean_items(items_col: F.Column) -> F.Column:
    return F.transform(
        items_col,
        lambda item: F.struct(
            _clean_str(item["item_id"]).alias("item_id"),
            _clean_str(item["item_name"]).alias("item_name"),
            _clean_str(item["item_category"]).alias("item_category"),
            item["price"].alias("price"),
            item["quantity"].alias("quantity"),
            item["item_revenue"].alias("item_revenue"),
        ),
    )


def _clean_ecommerce(ecommerce_col: F.Column) -> F.Column:
    return F.when(
        ecommerce_col.isNotNull(),
        F.struct(
            _clean_str(ecommerce_col["transaction_id"]).alias("transaction_id"),
            ecommerce_col["purchase_revenue"].alias("purchase_revenue"),
            ecommerce_col["unique_items"].alias("unique_items"),
            ecommerce_col["total_item_quantity"].alias("total_item_quantity"),
        ),
    ).otherwise(F.lit(None))


# COMMAND ----------

# DBTITLE 1,ga4_events -> Silver
bronze_df = read_bronze(BT)
df = bronze_df

df, q = resolve_conflicts(df, KEY_COLS, CONTENT_COLS, bronze_table=BT)
write_quarantine(q.withColumn("source_system", F.lit(SOURCE)), RID)

df = df.withColumn("items", _clean_items(F.col("items"))).withColumn(
    "ecommerce", _clean_ecommerce(F.col("ecommerce"))
)

# D1: pivot the closed, 100%-populated event_params vocabulary (6 keys) to
# named columns -- exposes ga_session_id, needed for GA4's session grain.
_PARAM_KEYS = {
    "session_id": ("ga_session_id", "int_value"),
    "session_number": ("ga_session_number", "int_value"),
    "page_location": ("page_location", "string_value"),
    "page_title": ("page_title", "string_value"),
    "search_term": ("search_term", "string_value"),
    "unique_search_term": ("unique_search_term", "string_value"),
}
for _out_col, (_key_name, _value_field) in _PARAM_KEYS.items():
    _matched = F.filter(
        F.col("event_params"), lambda x, _k=_key_name: x["key"] == F.lit(_k)
    )
    _first = F.element_at(_matched, 1)
    df = df.withColumn(_out_col, _first.getField("value").getField(_value_field))

df = df.withColumn("_srid", sha_key(*KEY_COLS))
df = add_provenance(df, SOURCE, "_srid", RID)
write_silver(df, BT, source=SOURCE, component=COMPONENT, rid=RID)
_findings_blocks = inspect_table(
    df,
    BT,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=KEY_COLS,
    df_before=bronze_df,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{BT}",
    BT,
    _findings_blocks,
)
