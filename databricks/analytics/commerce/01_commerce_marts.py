# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ANALYTICS COMMERCE -- MARTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the 3 `commerce_analytics` tables from Gold `commerce_session` and
# MAGIC `commerce_event`: GA4 demand per day, the session funnel per source and day, and
# MAGIC demand per product category and day (REES46 event, session and distinct product
# MAGIC counts).

# COMMAND ----------

# DBTITLE 1,Gold library
# MAGIC %run ../../gold/_gold_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "rees46"
COMPONENT = "analytics/commerce/01_commerce_marts"
RID = analytics_run_id()
RULES = load_yaml("src/schemas/mappings/analytics_rules.yml")
GATE = {"source": SOURCE, "component": COMPONENT, "rid": RID}
COMMON = {**GATE, "schema": ANALYTICS_SCHEMAS["commerce"]}
EVENT_KINDS = ("view", "cart", "purchase")
STEPS = sorted({step for steps in RULES["funnel_steps"].values() for step in steps})
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Read Gold
commerce_session = read_gold("commerce_session", source=SOURCE)
shop_events = read_gold("commerce_event", source=SOURCE).filter(
    F.col("source_system") == SOURCE
)

# COMMAND ----------

# DBTITLE 1,Transform -- ga4_demand_daily (GA4 sessions per country and day)
TABLES["ga4_demand_daily"] = (
    add_provenance(
        commerce_session.filter(F.col("source_system") == "ga4")
        .groupBy("geo_country", "local_date")
        .agg(
            F.count(F.lit(1)).alias("session_count"),
            F.sum("transaction_count").alias("transaction_count"),
            F.sum("revenue").alias("revenue"),
            F.sum("event_count").alias("event_count"),
        ),
        "ga4",
        RID,
    ),
    ["geo_country", "local_date"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- customer_journey_daily (session funnel per source and day)
reached = {}
for step in STEPS:
    flag = F.lit(None).cast("boolean")
    for source_system, steps in RULES["funnel_steps"].items():
        if step in steps:
            events = sum(
                (
                    F.coalesce(F.col(f"{name}_event_count"), F.lit(0))
                    for name in steps[step]
                ),
                F.lit(0),
            )
            flag = F.when(
                F.col("source_system") == source_system, events > 0
            ).otherwise(flag)
    reached[step] = flag
journey_grain = ["source_system", "local_date"]
TABLES["customer_journey_daily"] = (
    add_provenance(
        commerce_session.groupBy(*journey_grain)
        .agg(
            F.count(F.lit(1)).alias("session_count"),
            F.countDistinct("user_key").alias("user_count"),
            *[
                F.count(F.when(flag, 1)).alias(f"{step}_session_count")
                for step, flag in reached.items()
            ],
            F.count(F.when(F.col("event_count") == 1, 1)).alias(
                "single_event_session_count"
            ),
            F.avg("event_count").alias("mean_events_per_session"),
            F.sum("transaction_count").alias("transaction_count"),
            F.sum("revenue").alias("revenue"),
        )
        .withColumn(
            "purchase_session_share", ratio("purchase_session_count", "session_count")
        )
        .withColumn(
            "view_to_cart_rate", ratio("cart_session_count", "view_session_count")
        )
        .withColumn(
            "cart_to_purchase_rate",
            ratio("purchase_session_count", "cart_session_count"),
        ),
        SOURCE,
        RID,
    ),
    journey_grain,
)

# COMMAND ----------

# DBTITLE 1,Transform -- product_demand_daily (REES46 demand per category and day)
# The `category_l1_*` columns count over the whole top-level category of the day (a
# distinct count cannot be added over the second level), per event kind.
event_type = F.col("event_type")
category_grain = ["category_l1", "category_l2", "local_date"]
top_grain = ["category_l1", "local_date"]
top_category = shop_events.groupBy(*top_grain).agg(
    *[
        F.countDistinct(F.when(event_type == kind, F.col("user_session"))).alias(
            f"category_l1_{kind}_session_count"
        )
        for kind in EVENT_KINDS
    ],
    *[
        F.countDistinct(F.when(event_type == kind, F.col("product_id"))).alias(
            f"category_l1_{kind}_distinct_product_count"
        )
        for kind in EVENT_KINDS
    ],
)
TABLES["product_demand_daily"] = (
    add_provenance(
        shop_events.groupBy(*category_grain)
        .agg(
            *[
                F.count(F.when(event_type == kind, 1)).alias(f"{kind}_count")
                for kind in EVENT_KINDS
            ],
            F.sum(F.when(event_type == "purchase", F.col("price"))).alias(
                "purchase_value"
            ),
            F.countDistinct("user_session").alias("session_count"),
            F.countDistinct("product_id").alias("distinct_product_count"),
            F.countDistinct("user_id").alias("distinct_user_count"),
        )
        .withColumn("view_to_purchase_rate", ratio("purchase_count", "view_count"))
        .join(top_category, top_grain, "left"),
        SOURCE,
        RID,
    ),
    category_grain,
)

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

# DBTITLE 1,Validation -- every table exists and has rows
validate_tables(TABLES, **COMMON)
