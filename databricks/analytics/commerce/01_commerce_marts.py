# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ANALYTICS COMMERCE -- MARTS AND METRICS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the 5 `commerce_analytics` tables from Gold `commerce_session` and
# MAGIC the REES46 event tables: GA4 and REES46 demand per day, the session funnel per
# MAGIC source and day, demand per product category and day, and `metric_definition`.

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
EVENT_TABLES = ("commerce_view_event", "commerce_cart_event", "commerce_purchase_event")
STEPS = sorted({step for steps in RULES["funnel_steps"].values() for step in steps})
METRICS = [
    (
        "ga4_sessions",
        "Daily GA4 session count, by country.",
        "count(*) grouped by geo_country, local_date",
        "ga4_demand_daily",
        "geo_country x local_date",
        "ga4",
    ),
    (
        "ga4_revenue",
        "Daily GA4 purchase revenue, by country.",
        "sum(revenue) grouped by geo_country, local_date",
        "ga4_demand_daily",
        "geo_country x local_date",
        "ga4",
    ),
    (
        "rees46_event_demand",
        (
            "Daily REES46 event volume, by top-level category and event type. No "
            "currency -- relative measure only."
        ),
        "count(*) grouped by local_date, category_l1, event_type",
        "rees46_demand_daily",
        "local_date x category_l1 x event_type",
        "rees46",
    ),
    (
        "purchase_session_share",
        "Share of the sessions of a day with at least one purchase event, by source.",
        "purchase_session_count / session_count grouped by source_system, local_date",
        "customer_journey_daily",
        "source_system x local_date",
        "rees46",
    ),
    (
        "category_purchases",
        "Daily REES46 purchase events, by product category.",
        (
            "count(event_type = 'purchase') grouped by category_l1, category_l2, "
            "local_date"
        ),
        "product_demand_daily",
        "category_l1 x category_l2 x local_date",
        "rees46",
    ),
]
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Read Gold
commerce_session = read_gold("commerce_session", source=SOURCE)
shop_events = read_gold(EVENT_TABLES[0], source=SOURCE)
for table in EVENT_TABLES[1:]:
    shop_events = shop_events.unionByName(read_gold(table, source=SOURCE))

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

# DBTITLE 1,Transform -- rees46_demand_daily (REES46 events per category, type and day)
rees46_grain = ["local_date", "category_l1", "event_type"]
TABLES["rees46_demand_daily"] = (
    add_provenance(
        shop_events.groupBy(*rees46_grain).agg(
            F.count(F.lit(1)).alias("event_count"),
            F.countDistinct("user_session").alias("session_count"),
            F.countDistinct("product_id").alias("distinct_product_count"),
        ),
        SOURCE,
        RID,
    ),
    rees46_grain,
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
event_type = F.col("event_type")
category_grain = ["category_l1", "category_l2", "local_date"]
TABLES["product_demand_daily"] = (
    add_provenance(
        shop_events.groupBy(*category_grain)
        .agg(
            *[
                F.count(F.when(event_type == kind, 1)).alias(f"{kind}_count")
                for kind in ("view", "cart", "purchase")
            ],
            F.sum(F.when(event_type == "purchase", F.col("price"))).alias(
                "purchase_value"
            ),
            F.countDistinct("product_id").alias("distinct_product_count"),
            F.countDistinct("user_id").alias("distinct_user_count"),
        )
        .withColumn("view_to_purchase_rate", ratio("purchase_count", "view_count")),
        SOURCE,
        RID,
    ),
    category_grain,
)

# COMMAND ----------

# DBTITLE 1,Transform -- metric_definition (one row per commerce metric)
TABLES["metric_definition"] = (
    add_provenance(
        spark.createDataFrame(
            METRICS,
            "metric_key string, definition string, formula string, "
            "source_mart string, grain string, source_system string",
        ),
        SOURCE,
        RID,
    ),
    ["metric_key"],
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
