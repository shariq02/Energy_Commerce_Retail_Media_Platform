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
# MAGIC **Purpose:** the 4 `commerce_analytics` tables from Gold `commerce_session` and
# MAGIC `commerce_event`: GA4 demand per day, the session funnel per source and day, and
# MAGIC demand per product category and day (REES46 event, session and distinct product
# MAGIC counts); and the German retail trade volume per month against the electricity
# MAGIC price, temperature and consumption of the energy marts (runs after them).

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
HOME = RULES["home_market_area"]
EVENT_KINDS = ("view", "cart", "purchase")
STEPS = sorted({step for steps in RULES["funnel_steps"].values() for step in steps})
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Read Gold
commerce_session = read_gold("commerce_session", source=SOURCE)
shop_events = read_gold("commerce_event", source=SOURCE).filter(
    F.col("source_system") == SOURCE
)
retail_trade = read_gold("retail_trade_index", source=SOURCE)
ENERGY = ANALYTICS_SCHEMAS["energy"]
market_price = read_gold("market_price_daily", source=SOURCE, schema=ENERGY)
national_weather = read_gold("weather_energy_daily", source=SOURCE, schema=ENERGY)

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

# DBTITLE 1,Transform -- retail_energy_monthly (retail trade volume against the energy marts)
# The two sides meet on the calendar month of the home market area (Germany) only.
# The monthly price is the mean of the daily means weighted by their interval counts.
month = F.trunc("local_date", "month").alias("observation_month")
daily_price = F.col("mean_price_eur_per_mwh")
price_month = (
    one_resolution(
        market_price.filter(F.col("market_area_code") == HOME),
        ["local_date"],
        "interval_seconds",
        largest=True,
    )
    .groupBy(month)
    .agg(
        ratio(
            F.sum(daily_price * F.col("interval_count")),
            F.sum(F.when(daily_price.isNotNull(), F.col("interval_count"))),
        ).alias("mean_price_eur_per_mwh"),
        F.countDistinct(F.when(daily_price.isNotNull(), F.col("local_date"))).alias(
            "price_day_count"
        ),
    )
)
weather_month = (
    national_weather.filter(F.col("market_area_code") == HOME)
    .groupBy(month)
    .agg(
        F.avg("mean_air_temperature_degc").alias("mean_air_temperature_degc"),
        F.count("mean_air_temperature_degc").alias("temperature_day_count"),
        F.sum("consumption_mwh").alias("consumption_mwh"),
        F.count("consumption_mwh").alias("consumption_day_count"),
    )
)
TABLES["retail_energy_monthly"] = (
    add_provenance(
        retail_trade.select(
            "observation_month",
            "country_code",
            F.col("index_value").alias("retail_trade_volume_index"),
            F.col("unit_code").alias("retail_trade_unit_code"),
            F.col("observation_flag").alias("retail_trade_observation_flag"),
        )
        .join(F.broadcast(price_month), "observation_month", "left")
        .join(F.broadcast(weather_month), "observation_month", "left"),
        "eurostat",
        RID,
    ),
    ["observation_month", "country_code"],
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

# DBTITLE 1,Validation -- every table exists and has rows
validate_tables(TABLES, **COMMON)
