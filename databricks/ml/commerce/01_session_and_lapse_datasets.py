# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ML COMMERCE -- SESSION PURCHASE AND LAPSE DATASETS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** build the four REES46 and GA4 datasets of the families
# MAGIC `session_purchase` and `lapse`: the product as-of counts (kept tables), the
# MAGIC session prefix, the user history and the lapse features, the two targets, and
# MAGIC for each dataset the contract, the split, the fit and the family rows.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../_ml_inspect

# COMMAND ----------

# DBTITLE 1,ML build library
# MAGIC %run ../_ml_fit

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "commerce"
SOURCE = "rees46"
COMPONENT = "ml/commerce/session_and_lapse"
REES46, GA4 = "rees46_events", "ga4_events"
K_REES46, K_GA4 = 2, 10
REES46_LABEL_DAYS = 30
GA4_CUTOFFS = [GA4_LAPSE_TRAIN_CUTOFF, GA4_LAPSE_EVALUATION_CUTOFF]
# GA4 events next to a purchase leave the prefix features
ADJACENT = ("begin_checkout", "add_shipping_info", "add_payment_info", "purchase")
FRAMES, BLOCKS = {}, {}

# COMMAND ----------

# DBTITLE 1,Run identity and session time zone
rid = ml_run_id()
set_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Gold -- events and sessions with the session and user keys
events = read_gold("commerce_event", source=SOURCE)
rees46_events = events.filter(F.col("source_dataset") == REES46).withColumn(
    "session_key", F.concat_ws("|", "user_id", "user_session")
)
ga4_events = (
    events.filter(F.col("source_dataset") == GA4)
    .withColumn("user_id", F.col("user_pseudo_id"))
    .withColumn(
        "session_key",
        F.concat_ws("|", "user_pseudo_id", F.col("session_id").cast("string")),
    )
)
sessions = (
    read_gold("commerce_session", source=SOURCE)
    .withColumn("session_key", F.concat_ws("|", "source_user_id", "source_session_id"))
    .withColumn("user_id", F.col("source_user_id"))
    .withColumn("session_start_utc", F.col("first_event_utc"))
    .withColumn("session_end_utc", F.col("last_event_utc"))
    .withColumn("had_purchase", (F.col("purchase_event_count") > 0).cast("int"))
    .withColumn("total_revenue", F.col("revenue"))
)
rees46_sessions = sessions.filter(F.col("source_dataset") == REES46)
ga4_sessions = sessions.filter(F.col("source_dataset") == GA4)
ga4_items = read_gold("commerce_event_item", source="ga4").filter(
    F.col("item_id").isNotNull()
)

# COMMAND ----------

# DBTITLE 1,Product as-of counts -- two kept tables (the day before is read)
_daily = rees46_events.groupBy("product_id", "local_date").agg(
    F.sum((F.col("event_type") == "view").cast("int")).alias("views"),
    F.sum((F.col("event_type") == "cart").cast("int")).alias("carts"),
    F.sum((F.col("event_type") == "purchase").cast("int")).alias("purchases"),
    F.max_by("price", "event_timestamp_utc").alias("last_price"),
)
_w = (
    Window.partitionBy("product_id")
    .orderBy("local_date")
    .rowsBetween(Window.unboundedPreceding, 0)
)
product_asof_rees46 = (
    _daily.withColumn("product_pop_views", F.sum("views").over(_w))
    .withColumn("product_pop_carts", F.sum("carts").over(_w))
    .withColumn("product_pop_purchases", F.sum("purchases").over(_w))
    .select(
        "product_id",
        "local_date",
        "product_pop_views",
        "product_pop_carts",
        "product_pop_purchases",
        F.col("last_price").alias("product_pop_last_price"),
    )
)
_daily_g = (
    ga4_items.join(ga4_events.select("event_key", "local_date"), "event_key")
    .groupBy("item_id", "local_date")
    .agg(
        F.sum((F.col("event_name") == "view_item").cast("int")).alias("views"),
        F.sum((F.col("event_name") == "add_to_cart").cast("int")).alias("carts"),
        F.sum((F.col("event_name") == "purchase").cast("int")).alias("purchases"),
        F.max_by("price", "event_timestamp_utc").alias("last_price"),
    )
)
_wg = (
    Window.partitionBy("item_id")
    .orderBy("local_date")
    .rowsBetween(Window.unboundedPreceding, 0)
)
product_asof_ga4 = (
    _daily_g.withColumn("product_pop_views", F.sum("views").over(_wg))
    .withColumn("product_pop_carts", F.sum("carts").over(_wg))
    .withColumn("product_pop_purchases", F.sum("purchases").over(_wg))
    .select(
        F.col("item_id").alias("product_id"),
        "local_date",
        "product_pop_views",
        "product_pop_carts",
        "product_pop_purchases",
        F.col("last_price").alias("product_pop_last_price"),
    )
)

# COMMAND ----------

# DBTITLE 1,Write the product as-of tables
for _frame, _table in (
    (product_asof_rees46, "features_product_asof_rees46"),
    (product_asof_ga4, "features_product_asof_ga4"),
):
    write_stored_table(
        _frame,
        _table,
        ECO,
        key_cols=["product_id", "local_date"],
        source=SOURCE,
        component=f"{COMPONENT}/{_table}",
        rid=rid,
        section="features",
    )

# COMMAND ----------

# DBTITLE 1,Session prefix features -- REES46 (prefix of two events)
_w = Window.partitionBy("session_key").orderBy("event_timestamp_utc", "event_key")
_ranked = rees46_events.withColumn("rn", F.row_number().over(_w))
_prefix = (
    _ranked.filter(F.col("rn") <= K_REES46)
    .groupBy("session_key")
    .agg(
        F.first("user_id").alias("user_id"),
        F.min("event_timestamp_utc").alias("session_start_utc"),
        F.max("event_timestamp_utc").alias("as_of_ts"),
        F.count("*").alias("prefix_event_count"),
        F.sum((F.col("event_type") == "view").cast("int")).alias("prefix_view_count"),
        F.sum((F.col("event_type") == "cart").cast("int")).alias("prefix_cart_count"),
        F.countDistinct("product_id").alias("prefix_distinct_products"),
        F.min_by("product_id", "rn").alias("first_product_id"),
        F.min_by("category_l1", "rn").alias("prefix_first_category"),
        F.min_by("brand", "rn").alias("prefix_first_brand"),
        F.min_by("price", "rn").alias("prefix_first_price"),
        F.avg("price").alias("prefix_mean_price"),
        (
            F.max("event_timestamp_utc").cast("long")
            - F.min("event_timestamp_utc").cast("long")
        ).alias("prefix_span_seconds"),
    )
)
_local = F.from_utc_timestamp("session_start_utc", PROJECT_TIMEZONE)
_pop = product_asof_rees46.select(
    F.col("product_id").alias("first_product_id"),
    F.date_add("local_date", 1).alias("session_date"),
    "product_pop_views",
    "product_pop_carts",
    "product_pop_purchases",
    "product_pop_last_price",
)
prefix_rees46 = (
    _prefix.filter(F.col("prefix_event_count") == K_REES46)
    .withColumn("session_date", F.to_date(_local))
    .withColumn("prefix_hour_of_day", F.hour(_local))
    .withColumn("prefix_day_of_week", F.dayofweek(_local))
    .join(_pop, ["first_product_id", "session_date"], "left")
    .drop("first_product_id")
)
_below = _prefix.filter(F.col("prefix_event_count") < K_REES46).count()
_prefix_rees46_block = (
    "sessions_below_prefix_length",
    markdown_table(["k", "sessions_excluded"], [(K_REES46, _below)]),
)

# COMMAND ----------

# DBTITLE 1,Session prefix features -- GA4 (prefix of ten events)
_ok = ~F.col("event_name").isin(*ADJACENT)
_wg = Window.partitionBy("session_key").orderBy("event_timestamp_utc", "event_key")
_ranked_g = ga4_events.withColumn("rn", F.row_number().over(_wg))
_prefix_g = (
    _ranked_g.filter(F.col("rn") <= K_GA4)
    .groupBy("session_key")
    .agg(
        F.first("user_pseudo_id").alias("user_pseudo_id"),
        F.min("event_timestamp_utc").alias("session_start_utc"),
        F.max("event_timestamp_utc").alias("as_of_ts"),
        F.count("*").alias("prefix_event_count"),
        F.sum((_ok & (F.col("event_name") == "page_view")).cast("int")).alias(
            "prefix_page_view_count"
        ),
        F.sum((_ok & (F.col("event_name") == "view_item")).cast("int")).alias(
            "prefix_view_item_count"
        ),
        F.sum((_ok & (F.col("event_name") == "add_to_cart")).cast("int")).alias(
            "prefix_add_to_cart_count"
        ),
        F.sum((_ok & F.col("search_term").isNotNull()).cast("int")).alias(
            "prefix_search_count"
        ),
        F.countDistinct(F.when(_ok, F.col("page_location"))).alias(
            "prefix_distinct_pages"
        ),
        F.min_by("session_number", "rn").alias("prefix_session_number"),
        F.min_by("geo_country", "rn").alias("prefix_geo_country"),
        (
            F.max("event_timestamp_utc").cast("long")
            - F.min("event_timestamp_utc").cast("long")
        ).alias("prefix_span_seconds"),
    )
)
_local_g = F.from_utc_timestamp("session_start_utc", PROJECT_TIMEZONE)
prefix_ga4 = (
    _prefix_g.filter(F.col("prefix_event_count") == K_GA4)
    .withColumn("session_date", F.to_date(_local_g))
    .withColumn("prefix_hour_of_day", F.hour(_local_g))
    .withColumn("prefix_day_of_week", F.dayofweek(_local_g))
)
_below_g = _prefix_g.filter(F.col("prefix_event_count") < K_GA4).count()
_prefix_ga4_block = (
    "sessions_below_prefix_length",
    markdown_table(
        ["k", "sessions_excluded", "sessions_total"],
        [(K_GA4, _below_g, _prefix_g.count())],
    ),
)

# COMMAND ----------

# DBTITLE 1,User history before each session -- REES46 and GA4
_o = Window.partitionBy("user_id").orderBy("session_start_utc", "session_key")
_prior = _o.rowsBetween(Window.unboundedPreceding, -1)
user_asof_rees46 = rees46_sessions.select(
    "session_key",
    "user_id",
    F.count("*").over(_prior).alias("user_hist_session_count"),
    F.coalesce(F.sum("had_purchase").over(_prior), F.lit(0)).alias(
        "user_hist_purchase_sessions"
    ),
    F.coalesce(F.sum("event_count").over(_prior), F.lit(0)).alias(
        "user_hist_event_count"
    ),
    F.coalesce(F.sum("distinct_product_count").over(_prior), F.lit(0)).alias(
        "user_hist_product_views"
    ),
    (
        (
            F.col("session_start_utc").cast("long")
            - F.lag("session_end_utc").over(_o).cast("long")
        )
        / 86400.0
    ).alias("user_hist_days_since_last_session"),
    (
        (
            F.col("session_start_utc").cast("long")
            - F.first("session_start_utc")
            .over(_o.rowsBetween(Window.unboundedPreceding, Window.currentRow))
            .cast("long")
        )
        / 86400.0
    ).alias("user_hist_days_since_first_session"),
)
_og = Window.partitionBy("user_id").orderBy("session_start_utc", "session_key")
_pg = _og.rowsBetween(Window.unboundedPreceding, -1)
user_asof_ga4 = ga4_sessions.select(
    "session_key",
    F.col("user_id").alias("user_pseudo_id"),
    F.count("*").over(_pg).alias("user_hist_session_count"),
    F.coalesce(F.sum("transaction_count").over(_pg), F.lit(0)).alias(
        "user_hist_transaction_count"
    ),
    F.coalesce(F.sum("total_revenue").over(_pg), F.lit(0.0)).alias("user_hist_revenue"),
    F.coalesce(F.sum("event_count").over(_pg), F.lit(0)).alias("user_hist_event_count"),
    (
        (
            F.col("session_start_utc").cast("long")
            - F.lag("session_end_utc").over(_og).cast("long")
        )
        / 86400.0
    ).alias("user_hist_days_since_last_session"),
)

# COMMAND ----------

# DBTITLE 1,Session purchase targets -- the label after the prefix
_exclusion = {}
for _name, _events, _k, _type_col in (
    ("rees46", rees46_events, K_REES46, "event_type"),
    ("ga4", ga4_events, K_GA4, "event_name"),
):
    _wk = Window.partitionBy("session_key").orderBy("event_timestamp_utc", "event_key")
    _r = _events.withColumn("rn", F.row_number().over(_wk))
    _candidates = _r.groupBy("session_key").agg(
        F.first("user_id").alias("user_id"),
        F.min("event_timestamp_utc").alias("session_start_utc"),
        F.count("*").alias("event_count"),
        F.max(
            ((F.col("rn") <= _k) & (F.col(_type_col) == "purchase")).cast("int")
        ).alias("purchase_in_prefix"),
        F.max(
            ((F.col("rn") > _k) & (F.col(_type_col) == "purchase")).cast("int")
        ).alias("purchase_after_prefix"),
    )
    _target = (
        _candidates.filter(
            (F.col("event_count") >= _k) & (F.col("purchase_in_prefix") == 0)
        )
        .select(
            "session_key",
            "user_id",
            F.to_date(
                F.from_utc_timestamp("session_start_utc", PROJECT_TIMEZONE)
            ).alias("session_date"),
            (F.col("purchase_after_prefix") == 1).alias("target_purchase_after_prefix"),
        )
        .withColumn("provenance_tier", F.lit("constructed"))
    )
    _exclusion[_name] = (
        _target,
        (
            "excluded_sessions",
            markdown_table(
                ["reason", "sessions"],
                [
                    (
                        "fewer_events_than_k",
                        _candidates.filter(F.col("event_count") < _k).count(),
                    ),
                    (
                        "purchase_inside_prefix",
                        _candidates.filter(
                            (F.col("event_count") >= _k)
                            & (F.col("purchase_in_prefix") == 1)
                        ).count(),
                    ),
                ],
            ),
        ),
    )
target_purchase_rees46, _exclusion_rees46 = _exclusion["rees46"]
target_purchase_ga4, _exclusion_ga4 = _exclusion["ga4"]

# COMMAND ----------

# DBTITLE 1,Lapse features and targets -- REES46 (one cutoff) and GA4 (two cutoffs)
_c = F.lit(REES46_EVALUATION_START).cast("timestamp")
_end = F.date_add(F.lit(REES46_EVALUATION_START).cast("date"), REES46_LABEL_DAYS).cast(
    "timestamp"
)
_before = rees46_sessions.filter(F.col("session_start_utc") < _c)
lapse_features_rees46 = (
    _before.groupBy("user_id")
    .agg(
        F.count("*").alias("lapse_session_count"),
        F.sum("event_count").alias("lapse_event_count"),
        F.sum("had_purchase").alias("lapse_purchase_sessions"),
        F.sum("distinct_product_count").alias("lapse_product_views"),
        F.countDistinct(F.to_date("session_start_utc")).alias("lapse_active_days"),
        ((_c.cast("long") - F.max("session_end_utc").cast("long")) / 86400.0).alias(
            "lapse_days_since_last_session"
        ),
        ((_c.cast("long") - F.min("session_start_utc").cast("long")) / 86400.0).alias(
            "lapse_days_since_first_session"
        ),
    )
    .withColumn("cutoff_date", F.lit(REES46_EVALUATION_START).cast("date"))
)
_cohort = rees46_sessions.groupBy("user_id").agg(
    F.max((F.col("session_start_utc") < _c).cast("int")).alias("in_cohort"),
    F.max(
        ((F.col("session_start_utc") >= _c) & (F.col("session_start_utc") < _end)).cast(
            "int"
        )
    ).alias("returned"),
)
target_lapse_rees46 = (
    _cohort.filter(F.col("in_cohort") == 1)
    .select(
        "user_id",
        F.lit(REES46_EVALUATION_START).cast("date").alias("cutoff_date"),
        (F.col("returned") == 1).alias("target_returned"),
        F.lit(REES46_LABEL_DAYS).alias("label_window_days"),
    )
    .withColumn("provenance_tier", F.lit("constructed"))
)
BLOCKS["lapse_rees46"] = [
    (
        "cohort",
        markdown_table(
            ["users_seen", "users_in_cohort"],
            [(_cohort.count(), target_lapse_rees46.count())],
        ),
    )
]
_data_end = ga4_sessions.agg(F.max("session_start_utc")).first()[0]
_features_g, _targets_g = [], []
for _cut in GA4_CUTOFFS:
    _cc = F.lit(_cut).cast("timestamp")
    _features_g.append(
        ga4_sessions.filter(F.col("session_start_utc") < _cc)
        .groupBy("user_id")
        .agg(
            F.count("*").alias("lapse_session_count"),
            F.sum("event_count").alias("lapse_event_count"),
            F.sum("transaction_count").alias("lapse_transaction_count"),
            F.sum("total_revenue").alias("lapse_revenue"),
            F.countDistinct(F.to_date("session_start_utc")).alias("lapse_active_days"),
            (
                (_cc.cast("long") - F.max("session_end_utc").cast("long")) / 86400.0
            ).alias("lapse_days_since_last_session"),
        )
        .withColumn("cutoff_date", F.lit(_cut).cast("date"))
    )
    _targets_g.append(
        ga4_sessions.groupBy("user_id")
        .agg(
            F.max((F.col("session_start_utc") < _cc).cast("int")).alias("in_cohort"),
            F.max((F.col("session_start_utc") >= _cc).cast("int")).alias("returned"),
        )
        .filter(F.col("in_cohort") == 1)
        .select(
            "user_id",
            F.lit(_cut).cast("date").alias("cutoff_date"),
            (F.col("returned") == 1).alias("target_returned"),
            F.datediff(F.lit(_data_end).cast("date"), F.lit(_cut).cast("date")).alias(
                "label_window_days"
            ),
        )
    )
lapse_features_ga4 = _features_g[0].unionByName(_features_g[1])
target_lapse_ga4 = (
    _targets_g[0]
    .unionByName(_targets_g[1])
    .withColumn("provenance_tier", F.lit("constructed"))
)

# COMMAND ----------

# DBTITLE 1,Cart abandonment, GA4 session revenue and retail demand
# cart abandonment: the prefix of a REES46 session ends at its first cart event
_wc = Window.partitionBy("session_key").orderBy("event_timestamp_utc", "event_key")
_ranked_cart = rees46_events.withColumn("rn", F.row_number().over(_wc))
_first_cart = (
    _ranked_cart.filter(F.col("event_type") == "cart")
    .groupBy("session_key")
    .agg(F.min("rn").alias("cart_rn"))
)
_cart_events = _ranked_cart.join(_first_cart, "session_key")
_cart_prefix = (
    _cart_events.filter(F.col("rn") <= F.col("cart_rn"))
    .groupBy("session_key")
    .agg(
        F.first("user_id").alias("user_id"),
        F.min("event_timestamp_utc").alias("session_start_utc"),
        F.max("event_timestamp_utc").alias("as_of_ts"),
        F.count("*").alias("prefix_event_count"),
        F.sum((F.col("event_type") == "view").cast("int")).alias("prefix_view_count"),
        F.countDistinct("product_id").alias("prefix_distinct_products"),
        F.max_by("product_id", "rn").alias("cart_product_id"),
        F.max_by("category_l1", "rn").alias("prefix_cart_category"),
        F.max_by("brand", "rn").alias("prefix_cart_brand"),
        F.max_by("price", "rn").alias("prefix_cart_price"),
        (
            F.max("event_timestamp_utc").cast("long")
            - F.min("event_timestamp_utc").cast("long")
        ).alias("prefix_span_seconds"),
    )
)
_local_cart = F.from_utc_timestamp("session_start_utc", PROJECT_TIMEZONE)
_cart_popularity = product_asof_rees46.select(
    F.col("product_id").alias("cart_product_id"),
    F.date_add("local_date", 1).alias("session_date"),
    "product_pop_views",
    "product_pop_carts",
    "product_pop_purchases",
    "product_pop_last_price",
)
cart_features = (
    _cart_prefix.withColumn("session_date", F.to_date(_local_cart))
    .withColumn("prefix_hour_of_day", F.hour(_local_cart))
    .withColumn("prefix_day_of_week", F.dayofweek(_local_cart))
    .join(_cart_popularity, ["cart_product_id", "session_date"], "left")
    .drop("cart_product_id")
)
_after_cart = (
    _cart_events.filter(F.col("rn") > F.col("cart_rn"))
    .groupBy("session_key")
    .agg(
        F.max((F.col("event_type") == "purchase").cast("int")).alias(
            "purchase_after_cart"
        )
    )
)
target_cart = (
    cart_features.select("session_key", "user_id", "session_date")
    .join(_after_cart, "session_key", "left")
    .select(
        "session_key",
        "user_id",
        "session_date",
        (F.coalesce(F.col("purchase_after_cart"), F.lit(0)) == 1).alias(
            "target_purchase_after_cart"
        ),
    )
    .withColumn("provenance_tier", F.lit("constructed"))
)
# revenue: GA4 sessions with a purchase and a full prefix of ten events
target_revenue = (
    ga4_sessions.filter(
        (F.col("transaction_count") > 0) & F.col("total_revenue").isNotNull()
    )
    .join(prefix_ga4.select("session_key"), "session_key", "left_semi")
    .select(
        "session_key",
        "user_id",
        F.to_date(F.from_utc_timestamp("session_start_utc", PROJECT_TIMEZONE)).alias(
            "session_date"
        ),
        F.col("total_revenue").alias("target_session_revenue"),
    )
    .withColumn("provenance_tier", F.lit("constructed"))
)
# retail demand: the monthly change of the German retail volume, features of the
# month before (31 days)
_retail = spark.table(f"{CATALOG}.commerce_analytics.retail_energy_monthly").filter(
    F.col("retail_trade_volume_index").isNotNull()
)
_months = Window.orderBy("observation_month")
_volume = F.col("retail_trade_volume_index")
target_retail = (
    _retail.select(
        "observation_month",
        (_volume - F.lag(_volume).over(_months)).alias("target_retail_volume_change"),
        (F.lag(_volume).over(_months) - F.lag(_volume, 2).over(_months)).alias(
            "retail_change_lag31d"
        ),
        F.lag("mean_price_eur_per_mwh").over(_months).alias("price_lag31d"),
        F.lag("mean_air_temperature_degc").over(_months).alias("temperature_lag31d"),
        F.lag("consumption_mwh").over(_months).alias("consumption_lag31d"),
    )
    .filter(F.col("target_retail_volume_change").isNotNull())
    .withColumn("provenance_tier", F.lit("constructed"))
)

# COMMAND ----------

# DBTITLE 1,Assemble the datasets (the join is in memory, no view)
FRAMES["session_purchase_rees46"] = assembled_frame(
    target_purchase_rees46,
    [
        (prefix_rees46, ["session_key"], ["user_id", "session_date"]),
        (user_asof_rees46, ["session_key"], ["user_id"]),
    ],
)
FRAMES["session_purchase_ga4"] = assembled_frame(
    target_purchase_ga4,
    [
        (prefix_ga4, ["session_key"], ["user_pseudo_id", "session_date"]),
        (user_asof_ga4, ["session_key"], ["user_pseudo_id"]),
    ],
)
FRAMES["lapse_rees46"] = assembled_frame(
    target_lapse_rees46, [(lapse_features_rees46, ["user_id", "cutoff_date"])]
)
FRAMES["lapse_ga4"] = target_lapse_ga4.join(
    lapse_features_ga4, ["user_id", "cutoff_date"], "inner"
)
FRAMES["cart_abandonment"] = assembled_frame(
    target_cart,
    [
        (cart_features, ["session_key"], ["user_id", "session_date"]),
        (user_asof_rees46, ["session_key"], ["user_id"]),
    ],
)
FRAMES["session_revenue"] = assembled_frame(
    target_revenue,
    [
        (prefix_ga4, ["session_key"], ["user_pseudo_id", "session_date"]),
        (user_asof_ga4, ["session_key"], ["user_pseudo_id"]),
    ],
)
FRAMES["retail_demand_monthly"] = assembled_frame(target_retail, [])
BLOCKS["session_purchase_rees46"] = [_exclusion_rees46, _prefix_rees46_block]
BLOCKS["session_purchase_ga4"] = [_exclusion_ga4, _prefix_ga4_block]

# COMMAND ----------

# DBTITLE 1,Build the datasets -- contract, split, fit and the family rows
build_datasets(FRAMES, rid=rid, source=SOURCE, blocks=BLOCKS)

# COMMAND ----------

# DBTITLE 1,Summary
for _dataset in FRAMES:
    print(
        _dataset,
        read_ml("dataset_manifest", ecosystem=ECO)
        .filter(F.col("dataset_id") == _dataset)
        .first()["status"],
    )
