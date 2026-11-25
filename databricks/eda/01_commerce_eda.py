# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- COMMERCE (GA4 AND REES46)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** exploratory analysis of the commerce data in one notebook. Each section profiles one group of
# MAGIC tables and writes its findings to `src/schemas/profiling/<source>.md` (one section key per group).

# COMMAND ----------

# MAGIC %run ./_eda_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- GA4 COMMERCE EVENTS
# MAGIC
# MAGIC **Purpose:** Profile ga4_events (product/journey/transaction events
# MAGIC already filtered/projected at staging) -- schema, missingness, grain,
# MAGIC event/field vocabulary against the staged allowlists, session-level
# MAGIC funnel, temporal coverage, entity concentration -- as evidence for
# MAGIC Silver design.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "ga4"
NB_KEY = "01_events"
SECTION_TITLE = "GA4 commerce events (ga4_events)"
TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.ga4_events"

# The allowlists staging already applied -- this notebook checks the Bronze
# table actually stayed within them, not just re-derives them.
RETAINED_EVENTS = {
    "view_item",
    "view_item_list",
    "select_item",
    "view_promotion",
    "select_promotion",
    "view_search_results",
    "add_to_cart",
    "begin_checkout",
    "add_shipping_info",
    "add_payment_info",
    "purchase",
}
RETAINED_PARAM_KEYS = {
    "ga_session_id",
    "ga_session_number",
    "page_location",
    "page_title",
    "search_term",
    "unique_search_term",
}
SCALAR_COLS = ["event_date", "event_timestamp", "event_name", "user_pseudo_id"]

# GA4's own "no value" sentinel for string fields -- a real, non-null string,
# not an empty field. Staging deliberately keeps it as-is (raw fidelity;
# cleaning is Silver's job), so a plain isNotNull() population rate on these
# fields overstates genuine content -- checked explicitly below.
UNSET_SENTINELS = ("(not set)", "(none)", "")
PROMOTION_EVENTS = {"view_promotion", "select_promotion"}

# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Schema, row count, missingness (scalar + nested-field population, one pass)
df = spark.table(TABLE)
COLS = df.columns
has_geo = "geo_country" in COLS

scalar_aggs = [F.count(F.lit(1)).alias("__rows")]
for c in SCALAR_COLS + (["geo_country"] if has_geo else []):
    scalar_aggs += [
        F.sum(F.col(c).isNull().cast("long")).alias(c + "__m"),
        F.approx_count_distinct(c).alias(c + "__d"),
    ]
nested_aggs = [
    F.sum((F.size(F.col("event_params")) > 0).cast("long")).alias("event_params__pop"),
    F.sum(F.col("ecommerce").isNotNull().cast("long")).alias("ecommerce__pop"),
    F.sum((F.size(F.col("items")) > 0).cast("long")).alias("items__pop"),
]
r = df.agg(*scalar_aggs, *nested_aggs).first().asDict()
total = r["__rows"]

missrates = {
    c: r[c + "__m"] / total for c in SCALAR_COLS + (["geo_country"] if has_geo else [])
}
approx_card = {
    c: r[c + "__d"] for c in SCALAR_COLS + (["geo_country"] if has_geo else [])
}
nested_pop = {
    "event_params": r["event_params__pop"] / total,
    "ecommerce": r["ecommerce__pop"] / total,
    "items": r["items__pop"] / total,
}
print(f"rows={total}  columns={len(COLS)}  ->  {COLS}")
for c, rate in missrates.items():
    print(f"  {c:<16} missing_rate={rate:.4f} approx_distinct={approx_card[c]}")
for c, rate in nested_pop.items():
    print(f"  {c:<16} populated_rate={rate:.4f}")
df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Grain / uniqueness -- exact, not HLL estimate
CANDIDATE_KEY = ["event_date", "event_timestamp", "user_pseudo_id", "event_name"]
grain = exact_uniqueness(df, CANDIDATE_KEY)
key_check = full_row_dup_count(
    df.select(*CANDIDATE_KEY), total=total
)  # exact duplicate-key-combination count on the candidate key alone
print("candidate key per-column:", grain)
print(f"rows sharing the full candidate key {CANDIDATE_KEY}: {key_check}")

# COMMAND ----------

# DBTITLE 1,event_name domain validation -- against the staged allowlist, not GA4 convention
event_domain = categorical_domain(df, "event_name", RETAINED_EVENTS, name="event_name")
if event_domain["unexpected_count"]:
    print(
        f"UNEXPECTED event_name value(s) present in Bronze: {event_domain['unexpected']} "
        "-- the staging filter did not hold, investigate before Silver."
    )
else:
    print("OK  every event_name in Bronze is within the retained allowlist.")
if "refund" in (event_domain["unexpected"] or []):
    print("FAIL  'refund' present in Bronze -- should never occur.")
print("unused (allowlisted but zero rows):", event_domain["unused_allowed"])

# COMMAND ----------

# DBTITLE 1,event_name funnel + daily volume (one pass)
day_type = [
    x
    for x in df.groupBy("event_date", "event_name")
    .count()
    .orderBy("event_date", "event_name")
    .collect()
    if x["event_date"] and x["event_name"]
]
days = sorted({x["event_date"] for x in day_type})
funnel = sorted(
    (
        (en, sum(x["count"] for x in day_type if x["event_name"] == en))
        for en in {x["event_name"] for x in day_type}
    ),
    key=lambda p: -p[1],
)
funnel_map = dict(funnel)
by_day = [(d, sum(x["count"] for x in day_type if x["event_date"] == d)) for d in days]
print("event_name funnel:", funnel)
print("min/max day:", days[0] if days else None, days[-1] if days else None)

# COMMAND ----------

# DBTITLE 1,event_params key vocabulary -- explode once, confirm allowlist + population
params_exploded = df.select(F.explode("event_params").alias("p")).select(
    F.col("p.key").alias("key")
)
key_counts = [
    (x["key"], x["count"])
    for x in params_exploded.groupBy("key").count().orderBy(F.desc("count")).collect()
]
observed_keys = {k for k, _ in key_counts}
unapproved_keys = observed_keys - RETAINED_PARAM_KEYS
print("event_params key population:", key_counts)
if unapproved_keys:
    print(
        f"FAIL  event_params carries key(s) outside the retained allowlist: {unapproved_keys}"
    )
else:
    print("OK  every event_params key in Bronze is within the retained allowlist.")

# COMMAND ----------

# DBTITLE 1,ecommerce struct -- field population + revenue distribution
ecom_fields = [
    "transaction_id",
    "purchase_revenue",
    "unique_items",
    "total_item_quantity",
]
ecom_aggs = [F.count(F.lit(1)).alias("__pop_rows")]
for f in ecom_fields:
    ecom_aggs.append(
        F.sum(F.col("ecommerce")[f].isNotNull().cast("long")).alias(f + "__pop")
    )
er = df.where(F.col("ecommerce").isNotNull()).agg(*ecom_aggs).first().asDict()
ecom_pop_rows = er["__pop_rows"]
ecom_field_pop = {
    f: (er[f + "__pop"] / ecom_pop_rows if ecom_pop_rows else 0.0) for f in ecom_fields
}
print(
    f"rows with a populated ecommerce struct: {ecom_pop_rows} ({ecom_pop_rows / total:.4f} of all rows)"
)
print(
    "field population within those rows (isNotNull -- includes the sentinel):",
    ecom_field_pop,
)

# transaction_id's isNotNull population above counts GA4's own "(not set)"
# sentinel as populated. Real (genuine-value) rate, computed separately:
txn_sentinel = F.col("ecommerce.transaction_id").isin(*UNSET_SENTINELS)
txn_real = df.where(
    F.col("ecommerce.transaction_id").isNotNull() & ~txn_sentinel
).count()
txn_sentinel_rows = df.where(txn_sentinel).count()
txn_real_rate = txn_real / ecom_pop_rows if ecom_pop_rows else 0.0
print(
    f"transaction_id real (non-sentinel) population: {txn_real} of {ecom_pop_rows} "
    f"ecommerce-populated rows ({txn_real_rate:.4f}) -- {txn_sentinel_rows} rows carry "
    "the '(not set)' sentinel instead of a real value."
)

revenue_stats = (
    df.where(F.col("ecommerce.purchase_revenue").isNotNull())
    .agg(
        F.min("ecommerce.purchase_revenue").alias("min"),
        F.max("ecommerce.purchase_revenue").alias("max"),
        F.avg("ecommerce.purchase_revenue").alias("avg"),
        F.percentile_approx("ecommerce.purchase_revenue", [0.5, 0.95, 0.99]).alias(
            "p50_95_99"
        ),
    )
    .first()
    .asDict()
)
print("purchase_revenue distribution:", revenue_stats)

# COMMAND ----------

# DBTITLE 1,items -- explode once, field population + top categories + price/revenue
items_exploded = df.select("event_name", F.explode("items").alias("i")).select(
    "event_name",
    F.col("i.item_id").alias("item_id"),
    F.col("i.item_name").alias("item_name"),
    F.col("i.item_category").alias("item_category"),
    F.col("i.price").alias("price"),
    F.col("i.quantity").alias("quantity"),
    F.col("i.item_revenue").alias("item_revenue"),
    F.col("event_name").isin(*PROMOTION_EVENTS).alias("is_promotion_entry"),
)
items_total = items_exploded.count()
item_field_pop = {}
for f in ["item_id", "item_name", "item_category", "price", "quantity", "item_revenue"]:
    item_field_pop[f] = (
        items_exploded.where(F.col(f).isNotNull()).count() / items_total
        if items_total
        else 0.0
    )
print(f"total item entries: {items_total}")
print(
    "item field population rate (isNotNull -- includes the sentinel):", item_field_pop
)

# item_category/item_id/item_name real (non-sentinel) population -- GA4's
# "(not set)" is a non-null string, so isNotNull() above overstates genuine
# content on these three string fields.
sentinel_pop = {}
for f in ["item_id", "item_name", "item_category"]:
    real = items_exploded.where(
        F.col(f).isNotNull() & ~F.col(f).isin(*UNSET_SENTINELS)
    ).count()
    sentinel_pop[f] = real / items_total if items_total else 0.0
print("real (non-sentinel) population rate:", sentinel_pop)

# item_id is overloaded -- GA4 reuses it for promotion/campaign identifiers
# on view_promotion / select_promotion entries, not just real products.
promo_entries = items_exploded.where(F.col("is_promotion_entry")).count()
print(
    f"item entries from promotion events (view_promotion/select_promotion): "
    f"{promo_entries} of {items_total} ({promo_entries / items_total:.4f})"
    if items_total
    else "item entries from promotion events: 0"
)

top_categories = [
    (x["item_category"], x["count"])
    for x in items_exploded.groupBy("item_category")
    .count()
    .orderBy(F.desc("count"))
    .limit(20)
    .collect()
    if x["item_category"]
]
print("top item_category:", top_categories)

price_p99 = (
    items_exploded.where(F.col("price").isNotNull())
    .agg(F.percentile_approx("price", 0.99).alias("p99"))
    .first()["p99"]
)
qty_rev_stats = (
    items_exploded.agg(
        F.percentile_approx("quantity", [0.5, 0.9, 0.99]).alias("qty_p50_90_99"),
        F.percentile_approx("item_revenue", [0.5, 0.9, 0.99]).alias("rev_p50_90_99"),
    )
    .first()
    .asDict()
)
print("quantity / item_revenue distribution:", qty_rev_stats)

# COMMAND ----------


# DBTITLE 1,Session-level funnel -- (user_pseudo_id, ga_session_id) grain


def _param_value(col_name, key):
    matched = F.filter(F.col(col_name), lambda x: x["key"] == F.lit(key))
    v = F.element_at(matched, 1)["value"]
    return F.coalesce(
        v["int_value"].cast("string"),
        v["string_value"],
        v["float_value"].cast("string"),
        v["double_value"].cast("string"),
    )


sessions = df.select(
    "user_pseudo_id",
    _param_value("event_params", "ga_session_id").alias("ga_session_id"),
    "event_name",
)
session_roll = sessions.groupBy("user_pseudo_id", "ga_session_id").agg(
    F.count(F.lit(1)).alias("events"),
    F.max((F.col("event_name") == "view_item").cast("int")).alias("has_view"),
    F.max((F.col("event_name") == "view_search_results").cast("int")).alias(
        "has_search"
    ),
    F.max((F.col("event_name") == "add_to_cart").cast("int")).alias("has_cart"),
    F.max((F.col("event_name") == "begin_checkout").cast("int")).alias("has_checkout"),
    F.max((F.col("event_name") == "purchase").cast("int")).alias("has_purchase"),
)
sc = (
    session_roll.agg(
        F.count(F.lit(1)).alias("sessions"),
        F.expr("approx_percentile(events, array(0.5, 0.9, 0.99))").alias(
            "events_p50_90_99"
        ),
        F.max("events").alias("max_events"),
        F.sum("has_view").alias("with_view"),
        F.sum("has_search").alias("with_search"),
        F.sum("has_cart").alias("with_cart"),
        F.sum("has_checkout").alias("with_checkout"),
        F.sum("has_purchase").alias("with_purchase"),
        F.sum(
            (
                F.col("has_cart").cast("boolean")
                & F.col("has_purchase").cast("boolean")
            ).cast("int")
        ).alias("cart_and_purchase"),
    )
    .first()
    .asDict()
)
print(sc)
if sc["with_cart"]:
    print(
        f"cart->purchase session rate = {sc['cart_and_purchase'] / sc['with_cart']:.4f}"
    )
session_funnel = [
    ("sessions", sc["sessions"]),
    ("with view", sc["with_view"]),
    ("with search", sc["with_search"]),
    ("with cart", sc["with_cart"]),
    ("with checkout", sc["with_checkout"]),
    ("with purchase", sc["with_purchase"]),
]

# COMMAND ----------

# DBTITLE 1,Entity cardinality + concentration
# item_id concentration is split product vs promotion entries -- GA4 reuses
# item_id for promotion/campaign identifiers on view_promotion/
# select_promotion, so a single combined figure conflates two different kinds
# of entity and overstates "one product" concentration.
user_roll = df.groupBy("user_pseudo_id").agg(F.count(F.lit(1)).alias("rows"))
product_entries = items_exploded.where(~F.col("is_promotion_entry"))
promo_item_entries = items_exploded.where(F.col("is_promotion_entry"))
product_item_roll = product_entries.groupBy("item_id").agg(
    F.count(F.lit(1)).alias("rows")
)
promo_item_roll = promo_item_entries.groupBy("item_id").agg(
    F.count(F.lit(1)).alias("rows")
)
product_entries_total = product_entries.count()

concentration = {}
for name, roll, base_total in (
    ("user_pseudo_id", user_roll, total),
    ("item_id (product events only)", product_item_roll, product_entries_total),
    ("item_id (promotion events only)", promo_item_roll, promo_entries),
):
    top = [x["rows"] for x in roll.orderBy(F.desc("rows")).limit(50).collect()]
    concentration[name] = {
        "approx_distinct": roll.select(
            F.approx_count_distinct(F.col(roll.columns[0]))
        ).first()[0],
        "top10_share": sum(top[:10]) / base_total if base_total else 0.0,
        "top50_share": sum(top) / base_total if base_total else 0.0,
        "max_rows_one_entity": top[0] if top else 0,
    }
    print(f"{name}: {concentration[name]}")

# COMMAND ----------

# DBTITLE 1,Duplicate key -- identical vs conflicting (candidate key, scalar columns only)
db = dup_key_composition(df.select(*SCALAR_COLS), CANDIDATE_KEY, hash_cols=SCALAR_COLS)
print(
    f"duplicate {CANDIDATE_KEY} groups={db['dup_groups']}  identical={db['identical']}  "
    f"conflicting={db['conflicting']}"
)

# COMMAND ----------

# DBTITLE 1,Geography -- events, users, purchases and revenue by country
geo_dist = None
if has_geo:
    rows = (
        df.groupBy("geo_country")
        .agg(
            F.count(F.lit(1)).alias("events"),
            F.approx_count_distinct("user_pseudo_id").alias("users"),
            F.sum((F.col("event_name") == "view_item").cast("long")).alias(
                "item_views"
            ),
            F.sum((F.col("event_name") == "purchase").cast("long")).alias("purchases"),
            F.sum(
                F.when(
                    F.col("event_name") == "purchase",
                    F.col("ecommerce.purchase_revenue").cast("double"),
                )
            ).alias("revenue"),
        )
        .orderBy(F.desc("events"))
        .collect()
    )
    ev_total = sum(x["events"] for x in rows)
    geo_dist = {
        "countries": len(rows),
        "top1_share": round(rows[0]["events"] / ev_total, 4) if rows else None,
        "top5_share": round(sum(x["events"] for x in rows[:5]) / ev_total, 4)
        if rows
        else None,
        "not_set_events": sum(
            x["events"]
            for x in rows
            if x["geo_country"] in UNSET_SENTINELS or x["geo_country"] is None
        ),
        "top": [
            {
                "country": x["geo_country"],
                "events": x["events"],
                "users": x["users"],
                "events_per_user": round(x["events"] / x["users"], 1)
                if x["users"]
                else None,
                "purchases": x["purchases"],
                "purchases_per_1000_item_views": round(
                    1000 * x["purchases"] / x["item_views"], 2
                )
                if x["item_views"]
                else None,
                "revenue": round(x["revenue"], 0) if x["revenue"] is not None else None,
            }
            for x in rows[:15]
        ],
        "countries_with_a_purchase": sum(1 for x in rows if x["purchases"]),
    }
    print(geo_dist["countries"], geo_dist["top1_share"], geo_dist["top"][:3])

# COMMAND ----------

# DBTITLE 1,Temporal patterns -- weekday, week, hour of day and busiest days
import datetime as _dtm


def _ymd(value):
    t = str(value)
    return _dtm.date(int(t[:4]), int(t[4:6]), int(t[6:8]))


weekday_events, weekday_purchases, weekday_days = {}, {}, {}
week_events = {}
for d, n in by_day:
    dd = _ymd(d)
    wd = dd.weekday()
    weekday_events[wd] = weekday_events.get(wd, 0) + n
    weekday_days[wd] = weekday_days.get(wd, 0) + 1
    iso = dd.isocalendar()
    key = f"{iso[0]}-W{iso[1]:02d}"
    week_events[key] = week_events.get(key, 0) + n
purch_by_day = {}
for x in day_type:
    if x["event_name"] == "purchase":
        purch_by_day[x["event_date"]] = x["count"]
        wd = _ymd(x["event_date"]).weekday()
        weekday_purchases[wd] = weekday_purchases.get(wd, 0) + x["count"]
temporal_patterns = {
    "mean_events_per_weekday": {
        wd: round(weekday_events[wd] / weekday_days[wd])
        for wd in sorted(weekday_events)
    },
    "purchases_by_weekday": dict(sorted(weekday_purchases.items())),
    "weekly_events": sorted(week_events.items()),
    "busiest_days_events": sorted(by_day, key=lambda p: -p[1])[:8],
    "busiest_days_purchases": sorted(purch_by_day.items(), key=lambda p: -p[1])[:8],
}
hour_rows = (
    df.select(
        F.hour(
            (F.col("event_timestamp").cast("long") / 1000000).cast("timestamp")
        ).alias("h"),
        "event_name",
    )
    .groupBy("h", "event_name")
    .count()
    .collect()
)
hour_all, hour_purchase, hour_view = {}, {}, {}
for x in hour_rows:
    hour_all[x["h"]] = hour_all.get(x["h"], 0) + x["count"]
    if x["event_name"] == "purchase":
        hour_purchase[x["h"]] = x["count"]
    if x["event_name"] == "view_item":
        hour_view[x["h"]] = x["count"]
temporal_patterns["hour_events"] = sorted(hour_all.items())
temporal_patterns["hour_purchases"] = sorted(hour_purchase.items())
print(
    {k: (v if k != "weekly_events" else len(v)) for k, v in temporal_patterns.items()}
)

# COMMAND ----------

# DBTITLE 1,Purchases and revenue -- transactions, duplicates and revenue against item revenue
purch = df.where(F.col("event_name") == "purchase").select(
    F.col("ecommerce.transaction_id").cast("string").alias("tx"),
    F.col("ecommerce.purchase_revenue").cast("double").alias("rev"),
    "event_date",
    F.aggregate(
        "items",
        F.lit(0.0),
        lambda acc, x: acc + F.coalesce(x["item_revenue"].cast("double"), F.lit(0.0)),
    ).alias("item_sum"),
    F.size("items").alias("n_items"),
)
real_tx = F.col("tx").isNotNull() & ~F.col("tx").isin(*UNSET_SENTINELS)
tx_stats = (
    purch.agg(
        F.count(F.lit(1)).alias("purchase_events"),
        F.sum(real_tx.cast("long")).alias("with_real_transaction_id"),
        F.countDistinct(F.when(real_tx, F.col("tx"))).alias("distinct_transactions"),
        F.sum(F.col("rev").isNotNull().cast("long")).alias("with_revenue"),
        F.sum((F.col("rev") > 0).cast("long")).alias("positive_revenue"),
        F.sum(
            F.col("rev").isNotNull().cast("long") * (F.col("item_sum") > 0).cast("long")
        ).alias("with_revenue_and_item_revenue"),
        F.sum(
            (
                F.col("rev").isNotNull()
                & (F.col("item_sum") > 0)
                & (F.abs(F.col("rev") - F.col("item_sum")) <= 0.01 * F.col("rev"))
            ).cast("long")
        ).alias("revenue_matches_item_sum_within_1pct"),
        F.avg("n_items").alias("mean_items_per_purchase_event"),
        F.sum(F.col("rev")).alias("total_revenue"),
    )
    .first()
    .asDict()
)
rev_by_day = sorted(
    [
        (x["event_date"], round(x["r"], 0))
        for x in purch.where(F.col("rev").isNotNull())
        .groupBy("event_date")
        .agg(F.sum("rev").alias("r"))
        .collect()
        if x["r"] is not None
    ],
    key=lambda p: -p[1],
)[:8]
print(tx_stats, rev_by_day)

# COMMAND ----------

# DBTITLE 1,Item categories -- top-level groups, price and revenue
cat_items = items_exploded.where(
    F.col("item_category").isNotNull() & ~F.col("item_category").isin(*UNSET_SENTINELS)
).withColumn("top", F.split(F.col("item_category"), "/").getItem(0))
category_groups = [
    {
        "group": x["top"],
        "entries": x["entries"],
        "items": x["items"],
        "median_price": x["p50"],
        "revenue": x["revenue"],
    }
    for x in cat_items.groupBy("top")
    .agg(
        F.count(F.lit(1)).alias("entries"),
        F.approx_count_distinct("item_id").alias("items"),
        F.percentile_approx(F.col("price").cast("double"), 0.5).alias("p50"),
        F.sum(F.col("item_revenue").cast("double")).alias("revenue"),
    )
    .orderBy(F.desc("entries"))
    .limit(12)
    .collect()
]
distinct_categories = cat_items.select("item_category").distinct().count()
print(distinct_categories, category_groups[:3])

# COMMAND ----------

# DBTITLE 1,Figures (each gated so a blank result is never referenced)
figs = []
_specs = [
    (
        barplot,
        (funnel, "GA4 -- event_name funnel", "event_name", "events"),
        {"logy": True, "rot": 30, "filename": "ga4_funnel.png"},
        ("GA4 event_name funnel", "ga4_funnel.png"),
    ),
    (
        barplot,
        (
            session_funnel,
            "GA4 -- sessions reaching each funnel stage",
            "stage",
            "sessions",
        ),
        {"filename": "ga4_session_funnel.png"},
        ("GA4 sessions reaching each funnel stage", "ga4_session_funnel.png"),
    ),
    (
        lineplot,
        (by_day, "GA4 -- events per day", "day", "events"),
        {"filename": "ga4_events_per_day.png"},
        ("GA4 events per day", "ga4_events_per_day.png"),
    ),
    (
        barplot,
        (
            top_categories,
            "GA4 -- top 20 item_category by event volume",
            "item_category",
            "events",
        ),
        {"rot": 90, "filename": "ga4_top_categories.png"},
        ("GA4 top item_category by event volume", "ga4_top_categories.png"),
    ),
]
for fn, args, kw, ref in _specs:
    if fn(*args, **kw):
        figs.append(ref)

if price_p99:
    price_sample = (
        items_exploded.where(F.col("price").isNotNull() & (F.col("price") <= price_p99))
        .select("price")
        .sample(0.2, seed=42)
        .limit(100_000)
        .toPandas()["price"]
        .tolist()
    )
    if histplot(
        price_sample,
        f"GA4 item price -- distribution (sampled, <=p99={price_p99})",
        "price",
        filename="ga4_price_distribution.png",
    ):
        figs.append(
            ("GA4 item price distribution (sampled)", "ga4_price_distribution.png")
        )

# COMMAND ----------

# DBTITLE 1,Findings
print("event funnel:", funnel)
print(
    "session funnel:",
    {
        k: sc[k]
        for k in (
            "with_view",
            "with_search",
            "with_cart",
            "with_checkout",
            "with_purchase",
        )
    },
)
print("concentration:", concentration)
print(
    "candidate key duplicate groups:",
    db["dup_groups"],
    " identical:",
    db["identical"],
    " conflicting:",
    db["conflicting"],
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/ga4.md
_prof = ["| column | missing rate | approx_distinct |", "|---|---|---|"]
for c in SCALAR_COLS + (["geo_country"] if has_geo else []):
    _prof.append(f"| {c} | {missrates[c]:.4f} | {approx_card[c]} |")
_nested_pop_line = (
    f"Nested field population: event_params={nested_pop['event_params']:.4f}, "
    f"ecommerce={nested_pop['ecommerce']:.4f}, items={nested_pop['items']:.4f}."
)
_prof += [
    "",
    _nested_pop_line,
    f"Rows: {total}.",
]

_dq = [
    para(
        f"Candidate key {CANDIDATE_KEY}: distinct/row ratio",
        str({c: grain[c]["ratio"] for c in grain}),
        f"-- rows sharing the full key combination: {key_check}.",
    ),
    f"Duplicate key groups: {db['dup_groups']} (identical={db['identical']}, conflicting={db['conflicting']}).",
    "",
    "ecommerce field population (of rows with a populated ecommerce struct, isNotNull -- includes the sentinel):",
    str(ecom_field_pop),
    para(
        f"transaction_id real (non-sentinel) population: {txn_real} of {ecom_pop_rows}",
        f"({txn_real_rate:.4f}); {txn_sentinel_rows} rows carry '(not set)' instead of a real value.",
    ),
    "",
    "items field population (of exploded item entries, isNotNull -- includes the sentinel):",
    str(item_field_pop),
    para(
        "real (non-sentinel) population for item_id/item_name/item_category:",
        str(sentinel_pop),
    ),
    (
        para(
            f"item entries from promotion events (view_promotion/select_promotion): {promo_entries}",
            f"of {items_total} ({promo_entries / items_total:.4f}).",
        )
        if items_total
        else "item entries from promotion events: 0."
    ),
]

_domain = [
    para(
        "event_name vs the staged allowlist:",
        f"unexpected={event_domain['unexpected'] or 'none'},",
        f"unused={event_domain['unused_allowed'] or 'none'}.",
    ),
    para(
        "event_params keys vs the staged allowlist:",
        f"unapproved={sorted(unapproved_keys) or 'none'}.",
    ),
]
if event_domain["unexpected_count"] or unapproved_keys:
    _domain.append(
        "-> a value outside the staging allowlist is a STAGING/loading defect, not a source finding."
    )

_coverage = [
    para(
        f"{total} events over {days[0] if days else 'n/a'}..{days[-1] if days else 'n/a'}.",
        f"Funnel: {funnel} -- purchases are ~{funnel_map.get('purchase', 0) / total:.2%} of retained events.",
    ),
    para(
        "This table is already the staged, filtered subset -- browsing/engagement events",
        "(page_view, user_engagement, scroll, session_start, first_visit, click) were excluded",
        "before Bronze; this profile evidences the retained events only, not the raw GA4 export.",
    ),
]

_entities = [
    para(
        f"Approx distinct: user_pseudo_id={approx_card.get('user_pseudo_id')},",
        f"item_id (product)≈{concentration['item_id (product events only)']['approx_distinct']},",
        f"item_id (promotion)≈{concentration['item_id (promotion events only)']['approx_distinct']}.",
    ),
    "",
    "Concentration:",
]
for c, v in concentration.items():
    _entities.append(
        f"- {c}: approx_distinct={v['approx_distinct']}, top10={v['top10_share']:.4f}, "
        f"top50={v['top50_share']:.4f}, max_one_entity={v['max_rows_one_entity']}"
    )

_dist = [
    f"purchase_revenue distribution: {revenue_stats}.",
    f"item quantity / item_revenue (p50/90/99): {qty_rev_stats}.",
    f"Top item_category: {top_categories[:10]}.",
]

_rel = [
    f"Session funnel (grain: user_pseudo_id, ga_session_id): {session_funnel}.",
]
if sc["with_cart"]:
    _rel.append(
        f"cart->purchase session rate = {sc['cart_and_purchase'] / sc['with_cart']:.4f}."
    )

_geo = []
if geo_dist:
    _geo.append(
        f"`geo_country`: {geo_dist['countries']} values; the largest holds {geo_dist['top1_share']} of the events, the five largest {geo_dist['top5_share']}; "
        f"events with an unset country {geo_dist['not_set_events']}; countries with at least one purchase event {geo_dist['countries_with_a_purchase']}."
    )
    _geo.append(
        "Largest countries (events, users, events per user, purchase events, purchases per 1000 item views, purchase revenue):"
    )
    for x in geo_dist["top"]:
        _geo.append(
            f"- {x['country']}: {x['events']}, {x['users']}, {x['events_per_user']}, {x['purchases']}, {x['purchases_per_1000_item_views']}, {x['revenue']}"
        )
else:
    _geo.append("- No geo_country column in this table.")

_tp = [
    f"Mean events per calendar day by weekday (0 = Monday): {temporal_patterns['mean_events_per_weekday']}.",
    f"Purchase events by weekday: {temporal_patterns['purchases_by_weekday']}.",
    f"Events per ISO week: {temporal_patterns['weekly_events']}.",
    f"Busiest days by events (day, events): {temporal_patterns['busiest_days_events']}.",
    f"Busiest days by purchase events (day, purchases): {temporal_patterns['busiest_days_purchases']}.",
    f"Events by hour of day, UTC (hour, events): {temporal_patterns['hour_events']}.",
    f"Purchase events by hour of day, UTC (hour, purchases): {temporal_patterns['hour_purchases']}.",
    f"Highest purchase-revenue days (day, revenue): {rev_by_day}.",
]

_txn = [
    para(
        f"Purchase events {tx_stats['purchase_events']}: {tx_stats['with_real_transaction_id']} carry a real transaction id, {tx_stats['distinct_transactions']} distinct transactions",
        f"({tx_stats['with_real_transaction_id'] - tx_stats['distinct_transactions']} repeated ids); {tx_stats['with_revenue']} carry a revenue value ({tx_stats['positive_revenue']} above zero), total {tx_stats['total_revenue']}.",
    ),
    f"Purchase revenue against the sum of item revenue of the same event: {tx_stats['revenue_matches_item_sum_within_1pct']} of {tx_stats['with_revenue_and_item_revenue']} events with both agree within 1%; mean items per purchase event {tx_stats['mean_items_per_purchase_event']}.",
    f"Item categories: {distinct_categories} distinct values; top-level groups (group, entries, distinct items, median price, item revenue): {category_groups}.",
    "Category paths use '/' as a separator, and some labels contain '/' themselves, so the first segment is a group label, not a strict hierarchy level.",
]

_areas = {
    "Domain understanding": [
        "retail web-shop events (item views, promotions, search, cart, checkout, purchase) for one store, with user, session, item and transaction fields",
        f"events by type: {funnel}",
        f"top-level item groups: {[(g['group'], g['entries']) for g in category_groups[:5]]}",
    ],
    "Structure and engineering": [
        "one Bronze table with nested arrays (event_params, items) and a struct (ecommerce); already filtered to a retained event set",
        f"GA4's '(not set)' string is kept as a value: transaction id real in {tx_stats['with_real_transaction_id']} of {tx_stats['purchase_events']} purchase events",
        f"scalar key duplicates: {db['dup_groups']}",
    ],
    "Temporal": [
        f"{days[0] if days else None} .. {days[-1] if days else None} ({len(days)} days)",
        f"weekday means {temporal_patterns['mean_events_per_weekday']}; busiest days {temporal_patterns['busiest_days_events'][:3]}",
        f"purchase hours {temporal_patterns['hour_purchases'][:6]} ...",
    ],
    "Spatial": [
        f"country only: {geo_dist['countries'] if geo_dist else 'n/a'} values, top 5 hold {geo_dist['top5_share'] if geo_dist else 'n/a'}; purchases in {geo_dist['countries_with_a_purchase'] if geo_dist else 'n/a'} countries",
    ],
    "Data quality": [
        f"repeated transaction ids among purchase events: {tx_stats['with_real_transaction_id'] - tx_stats['distinct_transactions']}",
        f"item id/name/category real population: {sentinel_pop}",
        f"item entries from promotion events: {promo_entries} of {items_total}",
    ],
    "Statistical patterns": [
        f"user concentration: {concentration['user_pseudo_id']}",
        f"price / revenue / quantity percentiles: {qty_rev_stats}",
    ],
    "Relationships": [
        f"session funnel: {session_funnel}",
        f"purchase revenue vs item revenue agreement: {tx_stats['revenue_matches_item_sum_within_1pct']} of {tx_stats['with_revenue_and_item_revenue']}",
    ],
    "Analytics use": [
        "funnel, product, category, country and time analysis of shop behaviour and revenue"
    ],
    "ML use": [
        "session-level conversion is a natural label (purchase in session); the sample window is one holiday season"
    ],
    "AI / knowledge use": [
        f"item names and category paths are a small product taxonomy ({distinct_categories} categories); search terms exist as an event parameter",
    ],
}

_findings = []
if key_check:
    _findings.append(
        f"- {key_check} rows share the candidate key {CANDIDATE_KEY} -- it is not a clean row identifier as-is."
    )
if event_domain["unexpected_count"]:
    _findings.append(
        f"- Unexpected event_name value(s) in Bronze: {event_domain['unexpected']}."
    )
if unapproved_keys:
    _findings.append(
        f"- Unapproved event_params key(s) in Bronze: {sorted(unapproved_keys)}."
    )
if db["conflicting"]:
    _findings.append(
        f"- {db['conflicting']} duplicate-key groups have conflicting non-key values."
    )
_findings.append(
    f"- Staging allowlists held cleanly in Bronze (event_name, event_params keys), but "
    f"GA4's own '(not set)' sentinel is not a null and passes through as-is (deliberate -- "
    f"raw fidelity at Bronze, cleaning is Silver's job): item_category real population "
    f"{sentinel_pop['item_category']:.1%} vs {item_field_pop['item_category']:.1%} isNotNull; "
    f"transaction_id real population {txn_real_rate:.1%} vs {ecom_field_pop['transaction_id']:.1%} isNotNull."
)
if promo_entries:
    _findings.append(
        f"- item_id is overloaded: {promo_entries} of {items_total} item entries "
        f"({promo_entries / items_total:.1%}) come from promotion events "
        "(view_promotion/select_promotion) and carry a campaign identifier, not a product id -- "
        "combined item_id concentration figures would conflate the two."
    )
_findings_md = "\n".join(_findings)

_silver = [
    "- Grain = one row per event; if the candidate key is not unique, add a synthetic row id at Silver.",
    "- Session grain is (user_pseudo_id, ga_session_id), not user_pseudo_id alone.",
    "- ecommerce/items are sparse (populated only on transaction-relevant events) -- keep as nullable structs, not flattened columns with a default.",
    "- Normalise GA4's '(not set)'/'(none)' sentinel to null on item_id/item_name/item_category/ecommerce.transaction_id at Silver -- Bronze intentionally keeps it raw.",
    "- Model item_id from promotion events (view_promotion/select_promotion) separately from product item_id -- they are different entity types sharing one field.",
]

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            para(
                "One row per event; modelling grain for conversion is",
                "(user_pseudo_id, ga_session_id), matched against the session-funnel evidence above.",
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            "Single Bronze table -- no join here. items is 1:N per event (multiple line items per event), already exploded for profiling, not for Silver grain.",
        ),
        (
            "Target contamination",
            para(
                "Target: purchase conversion at session grain",
                f"({sc['with_purchase']} of {sc['sessions']} sessions).",
                "The purchase event itself, and any session flag computed over the whole session, must be excluded from features predicting it.",
            ),
        ),
        (
            "Temporal / post-event leakage",
            "has_cart/has_checkout/has_purchase are computed over the WHOLE session regardless of order -- recompute from events strictly before the prediction point for a mid-session model.",
        ),
        (
            "Proxy leakage",
            "add_to_cart is a near-perfect proxy for an imminent purchase within the session.",
        ),
        (
            "Split / entity leakage",
            f"Split by user_pseudo_id ({approx_card.get('user_pseudo_id')} users), not by row or session.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            "item_category/item_name are read at event time from GA4's own export -- no external catalog join in this Bronze table to misalign.",
        ),
        (
            "Survivorship / coverage bias",
            f"Only events on days {days[0] if days else 'n/a'}..{days[-1] if days else 'n/a'} appear -- this is a fixed historical sample, not a live feed; nothing outside that window is observable.",
        ),
        (
            "Missingness leakage",
            f"ecommerce/items populated only on {nested_pop['ecommerce']:.1%}/{nested_pop['items']:.1%} of rows -- an 'is-transaction-event' flag is largely redundant with event_name itself.",
        ),
        (
            "Duplicate-event leakage",
            f"Candidate-key duplicate groups: {db['dup_groups']} (identical={db['identical']}, conflicting={db['conflicting']}) -- resolve before counting events or splitting.",
        ),
        (
            "Target / feature temporal misalignment",
            "event_timestamp is microsecond-precision -- ordering within a session is well-defined, unlike REES46's second-precision timestamps.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            "purchase_revenue is transaction-level, item_revenue is line-item-level -- do not sum item_revenue and compare to purchase_revenue without checking they use the same currency convention.",
        ),
        (
            "Data-generation-process leakage",
            "This is Google's own public sample export, not a live property -- traffic patterns reflect the sample's own collection process, not a real production funnel.",
        ),
        (
            "Class / label instability",
            f"Funnel is skewed ({funnel}) -- purchase is ~{funnel_map.get('purchase', 0) / total:.2%} of retained events; do not evaluate a conversion classifier with plain accuracy.",
        ),
        (
            "Label availability lag",
            "A purchase is logged at the event; no payment/fulfillment lag is represented in this sample.",
        ),
        (
            "Source / version / regime change",
            "Single fixed 92-day window -- no cross-period regime comparison is possible from this table alone.",
        ),
        (
            "Sample-vs-full divergence",
            "The price figure is a 20%-sampled, p99-clipped subset of exploded item entries; use the full-table field-population/funnel aggregates above for any threshold decision.",
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    [
        ("Profile", "\n".join(_prof)),
        ("Data Quality", "\n".join(_dq)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Distributions", "\n".join(_dist)),
        ("Relationships", "\n".join(_rel)),
        ("Geography", "\n".join(_geo)),
        ("Temporal Patterns", "\n".join(_tp)),
        ("Purchases and Revenue", "\n".join(_txn)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Observations by Area", area_block(_areas)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- REES46 ECOMMERCE EVENTS
# MAGIC
# MAGIC **Purpose:** Profile rees46_events (~110M view / cart / purchase
# MAGIC events, Oct+Nov 2019) -- schema, missingness, constant columns,
# MAGIC event_type funnel, session-level view->cart->purchase conversion,
# MAGIC repeat behaviour, temporal coverage & activity (hour-of-day,
# MAGIC day-of-week), product / user / session cardinality & concentration,
# MAGIC price distributions & anomalies, in-session event-sequence
# MAGIC consistency, category/brand stability, duplicate-key (identical vs
# MAGIC conflicting) -- as evidence for Silver design.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "rees46"
NB_KEY = "01_events"
SECTION_TITLE = "REES46 ecommerce events (rees46_events)"
TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.rees46_events"
# REES46's event_time is "YYYY-MM-DD HH:MM:SS UTC"; substring(1,19) is the
# parseable ISO prefix. The dataset is a two-month snapshot, Oct+Nov 2019.
TS_TZ = "UTC"

# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()

print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Schema, row count, missingness, approx distinct, constant columns (one pass)
df = spark.table(TABLE)
COLS = df.columns
ts = F.try_to_timestamp(F.substring("event_time", 1, 19))
price = safe_num("price")

P = profile_frame(df)
total = P["total"]
missrates = {c: P["miss"][c] / total for c in COLS}
approx_card = P["acd"]
constant_cols = constant_column_names(df, P)
print(f"rows={total}  columns={len(COLS)}  ->  {COLS}")
for c in COLS:
    print(
        f"  {c:<16} missing={P['miss'][c]:>14} "
        f"rate={missrates[c]:.4f} approx_distinct={approx_card[c]}"
    )
print("constant columns:", constant_cols)
print(
    "cardinality is approximate (approx_count_distinct); exact where a check needs it below"
)
df.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Daily x event_type rollup -- funnel, per-day, per-month, min/max all derived
day_type = [
    x
    for x in df.groupBy(F.substring("event_time", 1, 10).alias("day"), "event_type")
    .count()
    .orderBy("day", "event_type")
    .collect()
    # drop rows whose event_time did not parse to a day / has no event_type --
    # a None key would break every sorted() / slice below
    if x["day"] and x["event_type"]
]
days = sorted({x["day"] for x in day_type})
event_types = sorted({x["event_type"] for x in day_type})
funnel = [
    (et, sum(x["count"] for x in day_type if x["event_type"] == et))
    for et in event_types
]
funnel.sort(key=lambda p: -p[1])
funnel_map = dict(funnel)
by_day = [(d, sum(x["count"] for x in day_type if x["day"] == d)) for d in days]
by_month = {}
for d, n in by_day:
    by_month[d[:7]] = by_month.get(d[:7], 0) + n
print("event_type funnel:", funnel)
print("min/max day:", days[0], days[-1])
print("per month:", sorted(by_month.items()))

# COMMAND ----------

# DBTITLE 1,price + category/brand completeness by event_type (one grouped pass)
by_type = (
    df.groupBy("event_type")
    .agg(
        F.min(price).alias("price_min"),
        F.max(price).alias("price_max"),
        F.avg(price).alias("price_avg"),
        F.percentile_approx(price, [0.5, 0.95, 0.99]).alias("price_p50_95_99"),
        F.sum(
            F.when(
                F.col("price").isNull() | (F.trim(F.col("price")) == ""), 1
            ).otherwise(0)
        ).alias("price_missing"),
        F.sum(
            F.when(
                F.col("price").isNotNull()
                & (F.trim(F.col("price")) != "")
                & price.isNull(),
                1,
            ).otherwise(0)
        ).alias("price_non_numeric"),
        F.sum(F.when(price <= 0, 1).otherwise(0)).alias("price_le_zero"),
        F.sum(F.when(price < 0, 1).otherwise(0)).alias("price_negative"),
        F.sum(
            F.when(
                F.col("category_code").isNull()
                | (F.trim(F.col("category_code")) == ""),
                1,
            ).otherwise(0)
        ).alias("category_code_missing"),
        F.sum(
            F.when(
                F.col("brand").isNull() | (F.trim(F.col("brand")) == ""), 1
            ).otherwise(0)
        ).alias("brand_missing"),
        F.count(F.lit(1)).alias("rows"),
    )
    .collect()
)
by_type_map = {x["event_type"]: x.asDict() for x in by_type}
for et, x in by_type_map.items():
    print(et, x)
price_p99 = max(x["price_p50_95_99"][2] for x in by_type if x["price_p50_95_99"])
zero_purch = by_type_map.get("purchase", {}).get("price_le_zero", 0)
neg_price = sum(x["price_negative"] for x in by_type)
print(
    f"overall p99 price={price_p99}  zero-price purchases={zero_purch}  negative price rows={neg_price}"
)

# COMMAND ----------

# DBTITLE 1,Top brands + category_code roll-up (one groupBy each; feeds top-N and the code<->id check)
top_brands = [
    (x["brand"], x["count"])
    for x in df.groupBy("brand").count().orderBy(F.desc("count")).limit(20).collect()
]
# One groupBy(category_code) -> both the top-20 and the code->id consistency
# check (was two full scans). category_code is a small catalog taxonomy.
_ccr = (
    df.groupBy("category_code")
    .agg(
        F.count(F.lit(1)).alias("cnt"),
        F.approx_count_distinct("category_id").alias("ids"),
    )
    .collect()
)
top_cats = sorted(((x["category_code"], x["cnt"]) for x in _ccr), key=lambda p: -p[1])[
    :20
]
cc2 = sum(
    1
    for x in _ccr
    if x["category_code"] and str(x["category_code"]).strip() and (x["ids"] or 0) > 1
)
print("top brands:", top_brands)
print("top category_code:", top_cats)

# COMMAND ----------

# DBTITLE 1,product_id stability + category_id <-> category_code consistency
prod_roll = df.groupBy("product_id").agg(
    F.count(F.lit(1)).alias("rows"),
    F.approx_count_distinct("category_id").alias("d_cat_id"),
    F.approx_count_distinct("brand").alias("d_brand"),
)
ps = prod_roll.agg(
    F.sum((F.col("d_cat_id") > 1).cast("long")).alias("multi_cat"),
    F.sum((F.col("d_brand") > 1).cast("long")).alias("multi_brand"),
).first()
n_multi_cat, n_multi_brand = ps["multi_cat"], ps["multi_brand"]
print(
    f"products with >1 category_id: {n_multi_cat}   products with >1 brand: {n_multi_brand}"
)

cc = (
    df.groupBy("category_id")
    .agg(F.approx_count_distinct("category_code").alias("codes"))
    .agg(F.sum((F.col("codes") > 1).cast("long")))
    .first()[0]
)
print(
    f"category_ids mapping to >1 category_code: {cc}   category_codes mapping to >1 category_id: {cc2}"
)

# COMMAND ----------

# DBTITLE 1,Session rollup -- events, users, funnel flags, big/burst flags (one pass)
session_roll = df.groupBy("user_session").agg(
    F.count(F.lit(1)).alias("events"),
    F.approx_count_distinct("user_id").alias("users"),
    F.max((F.col("event_type") == "view").cast("int")).alias("has_view"),
    F.max((F.col("event_type") == "cart").cast("int")).alias("has_cart"),
    F.max((F.col("event_type") == "purchase").cast("int")).alias("has_purchase"),
)
sc = (
    session_roll.agg(
        F.count(F.lit(1)).alias("sessions"),
        F.expr("approx_percentile(events, array(0.5, 0.9, 0.99))").alias(
            "events_p50_90_99"
        ),
        # bucketed histogram computed in this same pass -- avoids a second full
        # groupBy(user_session) just to draw the distribution
        F.expr("histogram_numeric(events, 50)").alias("events_hist"),
        F.max("events").alias("max_events"),
        F.sum((F.col("users") > 1).cast("long")).alias("multi_user_sessions"),
        F.sum((F.col("events") > 1000).cast("long")).alias("big_sessions"),
        F.sum("has_view").alias("with_view"),
        F.sum("has_cart").alias("with_cart"),
        F.sum("has_purchase").alias("with_purchase"),
        F.sum(
            (
                F.col("has_view").cast("boolean") & F.col("has_cart").cast("boolean")
            ).cast("int")
        ).alias("view_and_cart"),
        F.sum(
            (
                F.col("has_cart").cast("boolean")
                & F.col("has_purchase").cast("boolean")
            ).cast("int")
        ).alias("cart_and_purchase"),
        F.sum(
            (
                F.col("has_view").cast("boolean")
                & F.col("has_cart").cast("boolean")
                & F.col("has_purchase").cast("boolean")
            ).cast("int")
        ).alias("full_path"),
    )
    .first()
    .asDict()
)
print(sc)
if sc["with_view"]:
    print(f"view->cart session rate     = {sc['view_and_cart'] / sc['with_view']:.4f}")
if sc["with_cart"]:
    print(
        f"cart->purchase session rate = {sc['cart_and_purchase'] / sc['with_cart']:.4f}"
    )
session_funnel = [
    ("sessions", sc["sessions"]),
    ("with view", sc["with_view"]),
    ("with cart", sc["with_cart"]),
    ("with purchase", sc["with_purchase"]),
]
events_hist = [
    (round(b["x"], 1), int(b["y"]))
    for b in (sc.get("events_hist") or [])
    if b and b["y"]
]

# COMMAND ----------

# DBTITLE 1,User rollup -- buyers, repeat buyers, multi-session users (one pass)
user_roll = df.groupBy("user_id").agg(
    F.count(F.lit(1)).alias("rows"),
    F.approx_count_distinct("user_session").alias("sessions"),
    F.sum((F.col("event_type") == "purchase").cast("long")).alias("purchases"),
)
ur = (
    user_roll.agg(
        F.sum((F.col("purchases") > 0).cast("long")).alias("buyers"),
        F.sum((F.col("purchases") > 1).cast("long")).alias("repeat_buyers"),
        F.expr("approx_percentile(purchases, array(0.5, 0.9, 0.99))").alias(
            "purchases_p50_90_99"
        ),
        F.max("purchases").alias("max_purchases"),
        F.sum((F.col("sessions") > 1).cast("long")).alias("multi_session_users"),
    )
    .first()
    .asDict()
)
print("repeat purchase / multi-session:", ur)
rebought = (
    df.where(F.col("event_type") == "purchase")
    .groupBy("user_id", "product_id")
    .count()
    .where(F.col("count") > 1)
    .count()
)
print("(user, product) purchased more than once:", rebought)

# COMMAND ----------

# DBTITLE 1,Concentration -- top-N entity share (derived from the user/product rollups)
# Reuses user_roll / prod_roll (each carries a "rows" count) instead of a third and
# fourth full groupBy of the 110M-row table.
concentration = {}
for c, roll in (("user_id", user_roll), ("product_id", prod_roll)):
    top = [x["rows"] for x in roll.orderBy(F.desc("rows")).limit(50).collect()]
    concentration[c] = {
        "approx_distinct": approx_card[c],
        "top10_share": sum(top[:10]) / total,
        "top50_share": sum(top) / total,
        "max_rows_one_entity": top[0] if top else 0,
    }
    print(f"{c}: {concentration[c]}")

# COMMAND ----------

# DBTITLE 1,In-session event-sequence consistency (one windowed pass)
# Running boolean flags ("has a cart/view appeared earlier in this session"),
# NOT collect_set over an unbounded window -- collect_set copies a growing set
# per row, which is O(events^2) for a session and blows up on bot sessions with
# thousands of events. F.max over the same window is O(events).
seq = df.select(
    "user_session",
    "event_type",
    F.try_to_timestamp(F.substring("event_time", 1, 19)).alias("ts"),
)
_prior = (
    Window.partitionBy("user_session")
    .orderBy("ts")
    .rowsBetween(Window.unboundedPreceding, -1)
)
seq = seq.withColumn(
    "cart_before",
    F.max(F.when(F.col("event_type") == "cart", 1).otherwise(0)).over(_prior),
).withColumn(
    "view_before",
    F.max(F.when(F.col("event_type") == "view", 1).otherwise(0)).over(_prior),
)
sqv = seq.agg(
    F.sum(
        (
            (F.col("event_type") == "purchase")
            & (F.coalesce(F.col("cart_before"), F.lit(0)) == 0)
        ).cast("long")
    ).alias("purchase_no_cart"),
    F.sum(
        (
            (F.col("event_type") == "cart")
            & (F.coalesce(F.col("view_before"), F.lit(0)) == 0)
        ).cast("long")
    ).alias("cart_no_view"),
).first()
purch_no_cart, cart_no_view = sqv["purchase_no_cart"], sqv["cart_no_view"]
print(
    f"purchases with NO prior cart in the session : {purch_no_cart} of {funnel_map.get('purchase', 0)}"
)
print(
    f"carts with NO prior view in the session     : {cart_no_view} of {funnel_map.get('cart', 0)}"
)

# COMMAND ----------

# DBTITLE 1,Anomaly -- same-timestamp bursts within a session
burst = (
    df.groupBy("user_session", "event_time")
    .count()
    .where(F.col("count") > 20)
    .select("user_session")
    .distinct()
    .count()
)
print("sessions with >20 events sharing one event_time (burst):", burst)
print("sessions with >1000 events:", sc["big_sessions"])
print("zero-price purchases:", zero_purch, " negative price rows:", neg_price)

# COMMAND ----------

# DBTITLE 1,Activity by hour-of-day and weekday (one pass)
hod_dow = (
    df.groupBy(F.hour(ts).alias("hod"), F.date_format(ts, "E").alias("dow"))
    .count()
    .collect()
)
hod = {}
dow = {}
for x in hod_dow:
    # F.hour / F.date_format are NULL for an unparsed event_time -> skip so a
    # None key never reaches sorted()
    if x["hod"] is not None:
        hod[x["hod"]] = hod.get(x["hod"], 0) + x["count"]
    if x["dow"] is not None:
        dow[x["dow"]] = dow.get(x["dow"], 0) + x["count"]
print("by hour:", sorted(hod.items()))
print("by weekday:", sorted(dow.items()))

# COMMAND ----------

# DBTITLE 1,Categorical domain (`event_type`) + Oct-vs-Nov regime evidence
event_type_domain = categorical_domain(
    df, "event_type", ("view", "cart", "purchase"), name="event_type"
)
if event_type_domain["unexpected_count"]:
    print(
        f"UNEXPECTED event_type value(s): {event_type_domain['unexpected']} -- ingestion defect"
    )

# funnel composition per month (from the already-collected day x event_type rollup)
month_funnel = {}
for x in day_type:
    mo = x["day"][:7]
    month_funnel.setdefault(mo, {}).setdefault(x["event_type"], 0)
    month_funnel[mo][x["event_type"]] += x["count"]
for mo, f in sorted(month_funnel.items()):
    tot = sum(f.values())
    print(f"{mo}: {f}  purchase share = {f.get('purchase', 0) / tot:.4%}")

# per-month completeness of the price / catalogue fields
month_pop = population_by_group(
    df.withColumn("__month", F.substring("event_time", 1, 7)),
    "__month",
    ["price", "category_code", "brand"],
)
for mo, mv in sorted(month_pop.items()):
    print(
        f"{mo}: rows={mv['rows']}  "
        + str({c: d["null_rate"] for c, d in mv["columns"].items()})
    )

# COMMAND ----------

# DBTITLE 1,Duplicate key -- identical vs conflicting (one grouped pass, no full-row distinct)
key = ["user_session", "product_id", "event_type", "event_time"]
_dc = dup_key_composition(df, key)
db = {"dup_key_groups": _dc["dup_groups"], **_dc}
dup_identical, dup_conflicting = db["identical"], db["conflicting"]
print(
    f"duplicate {key} groups={db['dup_key_groups']}  identical={dup_identical}  conflicting={dup_conflicting}"
)

# COMMAND ----------

# DBTITLE 1,Price sample for histograms (one bounded sampled pass)
price_pdf = (
    df.select(F.col("event_type"), price.alias("price"))
    .where(F.col("price").isNotNull() & (F.col("price") <= price_p99))
    .sample(0.02, seed=42)
    .limit(250_000)
    .toPandas()
)
print(f"price sample rows: {len(price_pdf)}")

# COMMAND ----------

# DBTITLE 1,Figures (each gated so a blank result is never referenced)
figs = []
_specs = [
    (
        barplot,
        (funnel, "REES46 -- event_type funnel", "event_type", "events"),
        {"logy": True, "filename": "rees46_funnel.png"},
        ("REES46 event_type funnel", "rees46_funnel.png"),
    ),
    (
        barplot,
        (
            session_funnel,
            "REES46 -- sessions reaching each funnel stage",
            "stage",
            "sessions",
        ),
        {"filename": "rees46_session_funnel.png"},
        ("REES46 sessions reaching each funnel stage", "rees46_session_funnel.png"),
    ),
    (
        barplot,
        (sorted(hod.items()), "REES46 -- events by hour of day", "hour", "events"),
        {"filename": "rees46_events_by_hour_of_day.png"},
        ("REES46 events by hour of day", "rees46_events_by_hour_of_day.png"),
    ),
    (
        barplot,
        (sorted(dow.items()), "REES46 -- events by weekday", "weekday", "events"),
        {"rot": 30, "filename": "rees46_events_by_weekday.png"},
        ("REES46 events by weekday", "rees46_events_by_weekday.png"),
    ),
    (
        barplot,
        (
            list(missrates.items()),
            "REES46 -- missing rate per column",
            "column",
            "rate",
        ),
        {"rot": 30, "filename": "rees46_missing_rate_per_column.png"},
        ("REES46 missing rate per column", "rees46_missing_rate_per_column.png"),
    ),
    (
        lineplot,
        (by_day, "REES46 -- events per day", "day", "events"),
        {"filename": "rees46_events_per_day.png"},
        ("REES46 events per day", "rees46_events_per_day.png"),
    ),
    (
        barplot,
        (top_brands, "REES46 -- top 20 brands by event volume", "brand", "events"),
        {"rot": 90, "filename": "rees46_top_brands.png"},
        ("REES46 top brands by event volume", "rees46_top_brands.png"),
    ),
    (
        barplot,
        (
            top_cats,
            "REES46 -- top 20 category_code by event volume",
            "category_code",
            "events",
        ),
        {"rot": 90, "filename": "rees46_top_categories.png"},
        ("REES46 top 20 category_code by event volume", "rees46_top_categories.png"),
    ),
    (
        barplot,
        (
            [
                ("multi-category products", n_multi_cat),
                ("multi-brand products", n_multi_brand),
            ],
            "REES46 -- products with unstable category / brand",
            "",
            "products",
        ),
        {"filename": "rees46_unstable_product_attributes.png"},
        (
            "REES46 products with unstable category / brand",
            "rees46_unstable_product_attributes.png",
        ),
    ),
]
for fn, args, kw, ref in _specs:
    if fn(*args, **kw):
        figs.append(ref)

# COMMAND ----------

# DBTITLE 1,Figure -- events per day by event_type (multi-line)
_fig, _ax = plt.subplots(figsize=(13, 4))
for et in event_types:
    s = {x["day"]: x["count"] for x in day_type if x["event_type"] == et}
    _ax.plot(
        range(len(days)),
        [s.get(d, 0) for d in days],
        marker=".",
        label=et,
        linewidth=0.9,
    )
_ax.set_xticks(range(0, len(days), max(1, len(days) // 30)))
_ax.set_xticklabels(
    [days[i] for i in range(0, len(days), max(1, len(days) // 30))],
    rotation=90,
    fontsize=7,
)
_ax.legend()
_ax.set_title("REES46 -- events per day by type")
_fig.tight_layout()
_save_and_show(_fig, "rees46_events_per_day_by_type.png")
figs.append(("REES46 events per day by type", "rees46_events_per_day_by_type.png"))

# COMMAND ----------

# DBTITLE 1,Figure -- price distribution (sampled, clipped to p99) + events per session
if len(price_pdf) and histplot(
    price_pdf["price"].tolist(),
    f"REES46 price -- distribution (sampled, <=p99={price_p99})",
    "price",
    filename="rees46_price_distribution.png",
):
    figs.append(
        (
            "REES46 price distribution (sampled, clipped to p99)",
            "rees46_price_distribution.png",
        )
    )
for et in event_types:
    ev = price_pdf.loc[price_pdf["event_type"] == et, "price"].tolist()
    if histplot(
        ev,
        f"REES46 price -- {et} (sampled, <=p99)",
        "price",
        filename=f"rees46_price_distribution_{et}.png",
    ):
        figs.append(
            (
                f"REES46 price distribution -- {et} (sampled)",
                f"rees46_price_distribution_{et}.png",
            )
        )
if events_hist and barplot(
    events_hist,
    "REES46 -- events per session (histogram_numeric, 50 buckets)",
    "events in session (bucket centre)",
    "sessions",
    rot=90,
    logy=True,
    filename="rees46_events_per_session.png",
):
    figs.append(
        ("REES46 events per session distribution", "rees46_events_per_session.png")
    )

# COMMAND ----------

# DBTITLE 1,Findings
print("constant columns:", constant_cols)
print("event funnel:", funnel)
print(
    "session funnel view/cart/purchase:",
    {
        k: sc[k]
        for k in (
            "with_view",
            "with_cart",
            "with_purchase",
            "view_and_cart",
            "cart_and_purchase",
            "full_path",
        )
    },
)
print(
    "repeat buyers:",
    ur["repeat_buyers"],
    "of",
    ur["buyers"],
    " (user,product) rebought:",
    rebought,
)
print(
    "multi-session users:",
    ur["multi_session_users"],
    " multi-user sessions:",
    sc["multi_user_sessions"],
)
print("concentration:", concentration)
print(
    "in-session sequence violations: purchase-no-cart =",
    purch_no_cart,
    " cart-no-view =",
    cart_no_view,
)
print(
    "anomalies: >1000-event sessions =",
    sc["big_sessions"],
    " burst sessions =",
    burst,
    " zero-price purchases =",
    zero_purch,
    " negative price =",
    neg_price,
)
print(
    "category stability: products multi-cat =",
    n_multi_cat,
    " multi-brand =",
    n_multi_brand,
    " cat_id->code conflicts =",
    cc,
    " code->cat_id conflicts =",
    cc2,
)
print(
    "dup key: groups =",
    db["dup_key_groups"],
    " identical =",
    dup_identical,
    " conflicting =",
    dup_conflicting,
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/rees46.md
_prof = ["| column | missing rate | approx_distinct |", "|---|---|---|"]
for c in COLS:
    _prof.append(f"| {c} | {missrates[c]:.4f} | {approx_card[c]} |")
_prof += [
    "",
    (
        f"Rows: {total}. Constant columns: {constant_cols or 'none'}. "
        "Cardinality is approximate (approx_count_distinct)."
    ),
]

_dq = [
    (
        f"Duplicate key {key}: groups={db['dup_key_groups']}, identical={dup_identical}, "
        f"conflicting={dup_conflicting}."
    ),
    f"Zero/negative-price purchases: price<=0 in purchases = {zero_purch}; negative price rows = {neg_price}.",
    "",
    "Price / category / brand completeness by event_type:",
    "",
    "| event_type | rows | price missing | price<=0 | category_code missing | brand missing |",
    "|---|---|---|---|---|---|",
]
for et, x in by_type_map.items():
    _dq.append(
        f"| {et} | {x['rows']} | {x['price_missing']} | {x['price_le_zero']} | "
        f"{x['category_code_missing']} | {x['brand_missing']} |"
    )

_temporal = [
    para(
        f"event_time is '{TS_TZ}' (suffix stripped, ISO prefix parsed).",
        f"Day range: {days[0]} .. {days[-1]} -- a fixed two-month snapshot (Oct+Nov 2019),",
        "not a live feed; any model built on it is frozen to that window and its promotions",
        "(the Nov spike is Black Friday / Singles' Day).",
    ),
    f"Rows per month: {sorted(by_month.items())}.",
    f"Events by hour of day: {sorted(hod.items())}.",
    f"Events by weekday: {sorted(dow.items())}.",
    para(
        "Hour-of-day is in UTC -- the REES46 shop's local peak shifts by the shop's timezone;",
        "a diurnal feature must be computed in the shop's local time, not UTC.",
    ),
]

_domain = [
    para(
        "`event_type` vs the known set (view / cart / purchase):",
        f"unexpected={event_type_domain['unexpected'] or 'none'},",
        f"unused={event_type_domain['unused_allowed'] or 'none'}.",
    ),
]
if event_type_domain["unexpected_count"]:
    _domain.append(
        "-> an unexpected event_type is an INGESTION defect, not a source finding."
    )

_regime = [
    para(
        "October vs November 2019 -- November carries Black Friday / Singles' Day /",
        "Cyber Monday, so its funnel and catalogue completeness differ from a normal",
        "month. Measured, not assumed:",
    ),
    "",
    "| month | events | purchase share | price null | category_code null | brand null |",
    "|---|---|---|---|---|---|",
]
for mo in sorted(month_funnel):
    f = month_funnel[mo]
    tot = sum(f.values()) or 1
    mv = month_pop.get(mo, {"columns": {}})
    cols = mv["columns"]
    _regime.append(
        f"| {mo} | {tot} | {f.get('purchase', 0) / tot:.3%} | "
        f"{cols.get('price', {}).get('null_rate')} | "
        f"{cols.get('category_code', {}).get('null_rate')} | "
        f"{cols.get('brand', {}).get('null_rate')} |"
    )
_regime.append("")
_regime.append(
    para(
        "A model pooling the two months mixes promotion-driven and baseline behaviour --",
        "a promotion/seasonality indicator is warranted (not chosen here).",
    )
)

_coverage = [
    para(
        f"{total} events over {days[0]}..{days[-1]}. Funnel: {funnel} -- purchases are",
        f"~{funnel_map.get('purchase', 0) / total:.1%} of events.",
    ),
    para(
        f"category_code missing {missrates.get('category_code', 0):.1%},",
        f"brand missing {missrates.get('brand', 0):.1%} -- not missing at random: cheaper /",
        "long-tail products are less catalogued, so an 'is-catalogued' flag correlates with",
        "price and category and can leak.",
    ),
    para(
        "Only users/products/sessions active in this window appear -- a user who churned before",
        "October or joined after November is absent; a churn label defined on this window is",
        "right-censored.",
    ),
]

_entities = [
    (
        f"Approx distinct: user_id={approx_card.get('user_id')}, "
        f"product_id={approx_card.get('product_id')}, user_session={approx_card.get('user_session')}, "
        f"category_id={approx_card.get('category_id')}, brand={approx_card.get('brand')}."
    ),
    "",
    "Concentration:",
]
for c, v in concentration.items():
    _entities.append(
        f"- {c}: approx_distinct={v['approx_distinct']}, top10={v['top10_share']:.4f}, "
        f"top50={v['top50_share']:.4f}, max_one_entity={v['max_rows_one_entity']}"
    )
_entities += [
    "",
    f"Products with >1 category_id: {n_multi_cat}; with >1 brand: {n_multi_brand}.",
    f"category_id -> >1 category_code: {cc}; category_code -> >1 category_id: {cc2}.",
    f"Multi-user sessions: {sc['multi_user_sessions']}.",
]

_dist = ["| event_type | price min | max | avg | p50/95/99 |", "|---|---|---|---|---|"]
for et, x in by_type_map.items():
    _dist.append(
        f"| {et} | {x['price_min']} | {x['price_max']} | {x['price_avg']} | {x['price_p50_95_99']} |"
    )
_dist += [
    "",
    f"Overall p99 price: {price_p99}.",
    f"Top brands: {top_brands[:10]}.",
    f"Top category_code: {top_cats[:10]}.",
    f"Events per session (p50/90/99): {sc['events_p50_90_99']}, max {sc['max_events']}.",
    f"Purchases per user (p50/90/99): {ur['purchases_p50_90_99']}, max {ur['max_purchases']}.",
]

_rel = [
    f"Event funnel: {funnel}.",
    (
        f"Sessions: {sc['sessions']}; with view={sc['with_view']}, with cart={sc['with_cart']}, "
        f"with purchase={sc['with_purchase']}; full view->cart->purchase path={sc['full_path']}."
    ),
]
if sc["with_view"]:
    _rel.append(
        f"view->cart session rate = {sc['view_and_cart'] / sc['with_view']:.4f}."
    )
if sc["with_cart"]:
    _rel.append(
        f"cart->purchase session rate = {sc['cart_and_purchase'] / sc['with_cart']:.4f}."
    )
_rel += [
    (
        f"Buyers: {ur['buyers']}; repeat buyers: {ur['repeat_buyers']}; "
        f"multi-session users: {ur['multi_session_users']}; (user, product) rebought: {rebought}."
    ),
    (
        f"In-session sequence violations: purchase with no prior cart = {purch_no_cart} "
        f"of {funnel_map.get('purchase', 0)}; cart with no prior view = {cart_no_view} "
        f"of {funnel_map.get('cart', 0)}."
    ),
    (
        f"Same-timestamp burst sessions (>20 events on one event_time): {burst}. "
        f">1000-event sessions: {sc['big_sessions']}."
    ),
]

_findings = []
if constant_cols:
    _findings.append(f"- Constant columns: {constant_cols}.")
if dup_conflicting:
    _findings.append(
        f"- {dup_conflicting} duplicate {key} groups have conflicting non-key values."
    )
elif dup_identical:
    _findings.append(
        f"- {dup_identical} duplicate {key} groups are fully identical rows."
    )
_hi_miss = {c: round(missrates[c], 3) for c in COLS if missrates[c] > 0.1}
if _hi_miss:
    _findings.append(f"- Columns with >10% missing: {_hi_miss}.")
if zero_purch or neg_price:
    _findings.append(
        f"- Price anomalies: {zero_purch} zero-price purchases, {neg_price} negative-price rows."
    )
if purch_no_cart or cart_no_view:
    _findings.append(
        f"- Funnel not strictly ordered in-session: {purch_no_cart} purchases without a prior cart, "
        f"{cart_no_view} carts without a prior view."
    )
if n_multi_cat or n_multi_brand:
    _findings.append(
        f"- Product attributes drift: {n_multi_cat} products with >1 category_id, "
        f"{n_multi_brand} with >1 brand."
    )
if cc or cc2:
    _findings.append(
        f"- category_id <-> category_code is not a clean 1:1 mapping ({cc} / {cc2} conflicts)."
    )
if sc["multi_user_sessions"]:
    _findings.append(
        f"- {sc['multi_user_sessions']} sessions span more than one user_id."
    )
if burst:
    _findings.append(
        f"- {burst} sessions show same-timestamp event bursts (likely bot/instrumentation)."
    )
_findings_md = "\n".join(_findings)

_silver = []
if constant_cols:
    _silver.append(f"- Drop constant columns {constant_cols}.")
if dup_conflicting:
    _silver.append(
        f"- De-duplicate {key} with a deterministic rule; conflicting rows need explicit handling."
    )
elif dup_identical:
    _silver.append(f"- Apply distinct to collapse identical duplicate {key} rows.")
_silver.append("- Parse event_time to a timestamp; Silver grain = one row per event.")
if _hi_miss:
    _silver.append(
        "- category_code/brand are sparsely populated; keep as nullable dimensions with an 'unknown' member."
    )
if zero_purch or neg_price:
    _silver.append("- Flag non-positive prices rather than dropping the events.")
if n_multi_cat or n_multi_brand or cc or cc2:
    _silver.append(
        "- Build the product/category dimension as SCD or last-seen; do not assume stable product attributes."
    )
if sc["multi_user_sessions"]:
    _silver.append(
        "- Session grain must be (user_id, user_session), not user_session alone."
    )

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            para(
                "One row per event. The modelling grain is (user_id, user_session) for conversion, or",
                f"user_id for churn/LTV -- and {sc['multi_user_sessions']} sessions span >1 user_id, so",
                "session alone is not a clean entity.",
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                "Single Bronze table -- no join here. product_id/category_id/brand are event-level "
                "attributes. A future join to an external product catalog is unassessed."
            ),
        ),
        (
            "Target contamination",
            (
                f"Targets: purchase (conversion), repeat-purchase ({ur['repeat_buyers']} of "
                f"{ur['buyers']} buyers), full-funnel completion ({sc['full_path']} of {sc['sessions']}). "
                "The purchase event itself, and any session flag computed over the whole session, must "
                "be excluded from features for predicting that purchase."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "has_view / has_cart / has_purchase are computed over the WHOLE session regardless of "
                "order -- for a mid-session prediction they include post-cutoff events. Recompute every "
                "session feature from events with event_time strictly before the prediction point."
            ),
        ),
        (
            "Proxy leakage",
            (
                "A cart event is a near-perfect proxy for an imminent purchase; a product's aggregate "
                "conversion rate over the window includes the row being scored -- compute it leave-one-out "
                "or from a prior period."
            ),
        ),
        (
            "Split / entity leakage",
            (
                f"Split by user_id ({approx_card.get('user_id')} users), not by row or by session -- "
                f"{ur['multi_session_users']} users have multiple sessions and a user's behaviour is "
                "correlated across them."
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            (
                f"Product attributes drift: {n_multi_cat} products change category_id, {n_multi_brand} "
                "change brand. A static product join uses a category/brand that may post-date the event -- "
                "use a point-in-time / last-seen-before join."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                "Only entities active in Oct-Nov 2019 appear. A churn label defined on this window is "
                "right-censored; users who left earlier or joined later are simply absent."
            ),
        ),
        (
            "Missingness leakage",
            (
                f"category_code missing {missrates.get('category_code', 0):.1%}, brand "
                f"{missrates.get('brand', 0):.1%} -- correlated with price and long-tail products; an "
                "'is-catalogued' flag leaks that structure."
            ),
        ),
        (
            "Duplicate-event leakage",
            (
                f"Duplicate {key}: groups={db['dup_key_groups']}, identical={dup_identical}, "
                f"conflicting={dup_conflicting} -- de-duplicate before counting events or splitting so the "
                "same event is not on both sides."
            ),
        ),
        (
            "Target / feature temporal misalignment",
            (
                "event_time granularity is 1s and many events in a session share a timestamp "
                f"({burst} sessions have >20 events on one timestamp) -- ordering within a second is "
                "undefined, so a strict before/after cut can misalign the last feature and the label."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            (
                f"price is per-event (not per-line-total). {zero_purch} zero-price purchases and "
                f"{neg_price} negative prices exist -- confirm whether these are gifts / refunds before "
                "using price as a feature or a revenue target."
            ),
        ),
        (
            "Data-generation-process leakage",
            (
                f"Same-timestamp bursts ({burst} sessions) and multi-user sessions "
                f"({sc['multi_user_sessions']}) look like bot / instrumentation artefacts -- a feature "
                "keyed on burst behaviour encodes the tracking pipeline, not the shopper."
            ),
        ),
        (
            "Class / label instability",
            (
                f"Funnel is skewed ({funnel}) -- purchase is ~{funnel_map.get('purchase', 0) / total:.1%} "
                "of events; a conversion classifier faces severe imbalance, do not evaluate with accuracy. "
                "event_type itself is a stable 3-value enum."
            ),
        ),
        (
            "Label availability lag",
            (
                "A purchase is logged at the event; there is no lag within this dataset. A real "
                "deployment would need to wait for payment confirmation / returns before the label is final."
            ),
        ),
        (
            "Source / version / regime change",
            (
                "November 2019 contains Black Friday / Singles' Day / Cyber Monday. Regime / Version "
                "Evidence above measures the Oct-vs-Nov shift in purchase share and catalogue "
                "completeness -- a model across the window must carry a promotion/seasonality indicator."
            ),
        ),
        (
            "Sample-vs-full divergence",
            (
                "price_pdf is a 2% sample capped at 250k rows (clipped to p99); the events-per-session "
                "histogram is a full-table histogram_numeric. Use the full-table by_type_map / sc / ur "
                "aggregates for any feature-quality or threshold decision, not the price figure."
            ),
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    [
        ("Profile", "\n".join(_prof)),
        ("Data Quality", "\n".join(_dq)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Temporal Semantics", "\n".join(_temporal)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Distributions", "\n".join(_dist)),
        ("Relationships", "\n".join(_rel)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- EUROSTAT RETAIL TRADE
# MAGIC
# MAGIC **Purpose:** Profile the Bronze table `eurostat_retail_trade` (German retail trade volume
# MAGIC index, monthly): nulls, constant columns, key, rates per column kind and
# MAGIC date-like columns -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "eurostat"
NB_KEY = "01_retail_trade"
SECTION_TITLE = "Retail trade volume table"
TABLES = ["eurostat_retail_trade"]

# COMMAND ----------

# DBTITLE 1,Read tables
FRAMES = {t: bronze_frame(f"{CATALOG}.{BRONZE_SCHEMA}.{t}") for t in TABLES}

# COMMAND ----------

# DBTITLE 1,Standard EDA -- profile, keys, rates, dates and findings export
standard_group_eda(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    FRAMES,
    TABLES,
    "eurostat_01_rows.png",
    coverage=False,
    links=False,
)
