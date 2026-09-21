# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- GA4 COMMERCE EVENTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
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

# MAGIC %run ../_eda_common

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
