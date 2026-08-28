# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- REES46 ECOMMERCE EVENTS
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
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
import matplotlib.pyplot as plt
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.rees46_events"

# COMMAND ----------


# DBTITLE 1,Helpers
def barplot(pairs, title, xlabel, ylabel="rows", rot=0, figsize=(10, 4), log=False):
    plt.figure(figsize=figsize)
    plt.bar([str(p[0]) for p in pairs], [p[1] for p in pairs], log=log)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=rot, ha="right" if rot else "center")
    plt.tight_layout()
    plt.show()


def histplot(values, title, xlabel, bins=60, log=False):
    plt.figure(figsize=(10, 4))
    plt.hist(values, bins=bins, log=log)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel("count")
    plt.tight_layout()
    plt.show()


# COMMAND ----------

# DBTITLE 1,Schema, row count, missingness, approx distinct, constant columns (one pass)
df = spark.table(TABLE)
COLS = df.columns
ts = F.to_timestamp(F.substring("event_time", 1, 19))
price = F.col("price").cast("double")

prof_exprs = [F.count(F.lit(1)).alias("__rows")]
for c in COLS:
    miss = F.col(c).isNull() | (F.trim(F.col(c)) == "")
    prof_exprs += [
        F.sum(miss.cast("long")).alias(c + "__m"),
        F.approx_count_distinct(c).alias(c + "__d"),
    ]
r = df.agg(*prof_exprs).first().asDict()
total = r["__rows"]
missrates = {c: r[c + "__m"] / total for c in COLS}
approx_card = {c: r[c + "__d"] for c in COLS}
constant_cols = [c for c in COLS if r[c + "__d"] <= 1]
print(f"rows={total}  columns={len(COLS)}  ->  {COLS}")
for c in COLS:
    print(
        f"  {c:<16} missing={r[c + '__m']:>14} rate={missrates[c]:.4f} approx_distinct={r[c + '__d']}"
    )
print("constant columns:", constant_cols)
print(
    "cardinality is approximate (approx_count_distinct); exact where a check needs it below"
)
df.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Daily x event_type rollup -- funnel, per-day, per-month, min/max all derived
day_type = (
    df.groupBy(F.substring("event_time", 1, 10).alias("day"), "event_type")
    .count()
    .orderBy("day", "event_type")
).collect()
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
        F.expr(
            "percentile_approx(cast(price as double), array(0.5, 0.95, 0.99))"
        ).alias("price_p50_95_99"),
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

# DBTITLE 1,Top brands / categories by event volume
top_brands = [
    (x["brand"], x["count"])
    for x in df.groupBy("brand").count().orderBy(F.desc("count")).limit(20).collect()
]
top_cats = [
    (x["category_code"], x["count"])
    for x in df.groupBy("category_code")
    .count()
    .orderBy(F.desc("count"))
    .limit(20)
    .collect()
]
print("top brands:", top_brands)
print("top category_code:", top_cats)

# COMMAND ----------

# DBTITLE 1,product_id stability + category_id <-> category_code consistency
prod_roll = df.groupBy("product_id").agg(
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
cc2 = (
    df.where(F.trim(F.coalesce(F.col("category_code"), F.lit(""))) != "")
    .groupBy("category_code")
    .agg(F.approx_count_distinct("category_id").alias("ids"))
    .agg(F.sum((F.col("ids") > 1).cast("long")))
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
        F.max("events").alias("max_events"),
        F.sum((F.col("users") > 1).cast("long")).alias("multi_user_sessions"),
        F.sum((F.col("events") > 1000).cast("long")).alias("big_sessions"),
        F.sum("has_view").alias("with_view"),
        F.sum("has_cart").alias("with_cart"),
        F.sum("has_purchase").alias("with_purchase"),
        F.sum((F.col("has_view") & F.col("has_cart")).cast("int")).alias(
            "view_and_cart"
        ),
        F.sum((F.col("has_cart") & F.col("has_purchase")).cast("int")).alias(
            "cart_and_purchase"
        ),
        F.sum(
            (F.col("has_view") & F.col("has_cart") & F.col("has_purchase")).cast("int")
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
session_events_sample = [
    x["events"]
    for x in session_roll.select("events")
    .sample(0.02, seed=42)
    .limit(200_000)
    .collect()
]

# COMMAND ----------

# DBTITLE 1,User rollup -- buyers, repeat buyers, multi-session users (one pass)
user_roll = df.groupBy("user_id").agg(
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

# DBTITLE 1,Concentration -- top-N entity share (bounded collect)
concentration = {}
for c in ("user_id", "product_id"):
    pc = df.groupBy(c).count()
    top = [x["count"] for x in pc.orderBy(F.desc("count")).limit(50).collect()]
    concentration[c] = {
        "approx_distinct": approx_card[c],
        "top10_share": sum(top[:10]) / total,
        "top50_share": sum(top) / total,
        "max_rows_one_entity": top[0] if top else 0,
    }
    print(f"{c}: {concentration[c]}")

# COMMAND ----------

# DBTITLE 1,In-session event-sequence consistency (one windowed pass)
w = Window.partitionBy("user_session").orderBy(ts)
seq = df.select("event_type", ts.alias("ts")).withColumn(
    "prior_types",
    F.collect_set("event_type").over(w.rowsBetween(Window.unboundedPreceding, -1)),
)
sqv = seq.agg(
    F.sum(
        (
            (F.col("event_type") == "purchase")
            & ~F.array_contains(F.col("prior_types"), "cart")
        ).cast("long")
    ).alias("purchase_no_cart"),
    F.sum(
        (
            (F.col("event_type") == "cart")
            & ~F.array_contains(F.col("prior_types"), "view")
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
    hod[x["hod"]] = hod.get(x["hod"], 0) + x["count"]
    dow[x["dow"]] = dow.get(x["dow"], 0) + x["count"]
print("by hour:", sorted(hod.items()))
print("by weekday:", sorted(dow.items()))

# COMMAND ----------

# DBTITLE 1,Duplicate key -- identical vs conflicting (one grouped pass, no full-row distinct)
key = ["user_session", "product_id", "event_type", "event_time"]
dup = (
    df.groupBy(*key)
    .agg(
        F.count(F.lit(1)).alias("n"),
        F.countDistinct(F.hash(*[F.col(c) for c in COLS])).alias("row_variants"),
    )
    .where(F.col("n") > 1)
)
db = (
    dup.agg(
        F.count(F.lit(1)).alias("dup_key_groups"),
        F.sum((F.col("row_variants") == 1).cast("long")).alias("identical"),
        F.sum((F.col("row_variants") > 1).cast("long")).alias("conflicting"),
    )
    .first()
    .asDict()
)
dup_identical, dup_conflicting = db["identical"], db["conflicting"]
print(
    f"duplicate {key} groups={db['dup_key_groups']}  identical={dup_identical}  conflicting={dup_conflicting}"
)
dup.orderBy(F.desc("n")).limit(10).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Price sample for histograms (one bounded sampled pass)
price_pdf = (
    df.select(F.col("event_type"), price.alias("price"))
    .where(price.isNotNull() & (price <= price_p99))
    .sample(0.02, seed=42)
    .limit(250_000)
    .toPandas()
)
print(f"price sample rows: {len(price_pdf)}")

# COMMAND ----------

# DBTITLE 1,Figure -- event_type + session funnels
barplot(funnel, "REES46 -- event_type funnel", "event_type", "events")
barplot(
    session_funnel, "REES46 -- sessions reaching each funnel stage", "stage", "sessions"
)

# COMMAND ----------

# DBTITLE 1,Figure -- activity by hour of day and weekday
barplot(sorted(hod.items()), "REES46 -- events by hour of day", "hour", "events")
barplot(sorted(dow.items()), "REES46 -- events by weekday", "weekday", "events", rot=30)

# COMMAND ----------

# DBTITLE 1,Figure -- missingness rate per column
barplot(
    list(missrates.items()),
    "REES46 -- missing rate per column",
    "column",
    "rate",
    rot=30,
)

# COMMAND ----------

# DBTITLE 1,Figure -- events per day, overall and by event_type
plt.figure(figsize=(13, 4))
plt.plot([d for d, _ in by_day], [n for _, n in by_day], linewidth=0.9)
plt.title("REES46 -- events per day")
plt.xlabel("day")
plt.ylabel("events")
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()

plt.figure(figsize=(13, 4))
for et in event_types:
    series = {x["day"]: x["count"] for x in day_type if x["event_type"] == et}
    plt.plot(
        days, [series.get(d, 0) for d in days], marker=".", label=et, linewidth=0.9
    )
plt.legend()
plt.title("REES46 -- events per day by type")
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure -- price distribution (sampled, clipped to p99)
if len(price_pdf):
    histplot(
        price_pdf["price"].tolist(),
        f"REES46 price -- distribution (sampled, <=p99={price_p99})",
        "price",
    )
    for et in event_types:
        ev = price_pdf.loc[price_pdf["event_type"] == et, "price"].tolist()
        if ev:
            histplot(ev, f"REES46 price -- {et} (sampled, <=p99)", "price")

# COMMAND ----------

# DBTITLE 1,Figure -- top brands / categories, sessions, product stability
barplot(
    top_brands, "REES46 -- top 20 brands by event volume", "brand", "events", rot=90
)
barplot(
    top_cats,
    "REES46 -- top 20 category_code by event volume",
    "category_code",
    "events",
    rot=90,
)
if session_events_sample:
    histplot(
        session_events_sample,
        "REES46 -- events per session (sampled)",
        "events in session",
        bins=60,
        log=True,
    )
barplot(
    [("multi-category products", n_multi_cat), ("multi-brand products", n_multi_brand)],
    "REES46 -- products with unstable category / brand",
    "",
    "products",
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
