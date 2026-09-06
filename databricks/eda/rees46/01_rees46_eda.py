# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- REES46 ECOMMERCE EVENTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
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
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

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
seq = df.select(
    "user_session",
    "event_type",
    F.to_timestamp(F.substring("event_time", 1, 19)).alias("ts"),
)
w = Window.partitionBy("user_session").orderBy("ts")
seq = seq.withColumn(
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
if session_events_sample and histplot(
    session_events_sample,
    "REES46 -- events per session (sampled)",
    "events in session",
    bins=60,
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
                "price_pdf is a 2% sample capped at 250k rows (clipped to p99), session_events_sample a "
                "2% sample capped at 200k -- use the full-table by_type_map / sc / ur aggregates for any "
                "feature-quality or threshold decision, not these figures."
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
