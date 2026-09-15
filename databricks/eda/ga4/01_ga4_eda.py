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
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
