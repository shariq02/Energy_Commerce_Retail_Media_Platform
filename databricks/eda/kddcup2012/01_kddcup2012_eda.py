# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- KDD CUP 2012 TRACK 2 (CLICK PREDICTION)
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Profile kddcup2012_click_prediction (one Bronze table:
# MAGIC click / impression counts + ad / query / user id fields) -- schema,
# MAGIC missingness, constant columns, id-0 sentinel, class imbalance, id
# MAGIC cardinality & concentration, ad/query/user relationships, click
# MAGIC behaviour, duplicate-key (identical vs conflicting labels), CTR by
# MAGIC depth/position -- as evidence for Silver design. No timestamp column
# MAGIC exists in this dataset.

# COMMAND ----------

# DBTITLE 1,Imports
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.kddcup2012_click_prediction"
ID_COLS = [
    "url_hash",
    "ad_id",
    "advertiser_id",
    "query_id",
    "keyword_id",
    "title_id",
    "description_id",
    "user_id",
]
ZERO_SENTINEL = ["query_id", "keyword_id", "title_id", "description_id", "user_id"]

# COMMAND ----------


# DBTITLE 1,Helper
def barplot(pairs, title, xlabel, ylabel="rows", rot=0):
    plt.figure(figsize=(10, 4))
    plt.bar([str(p[0]) for p in pairs], [p[1] for p in pairs])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=rot, ha="right" if rot else "center")
    plt.tight_layout()
    plt.show()


# COMMAND ----------

# DBTITLE 1,Profile -- rows, missingness, approx distinct, constant columns (one agg)
df = spark.table(TABLE)
COLS = df.columns
exprs = [F.count(F.lit(1)).alias("__rows")]
for c in COLS:
    miss = F.col(c).isNull() | (F.trim(F.col(c)) == "")
    exprs += [
        F.sum(miss.cast("long")).alias(c + "__m"),
        F.approx_count_distinct(c).alias(c + "__d"),
    ]
r = df.agg(*exprs).first().asDict()
total = r["__rows"]
id_card = {c: r[c + "__d"] for c in ID_COLS}
constant_cols = [c for c in COLS if r[c + "__d"] <= 1]
print(f"rows={total}  columns={len(COLS)}  ->  {COLS}")
for c in COLS:
    print(
        f"  {c:<16} missing={r[c + '__m']:>10} rate={r[c + '__m'] / total:.4f} approx_distinct={r[c + '__d']}"
    )
print("constant columns:", constant_cols)
df.show(10, truncate=False)
distinct_rows = df.distinct().count()
print("exact full-row duplicates:", total - distinct_rows)

# COMMAND ----------

# DBTITLE 1,Scalar summary -- CTR, class imbalance, validity, id-0 sentinel (one agg)
clk, imp = F.col("click").cast("double"), F.col("impression").cast("double")
agg = [
    F.sum(clk).alias("clicks"),
    F.sum(imp).alias("impressions"),
    F.sum((F.col("click").cast("int") > 0).cast("long")).alias("positive_rows"),
    F.sum((clk > imp).cast("long")).alias("click_gt_impr"),
    F.sum(
        (F.col("position").cast("int") > F.col("depth").cast("int")).cast("long")
    ).alias("pos_gt_depth"),
]
for c in ZERO_SENTINEL:
    agg.append(
        F.sum(((F.col(c) == "0") | (F.col(c) == 0)).cast("long")).alias(c + "__zero")
    )
S = df.agg(*agg).first().asDict()
pos = S["positive_rows"]
neg = total - pos
ctr = S["clicks"] / S["impressions"] if S["impressions"] else None
sentinel_zero = {c: S[c + "__zero"] for c in ZERO_SENTINEL}
print(f"clicks={S['clicks']}  impressions={S['impressions']}  CTR={ctr}")
print(
    f"class: positive={pos} ({pos / total:.4%})  negative={neg} ({neg / total:.4%})  "
    f"imbalance neg:pos = {neg / max(pos, 1):.1f}:1"
)
print(
    "invalid click>impression:",
    S["click_gt_impr"],
    " position>depth:",
    S["pos_gt_depth"],
)
print(
    "id-0 (unknown) rows:",
    {c: (n, round(n / total, 4)) for c, n in sentinel_zero.items()},
)

# COMMAND ----------

# DBTITLE 1,Small-domain distributions -- click / impression / depth / position + CTR (one groupBy)
g = (
    df.groupBy("click", "impression", "depth", "position")
    .agg(
        F.count(F.lit(1)).alias("n"), F.sum(F.col("click").cast("int")).alias("clicks")
    )
    .collect()
)


def marginal(field):
    acc = {}
    for x in g:
        acc[x[field]] = acc.get(x[field], 0) + x["n"]
    return sorted(acc.items())


def ctr_by(field):
    rows, clicks = {}, {}
    for x in g:
        rows[x[field]] = rows.get(x[field], 0) + x["n"]
        clicks[x[field]] = clicks.get(x[field], 0) + x["clicks"]
    return [(k, clicks[k] / rows[k] if rows[k] else None) for k in sorted(rows)]


clk_dist, imp_dist = marginal("click"), marginal("impression")
depth_dist, pos_dist = marginal("depth"), marginal("position")
ctr_depth, ctr_pos = ctr_by("depth"), ctr_by("position")
print("click:", clk_dist, " impression:", imp_dist)
print("depth:", depth_dist, " position:", pos_dist)
print("CTR by depth:", ctr_depth, " by position:", ctr_pos)

# COMMAND ----------

# DBTITLE 1,Duplicate-key -- same context, conflicting click labels (one groupBy)
CONTEXT = [c for c in COLS if c not in ("click", "impression")]
kg = df.groupBy(*CONTEXT).agg(
    F.count(F.lit(1)).alias("n"),
    F.countDistinct("click").alias("distinct_click"),
)
kb = (
    kg.agg(
        F.sum((F.col("n") > 1).cast("long")).alias("dup_keys"),
        F.sum(((F.col("n") > 1) & (F.col("distinct_click") > 1)).cast("long")).alias(
            "conflicting"
        ),
    )
    .first()
    .asDict()
)
print(
    f"context keys with >1 row: {kb['dup_keys']}  of which conflicting click labels: {kb['conflicting']}"
)
kg.where((F.col("n") > 1) & (F.col("distinct_click") > 1)).orderBy(F.desc("n")).limit(
    20
).show(truncate=False)

# COMMAND ----------


# DBTITLE 1,Entity relationships -- ads per advertiser, keywords/ads per query, etc.
def card(parent, *children):
    d = df.groupBy(parent).agg(*[F.approx_count_distinct(c).alias(c) for c in children])
    for c in children:
        s = (
            d.agg(F.min(c).alias("min"), F.max(c).alias("max"), F.avg(c).alias("avg"))
            .first()
            .asDict()
        )
        print(f"{parent} -> {c}: {'1:1' if s['max'] == 1 else '1:N'}  {s}")


card("advertiser_id", "ad_id")
card("ad_id", "advertiser_id")
card("query_id", "keyword_id", "ad_id")
card("user_id", "query_id")

# COMMAND ----------

# DBTITLE 1,Concentration -- top entities + their CTR (one groupBy per entity, bounded collect)
concentration = {}
top_counts = {}
for c in ("ad_id", "advertiser_id", "query_id", "user_id"):
    top = (
        df.groupBy(c)
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.avg(F.col("click").cast("double")).alias("ctr"),
        )
        .orderBy(F.desc("rows"))
        .limit(5000)
        .collect()
    )
    top_counts[c] = [x["rows"] for x in top]
    concentration[c] = {
        "approx_distinct": id_card[c],
        "top10_share": sum(x["rows"] for x in top[:10]) / total,
        "top50_share": sum(x["rows"] for x in top[:50]) / total,
    }
    print(f"--- {c} --- {concentration[c]}")
    for x in top[:10]:
        print(f"   {x[c]}  rows={x['rows']}  ctr={x['ctr']:.4f}")

# COMMAND ----------

# DBTITLE 1,Figures
barplot(
    [("click=0", neg), ("click>0", pos)], "KDD -- click class balance", "class", "rows"
)
barplot(
    [(c, n / total) for c, n in sentinel_zero.items()],
    "KDD -- share of rows with id value 0 (unknown)",
    "column",
    "rate",
    rot=30,
)
barplot(clk_dist, "KDD -- click value distribution", "click", "rows")
barplot(depth_dist, "KDD -- depth distribution", "depth", "rows")
barplot(pos_dist, "KDD -- position distribution", "position", "rows")
barplot(ctr_depth, "KDD -- CTR by depth", "depth", "CTR")
barplot(ctr_pos, "KDD -- CTR by position", "position", "CTR")
plt.figure(figsize=(10, 4))
plt.bar(list(id_card), list(id_card.values()), log=True)
plt.title("KDD -- distinct value count per id column")
plt.ylabel("distinct (log)")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.show()
for c in ("ad_id", "query_id", "user_id"):
    counts = top_counts[c]
    plt.figure(figsize=(9, 4))
    plt.loglog(range(1, len(counts) + 1), counts, marker=".", linestyle="none")
    plt.title(f"KDD -- rows per {c} (top 5000, log-log)")
    plt.xlabel("rank")
    plt.ylabel("rows")
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# DBTITLE 1,Findings
print("constant columns:", constant_cols)
print(
    "id-0 (unknown) rates:", {c: round(n / total, 4) for c, n in sentinel_zero.items()}
)
print(f"class imbalance neg:pos = {neg / max(pos, 1):.1f}:1   overall CTR={ctr}")
print("context keys with conflicting click labels:", kb["conflicting"])
print("id cardinality (approx):", id_card)
print("concentration:", concentration)
