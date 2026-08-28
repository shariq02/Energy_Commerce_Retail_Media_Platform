# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- IPINYOU TRAINING AND LEADERBOARD
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Profile ipinyou_training and ipinyou_leaderboard (RTB
# MAGIC impression/click/conversion logs, bid logs excluded) -- schema,
# MAGIC missingness, constant columns, season/event_type partitions, the
# MAGIC impression -> click -> conversion funnel, per-season comparison,
# MAGIC temporal continuity, ad-slot dimensions, price ranges & distributions
# MAGIC & suspicious values, user / advertiser cardinality & concentration,
# MAGIC duplicate-key (identical vs conflicting) -- as evidence for Silver
# MAGIC design.

# COMMAND ----------

# DBTITLE 1,Imports
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
TABLES = {
    "training": f"{CATALOG}.{BRONZE_SCHEMA}.ipinyou_training",
    "leaderboard": f"{CATALOG}.{BRONZE_SCHEMA}.ipinyou_leaderboard",
}
# timestamp is YYYYMMDDHHMMSSmmm.
SLOT_LOWCARD = [
    "ad_slot_width",
    "ad_slot_height",
    "ad_slot_visibility",
    "ad_slot_format",
]
PRICE_COLS = ["bidding_price", "paying_price", "ad_slot_floor_price"]

# COMMAND ----------


# DBTITLE 1,Helpers
def barplot(pairs, title, xlabel, ylabel="rows", rot=0, figsize=(10, 4)):
    plt.figure(figsize=figsize)
    plt.bar([str(p[0]) for p in pairs], [p[1] for p in pairs])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=rot, ha="right" if rot else "center")
    plt.tight_layout()
    plt.show()


def histplot(values, title, xlabel, bins=50, log=False):
    plt.figure(figsize=(10, 4))
    plt.hist(values, bins=bins, log=log)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel("count")
    plt.tight_layout()
    plt.show()


# COMMAND ----------

# DBTITLE 1,Schema, row count, missingness, approx distinct, constant columns (one pass per table)
frames = {name: spark.table(t) for name, t in TABLES.items()}
prof = {}
for name, df in frames.items():
    cols = df.columns
    exprs = [F.count(F.lit(1)).alias("__rows")]
    for c in cols:
        miss = F.col(c).isNull() | (F.trim(F.col(c)) == "")
        exprs += [
            F.sum(miss.cast("long")).alias(c + "__m"),
            F.approx_count_distinct(c).alias(c + "__d"),
        ]
    r = df.agg(*exprs).first().asDict()
    prof[name] = {
        "cols": cols,
        "total": r["__rows"],
        "miss": {c: r[c + "__m"] for c in cols},
        "acd": {c: r[c + "__d"] for c in cols},
    }
    print("=" * 90, f"\n{name}  rows={prof[name]['total']}  ->  {cols}\n", "=" * 90)
    for c in cols:
        m = r[c + "__m"]
        print(
            f"  {c:<22} missing={m:>12} rate={m / prof[name]['total']:.4f} approx_distinct={r[c + '__d']}"
        )
    print("constant columns:", [c for c in cols if r[c + "__d"] <= 1])
    df.show(5, truncate=False)
totals = {n: prof[n]["total"] for n in frames}
const_cols = {n: [c for c in prof[n]["cols"] if prof[n]["acd"][c] <= 1] for n in frames}

# COMMAND ----------

# DBTITLE 1,Partitions: season / event_type / log_type (one grouped pass per table)
partitions = {}
for name, df in frames.items():
    gcols = [c for c in ("season", "event_type", "log_type") if c in df.columns]
    g = df.groupBy(*gcols).count().collect()
    partitions[name] = {}
    for c in gcols:
        acc = {}
        for x in g:
            acc[x[c]] = acc.get(x[c], 0) + x["count"]
        partitions[name][c] = sorted(acc.items(), key=lambda p: -p[1])
        print(f"{name}.{c}:", partitions[name][c])

# COMMAND ----------

# DBTITLE 1,Per-season comparison -- volume, entities, timestamp coverage, funnel counts
season_cmp = {}
for name, df in frames.items():
    has_et = "event_type" in df.columns
    agg = (
        df.groupBy("season")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.approx_count_distinct("advertiser_id").alias("advertisers"),
            F.approx_count_distinct("ipinyou_id").alias("users"),
            F.approx_count_distinct("timestamp").alias("distinct_ts"),
            F.min("timestamp").alias("min_ts"),
            F.max("timestamp").alias("max_ts"),
            F.expr("percentile_approx(cast(paying_price as double), 0.5)").alias(
                "median_paying"
            ),
            *(
                [
                    F.sum((F.col("event_type") == "impression").cast("long")).alias(
                        "impressions"
                    ),
                    F.sum((F.col("event_type") == "click").cast("long")).alias(
                        "clicks"
                    ),
                    F.sum((F.col("event_type") == "conversion").cast("long")).alias(
                        "conversions"
                    ),
                ]
                if has_et
                else [
                    F.sum(F.col("has_conversion").cast("double")).alias(
                        "conversions_sum"
                    )
                ]
            ),
        )
        .orderBy("season")
        .collect()
    )
    season_cmp[name] = [x.asDict() for x in agg]
    for x in season_cmp[name]:
        print(f"{name} season {x['season']}: {x}")

# COMMAND ----------

# DBTITLE 1,Impression -> click -> conversion funnel (training) + leaderboard labels
et_counts = dict(partitions["training"].get("event_type", []))
imp, clk, conv = (
    et_counts.get("impression", 0),
    et_counts.get("click", 0),
    et_counts.get("conversion", 0),
)
print(f"training funnel: impression={imp}  click={clk}  conversion={conv}")
if imp:
    print(
        f"  CTR={clk / imp:.6f}  conversion rate={conv / imp:.8f}"
        + (f"  click->conv={conv / clk:.6f}" if clk else "")
    )
funnel = [("impression", imp), ("click", clk), ("conversion", conv)]

lb = frames["leaderboard"]
lb_labels = {}
if "has_conversion" in lb.columns:
    g = lb.groupBy("has_conversion", "related_clicks_count").count().collect()
    for k in ("has_conversion", "related_clicks_count"):
        acc = {}
        for x in g:
            acc[x[k]] = acc.get(x[k], 0) + x["count"]
        lb_labels[k] = sorted(acc.items(), key=lambda p: str(p[0]))
        print(f"leaderboard.{k}:", lb_labels[k])

# COMMAND ----------

# DBTITLE 1,Ad-slot low-cardinality dimensions (one grouped pass per table)
slot_dist = {}
for name, df in frames.items():
    g = df.groupBy(*SLOT_LOWCARD).count().collect()
    slot_dist[name] = {}
    for c in SLOT_LOWCARD:
        acc = {}
        for x in g:
            acc[x[c]] = acc.get(x[c], 0) + x["count"]
        slot_dist[name][c] = sorted(acc.items(), key=lambda p: -p[1])[:15]
        print(f"{name}.{c}:", slot_dist[name][c])

# COMMAND ----------

# DBTITLE 1,Price ranges, distributions & suspicious values (one agg per table)
price_stats = {}
for name, df in frames.items():
    exprs = []
    for c in PRICE_COLS:
        v = F.col(c).cast("double")
        exprs += [
            F.min(v).alias(c + "_min"),
            F.max(v).alias(c + "_max"),
            F.avg(v).alias(c + "_avg"),
            F.expr(
                f"percentile_approx(cast(`{c}` as double), array(0.5, 0.95, 0.99))"
            ).alias(c + "_p"),
            F.sum((v < 0).cast("long")).alias(c + "_negative"),
            F.sum((v == 0).cast("long")).alias(c + "_zero"),
        ]
    pay = F.col("paying_price").cast("double")
    bid = F.col("bidding_price").cast("double")
    floor = F.col("ad_slot_floor_price").cast("double")
    exprs += [
        F.sum((pay > bid).cast("long")).alias("paying_gt_bidding"),
        F.sum((floor > pay).cast("long")).alias("floor_gt_paying"),
        F.sum((floor > bid).cast("long")).alias("floor_gt_bidding"),
    ]
    price_stats[name] = df.agg(*exprs).first().asDict()
    print(f"--- {name} ---")
    for c in PRICE_COLS:
        print(
            f"  {c}: min={price_stats[name][c + '_min']} max={price_stats[name][c + '_max']} "
            f"avg={price_stats[name][c + '_avg']} p50/95/99={price_stats[name][c + '_p']} "
            f"neg={price_stats[name][c + '_negative']} zero={price_stats[name][c + '_zero']}"
        )
    print(
        f"  paying>bidding={price_stats[name]['paying_gt_bidding']}  "
        f"floor>paying={price_stats[name]['floor_gt_paying']}  "
        f"floor>bidding={price_stats[name]['floor_gt_bidding']}"
    )

# COMMAND ----------

# DBTITLE 1,Entity cardinality (approx, reused from profile)
for name in frames:
    acd = prof[name]["acd"]
    print(
        f"{name}:",
        {
            c: acd[c]
            for c in (
                "bid_id",
                "ipinyou_id",
                "advertiser_id",
                "creative_id",
                "region",
                "city",
                "ad_exchange",
                "domain",
            )
            if c in acd
        },
    )

# COMMAND ----------

# DBTITLE 1,Temporal continuity -- events per (season, day, hour) in one pass
day_hour = {}
for name, df in frames.items():
    day_hour[name] = (
        df.select(
            "season",
            F.substring("timestamp", 1, 8).alias("day"),
            F.substring("timestamp", 1, 10).alias("hour"),
        )
        .where(F.length("hour") == 10)
        .groupBy("season", "day", "hour")
        .count()
        .collect()
    )
    by_season_day = {}
    for x in day_hour[name]:
        by_season_day.setdefault(x["season"], set()).add(x["day"])
    for s, dset in by_season_day.items():
        ds = sorted(dset)
        print(f"{name} season {s}: {len(ds)} distinct days, {ds[0]}..{ds[-1]}")

# COMMAND ----------

# DBTITLE 1,User / advertiser concentration (one groupBy per entity, bounded collect)
concentration = {}
for name, df in frames.items():
    acd = prof[name]["acd"]
    for c in ("ipinyou_id", "advertiser_id"):
        top = [
            x["count"]
            for x in df.groupBy(c).count().orderBy(F.desc("count")).limit(50).collect()
        ]
        concentration[(name, c)] = {
            "approx_distinct": acd[c],
            "top10_share": sum(top[:10]) / totals[name],
            "top50_share": sum(top) / totals[name],
            "max_rows_one_entity": top[0] if top else 0,
        }
        print(f"{name}.{c}: {concentration[(name, c)]}")
    adv_roll = (
        df.groupBy("advertiser_id")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            *(
                [
                    F.avg((F.col("event_type") == "click").cast("double")).alias(
                        "click_share"
                    )
                ]
                if "event_type" in df.columns
                else []
            ),
        )
        .orderBy(F.desc("rows"))
        .limit(15)
        .collect()
    )
    for x in adv_roll:
        print(f"  {name} adv {x['advertiser_id']}: {x.asDict()}")

# COMMAND ----------

# DBTITLE 1,Duplicate-key -- candidate-key uniqueness + identical vs conflicting (one pass per table)
dup_breakdown = {}
for name, df in frames.items():
    cols = df.columns
    key = ["season", "bid_id"] + (["event_type"] if "event_type" in cols else [])
    dk = df.groupBy(*key).agg(
        F.count(F.lit(1)).alias("n"),
        F.countDistinct(F.hash(*[F.col(c) for c in cols])).alias("row_variants"),
    )
    b = (
        dk.agg(
            F.count(F.lit(1)).alias("distinct_keys"),
            F.sum((F.col("n") > 1).cast("long")).alias("dup_groups"),
            F.sum(((F.col("n") > 1) & (F.col("row_variants") == 1)).cast("long")).alias(
                "identical"
            ),
            F.sum(((F.col("n") > 1) & (F.col("row_variants") > 1)).cast("long")).alias(
                "conflicting"
            ),
        )
        .first()
        .asDict()
    )
    dup_breakdown[name] = b
    print(
        f"{name}: key={key}  distinct_keys={b['distinct_keys']} (rows={totals[name]}, "
        f"unique={b['distinct_keys'] == totals[name]})  dup_groups={b['dup_groups']} "
        f"identical={b['identical']} conflicting={b['conflicting']}"
    )

# COMMAND ----------

# DBTITLE 1,Price sample for histograms (one bounded sampled pass per table)
price_pdf = {}
for name, df in frames.items():
    hi = max(
        price_stats[name][c + "_p"][2]
        for c in PRICE_COLS
        if price_stats[name][c + "_p"]
    )
    sel = [F.col(c).cast("double").alias(c) for c in PRICE_COLS]
    price_pdf[name] = (
        df.select(*sel)
        .where(F.greatest(*[F.col(c) for c in PRICE_COLS]).isNotNull())
        .sample(0.05, seed=42)
        .limit(200_000)
        .toPandas()
    )
    print(f"{name} price sample rows: {len(price_pdf[name])}  clip p99={hi}")

# COMMAND ----------

# DBTITLE 1,Figure -- funnel + partitions
plt.figure(figsize=(8, 4))
plt.bar([f[0] for f in funnel], [f[1] for f in funnel], log=True)
plt.title("iPinYou training -- impression -> click -> conversion funnel")
plt.ylabel("events (log)")
plt.tight_layout()
plt.show()
for name, parts in partitions.items():
    for c, pairs in parts.items():
        barplot(pairs, f"iPinYou {name} -- rows per {c}", c, "rows", rot=30)
for c, pairs in lb_labels.items():
    barplot(pairs[:30], f"iPinYou leaderboard -- {c} distribution", c, "rows", rot=45)

# COMMAND ----------

# DBTITLE 1,Figure -- events per day per season, and per hour
for name, rows in day_hour.items():
    seasons = sorted({x["season"] for x in rows})
    day_agg = {}
    hour_agg = {}
    for x in rows:
        sd = day_agg.setdefault(x["season"], {})
        sd[x["day"]] = sd.get(x["day"], 0) + x["count"]
        hour_agg[x["hour"]] = hour_agg.get(x["hour"], 0) + x["count"]
    plt.figure(figsize=(13, 4))
    for s in seasons:
        xs = sorted(day_agg[s])
        plt.plot(
            range(len(xs)),
            [day_agg[s][d] for d in xs],
            marker=".",
            label=f"season {s}",
            linewidth=0.8,
        )
    plt.legend()
    plt.title(f"iPinYou {name} -- events per day by season")
    plt.xlabel("day index")
    plt.tight_layout()
    plt.show()
    hrs = sorted(hour_agg)
    plt.figure(figsize=(13, 4))
    plt.plot(range(len(hrs)), [hour_agg[h] for h in hrs], linewidth=0.8)
    plt.title(f"iPinYou {name} -- event volume per hour")
    plt.xlabel("hour index")
    plt.ylabel("events")
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# DBTITLE 1,Figure -- price distributions (sampled, clipped to p99)
for name in frames:
    pdf = price_pdf[name]
    for c in PRICE_COLS:
        hi = price_stats[name][c + "_p"][2] if price_stats[name][c + "_p"] else None
        vals = pdf[c].dropna()
        if hi and hi > 0:
            vals = vals[(vals >= 0) & (vals <= hi)]
        if len(vals):
            histplot(
                vals.tolist(),
                f"iPinYou {name}.{c} -- distribution (sampled, <=p99={hi})",
                c,
            )

# COMMAND ----------

# DBTITLE 1,Findings
print("constant columns:", const_cols)
print("training funnel (impr, click, conv):", (imp, clk, conv))
print("dup key:", {n: dup_breakdown[n] for n in frames})
print("concentration:", concentration)
print(
    "suspicious prices:",
    {
        n: {
            k: price_stats[n][k]
            for k in ("paying_gt_bidding", "floor_gt_paying", "floor_gt_bidding")
        }
        for n in frames
    },
)
print("per-season comparison:", season_cmp)
