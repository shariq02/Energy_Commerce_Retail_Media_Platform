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
import contextlib
import os as _os
import re as _re

import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "ipinyou"
NB_KEY = "01_training_leaderboard"
SECTION_TITLE = "Training & leaderboard tables (ipinyou_training, ipinyou_leaderboard)"
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

# iPinYou's merged training log does not always spell the funnel stages out;
# accept the common aliases (and numeric codes) rather than matching the literal
# strings "impression"/"click"/"conversion", which produced a 0/0/0 funnel.
ET_ALIASES = {
    "impression": {"impression", "impressions", "imp", "i", "1"},
    "click": {"click", "clicks", "clk", "c", "2"},
    "conversion": {"conversion", "conversions", "conv", "cv", "3"},
}

# COMMAND ----------

# DBTITLE 1,Helpers

def _et_stage_expr(stage):
    return F.lower(F.trim(F.col("event_type").cast("string"))).isin(
        sorted(ET_ALIASES[stage])
    )


def barplot(pairs, title, xlabel, ylabel="rows", rot=0, figsize=(10, 4), filename=None):
    plt.figure(figsize=figsize)
    plt.bar([str(p[0]) for p in pairs], [p[1] for p in pairs])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=rot, ha="right" if rot else "center")
    plt.tight_layout()
    if filename:
        plt.savefig(fig_path(filename), dpi=110, bbox_inches="tight")
    plt.show()


def histplot(values, title, xlabel, bins=50, log=False, filename=None):
    plt.figure(figsize=(10, 4))
    plt.hist(values, bins=bins, log=log)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel("count")
    plt.tight_layout()
    if filename:
        plt.savefig(fig_path(filename), dpi=110, bbox_inches="tight")
    plt.show()


# COMMAND ----------

# DBTITLE 1,Profiling-export helper (writes src/schemas/profiling/<source>.md)

def _repo_root():
    p = _os.path.abspath(_os.getcwd())
    for _ in range(12):
        if _os.path.isdir(_os.path.join(p, "src", "schemas")) and _os.path.isdir(
            _os.path.join(p, "databricks", "eda")
        ):
            return p
        if _os.path.dirname(p) == p:
            break
        p = _os.path.dirname(p)
    with contextlib.suppress(Exception):
        wp = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        i = wp.rfind("/databricks/eda/")
        if i > 0:
            for cand in (wp[:i], "/Workspace" + wp[:i]):
                if _os.path.isdir(_os.path.join(cand, "src", "schemas")):
                    return cand
    raise RuntimeError(
        "repo root not found -- run from <repo>/databricks/eda/<source>/"
    )


def _profiling_dir():
    d = _os.path.join(_repo_root(), "src", "schemas", "profiling")
    _os.makedirs(_os.path.join(d, "figures"), exist_ok=True)
    return d


def fig_path(name):
    return _os.path.join(_profiling_dir(), "figures", name)


def fmt_pairs(pairs, n=25):
    # Render (label, value) pairs as markdown list lines, capped at n with a
    # "... (N more)" tail so the profiling .md never carries a 1000-row dump.
    items = list(pairs)
    out = [f"- {lbl}: {val}" for lbl, val in items[:n]]
    if len(items) > n:
        out.append(f"- ... ({len(items) - n} more)")
    return "\n".join(out)


def _facet_grid(items, suptitle, filename, ncols=3, panel=(4.6, 3.2)):
    items = [(str(k), draw) for k, draw in items if draw is not None]
    if not items:
        print(f"  _facet_grid: no data -> {filename}")
        return False
    ncols = min(ncols, len(items))
    nrows = -(-len(items) // ncols)
    fig, axes = plt.subplots(
        nrows, ncols, figsize=(panel[0] * ncols, panel[1] * nrows), squeeze=False
    )
    flat = list(axes.flatten())
    for ax, (title, draw) in zip(flat, items):
        draw(ax)
        ax.set_title(title, fontsize=9)
        ax.tick_params(labelsize=7)
    for ax in flat[len(items) :]:
        ax.set_visible(False)
    fig.suptitle(suptitle)
    fig.tight_layout()
    fig.savefig(fig_path(filename), dpi=110, bbox_inches="tight")
    plt.show()
    plt.close(fig)
    return True


def facet_bars(groups, suptitle, filename, rot=45, ncols=3, logy=False):
    def _mk(pairs):
        if not pairs:
            return None

        def draw(ax):
            ax.bar([str(p[0]) for p in pairs], [p[1] for p in pairs])
            if logy:
                ax.set_yscale("log")
            ax.tick_params(axis="x", labelrotation=rot)

        return draw

    src = groups.items() if hasattr(groups, "items") else groups
    return _facet_grid([(k, _mk(list(v))) for k, v in src], suptitle, filename, ncols)


def facet_hists(groups, suptitle, filename, bins=40, ncols=3, logy=True):
    def _mk(vals):
        if vals is None or not len(vals):
            return None

        def draw(ax):
            ax.hist(list(vals), bins=bins, log=logy)

        return draw

    src = groups.items() if hasattr(groups, "items") else groups
    return _facet_grid([(k, _mk(v)) for k, v in src], suptitle, filename, ncols)


def write_profiling(source, notebook_key, section_title, blocks, figures=None):
    d = _profiling_dir()
    md = _os.path.join(d, source + ".md")
    lines = [f"<!-- BEGIN {source}:{notebook_key} -->", f"## {section_title}", ""]
    for heading, body in blocks:
        if body is None or str(body).strip() == "":
            continue
        lines += [f"### {heading}", "", str(body).rstrip(), ""]
    for cap, name in figures or []:
        if not _os.path.exists(_os.path.join(d, "figures", name)):
            print(f"  profiling export: skipping absent figure {name}")
            continue
        lines += [f"### Figure -- {cap}", "", f"![{cap}](figures/{name})", ""]
    lines.append(f"<!-- END {source}:{notebook_key} -->")
    block = "\n".join(lines)
    existing = ""
    if _os.path.exists(md):
        with open(md, encoding="utf-8") as fh:
            existing = fh.read()
    pat = _re.compile(
        r"<!-- BEGIN "
        + _re.escape(source)
        + r":([\w.\-]+) -->.*?<!-- END "
        + _re.escape(source)
        + r":\1 -->",
        _re.DOTALL,
    )
    kept = {mm.group(1): mm.group(0) for mm in pat.finditer(existing)}
    kept[notebook_key] = block
    intro = f"_Auto-generated by the Phase 3 EDA notebooks (`databricks/eda/{source}/`). One `## ` section per notebook; re-running a notebook replaces its own section, other sections are preserved._"
    header = f"# {source.upper()} EDA PROFILE\n\n{intro}\n\n"
    body = "\n\n".join(kept[k] for k in sorted(kept))
    out = header + body + "\n"
    tmp = md + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(out)
    _os.replace(tmp, md)
    print(f"profiling export -> {md}  ('{notebook_key}', {len(kept)} section(s))")


# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()

print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

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
                    F.sum(_et_stage_expr("impression").cast("long")).alias(
                        "impressions"
                    ),
                    F.sum(_et_stage_expr("click").cast("long")).alias("clicks"),
                    F.sum(_et_stage_expr("conversion").cast("long")).alias(
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
print(f"training event_type distinct values (raw): {et_counts}")


def _et_bucket(counts, stage):
    al = ET_ALIASES[stage]
    return sum(int(v) for k, v in counts.items() if str(k).strip().lower() in al)


imp = _et_bucket(et_counts, "impression")
clk = _et_bucket(et_counts, "click")
conv = _et_bucket(et_counts, "conversion")
et_unmapped = {
    k: v
    for k, v in et_counts.items()
    if not any(str(k).strip().lower() in a for a in ET_ALIASES.values())
}
if et_unmapped:
    print(f"WARNING  unmapped event_type values (not in funnel): {et_unmapped}")
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
        v = F.when(
            F.col(c).rlike("^-?[0-9]+(\\.[0-9]+)?$"), F.col(c).cast("double")
        ).otherwise(F.lit(None))
        exprs += [
            F.min(v).alias(c + "_min"),
            F.max(v).alias(c + "_max"),
            F.avg(v).alias(c + "_avg"),
            F.expr(
                f"percentile_approx(case when `{c}` rlike '^-?[0-9]+(\\\\.[0-9]+)?$' then cast(`{c}` as double) else null end, array(0.5, 0.95, 0.99))"
            ).alias(c + "_p"),
            F.sum((v < 0).cast("long")).alias(c + "_negative"),
            F.sum((v == 0).cast("long")).alias(c + "_zero"),
        ]
    pay = F.when(
        F.col("paying_price").rlike("^-?[0-9]+(\\.[0-9]+)?$"),
        F.col("paying_price").cast("double"),
    ).otherwise(F.lit(None))
    bid = F.when(
        F.col("bidding_price").rlike("^-?[0-9]+(\\.[0-9]+)?$"),
        F.col("bidding_price").cast("double"),
    ).otherwise(F.lit(None))
    floor = F.when(
        F.col("ad_slot_floor_price").rlike("^-?[0-9]+(\\.[0-9]+)?$"),
        F.col("ad_slot_floor_price").cast("double"),
    ).otherwise(F.lit(None))
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
    sel = [
        F.when(F.col(c).rlike("^-?[0-9]+(\\.[0-9]+)?$"), F.col(c).cast("double"))
        .otherwise(F.lit(None))
        .alias(c)
        for c in PRICE_COLS
    ]
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
plt.savefig(fig_path("ipinyou_funnel.png"), dpi=110, bbox_inches="tight")
plt.show()
facet_bars(
    {
        f"{name}.{c}": pairs
        for name, parts in partitions.items()
        for c, pairs in parts.items()
    },
    "iPinYou -- rows per partition column, by table",
    "ipinyou_partitions.png",
    rot=30,
    ncols=3,
)
facet_bars(
    {c: pairs[:30] for c, pairs in lb_labels.items()},
    "iPinYou leaderboard -- label distributions",
    "ipinyou_leaderboard_labels.png",
    rot=45,
    ncols=2,
)

# COMMAND ----------

# DBTITLE 1,Figure -- events per day per season, and per hour
_day_items, _hour_items = [], []
for name, rows in day_hour.items():
    seasons = sorted({x["season"] for x in rows})
    day_agg, hour_agg = {}, {}
    for x in rows:
        sd = day_agg.setdefault(x["season"], {})
        sd[x["day"]] = sd.get(x["day"], 0) + x["count"]
        hour_agg[x["hour"]] = hour_agg.get(x["hour"], 0) + x["count"]

    def _day_draw(day_agg=day_agg, seasons=seasons):
        def draw(ax):
            for s in seasons:
                xs = sorted(day_agg[s])
                ax.plot(
                    range(len(xs)),
                    [day_agg[s][d] for d in xs],
                    marker=".",
                    label=f"season {s}",
                    linewidth=0.8,
                )
            ax.legend(fontsize=6)

        return draw

    def _hour_draw(hour_agg=hour_agg):
        def draw(ax):
            hrs = sorted(hour_agg)
            ax.plot(range(len(hrs)), [hour_agg[h] for h in hrs], linewidth=0.8)

        return draw

    _day_items.append((name, _day_draw()))
    _hour_items.append((name, _hour_draw()))
_facet_grid(
    _day_items,
    "iPinYou -- events per day by season, by table",
    "ipinyou_events_per_day_by_season.png",
    ncols=2,
)
_facet_grid(
    _hour_items,
    "iPinYou -- event volume per hour, by table",
    "ipinyou_event_volume_per_hour.png",
    ncols=2,
)

# COMMAND ----------

# DBTITLE 1,Figure -- price distributions (sampled, clipped to p99, faceted)
_price_groups = {}
for name in frames:
    pdf = price_pdf[name]
    for c in PRICE_COLS:
        hi = price_stats[name][c + "_p"][2] if price_stats[name][c + "_p"] else None
        vals = pdf[c].dropna()
        if hi and hi > 0:
            vals = vals[(vals >= 0) & (vals <= hi)]
        _price_groups[f"{name}.{c}"] = vals.tolist()
facet_hists(
    _price_groups,
    "iPinYou -- price distributions (sampled, clipped to p99)",
    "ipinyou_price_distributions.png",
    ncols=3,
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

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/ipinyou.md
_prof = []
for name in frames:
    p = prof[name]
    _prof.append(f"**{name}** -- rows: {p['total']}, columns: {p['cols']}")
    _prof.append(f"- constant columns: {const_cols[name] or 'none'}")
    _hi_miss = {
        c: round(p["miss"][c] / p["total"], 4)
        for c in p["cols"]
        if p["miss"][c] / p["total"] > 0.05
    }
    if _hi_miss:
        _prof.append(f"- columns >5% missing: {_hi_miss}")
    _prof.append("")

_dq = []
for name in frames:
    b = dup_breakdown[name]
    _dq.append(
        f"**{name}** duplicate key: distinct={b['distinct_keys']}, "
        f"unique={b['distinct_keys'] == totals[name]}, dup groups={b['dup_groups']}, "
        f"identical={b['identical']}, conflicting={b['conflicting']}."
    )
    ps = price_stats[name]
    _dq.append(
        f"- price consistency: paying>bidding={ps['paying_gt_bidding']}, "
        f"floor>paying={ps['floor_gt_paying']}, floor>bidding={ps['floor_gt_bidding']}."
    )
    _neg = {c: ps[c + "_negative"] for c in PRICE_COLS if ps[c + "_negative"]}
    if _neg:
        _dq.append(f"- negative prices: {_neg}.")
    _dq.append("")

_temporal = []
for name in frames:
    by_season_day = {}
    for x in day_hour[name]:
        by_season_day.setdefault(x["season"], set()).add(x["day"])
    for s, dset in sorted(by_season_day.items()):
        ds = sorted(dset)
        _temporal.append(
            f"- {name} season {s}: {len(ds)} distinct days, {ds[0]}..{ds[-1]}"
        )

_entities = []
for name in frames:
    acd = prof[name]["acd"]
    _entities.append(
        f"**{name}** approx distinct: "
        + str(
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
            }
        )
    )
_entities.append("")
_entities.append("Concentration (top-N share of rows):")
for (name, c), v in concentration.items():
    _entities.append(
        f"- {name}.{c}: approx_distinct={v['approx_distinct']}, "
        f"top10={v['top10_share']:.4f}, top50={v['top50_share']:.4f}, "
        f"max_one_entity={v['max_rows_one_entity']}"
    )

_dist = ["Per-table price stats (min / max / avg / p50-95-99):"]
for name in frames:
    ps = price_stats[name]
    for c in PRICE_COLS:
        _dist.append(
            f"- {name}.{c}: {ps[c + '_min']} / {ps[c + '_max']} / {ps[c + '_avg']} / {ps[c + '_p']}"
        )

_rel = [
    (
        f"Training funnel: impression={imp}, click={clk}, conversion={conv} "
        f"(event_type raw values: {et_counts})."
    )
]
if et_unmapped:
    _rel.append(f"event_type values not mapped to a funnel stage: {et_unmapped}.")
if imp:
    _rel.append(
        f"CTR = {clk / imp:.6f}"
        + (f"; click->conversion = {conv / clk:.6f}" if clk else "")
        + f"; conversion/impression = {conv / imp:.8f}."
    )
if lb_labels:
    _rel.append(f"Leaderboard labels: {lb_labels}.")
_rel.append(
    "Per-season comparison (rows / advertisers / users / funnel): see season_cmp cell."
)
for name in frames:
    for x in season_cmp[name]:
        _rel.append(
            f"- {name} season {x['season']}: rows={x.get('rows')}, "
            f"advertisers={x.get('advertisers')}, users={x.get('users')}"
        )

_findings = []
_bad_const = {n: const_cols[n] for n in frames if const_cols[n]}
if _bad_const:
    _findings.append(f"- Constant columns per table: {_bad_const}.")
for name in frames:
    b = dup_breakdown[name]
    if b["conflicting"]:
        _findings.append(
            f"- {name}: {b['conflicting']} candidate-key groups have conflicting rows."
        )
    elif b["dup_groups"]:
        _findings.append(
            f"- {name}: {b['dup_groups']} duplicate key groups (all identical rows)."
        )
    ps = price_stats[name]
    if ps["floor_gt_paying"] or ps["paying_gt_bidding"]:
        _findings.append(
            f"- {name}: price ordering violations "
            f"(paying>bidding={ps['paying_gt_bidding']}, floor>paying={ps['floor_gt_paying']})."
        )
if imp and clk:
    _findings.append(
        f"- Training funnel is extremely top-heavy (CTR {clk / imp:.5f}, "
        f"conversion rate {conv / imp:.7f}) -> severe class imbalance for modelling."
    )
_findings_md = (
    "\n".join(_findings) if _findings else "No material data-quality issues found."
)

_silver = []
if _bad_const:
    _silver.append(f"- Drop constant columns {_bad_const}.")
for name in frames:
    b = dup_breakdown[name]
    if b["conflicting"]:
        _silver.append(
            f"- {name}: de-duplicate the candidate key with a deterministic rule."
        )
    elif b["dup_groups"]:
        _silver.append(
            f"- {name}: apply distinct to collapse identical duplicate rows."
        )
_silver.append(
    "- Parse timestamp (YYYYMMDDHHMMSSmmm) into a proper event timestamp in Silver."
)
_silver.append(
    "- Silver grain: one row per RTB event; keep season as a partition/attribute."
)
if any(
    price_stats[n]["floor_gt_paying"] or price_stats[n]["paying_gt_bidding"]
    for n in frames
):
    _silver.append("- Add price-ordering validity flags (floor<=paying<=bidding).")

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    [
        ("Profile", "\n".join(_prof)),
        ("Data Quality", "\n".join(_dq)),
        ("Temporal", "\n".join(_temporal)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Distributions", "\n".join(_dist)),
        ("Relationships", "\n".join(_rel)),
        ("EDA Findings", _findings_md),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=[
        (
            "iPinYou training impression -> click -> conversion funnel",
            "ipinyou_funnel.png",
        ),
        (
            "iPinYou -- rows per partition column, by table",
            "ipinyou_partitions.png",
        ),
        (
            "iPinYou leaderboard -- label distributions",
            "ipinyou_leaderboard_labels.png",
        ),
        (
            "iPinYou -- events per day by season, by table",
            "ipinyou_events_per_day_by_season.png",
        ),
        (
            "iPinYou -- event volume per hour, by table",
            "ipinyou_event_volume_per_hour.png",
        ),
        (
            "iPinYou -- price distributions (sampled, clipped to p99)",
            "ipinyou_price_distributions.png",
        ),
    ],
)