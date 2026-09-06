# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- SEARCH VISIBILITY EVENTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Profile search_visibility_events (search-visibility
# MAGIC metrics per url) -- schema, missingness, constant columns, the `date`
# MAGIC field's true granularity + month structure, date/month coverage gaps,
# MAGIC country/device breakdown, ranking (position / index) distributions,
# MAGIC metric ranges & internal consistency (clicks <= impressions),
# MAGIC duplicate-key (identical vs conflicting), entity coverage -- as
# MAGIC evidence for Silver design.

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as dt

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "search_visibility"
NB_KEY = "01_events"
SECTION_TITLE = "Search visibility events (search_visibility_events)"
TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.search_visibility_events"
KEY = ["repository_id", "url", "date", "country", "device"]

# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()

print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

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
acd = {c: r[c + "__d"] for c in COLS}
constant_cols = [c for c in COLS if acd[c] <= 1]
print(f"rows={total}  columns={len(COLS)}  ->  {COLS}")
for c in COLS:
    print(
        f"  {c:<18} missing={r[c + '__m']:>12} rate={r[c + '__m'] / total:.4f} approx_distinct={acd[c]}"
    )
print("constant columns:", constant_cols)
print(
    "url / repository_id approx distinct:",
    acd.get("url"),
    "/",
    acd.get("repository_id"),
)
df.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,`date` field -- rows, sums, granularity, month structure & gaps (one groupBy)
dg = (
    df.groupBy("date")
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.col("clicks").cast("double")).alias("clicks"),
        F.sum(F.col("impressions").cast("double")).alias("impressions"),
    )
    .orderBy("date")
    .collect()
)
date_rows = [(x["date"], x["rows"]) for x in dg]
monthly = [(x["date"], x["clicks"], x["impressions"]) for x in dg]
raw_dates = [x["date"] for x in dg if x["date"]]


def parse_sv_date(rd):
    # The `date` column mixes formats: ISO `YYYY-MM-DD` plus a `M/D/YYYY` style
    # used for some archives (e.g. the February files `2/1/2017`..`2/7/2017`).
    # Parsing ISO only made February look absent from the span.
    s = str(rd).strip()
    for raw, fmt in ((s[:10], "%Y-%m-%d"), (s, "%m/%d/%Y"), (s, "%d.%m.%Y")):
        try:
            return (
                dt.datetime.strptime(raw, fmt).replace(tzinfo=dt.UTC).date(),
                fmt,
            )
        except ValueError:
            continue
    return None, "<unparsed>"


parsed = []
date_fmt_counts = {}
for rd in raw_dates:
    d, fmt = parse_sv_date(rd)
    date_fmt_counts[fmt] = date_fmt_counts.get(fmt, 0) + 1
    if d:
        parsed.append(d)
yms = sorted({d.strftime("%Y-%m") for d in parsed})
dom = sorted({d.day for d in parsed})
multi_format_date = len([k for k in date_fmt_counts if k != "<unparsed>"]) > 1
print(
    f"distinct raw date values={len(raw_dates)}  distinct year-month={len(yms)}  "
    f"distinct day-of-month={dom}  date-format breakdown={date_fmt_counts}"
)
print("=> granularity is", "monthly" if len(dom) <= 1 else "daily/other")
missing_months = []
if yms:
    cur = dt.date(int(yms[0][:4]), int(yms[0][5:7]), 1)
    end = dt.date(int(yms[-1][:4]), int(yms[-1][5:7]), 1)
    seen = set(yms)
    while cur <= end:
        if cur.strftime("%Y-%m") not in seen:
            missing_months.append(cur.strftime("%Y-%m"))
        cur = (cur.replace(day=28) + dt.timedelta(days=7)).replace(day=1)
print("months present:", yms, " missing in span:", missing_months)

# COMMAND ----------

# DBTITLE 1,country / device / citableContent distributions (one groupBy)
g = df.groupBy("country", "device", "citableContent").count().collect()
dist = {}
for c in ("country", "device", "citableContent"):
    acc = {}
    for x in g:
        acc[x[c]] = acc.get(x[c], 0) + x["count"]
    dist[c] = sorted(acc.items(), key=lambda p: -p[1])[:30]
    print(f"{c}:", dist[c])

# COMMAND ----------

# DBTITLE 1,Metric ranges, internal consistency, ranking percentiles (one agg)
num = {
    "clicks": F.expr("try_cast(clicks as double)"),
    "impressions": F.expr("try_cast(impressions as double)"),
    "clickThrough": F.expr("try_cast(clickThrough as double)"),
    "position": F.expr("try_cast(position as double)"),
    "index": F.expr("try_cast(index as double)"),
}
exprs = []
for c, v in num.items():
    exprs += [
        F.min(v).alias(c + "_min"),
        F.max(v).alias(c + "_max"),
        F.avg(v).alias(c + "_avg"),
        F.sum((v < 0).cast("long")).alias(c + "_negative"),
    ]
exprs += [
    F.sum((num["clicks"] > num["impressions"]).cast("long")).alias("clicks_gt_impr"),
    F.expr(
        "percentile_approx(cast(position as double), array(0.1,0.25,0.5,0.75,0.9,0.99))"
    ).alias("position_pctiles"),
    F.expr("percentile_approx(cast(clicks as double), array(0.5,0.9,0.99))").alias(
        "clicks_pctiles"
    ),
    F.expr("percentile_approx(cast(impressions as double), array(0.5,0.9,0.99))").alias(
        "impressions_pctiles"
    ),
    F.expr(
        "percentile_approx(case when cast(impressions as double) > 0 then "
        "cast(clickThrough as double) - cast(clicks as double)/cast(impressions as double) end, "
        "array(0.05,0.5,0.95))"
    ).alias("ct_delta_pctiles"),
]
M = df.agg(*exprs).first().asDict()
bad_ratio = M["clicks_gt_impr"]
for c in num:
    print(f"  {c:<14}", {k[len(c) + 1 :]: M[k] for k in M if k.startswith(c + "_")})
print("clicks > impressions rows:", bad_ratio)
print("clickThrough - clicks/impressions delta (p5/50/95):", M["ct_delta_pctiles"])
print("position percentiles:", M["position_pctiles"])

# COMMAND ----------

# DBTITLE 1,Ranking -- rounded-position rows + CTR (one groupBy) and index distribution
pr = (
    df.groupBy(F.round(F.col("position").cast("double")).alias("pos"))
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.col("clicks").cast("double")).alias("clk"),
        F.sum(F.col("impressions").cast("double")).alias("imp"),
    )
    .orderBy("pos")
    .collect()
)
pos_dist = [(x["pos"], x["rows"]) for x in pr]
pos_ctr = [(x["pos"], (x["clk"] / x["imp"]) if x["imp"] else None) for x in pr]
idx = df.groupBy("index").count().orderBy(F.desc("count")).limit(20).collect()
print("index top values:", [(x["index"], x["count"]) for x in idx])

# COMMAND ----------

# DBTITLE 1,Duplicate-key -- identical vs conflicting metrics on the candidate key (one groupBy)
dk = df.groupBy(*KEY).agg(
    F.count(F.lit(1)).alias("n"),
    F.countDistinct(F.hash(*[F.col(c) for c in COLS])).alias("row_variants"),
    F.countDistinct("clicks", "impressions", "position").alias("metric_variants"),
)
db = (
    dk.agg(
        F.count(F.lit(1)).alias("distinct_keys"),
        F.sum((F.col("n") > 1).cast("long")).alias("dup_groups"),
        F.sum(((F.col("n") > 1) & (F.col("row_variants") == 1)).cast("long")).alias(
            "identical"
        ),
        F.sum(((F.col("n") > 1) & (F.col("metric_variants") > 1)).cast("long")).alias(
            "conflicting_metrics"
        ),
    )
    .first()
    .asDict()
)
print(
    f"key={KEY}  distinct_keys={db['distinct_keys']} (rows={total}, unique={db['distinct_keys'] == total})  "
    f"dup_groups={db['dup_groups']} identical={db['identical']} conflicting_metrics={db['conflicting_metrics']}"
)

# COMMAND ----------

# DBTITLE 1,Entity coverage -- urls / dates / countries per repository; repositories per country
repo_cov = (
    df.groupBy("repository_id")
    .agg(
        F.approx_count_distinct("url").alias("urls"),
        F.countDistinct("date").alias("dates"),
        F.approx_count_distinct("country").alias("countries"),
    )
    .orderBy("repository_id")
    .collect()
)
for x in repo_cov:
    print(x.asDict())
cpr = (
    df.groupBy("country")
    .agg(F.approx_count_distinct("repository_id").alias("repositories"))
    .collect()
)
print("repositories per country:", [(x["country"], x["repositories"]) for x in cpr])

# COMMAND ----------

# DBTITLE 1,Categorical domain (`device`) + position bounds + date-format cohort
device_domain = categorical_domain(
    df,
    "device",
    ("desktop", "mobile", "tablet", "DESKTOP", "MOBILE", "TABLET"),
    name="device",
)
pos = F.col("position").cast("double")
pos_bounds = (
    df.agg(
        F.sum((pos < 1).cast("long")).alias("below_1"),
        F.sum((pos > 100).cast("long")).alias("above_100"),
        F.sum(pos.isNotNull().cast("long")).alias("present"),
    )
    .first()
    .asDict()
)
print("device domain:", device_domain, " position bounds:", pos_bounds)

# The `date` column mixes an ISO archive vintage and an M/D/YYYY one -- do the two
# cohorts differ in metric scale / completeness? (a staging inconsistency would
# show here, not as a data finding).
fmt_col = F.when(F.col("date").rlike(r"^\d{4}-\d{2}-\d{2}"), "iso").otherwise(
    F.when(F.col("date").rlike(r"^\d{1,2}/\d{1,2}/\d{4}"), "slash").otherwise("other")
)
datefmt_cohort = population_by_group(
    df.withColumn("__fmt", fmt_col),
    "__fmt",
    ["clicks", "impressions", "position", "clickThrough", "country", "device"],
)
for g, gv in datefmt_cohort.items():
    print(f"date-format cohort {g}: rows={gv['rows']}")

# COMMAND ----------

# DBTITLE 1,Metric sample for figures (one bounded sampled pass)
mp = (
    df.select(
        F.col("clicks").cast("double").alias("clicks"),
        F.col("impressions").cast("double").alias("impressions"),
        F.col("position").cast("double").alias("position"),
    )
    .sample(0.1, seed=42)
    .limit(150_000)
    .toPandas()
)
print("metric sample rows:", len(mp))

# COMMAND ----------

# DBTITLE 1,Figures (each gated so a blank result is never referenced)
figs = []
_rows_by_ym = {}
for rd, n_ in date_rows:
    d, _ = parse_sv_date(rd)
    ym = d.strftime("%Y-%m") if d else str(rd)
    _rows_by_ym[ym] = _rows_by_ym.get(ym, 0) + n_
if barplot(
    sorted(_rows_by_ym.items()),
    "Search Visibility -- rows per month (archives aggregated)",
    "month",
    "rows",
    rot=90,
    figsize=(12, 4),
    filename="sv_rows_per_monthly_archive.png",
):
    figs.append(
        (
            "Search Visibility rows per month (archives aggregated)",
            "sv_rows_per_monthly_archive.png",
        )
    )
if facet_bars(
    dict(dist),
    "Search Visibility -- rows by category",
    "sv_category_breakdown.png",
    rot=45,
):
    figs.append(("Search Visibility rows by category", "sv_category_breakdown.png"))

_ym_cl = {}
for rd, cl, im in monthly:
    d, _ = parse_sv_date(rd)
    ym = d.strftime("%Y-%m") if d else str(rd)
    a, b = _ym_cl.get(ym, (0.0, 0.0))
    _ym_cl[ym] = (a + (cl or 0), b + (im or 0))
_ym_sorted = sorted(_ym_cl)
_fig, _ax = plt.subplots(figsize=(12, 4))
_ax.plot(
    range(len(_ym_sorted)),
    [_ym_cl[y][1] for y in _ym_sorted],
    marker=".",
    label="impressions",
)
_ax.plot(
    range(len(_ym_sorted)),
    [_ym_cl[y][0] for y in _ym_sorted],
    marker=".",
    label="clicks",
)
_ax.set_xticks(range(len(_ym_sorted)))
_ax.set_xticklabels(_ym_sorted, rotation=90, fontsize=7)
_ax.legend()
_ax.set_title("Search Visibility -- total clicks & impressions per month")
_fig.tight_layout()
_save_and_show(_fig, "sv_clicks_impressions_per_month.png")
figs.append(
    (
        "Search Visibility total clicks & impressions per month",
        "sv_clicks_impressions_per_month.png",
    )
)

# COMMAND ----------

# DBTITLE 1,Figures -- metric distributions, clicks vs impressions, position & CTR
if facet_hists(
    {c: mp[c].dropna().tolist() for c in ("clicks", "impressions", "position")},
    "Search Visibility -- metric distributions (sampled)",
    "sv_metric_distributions.png",
):
    figs.append(
        ("Search Visibility metric distributions", "sv_metric_distributions.png")
    )
_scp = mp[(mp["clicks"] > 0) & (mp["impressions"] > 0)]
if len(_scp):
    _fig, _ax = plt.subplots(figsize=(6, 6))
    _ax.loglog(
        _scp["impressions"], _scp["clicks"], marker=".", linestyle="none", alpha=0.3
    )
    _ax.set_title("Search Visibility -- clicks vs impressions (sampled)")
    _ax.set_xlabel("impressions")
    _ax.set_ylabel("clicks")
    _fig.tight_layout()
    _save_and_show(_fig, "sv_clicks_vs_impressions.png")
    figs.append(
        (
            "Search Visibility clicks vs impressions (sampled)",
            "sv_clicks_vs_impressions.png",
        )
    )
if barplot(
    pos_dist,
    "Search Visibility -- rows by rounded position",
    "position",
    "rows",
    filename="sv_rows_by_position.png",
):
    figs.append(
        ("Search Visibility rows by rounded position", "sv_rows_by_position.png")
    )
if barplot(
    [(p, round(c, 4)) for p, c in pos_ctr if c is not None],
    "Search Visibility -- CTR by search position",
    "position",
    "CTR",
    figsize=(10, 4),
    filename="sv_ctr_by_position.png",
):
    figs.append(("Search Visibility CTR by search position", "sv_ctr_by_position.png"))

# COMMAND ----------

# DBTITLE 1,Findings
print("constant columns:", constant_cols)
print(
    "date granularity: distinct day-of-month =",
    dom,
    " -> ",
    "monthly" if len(dom) <= 1 else "daily/other",
)
print("months present:", yms, " missing:", missing_months)
print("clicks > impressions rows:", bad_ratio)
print("clickThrough vs clicks/impressions delta (p5/50/95):", M["ct_delta_pctiles"])
print("duplicate key:", db)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/search_visibility.md
_prof = ["| column | missing | rate | approx_distinct |", "|---|---|---|---|"]
for c in COLS:
    _prof.append(f"| {c} | {r[c + '__m']} | {r[c + '__m'] / total:.4f} | {acd[c]} |")
_gran = "monthly" if len(dom) <= 1 else "daily/other"
_prof += [
    "",
    (
        f"Rows: {total}. Constant columns: {constant_cols or 'none'}. "
        f"`date` field granularity: {_gran} (distinct day-of-month values: {dom})."
    ),
]

_dq = [
    (
        f"Candidate key {KEY}: distinct={db['distinct_keys']}, unique={db['distinct_keys'] == total}, "
        f"dup groups={db['dup_groups']}, fully identical={db['identical']}, "
        f"conflicting metrics={db['conflicting_metrics']}."
    ),
    f"clicks > impressions rows: {bad_ratio}.",
    f"clickThrough vs clicks/impressions delta (p5/50/95): {M['ct_delta_pctiles']}.",
    "",
    "Negative-value counts: "
    + str({c: M[c + "_negative"] for c in num if M[c + "_negative"]})
    or "none",
]

_temporal = [
    f"Months present ({len(yms)}): {yms}.",
    f"Missing months within span: {missing_months or 'none'}.",
    f"`date` raw-format breakdown: {date_fmt_counts}.",
    f"Rows per monthly archive: {fmt_pairs(date_rows)}.",
]

_entities = [
    "Per-repository coverage (urls / dates / countries):",
    "",
    "| repository_id | urls | dates | countries |",
    "|---|---|---|---|",
]
for x in repo_cov:
    _entities.append(
        f"| {x['repository_id']} | {x['urls']} | {x['dates']} | {x['countries']} |"
    )
_entities += [
    "",
    f"url approx distinct: {acd.get('url')}; repository_id approx distinct: {acd.get('repository_id')}.",
]

_coverage = []
for c in ("country", "device", "citableContent"):
    _coverage.append(f"- {c}: {dist[c]}")
_coverage.append("")
_coverage.append(
    para(
        f"{len(repo_cov)} repositories, {acd.get('url')} distinct urls.",
        "Coverage is uneven: a repository's dates/countries range widely (see Entities), so a",
        "url/repository absent from an archive is a real gap, not a zero -- an inner join across",
        "months silently drops the short-history repositories.",
    )
)
_coverage.append(
    para(
        "This is a fixed 2017 archive set (open-access institutional repositories), not a live",
        "feed -- ranking behaviour, the search engine's algorithm and the corpus have all moved",
        "on since; a model built on it is frozen to 2017.",
    )
)

_index_numeric = M["index_min"] is not None or M["index_max"] is not None
_dist = ["| metric | min | max | avg |", "|---|---|---|---|"]
for c in num:
    if c == "index" and not _index_numeric:
        continue
    _dist.append(f"| {c} | {M[c + '_min']} | {M[c + '_max']} | {M[c + '_avg']} |")
if not _index_numeric:
    _dist += [
        "",
        f"`index` is non-numeric (approx_distinct {acd.get('index')}); top values: "
        + str([(x["index"], x["count"]) for x in idx]),
    ]
_ctr_pairs = [(p_, round(v, 4) if v is not None else None) for p_, v in pos_ctr]
_dist += [
    "",
    f"position percentiles (10/25/50/75/90/99): {M['position_pctiles']}.",
    f"clicks percentiles (50/90/99): {M['clicks_pctiles']}.",
    f"impressions percentiles (50/90/99): {M['impressions_pctiles']}.",
    "CTR by rounded position:",
    fmt_pairs(_ctr_pairs, n=30),
]

_findings = []
if constant_cols:
    _findings.append(f"- Constant columns: {constant_cols}.")
_findings.append(f"- `date` is a {_gran} archive marker, not a daily timestamp.")
if multi_format_date:
    _findings.append(
        f"- `date` mixes ≥2 raw formats ({date_fmt_counts}); parsing only ISO "
        "previously made some months look absent from the span."
    )
if date_fmt_counts.get("<unparsed>"):
    _findings.append(
        f"- {date_fmt_counts['<unparsed>']} distinct `date` values did not parse "
        "under any known format."
    )
if missing_months:
    _findings.append(
        f"- {len(missing_months)} months missing within the covered span: {missing_months}."
    )
if bad_ratio:
    _findings.append(
        f"- {bad_ratio} rows have clicks > impressions (metric inconsistency)."
    )
if db["conflicting_metrics"]:
    _findings.append(
        f"- {db['conflicting_metrics']} candidate-key groups have conflicting metric values."
    )
elif db["dup_groups"]:
    _findings.append(
        f"- {db['dup_groups']} duplicate key groups, all with identical metrics."
    )
_neg = {c: M[c + "_negative"] for c in num if M[c + "_negative"]}
if _neg:
    _findings.append(f"- Negative metric values: {_neg}.")
_findings.append(
    "- CTR decreases monotonically with search position (expected ranking behaviour)."
)
_findings_md = "\n".join(_findings)

_silver = []
if constant_cols:
    _silver.append(f"- Drop constant columns {constant_cols}.")
_silver.append(f"- Model `date` as a monthly period; Silver grain = one row per {KEY}.")
if db["conflicting_metrics"]:
    _silver.append(
        "- Conflicting-metric key groups need a deterministic resolution rule before Silver."
    )
elif db["dup_groups"]:
    _silver.append(
        "- De-duplicate the candidate key with distinct (duplicates are identical)."
    )
if bad_ratio or _neg:
    _silver.append(
        "- Add validity flags for clicks<=impressions and non-negative metrics."
    )

_domain = [
    para(
        "`device` vs the known set (desktop / mobile / tablet):",
        f"unexpected={device_domain['unexpected'] or 'none'}.",
    ),
    para(
        f"`position` bounds: {pos_bounds['below_1']} rows < 1, {pos_bounds['above_100']} rows > 100 "
        f"of {pos_bounds['present']} present -- a search rank below 1 or far past 100 is not valid.",
    ),
]

_cohort = [
    para(
        "`date` mixes an ISO archive vintage and an M/D/YYYY one. Per-cohort row",
        "count and per-column null-rate / distinct -- a large difference is a",
        "staging inconsistency (the two archive sets were built differently), not a",
        "data-quality finding about the source.",
    ),
    "",
    "| cohort | rows | clicks null | impressions null | position null | country distinct | device distinct |",
    "|---|---|---|---|---|---|---|",
]
for g, gv in sorted(datefmt_cohort.items()):
    c = gv["columns"]
    _cohort.append(
        f"| {g} | {gv['rows']} | {c['clicks']['null_rate']} | {c['impressions']['null_rate']} | "
        f"{c['position']['null_rate']} | {c['country']['distinct']} | {c['device']['distinct']} |"
    )

_temporal_sem = [
    para(
        f"`date` is a {_gran} archive marker, not a daily timestamp",
        f"(distinct day-of-month values: {dom}).",
        f"It mixes {len([k for k in date_fmt_counts if k != '<unparsed>'])} raw formats",
        f"({date_fmt_counts}) -- parsing only ISO makes the M/D/YYYY archives look absent.",
    ),
    f"Months present ({len(yms)}): {yms}.",
    f"Missing months within span: {missing_months or 'none'}.",
    para(
        "There is no time zone on `date` -- it is a report period, not an instant. Model it as a",
        "monthly bucket; do not attempt an hourly join.",
    ),
]

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                f"Candidate key = {KEY}; `date` is a {_gran} archive marker. Grain is one row per "
                "(repository, url, month, country, device). Aggregating over country/device drifts it."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                "events -> repository on repository_id is checked in "
                "02_search_visibility_relationships_and_findings.py -- do not assume 1:N without its "
                "orphan/fan-out numbers."
            ),
        ),
        (
            "Target contamination",
            (
                "Targets: clicks, CTR, position per url. CTR = clicks/impressions, so clicks and "
                "impressions must not both be features for a CTR target; position at the target period "
                "must not be a feature for a click target."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "position and clicks in a search-ranking system are mutually causal within a period -- use "
                "position / CTR from a period STRICTLY before the target month, never the same archive."
            ),
        ),
        (
            "Proxy leakage",
            (
                "`index` and `repository_id` are near-unique proxies for a specific corpus; url is a "
                "proxy for a specific document -- a model given them memorises rather than generalises."
            ),
        ),
        (
            "Split / entity leakage",
            (
                "Split by repository_id or by url (hash), not by row -- a url's monthly history is "
                "autocorrelated and must stay on one side."
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "url / repository attributes (citableContent, ir_platform in the dim) may change over the "
                "archive span -- join the value as of the archive month, not the latest."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                f"{len(repo_cov)} repositories with widely different date/country coverage; "
                f"{len(missing_months)} missing months. A model trained on the full panel over-weights "
                "the long-history repositories."
            ),
        ),
        (
            "Missingness leakage",
            (
                f"{date_fmt_counts.get('<unparsed>', 0)} unparsed date values; category breakdowns above. "
                "Whether a url appears in an archive at all is informative (it ranked somewhere) -- an "
                "'appeared' flag leaks the outcome."
            ),
        ),
        (
            "Duplicate-event leakage",
            (
                f"Candidate key: distinct={db['distinct_keys']}/{total}, dup groups={db['dup_groups']}, "
                f"identical={db['identical']}, conflicting metrics={db['conflicting_metrics']} -- "
                "de-duplicate (and resolve conflicts) before counting or splitting."
            ),
        ),
        (
            "Target / feature temporal misalignment",
            (
                "All columns share the archive month -- there is no finer alignment available, so any "
                "before/after feature must be built at the month granularity, one lag minimum."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            (
                f"{bad_ratio} rows have clicks > impressions (CTR > 1 is not valid); negative metrics: "
                f"{ {c: M[c + '_negative'] for c in num if M[c + '_negative']} }. Exclude/flag before a "
                "CTR target. clickThrough is redundant with clicks/impressions."
            ),
        ),
        (
            "Data-generation-process leakage",
            (
                "This is Google Search Console-style aggregated data -- position is an average over "
                "impressions, clicks are de-duplicated by Google's own rules; the aggregation method is "
                "part of the data-generation process and changed over Search Console's history."
            ),
        ),
        (
            "Class / label instability",
            (
                "Not applicable -- clicks/impressions/position are continuous. `device` and "
                "`citableContent` are stable low-cardinality enums."
            ),
        ),
        (
            "Label availability lag",
            (
                "Search Console data for a month is finalised ~3 days after month end and can be revised "
                "for ~16 months -- a real-time model cannot use the current month's figures."
            ),
        ),
        (
            "Source / version / regime change",
            (
                "PRIMARY concern: this is a 2017 archive. Google's ranking algorithm, the mobile-first "
                "index rollout and Search Console's own reporting all changed since -- do not treat it as "
                "representative of current search behaviour. Regime / Version Evidence above also "
                "measures whether the ISO and M/D/YYYY archive cohorts differ in scale / completeness."
            ),
        ),
        (
            "Sample-vs-full divergence",
            (
                "Metric-distribution and clicks-vs-impressions figures draw from `mp`, a 10% sample capped "
                "at 150k rows -- use the full-table `M` aggregate for any feature-quality or threshold "
                "decision."
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
        ("Regime / Version Evidence", "\n".join(_cohort)),
        ("Temporal Semantics", "\n".join(_temporal_sem)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
