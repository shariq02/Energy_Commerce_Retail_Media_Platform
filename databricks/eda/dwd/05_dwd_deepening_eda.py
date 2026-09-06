# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- DWD DEEPENING MEASUREMENTS & MISSING VALUE PERIODS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the eight DWD deepening measurement Bronze tables
# MAGIC (dew_point, soil_temperature, visibility, cloud_type, wind_synop,
# MAGIC extreme_wind, weather_phenomena, solar) and the missing_value_periods
# MAGIC metadata table alongside the original seven measurements profiled in
# MAGIC 01_weather_measurements_eda.py -- schema, missingness, constant columns,
# MAGIC station coverage, hourly continuity, per-station duplicates, value
# MAGIC distributions, the QN quality-flag domain, per-decade regime evidence,
# MAGIC and the layered modelling-risk checklist. Column semantics (units,
# MAGIC plausibility bounds) are not assumed ahead of a live-schema read.

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
SOURCE = "dwd"
NB_KEY = "05_deepening"
SECTION_TITLE = "Deepening measurements (dew_point ... solar) & missing_value_periods"
MEASUREMENTS = [
    "dew_point",
    "soil_temperature",
    "visibility",
    "cloud_type",
    "wind_synop",
    "extreme_wind",
    "weather_phenomena",
    "solar",
]
TABLES = {m: f"{CATALOG}.{BRONZE_SCHEMA}.dwd_{m}" for m in MEASUREMENTS}
META_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.dwd_missing_value_periods"
NON_VALUE = {"STATIONS_ID", "CITY", "MESS_DATUM", "EOR"}
DWD_QN_CODES = {"1", "2", "3", "5", "7", "9", "10"}
# Companion columns that are indices / text / alternative timestamps, not
# measured values: a `*_I` index byte, a `*_Text` label, the true-solar-time
# `MESS_DATUM_WOZ`.
_NON_VALUE_SUFFIXES = ("_I", "_TEXT", "_WOZ")


def as_ts(col):
    s = F.regexp_replace(F.col(col).cast("string"), r"\.0$", "")
    # try_to_timestamp -> NULL on bad input; a plain to_timestamp with a format
    # THROWS CANNOT_PARSE_TIMESTAMP under ANSI.
    return F.coalesce(
        F.try_to_timestamp(s, F.lit("yyyyMMddHH")),
        F.try_to_timestamp(s, F.lit("yyyyMMddHHmm")),
        F.try_to_timestamp(F.substring(s, 1, 10), F.lit("yyyyMMddHH")),
    )


def qn_col(df):
    # QN suffix varies by parameter (QN_8, QN_7, QN_2, QN_592, ...) -- match by
    # prefix, not a fixed list.
    return next((c for c in df.columns if c.upper().startswith("QN")), None)


def value_columns(df):
    return [
        c
        for c in df.columns
        if c.upper() not in NON_VALUE
        and not c.upper().startswith("QN")
        and not c.upper().endswith(_NON_VALUE_SUFFIXES)
    ]


# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Profile -- rows, missingness, approx distinct, min/max ts, constant columns (one agg per table)
frames = {m: spark.table(t) for m, t in TABLES.items()}
prof = {}
for m in MEASUREMENTS:
    df = frames[m]
    cols = df.columns
    ts_col = find_col(df, "MESS_DATUM")
    exprs = [F.count(F.lit(1)).alias("__rows")]
    if ts_col:
        exprs += [F.min(ts_col).alias("__min_ts"), F.max(ts_col).alias("__max_ts")]
    for c in cols:
        miss = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
        exprs += [
            F.sum(miss.cast("long")).alias(c + "__m"),
            F.approx_count_distinct(c).alias(c + "__d"),
        ]
    r = df.agg(*exprs).first().asDict()
    prof[m] = {
        "cols": cols,
        "total": r["__rows"],
        "min_ts": r.get("__min_ts"),
        "max_ts": r.get("__max_ts"),
        "miss": {c: r[c + "__m"] for c in cols},
        "acd": {c: r[c + "__d"] for c in cols},
    }
    sid = find_col(df, "STATIONS_ID")
    print(
        "=" * 90,
        f"\n{m}  rows={r['__rows']}  {r.get('__min_ts')}..{r.get('__max_ts')}  "
        f"stations~={r.get(sid + '__d') if sid else 'n/a'}\n",
        "=" * 90,
    )
    for c in cols:
        rate = prof[m]["miss"][c] / prof[m]["total"] if prof[m]["total"] else 0
        print(
            f"  {c:<26} missing={prof[m]['miss'][c]:>12} rate={rate:.4f} approx_distinct={prof[m]['acd'][c]}"
        )
    print("constant columns:", [c for c in cols if prof[m]["acd"][c] <= 1])
totals = {m: prof[m]["total"] for m in MEASUREMENTS}
constant_cols = {
    m: [c for c in prof[m]["cols"] if prof[m]["acd"][c] <= 1] for m in MEASUREMENTS
}

# COMMAND ----------

# DBTITLE 1,Station coverage + per-station row counts (one groupBy per table)
station_counts = {}
for m in MEASUREMENTS:
    df = frames[m]
    sid = find_col(df, "STATIONS_ID")
    if sid is None:
        station_counts[m] = {}
        continue
    g = df.groupBy(sid).count().collect()
    station_counts[m] = {str(x[sid]): x["count"] for x in g}
    print(f"--- {m} ---", sorted(station_counts[m].items()))

# COMMAND ----------

# DBTITLE 1,QN quality-flag distribution + domain (one groupBy per table, where present)
qn_dist = {}
qn_domain = {}
for m in MEASUREMENTS:
    df = frames[m]
    qn = qn_col(df)
    if qn is None:
        continue
    qn_domain[m] = categorical_domain(df, qn, DWD_QN_CODES, name=f"{m}.{qn}")
    g = df.groupBy(qn).count().orderBy(F.desc("count")).collect()
    qn_dist[m] = [(x[qn], x["count"]) for x in g]
    print(f"--- {m} ({qn}) ---", qn_dist[m], " domain:", qn_domain[m])

# COMMAND ----------

# DBTITLE 1,Duplicate (station, timestamp) -- identical vs conflicting (one groupBy per table)
dup_breakdown = {}
for m in MEASUREMENTS:
    df = frames[m]
    sid, dts = find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")
    if sid is None or dts is None:
        continue
    cols = df.columns
    dk = df.groupBy(sid, dts).agg(
        F.count(F.lit(1)).alias("n"),
        F.countDistinct(F.hash(*[F.col(c) for c in cols])).alias("row_variants"),
    )
    b = (
        dk.agg(
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
    dup_breakdown[m] = b
    print(f"{m:<18} {b}")

# COMMAND ----------

# DBTITLE 1,Value columns -- range, percentiles, zero rows (one agg per table, no assumed plausibility bounds)
value_stats = {}
for m in MEASUREMENTS:
    df = frames[m]
    vcols = value_columns(df)
    exprs = []
    for c in vcols:
        v = safe_num(c)
        exprs += [
            F.min(v).alias(c + "_min"),
            F.max(v).alias(c + "_max"),
            F.avg(v).alias(c + "_mean"),
            F.stddev(v).alias(c + "_sd"),
            F.percentile_approx(v, [0.01, 0.5, 0.99]).alias(c + "_p"),
            F.sum((v == -999).cast("long")).alias(c + "_sentinel"),
            F.sum((v == 0).cast("long")).alias(c + "_zero"),
        ]
    value_stats[m] = df.agg(*exprs).first().asDict() if exprs else {}
    for c in vcols:
        print(
            f"{m}.{c:<14}",
            {
                k[len(c) + 1 :]: value_stats[m][k]
                for k in value_stats[m]
                if k.startswith(c + "_")
            },
        )

# COMMAND ----------

# DBTITLE 1,Hourly continuity vs an INDEPENDENT calendar + longest gap per station
freq_cov = {}
for m in MEASUREMENTS:
    df = frames[m]
    sid, dts = find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")
    if sid is None or dts is None:
        continue
    w = Window.partitionBy("station").orderBy("ts")
    per_station = (
        df.select(F.col(sid).alias("station"), as_ts(dts).alias("ts"))
        .where(F.col("ts").isNotNull())
        .distinct()
        .withColumn(
            "gap_h",
            (F.col("ts").cast("long") - F.lag("ts").over(w).cast("long")) / 3600 - 1,
        )
        .groupBy("station")
        .agg(
            F.min("ts").alias("min_ts"),
            F.max("ts").alias("max_ts"),
            F.count(F.lit(1)).alias("observed_hours"),
            F.max(F.when(F.col("gap_h") > 0, F.col("gap_h"))).alias(
                "longest_gap_hours"
            ),
        )
        .withColumn(
            "expected_hours",
            (
                (F.col("max_ts").cast("long") - F.col("min_ts").cast("long")) / 3600 + 1
            ).cast("long"),
        )
        .withColumn(
            "coverage_pct",
            F.round(F.col("observed_hours") / F.col("expected_hours") * 100, 2),
        )
        .orderBy("station")
    )
    freq_cov[m] = [x.asDict() for x in per_station.collect()]
    print(f"--- {m} ---", freq_cov[m])

# COMMAND ----------

# DBTITLE 1,Regime evidence -- per-decade rows / QN vocabulary
regime = {}
for m in MEASUREMENTS:
    df = frames[m]
    dts = find_col(df, "MESS_DATUM")
    qn = qn_col(df)
    if dts is None:
        continue
    decade = (F.floor(F.year(as_ts(dts)) / 10) * 10).cast("int")
    probe = [c for c in ([qn] if qn else []) + value_columns(df)]
    regime[m] = population_by_group(
        df.withColumn("__decade", decade).where(F.col("__decade").isNotNull()),
        "__decade",
        probe,
    )
    print(f"{m} by decade:", {d: dv["rows"] for d, dv in sorted(regime[m].items())})

# COMMAND ----------

# DBTITLE 1,missing_value_periods -- small metadata table, collected in Python
mvp = spark.table(META_TABLE)
mvp_recs = [x.asDict() for x in mvp.collect()]
mvp_cols = mvp.columns
mvp_total = len(mvp_recs)
mvp_dups = mvp_total - len({tuple(sorted(d.items())) for d in mvp_recs})
mvp_consts = [c for c in mvp_cols if len({d[c] for d in mvp_recs}) <= 1]
print(
    f"missing_value_periods  rows={mvp_total}  cols={mvp_cols}  "
    f"full_row_duplicates={mvp_dups}  constant_columns={mvp_consts}"
)
for c in mvp_cols:
    missing = sum(1 for d in mvp_recs if d[c] is None or str(d[c]).strip() == "")
    print(f"  {c:<30} missing={missing:>6}  distinct={len({d[c] for d in mvp_recs})}")

# COMMAND ----------

# DBTITLE 1,Figure -- deepening measurement overview (rows / stations)
figs = []
if facet_bars(
    {
        "rows per measurement": [(m, totals[m]) for m in MEASUREMENTS],
        "distinct stations": [(m, len(station_counts[m])) for m in MEASUREMENTS],
    },
    "DWD deepening -- measurement overview",
    "dwd_deepening_overview.png",
    rot=30,
    ncols=2,
):
    figs.append(("DWD deepening -- measurement overview", "dwd_deepening_overview.png"))

# COMMAND ----------

# DBTITLE 1,Figure -- QN distribution + hourly coverage %
if qn_dist and facet_bars(
    qn_dist,
    "DWD deepening -- QN quality-flag distribution per measurement",
    "dwd_deepening_qn_distribution.png",
    rot=0,
):
    figs.append(
        (
            "DWD deepening -- QN quality-flag distribution",
            "dwd_deepening_qn_distribution.png",
        )
    )
if facet_bars(
    {
        m: [(r["station"], r["coverage_pct"]) for r in freq_cov.get(m, [])]
        for m in MEASUREMENTS
    },
    "DWD deepening -- hourly coverage % per station, by measurement",
    "dwd_deepening_hourly_coverage_pct.png",
):
    figs.append(
        (
            "DWD deepening -- hourly coverage % per station",
            "dwd_deepening_hourly_coverage_pct.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Findings
findings_lines = []
for m in MEASUREMENTS:
    cov = [
        r["coverage_pct"] for r in freq_cov.get(m, []) if r["coverage_pct"] is not None
    ]
    worst_gap = max(
        (r["longest_gap_hours"] or 0 for r in freq_cov.get(m, [])), default=0
    )
    b = dup_breakdown.get(m, {})
    findings_lines.append(
        f"{m}: rows={totals[m]}, stations={len(station_counts[m])}, "
        f"hourly coverage {min(cov) if cov else 'n/a'}-{max(cov) if cov else 'n/a'}%, "
        f"longest gap {worst_gap}h, "
        f"dup identical={b.get('identical')}/conflicting={b.get('conflicting')}, "
        f"constant cols={constant_cols[m]}"
    )
print("\n".join(findings_lines))
print(
    f"missing_value_periods: rows={mvp_total}, duplicates={mvp_dups}, constant={mvp_consts}"
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/dwd.md
_profile = [
    "| measurement | rows | cols | stations | first ts | last ts | constant cols |",
    "|---|---|---|---|---|---|---|",
]
for m in MEASUREMENTS:
    _profile.append(
        f"| {m} | {totals[m]} | {len(prof[m]['cols'])} | {len(station_counts[m])} | "
        f"{prof[m]['min_ts']} | {prof[m]['max_ts']} | {', '.join(constant_cols[m]) or '-'} |"
    )
_profile.append(
    f"| missing_value_periods | {mvp_total} | {len(mvp_cols)} | - | - | - | "
    f"{', '.join(mvp_consts) or '-'} |"
)

_dq = [
    "| measurement | dup key groups | identical | conflicting |",
    "|---|---|---|---|",
]
for m in MEASUREMENTS:
    b = dup_breakdown.get(m, {})
    _dq.append(
        f"| {m} | {b.get('dup_groups')} | {b.get('identical')} | {b.get('conflicting')} |"
    )
_dq.append(f"\nmissing_value_periods full-row duplicates: {mvp_dups}.")

_domain = [
    "QN quality-flag values vs the DWD hourly-historical code set (1/2/3/5/7/9/10):"
]
for m in MEASUREMENTS:
    d = qn_domain.get(m)
    if not d:
        _domain.append(f"- {m}: no QN column.")
        continue
    _domain.append(
        f"- {d['column']}: unexpected={d['unexpected'] or 'none'}, unused={d['unused_allowed'] or 'none'}."
    )
_domain.append(
    "An unexpected QN value is a parse artefact or a schema drift, not a real quality level."
)

_temporal = [
    para(
        "Hourly grid, expected one row per station per hour; MESS_DATUM parsed",
        "yyyyMMddHH with a trailing `.0` stripped. Coverage vs an independent",
        "calendar (span/3600 + 1), so <100% is a genuine gap.",
    ),
]
for m in MEASUREMENTS:
    cov = [
        r["coverage_pct"] for r in freq_cov.get(m, []) if r["coverage_pct"] is not None
    ]
    worst = max((r["longest_gap_hours"] or 0 for r in freq_cov.get(m, [])), default=0)
    _temporal.append(
        f"- {m}: {prof[m]['min_ts']}..{prof[m]['max_ts']}, "
        f"per-station coverage {min(cov) if cov else 'n/a'}-{max(cov) if cov else 'n/a'}%, longest gap {worst}h"
    )

_regime = [
    para(
        "Per-decade row count and QN vocabulary. The deepening network came online",
        "at different times per parameter -- a decade/era indicator and per-station",
        "availability window are warranted before pooling.",
    ),
    "",
]
for m in MEASUREMENTS:
    if m not in regime:
        continue
    qn = qn_col(frames[m])
    row = ", ".join(
        f"{d}s: rows={dv['rows']}"
        + (f", QN distinct={dv['columns'].get(qn, {}).get('distinct')}" if qn else "")
        for d, dv in sorted(regime[m].items())
    )
    _regime.append(f"- {m}: {row}")

_dist = []
for m in MEASUREMENTS:
    for c in value_columns(frames[m]):
        vs = value_stats[m]
        _dist.append(
            f"- {m}.`{c}`: min/max={vs.get(c + '_min')}/{vs.get(c + '_max')}, "
            f"p01/p50/p99={vs.get(c + '_p')}, mean={vs.get(c + '_mean')}, sd={vs.get(c + '_sd')}, "
            f"zero rows={vs.get(c + '_zero')}, -999 sentinel rows={vs.get(c + '_sentinel')}"
        )

_qn = ["QN quality flag distribution (where present):"]
for m, ps in qn_dist.items():
    _qn.append(f"- {m}: {ps}")

_any_conflict = any(b.get("conflicting", 0) > 0 for b in dup_breakdown.values())
_any_sentinel = any(
    value_stats[m].get(c + "_sentinel", 0) > 0
    for m in MEASUREMENTS
    for c in value_columns(frames[m])
)
_silver = []
if _any_sentinel:
    _silver.append(
        "- `-999` (and blank) appears as a sentinel in at least one of these columns -> must become NULL before any stat, matching the original seven measurements."
    )
if _any_conflict:
    _silver.append(
        "- (STATIONS_ID, MESS_DATUM) has conflicting duplicate rows in at least one measurement -> a conflict-resolution rule is required (rule not yet established)."
    )
_silver += [
    "- Constant columns above carry no information.",
    "- Hourly series are not necessarily continuous (coverage % / gaps above) -> no dense-grid assumption.",
    "- Value-column plausibility bounds were not assumed here -> define them from the distributions above before an out-of-range quality flag.",
    "- missing_value_periods declares known gap windows per station/parameter -> reconcile against the observed hourly-coverage gaps rather than assuming every gap is undeclared.",
    "- Decode QN against the DWD scheme valid for the record's era (see Regime / Version Evidence).",
]

_no_target = para(
    "No candidate ML target lives in these 8 deepening tables -- like the original",
    "seven they feed the shared weather feature source; missing_value_periods (03)",
    "is the nearest candidate target for a missingness use case.",
)
_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            "One row per (STATIONS_ID, MESS_DATUM) per measurement -- split by STATIONS_ID or contiguous date range, never by row.",
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            "Cross-table cardinality between these 8 tables, the original seven, and metadata is assessed in 04 -- verify before joining both groups as features.",
        ),
        ("Target contamination", _no_target),
        (
            "Temporal / post-event leakage",
            "QN_* flags (where present) are set alongside the value -- a forecasting feature may use only rows strictly before the prediction timestamp.",
        ),
        (
            "Proxy leakage",
            "STATIONS_ID / city identify a specific site -- a model given them memorises the station.",
        ),
        (
            "Split / entity leakage",
            "Split by STATIONS_ID -- a station's adjacent hourly rows and its rows across measurements are correlated.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            "Station attributes are time-varying (02) -- join on the validity window, not the latest metadata row.",
        ),
        (
            "Survivorship / coverage bias",
            "The deepening network came online per parameter over time (Regime / Version Evidence) -- early years under-represent the newer parameters.",
        ),
        (
            "Missingness leakage",
            "-999 / blank rate correlates with station, parameter and era -- an 'is-missing' feature can leak an outage window.",
        ),
        (
            "Duplicate-event leakage",
            f"Conflicting (station, ts) duplicates: { {m: dup_breakdown.get(m, {}).get('conflicting') for m in MEASUREMENTS} } -- resolve before counting or splitting.",
        ),
        (
            "Target / feature temporal misalignment",
            "MESS_DATUM is the observation hour -- align a feature/target pair to one hour convention.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            "Units are not in Bronze and plausibility bounds are not assumed here -- reconcile via parameter_unit (02) before combining parameters.",
        ),
        (
            "Data-generation-process leakage",
            "QN_* encodes DWD's QC decision, not the physical weather -- a feature keyed on it encodes the QC pipeline.",
        ),
        (
            "Class / label instability",
            para(
                "cloud_type / weather_phenomena are coded categoricals and the QN scheme",
                "changed over the archive's history (Regime / Version Evidence) -- a class",
                "defined by a raw code is only stable within one scheme vintage.",
            ),
        ),
        (
            "Label availability lag",
            "DWD publishes historical data with a lag and revises it -- a nowcast cannot use the current hour.",
        ),
        (
            "Source / version / regime change",
            "Per-decade rows and QN vocabulary in Regime / Version Evidence -- a decade/era indicator is warranted before pooling.",
        ),
        (
            "Sample-vs-full divergence",
            "No value-column figure is drawn from a sample here; every reported stat (value_stats, freq_cov, station_counts) is a full-table Spark aggregation.",
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Data Quality", "\n".join(_dq) + "\n\n" + "\n".join(_qn)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Temporal", "\n".join(_temporal)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", "\n".join(f"- {ln}" for ln in findings_lines)),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
