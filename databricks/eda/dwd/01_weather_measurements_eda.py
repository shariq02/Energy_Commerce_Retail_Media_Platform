# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- DWD WEATHER MEASUREMENTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the seven DWD weather-measurement Bronze tables
# MAGIC (air_temperature ... wind) -- schema, missingness, quality flags,
# MAGIC constant columns, temporal coverage & frequency against an independent
# MAGIC hourly grid, station x measurement coverage, per-station duplicates
# MAGIC (identical vs conflicting), value distributions & plausibility, the
# MAGIC QN quality-flag domain, per-decade regime evidence, and the layered
# MAGIC modelling-risk checklist -- as evidence for Silver design.

# COMMAND ----------

# DBTITLE 1,Imports
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "dwd"
NB_KEY = "01_weather_measurements"
SECTION_TITLE = "Weather measurements (air_temperature ... wind)"
MEASUREMENTS = [
    "air_temperature",
    "cloudiness",
    "moisture",
    "precipitation",
    "pressure",
    "sun",
    "wind",
]
TABLES = {m: f"{CATALOG}.{BRONZE_SCHEMA}.dwd_{m}" for m in MEASUREMENTS}
NON_VALUE = {"STATIONS_ID", "CITY", "MESS_DATUM", "EOR"}


# Any `QN*` column is a DWD quality byte, never a measured value. The numeric
# suffix varies by parameter (QN_9, QN_8, QN_7, QN_3, QN_592, QN_2, ...), so
# detect by prefix rather than a fixed list.
def qn_col(df):
    return next((c for c in df.columns if c.upper().startswith("QN")), None)


# DWD companion "Messverfahren-Index" columns -- string indicators, not measured
# values; treating them as numeric produced all-None distribution rows.
INDICATOR_COLS = {"V_N_I"}
# DWD hourly-historical quality levels (Qualitaetsniveau). A QN value outside
# this set is either a parse artefact or a schema change, not a real level.
DWD_QN_CODES = {"1", "2", "3", "5", "7", "9", "10"}

# Physical plausibility windows for the common DWD hourly parameters (values
# outside, and not the -999 sentinel, are "suspicious" not necessarily wrong).
PLAUSIBLE = {
    "TT_TU": (-40.0, 45.0),
    "RF_TU": (0.0, 100.0),
    "TF_STD": (-40.0, 45.0),
    "P": (900.0, 1080.0),
    "P0": (900.0, 1080.0),
    "N": (0.0, 8.0),
    "V_N": (0.0, 8.0),
    "R1": (0.0, 200.0),
    "RS_IND": (0.0, 1.0),
    "SD_SO": (0.0, 60.0),
    "F": (0.0, 60.0),
    "D": (0.0, 360.0),
}

# COMMAND ----------

# DBTITLE 1,Helpers -- DWD hourly timestamp + value-column selection


def as_ts(col):
    # MESS_DATUM is yyyyMMddHH, but a double-inferred column arrives as
    # "2025021300.0" -- to_timestamp(x, "yyyyMMddHH") then parses NOTHING. Strip a
    # trailing ".0", then try the compact hour / minute forms.
    s = F.regexp_replace(F.col(col).cast("string"), r"\.0$", "")
    # try_to_timestamp -> NULL on bad input; a plain to_timestamp with a format
    # THROWS CANNOT_PARSE_TIMESTAMP under ANSI (e.g. a stray "1981010723:38").
    return F.coalesce(
        F.try_to_timestamp(s, F.lit("yyyyMMddHH")),
        F.try_to_timestamp(s, F.lit("yyyyMMddHHmm")),
        F.try_to_timestamp(F.substring(s, 1, 10), F.lit("yyyyMMddHH")),
    )


def value_columns(df):
    return [
        c
        for c in df.columns
        if c.upper() not in NON_VALUE
        and c.upper() not in INDICATOR_COLS
        and not c.upper().startswith("QN")
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
    exprs = [
        F.count(F.lit(1)).alias("__rows"),
        F.min(find_col(df, "MESS_DATUM")).alias("__min_ts"),
        F.max(find_col(df, "MESS_DATUM")).alias("__max_ts"),
    ]
    for c in cols:
        miss = F.col(c).isNull() | (F.trim(F.col(c)) == "")
        exprs += [
            F.sum(miss.cast("long")).alias(c + "__m"),
            F.approx_count_distinct(c).alias(c + "__d"),
        ]
    r = df.agg(*exprs).first().asDict()
    prof[m] = {
        "cols": cols,
        "total": r["__rows"],
        "min_ts": r["__min_ts"],
        "max_ts": r["__max_ts"],
        "miss": {c: r[c + "__m"] for c in cols},
        "acd": {c: r[c + "__d"] for c in cols},
    }
    sid, city = find_col(df, "STATIONS_ID"), find_col(df, "city")
    print(
        "=" * 90,
        f"\n{m}  rows={r['__rows']}  {r['__min_ts']}..{r['__max_ts']}  "
        f"stations~={r[sid + '__d']}  cities~={r[city + '__d']}\n",
        "=" * 90,
    )
    for c in cols:
        print(
            f"  {c:<26} missing={r[c + '__m']:>12} rate={r[c + '__m'] / r['__rows']:.4f} approx_distinct={r[c + '__d']}"
        )
    print("constant columns:", [c for c in cols if r[c + "__d"] <= 1])
totals = {m: prof[m]["total"] for m in MEASUREMENTS}
coverage = {
    m: {
        "stations": prof[m]["acd"][find_col(frames[m], "STATIONS_ID")],
        "cities": prof[m]["acd"][find_col(frames[m], "city")],
        "min_ts": prof[m]["min_ts"],
        "max_ts": prof[m]["max_ts"],
    }
    for m in MEASUREMENTS
}
constant_cols = {
    m: [c for c in prof[m]["cols"] if prof[m]["acd"][c] <= 1] for m in MEASUREMENTS
}

# COMMAND ----------

# DBTITLE 1,Station / city breakdown + per-station row counts (one groupBy per table)
station_counts = {}
for m in MEASUREMENTS:
    df = frames[m]
    city, sid = find_col(df, "city"), find_col(df, "STATIONS_ID")
    g = df.groupBy(city, sid).count().collect()
    per_station = {}
    for x in g:
        per_station[str(x[sid])] = per_station.get(str(x[sid]), 0) + x["count"]
    station_counts[m] = per_station
    print(f"--- {m} ---", sorted((x[city], x[sid], x["count"]) for x in g))

# COMMAND ----------

# DBTITLE 1,QN quality-flag vs missingness / out-of-range (one groupBy per table)
qn_dist = {}
qn_quality = {}
qn_domain = {}
for m in MEASUREMENTS:
    df = frames[m]
    qn = qn_col(df)
    vcols = value_columns(df)
    if qn is None:
        continue
    qn_domain[m] = categorical_domain(df, qn, DWD_QN_CODES, name=f"{m}.{qn}")
    any_sentinel = F.lit(False)
    any_oor = F.lit(False)
    for c in vcols:
        v = safe_num(c)
        any_sentinel = any_sentinel | (v == -999)
        b = PLAUSIBLE.get(c.upper())
        if b:
            any_oor = any_oor | ((v != -999) & ((v < b[0]) | (v > b[1])))
    g = (
        df.groupBy(qn)
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(any_sentinel.cast("long")).alias("rows_with_-999"),
            F.sum(any_oor.cast("long")).alias("rows_out_of_range"),
        )
        .orderBy(F.desc("rows"))
        .collect()
    )
    qn_dist[m] = [(x[qn], x["rows"]) for x in g]
    qn_quality[m] = [x.asDict() for x in g]
    print(f"--- {m} ({qn}) ---", qn_quality[m], " domain:", qn_domain[m])

# COMMAND ----------

# DBTITLE 1,Duplicate (station, timestamp) -- identical vs conflicting (one groupBy per table)
dup_breakdown = {}
for m in MEASUREMENTS:
    df = frames[m]
    key = [find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")]
    dup_breakdown[m] = dup_key_composition(df, key)
    print(f"{m:<18} {dup_breakdown[m]}")

# COMMAND ----------

# DBTITLE 1,Value columns -- range, sentinel, percentiles, plausibility (one agg per table)
value_stats = {}
for m in MEASUREMENTS:
    df = frames[m]
    exprs = []
    for c in value_columns(df):
        v = safe_num(c)
        b = PLAUSIBLE.get(c.upper())
        exprs += [
            F.min(v).alias(c + "_min"),
            F.max(v).alias(c + "_max"),
            F.min(F.when(v != -999, v)).alias(c + "_min_ns"),
            F.max(F.when(v != -999, v)).alias(c + "_max_ns"),
            F.sum((v == -999).cast("long")).alias(c + "_sentinel"),
            F.avg(F.when(v != -999, v)).alias(c + "_mean"),
            F.stddev(F.when(v != -999, v)).alias(c + "_sd"),
            F.percentile_approx(F.when(v != -999, v), [0.01, 0.5, 0.99]).alias(
                c + "_p"
            ),
            F.sum((v == 0).cast("long")).alias(c + "_zero"),
            *(
                [
                    F.sum(((v != -999) & ((v < b[0]) | (v > b[1]))).cast("long")).alias(
                        c + "_oor"
                    )
                ]
                if b
                else []
            ),
        ]
    value_stats[m] = df.agg(*exprs).first().asDict()
    for c in value_columns(df):
        print(
            f"{m}.{c:<12}",
            {
                k[len(c) + 1 :]: value_stats[m][k]
                for k in value_stats[m]
                if k.startswith(c + "_")
            },
        )

# COMMAND ----------

# DBTITLE 1,Special values -- which candidate codes occur, in which columns
# Candidate codes are values that look like codes rather than measurements (round
# extremes seen in the value ranges). None is assumed to be a missing-value
# marker: the checks below record where each one occurs and let the data speak.
CANDIDATES = (-999.0, -99.9, -99.0, -9.9, -9.0, -1.0, 990.0, 999.0, 9999.0)
special = {}
for m in MEASUREMENTS:
    df = frames[m]
    cols = value_columns(df)
    exprs = [F.count(F.lit(1)).alias("rows")]
    for i, c in enumerate(cols):
        v = safe_num(c)
        exprs += [
            F.sum((v == cand).cast("long")).alias(f"n_{i}_{j}")
            for j, cand in enumerate(CANDIDATES)
        ]
    r = df.agg(*exprs).first().asDict()
    special[m] = {
        "rows": r["rows"],
        "hits": [
            (c, cand, r[f"n_{i}_{j}"])
            for i, c in enumerate(cols)
            for j, cand in enumerate(CANDIDATES)
            if r[f"n_{i}_{j}"]
        ],
    }
    print(m, special[m]["hits"])

# COMMAND ----------

# DBTITLE 1,Special values -- quality level, decade, station and co-occurrence per hit
special_detail = {}
for m in MEASUREMENTS:
    hits = special[m]["hits"]
    if not hits:
        continue
    df = frames[m]
    qn, sid, dts = qn_col(df), find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")
    flags = [(safe_num(c) == cand) for c, cand, _ in hits]

    def by(group_col, df=df, flags=flags):
        return (
            df.groupBy(group_col.alias("g"))
            .agg(
                F.count(F.lit(1)).alias("rows"),
                *[F.sum(fl.cast("long")).alias(f"h{i}") for i, fl in enumerate(flags)],
            )
            .collect()
        )

    decade = (F.floor(F.year(as_ts(dts)) / 10) * 10).cast("int")
    by_qn = by(F.col(qn)) if qn else []
    by_dec = by(decade)
    by_st = by(F.col(sid))
    any_special = None
    for c in value_columns(df):
        term = safe_num(c).isin(list(CANDIDATES)).cast("int")
        any_special = term if any_special is None else any_special + term
    bundle = [
        (x["k"], x["count"])
        for x in df.groupBy(any_special.alias("k")).count().orderBy("k").collect()
    ]
    special_detail[m] = {"bundle": bundle, "hits": []}
    for i, (c, cand, n) in enumerate(hits):
        top_st = sorted(
            ((x["g"], x[f"h{i}"]) for x in by_st), key=lambda t: -(t[1] or 0)
        )[:3]
        special_detail[m]["hits"].append(
            {
                "column": c,
                "code": cand,
                "rows": n,
                "share": round(n / special[m]["rows"], 5),
                "by_qn": [(x["g"], x[f"h{i}"], x["rows"]) for x in by_qn if x[f"h{i}"]],
                "by_decade": [
                    (x["g"], x[f"h{i}"])
                    for x in sorted(by_dec, key=lambda t: (t["g"] is None, t["g"]))
                    if x[f"h{i}"]
                ],
                "top_stations": top_st,
                "top3_station_share": round(sum(t[1] or 0 for t in top_st) / n, 3)
                if n
                else None,
            }
        )
    print(
        m,
        special_detail[m]["bundle"],
        [h["column"] + "=" + str(h["code"]) for h in special_detail[m]["hits"]],
    )

# COMMAND ----------

# DBTITLE 1,Value range once every candidate code is set aside
clean_range = {}
for m in MEASUREMENTS:
    df = frames[m]
    exprs = []
    cols = value_columns(df)
    for c in cols:
        v = F.when(~safe_num(c).isin(list(CANDIDATES)), safe_num(c))
        exprs += [F.min(v).alias(c + "_min"), F.max(v).alias(c + "_max")]
    r = df.agg(*exprs).first().asDict()
    clean_range[m] = {c: (r[c + "_min"], r[c + "_max"]) for c in cols}
    print(m, clean_range[m])

# COMMAND ----------

# DBTITLE 1,Seasonal (month) and diurnal (hour of day) profiles, as stored
PROFILE_COLS = {
    "air_temperature": ["TT_TU", "RF_TU"],
    "cloudiness": ["V_N"],
    "precipitation": ["R1"],
    "pressure": ["P"],
    "sun": ["SD_SO"],
    "wind": ["F"],
}
profiles = {}
for m, cols in PROFILE_COLS.items():
    df = frames[m]
    cols = [c for c in cols if c in df.columns]
    if not cols:
        continue
    ts = as_ts(find_col(df, "MESS_DATUM"))
    aggs = [
        F.avg(F.when(~safe_num(c).isin(list(CANDIDATES)), safe_num(c))).alias(c)
        for c in cols
    ]
    profiles[m] = {}
    for key, k in (("month", F.month(ts)), ("hour", F.hour(ts))):
        rows = (
            df.groupBy(k.alias("k"))
            .agg(*aggs)
            .where(F.col("k").isNotNull())
            .orderBy("k")
            .collect()
        )
        profiles[m][key] = {
            c: [(int(x["k"]), round(x[c], 2)) for x in rows if x[c] is not None]
            for c in cols
        }
    print(m, {k: {c: len(v) for c, v in d.items()} for k, d in profiles[m].items()})

# COMMAND ----------

# DBTITLE 1,Hourly continuity vs an INDEPENDENT calendar + longest gap per station
freq_cov = {}
for m in MEASUREMENTS:
    df = frames[m]
    sid, dts = find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")
    w = Window.partitionBy("station").orderBy("ts")
    # No pre-.distinct() shuffle: duplicate (station, hour) rows make lag() give
    # gap_h <= 0, which the `> 0` filters drop; observed_hours uses
    # countDistinct. Saves a full 91M-row shuffle per table.
    per_station = (
        df.select(F.col(sid).alias("station"), as_ts(dts).alias("ts"))
        .where(F.col("ts").isNotNull())
        .withColumn(
            "gap_h",
            (F.col("ts").cast("long") - F.lag("ts").over(w).cast("long")) / 3600 - 1,
        )
        .groupBy("station")
        .agg(
            F.min("ts").alias("min_ts"),
            F.max("ts").alias("max_ts"),
            F.countDistinct("ts").alias("observed_hours"),
            F.max(F.when(F.col("gap_h") > 0, F.col("gap_h"))).alias(
                "longest_gap_hours"
            ),
            F.sum(F.when(F.col("gap_h") > 0, F.col("gap_h")).otherwise(0)).alias(
                "total_missing_hours"
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

# DBTITLE 1,Regime evidence -- per-decade station count / QN vocabulary / value-column population
regime = {}
for m in MEASUREMENTS:
    df = frames[m]
    dts = find_col(df, "MESS_DATUM")
    qn = qn_col(df)
    decade = (F.floor(F.year(as_ts(dts)) / 10) * 10).cast("int")
    probe = [c for c in ([qn] if qn else []) + value_columns(df)]
    by_decade = population_by_group(
        df.withColumn("__decade", decade).where(F.col("__decade").isNotNull()),
        "__decade",
        probe,
    )
    regime[m] = by_decade
    print(f"{m} by decade:")
    for d, dv in sorted(by_decade.items()):
        qd = dv["columns"].get(qn, {}).get("distinct") if qn else None
        print(f"  {d}: rows={dv['rows']}  QN distinct={qd}")

# COMMAND ----------

# DBTITLE 1,Station x measurement coverage matrix (reuses per-station row counts)
all_stations = sorted({s for m in MEASUREMENTS for s in station_counts[m]})
coverage_matrix = [
    {"station": s, **{m: station_counts[m].get(s, 0) for m in MEASUREMENTS}}
    for s in all_stations
]
for row in coverage_matrix:
    print(row)

# COMMAND ----------

# DBTITLE 1,Value-column sample for histograms / box plots (one sampled pass per table)
value_pdf = {}
for m in MEASUREMENTS:
    df = frames[m]
    vcols = value_columns(df)
    value_pdf[m] = (
        df.select(*[F.when(safe_num(c) != -999, safe_num(c)).alias(c) for c in vcols])
        .sample(0.05, seed=42)
        .limit(150_000)
        .toPandas()
    )
    print(f"{m} value sample rows: {len(value_pdf[m])}")

# COMMAND ----------

# DBTITLE 1,Figure -- measurement overview (rows / stations / cities / year span)
figs = []
if facet_bars(
    {
        "rows per measurement": [(m, totals[m]) for m in MEASUREMENTS],
        "distinct stations": [(m, coverage[m]["stations"]) for m in MEASUREMENTS],
        "distinct cities": [(m, coverage[m]["cities"]) for m in MEASUREMENTS],
        "observation years spanned": [
            (
                m,
                int(str(coverage[m]["max_ts"])[:4])
                - int(str(coverage[m]["min_ts"])[:4])
                + 1,
            )
            for m in MEASUREMENTS
        ],
    },
    "DWD -- measurement overview",
    "dwd_measurement_overview.png",
    rot=30,
    ncols=2,
):
    figs.append(("DWD measurement overview", "dwd_measurement_overview.png"))

# COMMAND ----------

# DBTITLE 1,Figure -- QN distribution, dup composition, coverage %, longest gap
if facet_bars(
    qn_dist,
    "DWD -- QN quality-flag distribution per measurement",
    "dwd_qn_distribution.png",
    rot=0,
):
    figs.append(("DWD QN quality-flag distribution", "dwd_qn_distribution.png"))

_dup_pairs = {
    "fully-identical repeat groups": [
        (m, dup_breakdown[m]["identical"]) for m in MEASUREMENTS
    ],
    "keys with conflicting rows": [
        (m, dup_breakdown[m]["conflicting"]) for m in MEASUREMENTS
    ],
}
if facet_bars(
    _dup_pairs,
    "DWD -- duplicate key composition",
    "dwd_duplicate_key_composition.png",
    rot=30,
):
    figs.append(("DWD duplicate key composition", "dwd_duplicate_key_composition.png"))

if facet_bars(
    {m: [(r["station"], r["coverage_pct"]) for r in freq_cov[m]] for m in MEASUREMENTS},
    "DWD -- hourly coverage % per station, by measurement",
    "dwd_hourly_coverage_pct.png",
):
    figs.append(("DWD hourly coverage % per station", "dwd_hourly_coverage_pct.png"))

if facet_bars(
    {
        m: [(r["station"], r["longest_gap_hours"] or 0) for r in freq_cov[m]]
        for m in MEASUREMENTS
    },
    "DWD -- longest missing-hours gap per station, by measurement",
    "dwd_longest_gap_hours.png",
):
    figs.append(
        ("DWD longest missing-hours gap per station", "dwd_longest_gap_hours.png")
    )

# COMMAND ----------

# DBTITLE 1,Figure -- station x measurement coverage heatmap
grid = np.array(
    [[row[m] for m in MEASUREMENTS] for row in coverage_matrix], dtype=float
)
if grid.size:
    fig, ax = plt.subplots(figsize=(9, max(3, 0.4 * len(all_stations))))
    ax.imshow(
        np.where(grid > 0, np.log10(grid + 1), np.nan), aspect="auto", cmap="viridis"
    )
    ax.set_xticks(range(len(MEASUREMENTS)))
    ax.set_xticklabels(MEASUREMENTS, rotation=45, ha="right")
    ax.set_yticks(range(len(all_stations)))
    ax.set_yticklabels(all_stations)
    ax.set_title("DWD -- station x measurement coverage (log10 row count)")
    fig.tight_layout()
    _save_and_show(fig, "dwd_station_x_measurement_coverage.png")
    figs.append(
        ("DWD station x measurement coverage", "dwd_station_x_measurement_coverage.png")
    )

# COMMAND ----------

# DBTITLE 1,Figure -- value column spread per measurement (sampled, sentinel excluded)
if facet_hists(
    {
        f"{m}.{c}": value_pdf[m][c].dropna().tolist()
        for m in MEASUREMENTS
        for c in value_pdf[m].columns
        if value_pdf[m][c].notna().any()
    },
    "DWD -- value column distribution per measurement (sampled)",
    "dwd_value_column_spread.png",
    ncols=4,
):
    figs.append(
        ("DWD value column spread per measurement", "dwd_value_column_spread.png")
    )

# COMMAND ----------

# DBTITLE 1,Findings
findings_lines = []
for m in MEASUREMENTS:
    cov = [r["coverage_pct"] for r in freq_cov[m] if r["coverage_pct"] is not None]
    worst_gap = max((r["longest_gap_hours"] or 0 for r in freq_cov[m]), default=0)
    b = dup_breakdown[m]
    findings_lines.append(
        f"{m}: rows={totals[m]}, stations={coverage[m]['stations']}, "
        f"hourly coverage {min(cov) if cov else 'n/a'}-{max(cov) if cov else 'n/a'}%, "
        f"longest gap {worst_gap}h, dup identical={b['identical']}/conflicting={b['conflicting']}, "
        f"constant cols={constant_cols[m]}"
    )
print("\n".join(findings_lines))

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/dwd.md
_profile = [
    "| measurement | rows | cols | stations~ | cities~ | first ts | last ts | constant cols |",
    "|---|---|---|---|---|---|---|---|",
]
for m in MEASUREMENTS:
    _profile.append(
        f"| {m} | {totals[m]} | {len(prof[m]['cols'])} | {coverage[m]['stations']} | "
        f"{coverage[m]['cities']} | {coverage[m]['min_ts']} | {coverage[m]['max_ts']} | "
        f"{', '.join(constant_cols[m]) or '-'} |"
    )
_miss = ["Highest-missingness column per measurement (missing includes blank):"]
for m in MEASUREMENTS:
    mi = max(prof[m]["miss"].items(), key=lambda kv: kv[1])
    _miss.append(f"- {m}: `{mi[0]}` {mi[1]} ({mi[1] / totals[m]:.2%})")

_dq = [
    "| measurement | dup key groups | identical | conflicting | -999 sentinel (worst col) | out-of-range (worst col) |",
    "|---|---|---|---|---|---|",
]
for m in MEASUREMENTS:
    b = dup_breakdown[m]
    vs = value_stats[m]
    sent = max(
        ((c, vs.get(c + "_sentinel", 0)) for c in value_columns(frames[m])),
        key=lambda t: t[1],
        default=("-", 0),
    )
    oor = max(
        (
            (c, vs.get(c + "_oor", 0))
            for c in value_columns(frames[m])
            if c + "_oor" in vs
        ),
        key=lambda t: t[1],
        default=("-", 0),
    )
    _dq.append(
        f"| {m} | {b['dup_groups']} | {b['identical']} | {b['conflicting']} | {sent[0]}={sent[1]} | {oor[0]}={oor[1]} |"
    )

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
    para(
        "An unexpected QN value is a parse artefact or a schema drift, not a real",
        "quality level -- confirm the raw column encoding before decoding it.",
    )
)

_temporal = [
    para(
        "Hourly grid, expected one row per station per hour; MESS_DATUM parsed",
        "yyyyMMddHH (a trailing `.0` from a double-inferred column is stripped",
        "first). Coverage is observed_hours / (span/3600 + 1) -- an independent",
        "calendar, so <100% is a genuine gap.",
    ),
]
for m in MEASUREMENTS:
    cov = [r["coverage_pct"] for r in freq_cov[m] if r["coverage_pct"] is not None]
    worst = max((r["longest_gap_hours"] or 0 for r in freq_cov[m]), default=0)
    _temporal.append(
        f"- {m}: {coverage[m]['min_ts']}..{coverage[m]['max_ts']}, "
        f"per-station coverage {min(cov) if cov else 'n/a'}-{max(cov) if cov else 'n/a'}%, longest gap {worst}h"
    )

_regime = [
    para(
        "Per-decade row count and QN-vocabulary / value-column population. DWD's",
        "measurement network, instrumentation and QN scheme all changed over its",
        "multi-decade history -- a model pooling decades sees several regimes.",
    ),
    "",
]
for m in MEASUREMENTS:
    qn = qn_col(frames[m])
    _regime.append(f"- {m}:")
    for d, dv in sorted(regime[m].items()):
        qd = dv["columns"].get(qn, {}).get("distinct") if qn else "n/a"
        _regime.append(f"  - {d}s: rows={dv['rows']}, distinct QN codes={qd}")
_regime.append(
    para(
        "A registration-era / decade indicator and a per-station availability",
        "window are warranted before pooling -- not chosen here.",
    )
)

_coverage = [
    f"{len(all_stations)} distinct station ids across the 7 measurements: {all_stations}.",
    "Per-measurement station presence (row count per cell) -- see the exported heatmap figure.",
]
for row in coverage_matrix:
    _coverage.append(
        f"- station {row['station']}: "
        + ", ".join(f"{m}={row[m]}" for m in MEASUREMENTS)
    )
_coverage.append(
    para(
        "Station presence is uneven -- an inner join across all 7 measurements",
        "silently drops a station's rows for hours it lacks one parameter; this is",
        "a coverage bias toward the fully-instrumented stations.",
    )
)

_dist = []
for m in MEASUREMENTS:
    for c in value_columns(frames[m]):
        vs = value_stats[m]
        _dist.append(
            f"- {m}.`{c}`: min/max(no sentinel)={vs.get(c + '_min_ns')}/{vs.get(c + '_max_ns')}, "
            f"p01/p50/p99={vs.get(c + '_p')}, mean={vs.get(c + '_mean')}, sd={vs.get(c + '_sd')}, "
            f"zero rows={vs.get(c + '_zero')}, -999={vs.get(c + '_sentinel')}"
            + (f", out-of-range={vs.get(c + '_oor')}" if c + "_oor" in vs else "")
        )


def short_list(items, n=8):
    items = list(items)
    return items if len(items) <= n else f"{items[:n]} (+{len(items) - n} more)"


_special = [
    para(
        f"Candidate special codes searched in every value column: {CANDIDATES}.",
        "None is assumed to be a missing-value marker; each is characterised by where it occurs.",
    )
]
for m, sd in special_detail.items():
    _special.append(
        f"- {m}: rows with k candidate codes per row (k, rows): {sd['bundle']}."
    )
    for h in sd["hits"]:
        _special.append(
            f"  - `{h['column']}` = {h['code']}: {h['rows']} rows ({h['share']:.3%}); by quality level (level, rows with the code, rows at level) {short_list(h['by_qn'])}; "
            f"by decade (decade, rows) {short_list(h['by_decade'])}; top stations (station, rows) {h['top_stations']} = {h['top3_station_share']} of the code's rows."
        )
_special.append("Value range with every candidate code set aside (column: min, max):")
for m, cr in clean_range.items():
    _special.append(f"- {m}: {cr}")
_special.append(
    para(
        "Reading the evidence: a code confined to one quality level, appearing in the same rows across",
        "several columns, or concentrated in specific decades or stations points to a recording convention;",
        "a code spread across quality levels and stations with values on both sides points to a real value.",
        "The role of each code is not decided here -- it needs the source documentation.",
    )
)

_patterns = [
    "Mean by calendar month and by hour of day of MESS_DATUM (as stored, all stations, candidate codes excluded):"
]
for m, kinds_ in profiles.items():
    for key, cols in kinds_.items():
        for c, vs in cols.items():
            _patterns.append(f"- {m}.`{c}` by {key}: {vs}")

_qn = ["QN quality flag vs -999 sentinel / out-of-range rows:"]
for m in MEASUREMENTS:
    if m in qn_quality:
        _qn.append(f"- {m}: " + "; ".join(str(d) for d in qn_quality[m]))

_any_conflict = any(dup_breakdown[m]["conflicting"] for m in MEASUREMENTS)
_any_sentinel = any(
    value_stats[m].get(c + "_sentinel", 0) > 0
    for m in MEASUREMENTS
    for c in value_columns(frames[m])
)
_silver = []
if _any_sentinel:
    _silver.append(
        "- `-999` (and blank) is the DWD missing sentinel and is present -> must become NULL before any stat."
    )
if _any_conflict:
    _silver.append(
        "- (STATIONS_ID, MESS_DATUM) has conflicting duplicate rows in at least one measurement -> a conflict-resolution rule is required (rule not yet established)."
    )
_silver += [
    "- Identical (STATIONS_ID, MESS_DATUM) repeats can be de-duplicated safely.",
    "- Constant columns above carry no information.",
    "- Hourly series are not continuous (coverage % / gaps above) -> no dense-grid assumption.",
    "- Out-of-range non-sentinel values are flagged suspicious, not proven wrong -> keep raw + a quality flag.",
    "- Decode QN against the DWD scheme valid for the record's era (see Regime / Version Evidence).",
]

_no_target = para(
    "No candidate ML target lives in these tables -- raw per-station hourly",
    "measurements feeding the shared weather feature source, not a labelled table.",
)
_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            para(
                "One row per (STATIONS_ID, MESS_DATUM) per measurement.",
                "Pooling measurements or resampling to a coarser step drifts the grain;",
                "a station-level or contiguous-date split is required, never a random row split.",
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            para(
                "Cross-measurement joins on (STATIONS_ID, MESS_DATUM) are 1:1 where both",
                "sides are present (confirmed in 04) -- the risk is row LOSS on an inner",
                "join across uneven station coverage, not multiplication.",
            ),
        ),
        ("Target contamination", _no_target),
        (
            "Temporal / post-event leakage",
            para(
                "QN_* flags are set by DWD's QC alongside the value -- not available",
                "before the value; any forecasting feature may use only rows with",
                "MESS_DATUM strictly before the prediction timestamp.",
            ),
        ),
        (
            "Proxy leakage",
            "STATIONS_ID / city identify a specific site -- a model given them memorises the station.",
        ),
        (
            "Split / entity leakage",
            "Split by STATIONS_ID or by contiguous date range -- a station's adjacent hourly rows are highly correlated.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            para(
                "Station location / name / instrument are time-varying (metadata, 02) --",
                "a climatology or a station attribute joined to a historical row must use",
                "the value valid at that row's MESS_DATUM, not the latest.",
            ),
        ),
        (
            "Survivorship / coverage bias",
            para(
                "Hourly coverage above shows the real gaps; the station x measurement",
                "matrix shows uneven instrumentation. A pooled statistic is dominated by",
                "the long-history, fully-instrumented stations.",
            ),
        ),
        (
            "Missingness leakage",
            para(
                "-999 / blank rate correlates with station, parameter and era (Regime /",
                "Version Evidence) -- an 'is-missing' feature can leak an outage window.",
            ),
        ),
        (
            "Duplicate-event leakage",
            f"Conflicting (station, ts) duplicates per measurement: { {m: dup_breakdown[m]['conflicting'] for m in MEASUREMENTS} } -- resolve before counting or splitting.",
        ),
        (
            "Target / feature temporal misalignment",
            "MESS_DATUM is the observation hour; a feature/target pair must align to one hour convention (interval start vs end).",
        ),
        (
            "Unit / sign / circular-feature leakage",
            para(
                "Units are not in Bronze (reconcile via parameter_unit, 02). Net vs",
                "gross / related parameters within a measurement can be near-collinear.",
            ),
        ),
        (
            "Data-generation-process leakage",
            "QN_* encodes DWD's QC decision, not the physical weather -- a feature keyed on it encodes the QC pipeline.",
        ),
        (
            "Class / label instability",
            para(
                "QN codes are a DWD enumeration that changed across the archive's history",
                "(Regime / Version Evidence) -- a class defined by a raw QN is only stable",
                "within one scheme vintage.",
            ),
        ),
        (
            "Label availability lag",
            "DWD publishes historical data with a lag and revises it -- a nowcast cannot use the current hour's value.",
        ),
        (
            "Source / version / regime change",
            para(
                "Per-decade row count, QN vocabulary and value-column population are",
                "measured in Regime / Version Evidence -- a decade/era indicator is",
                "warranted before pooling.",
            ),
        ),
        (
            "Sample-vs-full divergence",
            para(
                "The value-column figure is drawn from a 5% sample capped at 150k rows;",
                "every reported statistic (value_stats, freq_cov, dup_breakdown) is a",
                "full Spark aggregation.",
            ),
        ),
    ]
)


_areas = {
    "Domain understanding": [
        "hourly station observations of air temperature, humidity, pressure, wind, precipitation, cloudiness and sunshine",
        f"candidate special codes found: { {m: sorted({(h['column'], h['code']) for h in sd['hits']}) for m, sd in special_detail.items()} }",
        f"seasonal and diurnal cycles measured for {list(profiles)}",
    ],
    "Structure and engineering": [
        "7 Bronze tables keyed (station, MESS_DATUM); all columns are strings and are cast with a safe numeric parse",
        "timestamps arrive as yyyyMMddHH, sometimes with a trailing .0",
        f"quality-level column per table: { {m: qn_col(frames[m]) for m in MEASUREMENTS} }",
    ],
    "Temporal": [
        f"spans { {m: (str(coverage[m]['min_ts'])[:10], str(coverage[m]['max_ts'])[:10]) for m in MEASUREMENTS} }",
        f"longest station gap (hours) per measurement { {m: max((r['longest_gap_hours'] or 0 for r in freq_cov[m]), default=0) for m in MEASUREMENTS} }",
    ],
    "Spatial": [
        f"{len(all_stations)} stations; station-level spatial patterns are in the relationships notebook"
    ],
    "Data quality": [
        f"duplicate keys { {m: (dup_breakdown[m]['dup_groups'], dup_breakdown[m]['conflicting']) for m in MEASUREMENTS} } (groups, conflicting)",
        f"-999 rows { {m: sum(value_stats[m].get(c + '_sentinel', 0) for c in value_columns(frames[m])) for m in MEASUREMENTS} }",
        "other candidate codes: see Special Values",
    ],
    "Statistical patterns": [
        "month and hour profiles (see Seasonal and Diurnal Profiles); value ranges per column in Distributions"
    ],
    "Relationships": ["cross-variable checks are in the relationships notebook"],
    "Analytics use": [
        "long hourly history at a small set of stations, with a quality flag per row"
    ],
    "ML use": [
        "continuous targets (temperature, pressure, wind, sunshine) with strong seasonal and diurnal structure; the quality flag and special codes must be handled before use"
    ],
    "AI / knowledge use": [
        "no text; parameter and station metadata form a small reference catalog (see the metadata notebook)"
    ],
}

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile) + "\n\n" + "\n".join(_miss)),
        ("Data Quality", "\n".join(_dq) + "\n\n" + "\n".join(_qn)),
        ("Special Values", "\n".join(_special)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Temporal", "\n".join(_temporal)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Coverage", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("Seasonal and Diurnal Profiles", "\n".join(_patterns)),
        ("EDA Findings", "\n".join(f"- {ln}" for ln in findings_lines)),
        ("ML-Readiness Evidence", _ml),
        ("Observations by Area", area_block(_areas)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
