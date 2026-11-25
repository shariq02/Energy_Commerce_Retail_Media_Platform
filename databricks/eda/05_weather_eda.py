# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- WEATHER (DWD, ACCUWEATHER, SEATTLE)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** exploratory analysis of the weather data in one notebook. Each section profiles one group of
# MAGIC tables and writes its findings to `src/schemas/profiling/<source>.md` (one section key per group).

# COMMAND ----------

# MAGIC %run ./_eda_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- DWD WEATHER MEASUREMENTS
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


# DBTITLE 1,Helpers -- value-column selection


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
frames = {m: bronze_frame(t) for m, t in TABLES.items()}
prof = {}
for m in MEASUREMENTS:
    df = frames[m]
    prof[m] = profile_frame(
        df,
        extra={
            "min_ts": F.min(find_col(df, "MESS_DATUM")),
            "max_ts": F.max(find_col(df, "MESS_DATUM")),
        },
    )
    prof[m].update(prof[m].pop("extra"))
    cols = prof[m]["cols"]
    sid, city = find_col(df, "STATIONS_ID"), find_col(df, "city")
    print(
        "=" * 90,
        f"\n{m}  rows={prof[m]['total']}  {prof[m]['min_ts']}..{prof[m]['max_ts']}  "
        f"stations~={prof[m]['acd'][sid]}  cities~={prof[m]['acd'][city]}\n",
        "=" * 90,
    )
    for c in cols:
        print(
            f"  {c:<26} missing={prof[m]['miss'][c]:>12} "
            f"rate={prof[m]['miss'][c] / prof[m]['total']:.4f} "
            f"approx_distinct={prof[m]['acd'][c]}"
        )
    print("constant columns:", [c for c in cols if prof[m]["acd"][c] <= 1])
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
constant_cols = {m: constant_column_names(frames[m], prof[m]) for m in MEASUREMENTS}

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
# Plausible physical range per value column. A candidate inside the range is an
# ordinary value there (-1 degC, 990 hPa) and is not searched; only candidates
# outside the range are. Columns without a known range keep every candidate.
PLAUSIBLE_RANGE = {
    "TT_TU": (-70.0, 60.0),
    "TT_STD": (-70.0, 60.0),
    "TF_STD": (-70.0, 60.0),
    "TD_STD": (-70.0, 60.0),
    "RF_TU": (0.0, 101.0),
    "RF_STD": (0.0, 101.0),
    "P": (300.0, 1100.0),
    "P0": (200.0, 1100.0),
    "P_STD": (200.0, 1100.0),
    "V_N": (0.0, 8.0),
    "N": (0.0, 8.0),
    "R1": (0.0, 300.0),
    "RS_IND": (0.0, 1.0),
    "WRTR": (0.0, 9.0),
    "SD_SO": (0.0, 60.0),
    "F": (0.0, 100.0),
    "D": (0.0, 360.0),
    "ABSF_STD": (0.0, 60.0),
    "VP_STD": (0.0, 100.0),
}


def codes_for(col):
    rng = PLAUSIBLE_RANGE.get(col.upper())
    return [x for x in CANDIDATES if rng is None or x < rng[0] or x > rng[1]]


def clean_value(col):
    v = safe_num(col)
    codes = codes_for(col)
    return F.when(~v.isin(codes), v) if codes else v


special = {}
for m in MEASUREMENTS:
    df = frames[m]
    cols = value_columns(df)
    exprs = [F.count(F.lit(1)).alias("rows")]
    for i, c in enumerate(cols):
        v = safe_num(c)
        exprs += [
            F.sum((v == cand).cast("long")).alias(f"n_{i}_{j}")
            for j, cand in enumerate(codes_for(c))
        ]
    r = df.agg(*exprs).first().asDict()
    special[m] = {
        "rows": r["rows"],
        "hits": [
            (c, cand, r[f"n_{i}_{j}"])
            for i, c in enumerate(cols)
            for j, cand in enumerate(codes_for(c))
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

    decade = (F.floor(F.year(dwd_timestamp(dts)) / 10) * 10).cast("int")
    by_qn = by(F.col(qn)) if qn else []
    by_dec = by(decade)
    by_st = by(F.col(sid))
    any_special = None
    for c in value_columns(df):
        codes = codes_for(c)
        term = safe_num(c).isin(codes).cast("int") if codes else F.lit(0)
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
        v = clean_value(c)
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
    ts = dwd_timestamp(find_col(df, "MESS_DATUM"))
    aggs = [F.avg(clean_value(c)).alias(c) for c in cols]
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
        df.select(F.col(sid).alias("station"), dwd_timestamp(dts).alias("ts"))
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
    decade = (F.floor(F.year(dwd_timestamp(dts)) / 10) * 10).cast("int")
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
        f"Candidate special codes {CANDIDATES}, searched in each value column only where the code lies outside that column's plausible range {PLAUSIBLE_RANGE}",
        "(a code inside the range, such as -1 degC or 990 hPa, is an ordinary value there).",
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
_special.append(
    "Value range with the out-of-range candidate codes of each column set aside (column: min, max):"
)
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- DWD STATION METADATA
# MAGIC
# MAGIC **Purpose:** Profile the four DWD metadata Bronze tables
# MAGIC (station_geography, station_name_history, device_instrument,
# MAGIC parameter_unit) -- schema, missingness, constant columns, duplicates, a
# MAGIC structural check for parser trailer rows, validity periods and their
# MAGIC ordering, station relocation / name-history analysis, spatial validity
# MAGIC of the coordinates, parameter -> measurement -> unit reconciliation,
# MAGIC metadata coverage gaps vs the measurement tables, and the layered
# MAGIC modelling-risk checklist. The metadata tables are small, collected once
# MAGIC and analysed in Python; only the measurement station set is scanned in
# MAGIC Spark.

# COMMAND ----------

# DBTITLE 1,Imports
from functools import reduce

import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "dwd"
NB_KEY = "02_station_metadata"
SECTION_TITLE = "Station metadata (geography, name history, device, parameter_unit)"
TABLES = {
    "station_geography": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_station_geography",
    "station_name_history": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_station_name_history",
    "device_instrument": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_device_instrument",
    "parameter_unit": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_parameter_unit",
}
MEASUREMENTS = [
    "air_temperature",
    "cloudiness",
    "moisture",
    "precipitation",
    "pressure",
    "sun",
    "wind",
]
MEASUREMENT_TABLES = {m: f"{CATALOG}.{BRONZE_SCHEMA}.dwd_{m}" for m in MEASUREMENTS}
META_NON_VALUE = {"STATIONS_ID", "CITY", "MESS_DATUM", "EOR", "V_N_I"}
# A generated DWD metadata export ends with a free-text trailer
# ("generiert: ... Deutscher Wetterdienst") that a naive CSV read turns into a
# data row -- a non-numeric STATIONS_ID or a cell carrying these tokens.
TRAILER_TOKENS = ("wetterdienst", "generiert", "erzeugt", "stand:")


def measurement_value_cols(cols):
    return [
        c
        for c in cols
        if c.upper() not in META_NON_VALUE and not c.upper().startswith("QN")
    ]


def find_key(cols, *cands):
    low = {c.lower(): c for c in cols}
    for x in cands:
        if x.lower() in low:
            return low[x.lower()]
    return None


def find_key_like(cols, *substrings):
    for c in cols:
        if any(s in c.lower() for s in substrings):
            return c
    return None


def to_float(x):
    # DWD metadata stores coordinates as strings, sometimes with a German
    # decimal comma; a bare float() silently dropped every lat/lon value.
    if x is None:
        return None
    try:
        return float(str(x).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None


# COMMAND ----------

# DBTITLE 1,Collect the 4 metadata tables (small) and profile in Python
meta = {}
for name, t in TABLES.items():
    df = bronze_frame(t)
    recs = [x.asDict() for x in df.collect()]
    cols = df.columns
    total = len(recs)
    dups = total - len({tuple(sorted(d.items())) for d in recs})
    consts = [c for c in cols if len({d[c] for d in recs}) <= 1]
    meta[name] = {"cols": cols, "recs": recs, "total": total}
    print(
        "=" * 90,
        f"\n{name}  rows={total}  cols={cols}  full_row_duplicates={dups}  constant_columns={consts}",
    )
    for c in cols:
        missing = sum(1 for d in recs if d[c] is None or str(d[c]).strip() == "")
        print(f"  {c:<30} missing={missing:>6}  distinct={len({d[c] for d in recs})}")
    for d in recs[:20]:
        print("  ", d)

# COMMAND ----------

# DBTITLE 1,Structural check -- CSV parser trailer rows
trailer = {}
for name, x in meta.items():
    sid = find_key(x["cols"], "Stations_id", "STATIONS_ID", "stations_id")
    bad_id = []
    token_rows = []
    for d in x["recs"]:
        if (
            sid
            and d[sid] is not None
            and not str(d[sid]).strip().removesuffix(".0").isdigit()
        ):
            bad_id.append(d[sid])
        joined = " ".join(str(v) for v in d.values() if v is not None).lower()
        if any(tok in joined for tok in TRAILER_TOKENS):
            token_rows.append({k: d[k] for k in list(d)[:3]})
    trailer[name] = {
        "non_numeric_station_id": bad_id[:10],
        "non_numeric_station_id_count": len(bad_id),
        "trailer_token_rows": token_rows[:5],
        "trailer_token_row_count": len(token_rows),
        "suspect": bool(bad_id or token_rows),
    }
    print(f"{name}: {trailer[name]}")
_trailer_hit = [n for n, v in trailer.items() if v["suspect"]]

# COMMAND ----------

# DBTITLE 1,Per-station row counts + validity-period ordering (Python)
station_counts = {}
period_stats = {}
for name, x in meta.items():
    sid = find_key(x["cols"], "Stations_id", "STATIONS_ID", "stations_id")
    if sid:
        acc = {}
        for d in x["recs"]:
            k = None if d[sid] is None else str(d[sid])
            acc[k] = acc.get(k, 0) + 1
        # a trailer row / bad parse yields a None or non-numeric station id --
        # keep the count but sort those last so sorted() never compares None<str
        station_counts[name] = sorted(
            acc.items(), key=lambda kv: (kv[0] is None, kv[0] or "")
        )
        print(f"{name} rows per station:", station_counts[name])
    von = find_key(x["cols"], "von_datum", "Von_Datum", "von")
    bis = find_key(x["cols"], "bis_datum", "Bis_Datum", "bis")
    if von and bis:
        open_ended = sum(1 for d in x["recs"] if not str(d[bis] or "").strip())
        inverted = sum(
            1
            for d in x["recs"]
            if str(d[bis] or "").strip()
            and str(d[bis]).strip().removesuffix(".0").isdigit()
            and str(d[von] or "").strip().removesuffix(".0").isdigit()
            and int(float(d[bis])) < int(float(d[von]))
        )
        period_stats[name] = (open_ended, inverted, x["total"])
        print(
            f"{name}: open_ended={open_ended}  inverted_ranges={inverted}  of {x['total']}"
        )

# COMMAND ----------

# DBTITLE 1,station_geography relocation + station_name_history changes (Python)
geo = meta["station_geography"]
gsid = find_key(geo["cols"], "Stations_id", "STATIONS_ID", "stations_id")
lat = find_key(
    geo["cols"], "Geogr_Breite", "geo_latitude_deg", "Geographische_Breite"
) or find_key_like(geo["cols"], "breit", "latitud")
lon = find_key(
    geo["cols"], "Geogr_Laenge", "geo_longitude_deg", "Geographische_Laenge"
) or find_key_like(geo["cols"], "laeng", "longitud")
elev = find_key(
    geo["cols"], "Stationshoehe", "station_elevation_m", "Stationshoehe_m"
) or find_key_like(geo["cols"], "hoehe", "elevation", "height")
print(f"geography columns -> id={gsid} lat={lat} lon={lon} elev={elev}")
geo_moves = {}
for d in geo["recs"]:
    g = geo_moves.setdefault(
        d[gsid], {"location_rows": 0, "lat": [], "lon": [], "elev": []}
    )
    g["location_rows"] += 1
    for k, col in (("lat", lat), ("lon", lon), ("elev", elev)):
        if col:
            fv = to_float(d[col])
            if fv is not None:
                g[k].append(fv)
for sid, g in geo_moves.items():
    g["lat_span"] = round(max(g["lat"]) - min(g["lat"]), 5) if g["lat"] else None
    g["lon_span"] = round(max(g["lon"]) - min(g["lon"]), 5) if g["lon"] else None
    g["elev_span_m"] = round(max(g["elev"]) - min(g["elev"]), 2) if g["elev"] else None
    # crude relocation distance: 1 deg lat ~ 111 km, 1 deg lon ~ 71 km at 52N
    g["reloc_km"] = (
        round(
            (((g["lat_span"] or 0) * 111) ** 2 + ((g["lon_span"] or 0) * 71) ** 2)
            ** 0.5,
            2,
        )
        if g["location_rows"] > 1
        else 0.0
    )
    print(
        f"station {sid}: { ({k: v for k, v in g.items() if k not in ('lat', 'lon', 'elev')}) }"
    )

nh = meta["station_name_history"]
nsid = find_key(nh["cols"], "Stations_id", "STATIONS_ID", "stations_id")
nname = find_key(nh["cols"], "Stationsname", "Betreibername", "Name")
name_changes = {}
for d in nh["recs"]:
    n = name_changes.setdefault(d[nsid], {"history_rows": 0, "names": set()})
    n["history_rows"] += 1
    if nname:
        n["names"].add(d[nname])
for sid, n in name_changes.items():
    print(
        f"station {sid}: history_rows={n['history_rows']}  distinct_names={len(n['names'])}"
    )

# COMMAND ----------

# DBTITLE 1,Spatial validity of the station coordinates (Germany bounding box)
geo_df = bronze_frame(TABLES["station_geography"])
spatial = None
if lat and lon:
    spatial = spatial_validity(geo_df, lat, lon, name="station_geography")
    print("spatial validity:", spatial)
big_moves = {sid: g["reloc_km"] for sid, g in geo_moves.items() if g["reloc_km"] > 5}
print("relocations > 5 km (station -> km):", big_moves)

# COMMAND ----------

# DBTITLE 1,parameter_unit -> measurement -> unit reconciliation
pu = meta["parameter_unit"]
pcode = find_key(
    pu["cols"], "Parameter", "parameter", "Kennung", "Parameter_ohne_Einheit"
)
punit = find_key(pu["cols"], "Einheit", "einheit", "unit")
declared = {str(d[pcode]) for d in pu["recs"]} if pcode else set()
print("parameter_unit declared codes:", sorted(declared))
if punit:
    upairs = {}
    for d in pu["recs"]:
        upairs[(d[pcode], d[punit])] = upairs.get((d[pcode], d[punit]), 0) + 1
    print("code -> unit:", upairs)
observed = {}
for m, t in MEASUREMENT_TABLES.items():
    observed[m] = measurement_value_cols(bronze_frame(t).columns)
observed_flat = {c for cs in observed.values() for c in cs}
codes_unknown = sorted(observed_flat - declared)
codes_unused = sorted(declared - observed_flat)
print("value codes in measurements NOT in parameter_unit:", codes_unknown)
print("parameter_unit codes never a measurement value column:", codes_unused)

# COMMAND ----------

# DBTITLE 1,Metadata coverage gaps vs the measurement station set (one Spark scan)
mstations = reduce(
    lambda a, b: a.union(b),
    (
        bronze_frame(t).select(
            F.col(find_key(bronze_frame(t).columns, "STATIONS_ID"))
            .cast("string")
            .alias("s")
        )
        for t in MEASUREMENT_TABLES.values()
    ),
).distinct()
measure_stations = {x["s"] for x in mstations.collect()}
print(
    f"distinct stations across measurements = {len(measure_stations)}: {sorted(measure_stations)}"
)
meta_gaps = {}
for name, x in meta.items():
    sid = find_key(x["cols"], "Stations_id", "STATIONS_ID", "stations_id")
    if sid is None:
        continue
    have = {str(d[sid]) for d in x["recs"]}
    missing = measure_stations - have
    meta_gaps[name] = len(missing)
    print(
        f"{name}: measurement stations with no row = {len(missing)}  {sorted(missing)}"
    )

# COMMAND ----------

# DBTITLE 1,Figure -- geographic station plot (lon x lat)
figs = []
if gsid and lat and lon and any(g["lon"] and g["lat"] for g in geo_moves.values()):
    fig, ax = plt.subplots(figsize=(7, 8))
    for sid, g in geo_moves.items():
        if g["lon"] and g["lat"]:
            ax.scatter(g["lon"], g["lat"], label=str(sid), s=60)
            ax.plot(g["lon"], g["lat"], linewidth=0.6)
    ax.legend(title="station id", fontsize=8)
    ax.set_title("DWD station_geography -- station locations (lines = relocations)")
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")
    fig.tight_layout()
    _save_and_show(fig, "dwd_station_geography.png")
    figs.append(
        (
            "DWD station_geography -- station locations & relocations",
            "dwd_station_geography.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Figure -- metadata overview (one faceted figure)
if facet_bars(
    {
        "geography: location rows per station": [
            (sid, g["location_rows"]) for sid, g in geo_moves.items()
        ],
        "geography: elevation span (m) per station": [
            (sid, g["elev_span_m"])
            for sid, g in geo_moves.items()
            if g["elev_span_m"] is not None
        ],
        "name_history: rows per station": [
            (sid, n["history_rows"]) for sid, n in name_changes.items()
        ],
        "measurement stations missing a metadata row": list(meta_gaps.items()),
        "metadata rows per table": [(n, meta[n]["total"]) for n in TABLES],
    },
    "DWD metadata -- overview",
    "dwd_metadata_overview.png",
    rot=45,
    ncols=2,
):
    figs.append(("DWD metadata -- overview", "dwd_metadata_overview.png"))

if facet_bars(
    dict(station_counts),
    "DWD metadata -- rows per station, by table",
    "dwd_metadata_rows_per_station.png",
    rot=45,
    ncols=2,
):
    figs.append(
        (
            "DWD metadata -- rows per station, by table",
            "dwd_metadata_rows_per_station.png",
        )
    )

if facet_bars(
    {
        name: [
            ("open-ended", open_ended),
            ("closed", total - open_ended),
            ("inverted", inverted),
        ]
        for name, (open_ended, inverted, total) in period_stats.items()
    },
    "DWD metadata -- validity-period row composition, by table",
    "dwd_metadata_validity_periods.png",
    rot=20,
    ncols=2,
):
    figs.append(
        (
            "DWD metadata -- validity-period row composition, by table",
            "dwd_metadata_validity_periods.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Findings
_relocations = {
    sid: g["location_rows"] for sid, g in geo_moves.items() if g["location_rows"] > 1
}
_renames = {
    sid: len(n["names"]) for sid, n in name_changes.items() if len(n["names"]) > 1
}
print("relocations (station -> location rows):", _relocations)
print("name changes (station -> distinct names):", _renames)
print("validity periods:", period_stats)
print("metadata coverage gaps vs measurements:", meta_gaps)
print("parser trailer suspected in:", _trailer_hit)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/dwd.md
_profile = [
    "| table | rows | cols | full-row dups | constant columns |",
    "|---|---|---|---|---|",
]
for name, x in meta.items():
    dups = x["total"] - len({tuple(sorted(d.items())) for d in x["recs"]})
    consts = [c for c in x["cols"] if len({d[c] for d in x["recs"]}) <= 1]
    _profile.append(
        f"| {name} | {x['total']} | {len(x['cols'])} | {dups} | {', '.join(consts) or '-'} |"
    )

_struct = [
    para(
        "A generated DWD metadata export ends with a free-text trailer",
        "(`generiert: ... Deutscher Wetterdienst`); a naive CSV read turns it into",
        "a data row with a non-numeric STATIONS_ID.",
    ),
]
for name, v in trailer.items():
    _struct.append(
        f"- {name}: {v['non_numeric_station_id_count']} non-numeric station id(s) "
        f"{v['non_numeric_station_id'][:5]}, {v['trailer_token_row_count']} row(s) carrying a "
        f"trailer token -> {'TRAILER ROW PRESENT' if v['suspect'] else 'clean'}."
    )
if _trailer_hit:
    _struct.append(
        para(
            f"-> INGESTION defect in {_trailer_hit}: fix the DWD parser in `scripts/ingestion/_energy_commerce_formats.py`",
            "to strip the trailer, re-stage, re-upload to the Volume and re-run the DWD",
            "Bronze loader; until then, filter non-numeric STATIONS_ID before any join.",
        )
    )
else:
    _struct.append("-> No trailer rows in the current Bronze metadata tables.")

_dq = ["Validity-period columns (von_datum / bis_datum):"]
for name, (open_ended, inverted, total) in period_stats.items():
    _dq.append(
        f"- {name}: open-ended={open_ended}, inverted ranges={inverted}, of {total} rows"
    )
_dq.append("")
_dq.append(
    "Per-station row counts: "
    + "; ".join(f"{name} -> {dict(pairs)}" for name, pairs in station_counts.items())
)

_domain = [
    f"parameter_unit declared codes: {sorted(declared)}",
    f"value-column codes present in measurements but NOT in parameter_unit: {codes_unknown}",
    f"parameter_unit codes never used as a measurement value column: {codes_unused}",
    "value columns per measurement: " + str(observed),
    para(
        "An unknown code is a measured parameter with no declared unit -- attach it",
        "at Silver from the DWD parameter description, do not leave the unit null.",
    ),
]

_spatial = ["Spatial validity of station_geography coordinates:"]
if spatial:
    _spatial.append(
        f"- present={spatial['present']}, missing={spatial['missing']}, "
        f"outside the Germany bounding box={spatial['outside_bbox']}, (0,0)={spatial['null_island']}, "
        f"lat/lon possibly swapped={spatial['looks_swapped']}."
    )
else:
    _spatial.append("- no lat/lon column located by name.")
_spatial.append(f"- relocations > 5 km (station -> km): {big_moves or 'none'}.")
_spatial.append(
    para(
        "A coordinate outside Germany / at (0,0) is a quarantine class, not a",
        "silent NULL. A multi-km relocation means the station's location is",
        "time-varying -- join on the von/bis window, not station id alone. There is",
        "no second coordinate source to cross-check against (LIMITATION).",
    )
)

_entities = [
    f"Distinct stations across the 7 measurement tables: {len(measure_stations)} -> {sorted(measure_stations)}.",
    "Metadata rows per station id:",
]
for name, pairs in station_counts.items():
    _entities.append(f"- {name}: {dict(pairs)}")

_relo = [
    "station_geography carries multiple location rows per station where coordinates/elevation changed over time:"
]
for sid, g in geo_moves.items():
    _relo.append(
        f"- station {sid}: {g['location_rows']} location row(s), lat span={g['lat_span']}, "
        f"lon span={g['lon_span']}, elevation span={g['elev_span_m']} m, ~{g['reloc_km']} km moved"
    )
_relo.append("")
_relo.append("station_name_history name/operator changes:")
for sid, n in name_changes.items():
    _relo.append(
        f"- station {sid}: {n['history_rows']} history row(s), {len(n['names'])} distinct name(s)"
    )

_gaps = ["Measurement stations with NO row in each metadata table:"]
for name, x in meta.items():
    sid = find_key(x["cols"], "Stations_id", "STATIONS_ID", "stations_id")
    if sid is None:
        continue
    have = {str(d[sid]) for d in x["recs"]}
    _gaps.append(
        f"- {name}: {meta_gaps.get(name, 0)} missing -> {sorted(measure_stations - have)}"
    )

_silver = []
if _trailer_hit:
    _silver.append(
        f"- BLOCKED for {_trailer_hit}: strip the parser trailer in the DWD parser and re-run Bronze."
    )
if _relocations:
    _silver.append(
        "- station_geography has >1 location row for some stations (relocations) -> station location is time-varying; join measurement rows on the von/bis window, not station_id alone."
    )
if _renames:
    _silver.append(
        "- station_name_history has >1 name/operator per station over time -> a second time-varying attribute stream."
    )
if any(v > 0 for v in meta_gaps.values()):
    _silver.append(
        "- Some measurement stations have no metadata row -> a left join must not drop the fact row; flag the unmatched station."
    )
if any(inv > 0 for _, inv, _ in period_stats.values()):
    _silver.append(
        "- Inverted von>bis validity ranges exist -> a fix/exclusion rule is required (rule not yet established)."
    )
_silver.append(
    "- device_instrument / parameter_unit are small static lookups -> reference dimensions; reconcile parameter codes with the measurement value-column names (list above)."
)

_no_target = para(
    "Station relocation and name/operator changes are event indicators, not",
    "attributes of a static station dimension -- if used as a target, treat them",
    "as change events; the metadata tables carry no other labelled outcome.",
)
_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            para(
                "station_geography / station_name_history are one row per (station,",
                "validity period), NOT one row per station -- a split must group by station id.",
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            para(
                f"measurement -> metadata is 1:N for the {len(_relocations)} relocated and",
                f"{len(_renames)} renamed stations unless scoped to the von/bis window -- an",
                "un-windowed join cartesian-explodes those stations' fact rows.",
            ),
        ),
        ("Target contamination", _no_target),
        (
            "Temporal / post-event leakage",
            para(
                "Joining a measurement row to a station's metadata must use the validity",
                "window covering that row's MESS_DATUM -- a later relocation's coordinates",
                "in a historical feature is future information.",
            ),
        ),
        (
            "Proxy leakage",
            "Station id / name / exact coordinates identify one site -- a model given them memorises the station.",
        ),
        (
            "Split / entity leakage",
            "Split by station id, not by row -- a station's multiple validity-period rows must stay on one side.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            "Same as Temporal -- use the metadata value valid during the archive row's period, never the latest.",
        ),
        (
            "Survivorship / coverage bias",
            f"{sum(1 for v in meta_gaps.values() if v)} metadata table(s) miss some measurement stations -- a joined feature set silently drops those stations' rows.",
        ),
        (
            "Missingness leakage",
            "Whether a station has a geography / name-history row correlates with how long it has been in the network.",
        ),
        (
            "Duplicate-event leakage",
            "Full-row duplicates per table shown in Profile -- de-duplicate before treating a validity-period row as one event.",
        ),
        (
            "Target / feature temporal misalignment",
            "von_datum / bis_datum bound the period; align a joined metadata attribute to the fact row's hour, not the period edge.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            "parameter_unit reconciliation above -- attach units before combining measured parameters.",
        ),
        (
            "Data-generation-process leakage",
            "A parser trailer row (Structural Integrity) would inject a non-station into every station-keyed join -- filter it first.",
        ),
        (
            "Class / label instability",
            "device_instrument / parameter_unit codes are DWD enumerations that evolve between archive versions -- pin the version.",
        ),
        ("Label availability lag", "Not applicable -- static reference metadata."),
        (
            "Source / version / regime change",
            para(
                "Validity periods span decades; the network, instrumentation and the",
                "metadata schema itself changed over that span -- a metadata attribute is",
                "only comparable within one era.",
            ),
        ),
        (
            "Sample-vs-full divergence",
            "All four metadata tables are fully collected (small); the measurement-station union is a full unsampled Spark scan.",
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Structural Integrity", "\n".join(_struct)),
        ("Data Quality", "\n".join(_dq)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Spatial Consistency", "\n".join(_spatial)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Coverage", "\n".join(_gaps)),
        ("Domain Findings", "\n".join(_relo)),
        ("ML-Readiness Evidence", _ml),
        (
            "EDA Findings",
            "\n".join(
                [
                    f"- relocations (station -> location rows): {_relocations}",
                    f"- name changes (station -> distinct names): {_renames}",
                    f"- validity periods (open-ended, inverted, total): {period_stats}",
                    f"- metadata coverage gaps vs measurements: {meta_gaps}",
                    f"- parser trailer suspected in: {_trailer_hit or 'none'}",
                ]
            ),
        ),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- DWD MISSING DATA
# MAGIC
# MAGIC **Purpose:** Profile dwd_missing_value_periods (the DWD-reported
# MAGIC missing-value windows) and reconcile it against the actual -999 /
# MAGIC blank values observed in the measurement tables -- reported vs
# MAGIC observed missingness, reported-period ordering, parameter-code domain,
# MAGIC missingness over time as regime evidence, a station x parameter
# MAGIC missingness matrix, and the layered modelling-risk checklist. The
# MAGIC reported-periods table is small and collected once; each measurement is
# MAGIC scanned twice (per-station rollup and per-year rollup).

# COMMAND ----------

# DBTITLE 1,Imports
import datetime as dt

import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "dwd"
NB_KEY = "03_missing_data"
SECTION_TITLE = "Missing data (reported periods vs observed -999/blank)"
MISSING_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.dwd_missing_value_periods"
MEASUREMENTS = [
    "air_temperature",
    "cloudiness",
    "moisture",
    "precipitation",
    "pressure",
    "sun",
    "wind",
]
MEASUREMENT_TABLES = {m: f"{CATALOG}.{BRONZE_SCHEMA}.dwd_{m}" for m in MEASUREMENTS}
NON_VALUE = {"STATIONS_ID", "CITY", "MESS_DATUM", "EOR"}
# DWD companion "Messverfahren-Index" columns -- not measured values, mostly
# blank; counting their blankness as "missing data" made cloudiness read as
# 100% missing and contradicted notebook 01.
INDICATOR_COLS = {"V_N_I"}


def value_cols(df):
    return [
        c
        for c in df.columns
        if c.upper() not in NON_VALUE
        and c.upper() not in INDICATOR_COLS
        and not c.upper().startswith("QN")
    ]


def to_dt(v):
    s = str(v or "").strip()
    s = s.removesuffix(".0")  # column inferred as double -> "2025021300.0"
    for fmt in (
        "%Y%m%d%H",
        "%Y%m%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y",
    ):
        try:
            return dt.datetime.strptime(s, fmt).replace(tzinfo=dt.UTC)
        except ValueError:
            continue
    return None


# COMMAND ----------

# DBTITLE 1,dwd_missing_value_periods -- collect once, analyse in Python
mv = bronze_frame(MISSING_TABLE)
MV_COLS = mv.columns
mv_recs = [x.asDict() for x in mv.collect()]
total = len(mv_recs)
sid_c = find_col(mv, "Stations_id", "STATIONS_ID", "stations_id")
param_c = find_col(mv, "Parameter", "parameter", "Kennung")
von_c = find_col(mv, "von_datum", "Von_Datum", "von")
bis_c = find_col(mv, "bis_datum", "Bis_Datum", "bis")
print(f"rows={total}  columns={MV_COLS}")
for c in MV_COLS:
    miss = sum(1 for d in mv_recs if d[c] is None or str(d[c]).strip() == "")
    print(f"  {c:<28} missing={miss:>6}  distinct={len({d[c] for d in mv_recs})}")
for d in mv_recs[:20]:
    print("  ", d)

mv_per_station = {}
mv_per_param = {}
for d in mv_recs:
    if sid_c:
        mv_per_station[d[sid_c]] = mv_per_station.get(d[sid_c], 0) + 1
    if param_c:
        mv_per_param[d[param_c]] = mv_per_param.get(d[param_c], 0) + 1
print("periods per station:", mv_per_station)
print("periods per parameter:", mv_per_param)

# COMMAND ----------

# DBTITLE 1,Reported period spans + von<=bis ordering (temporal consistency)
_TO_DT_UNPARSED: list = []
reported_gaps = {}
period_spans = []
inverted = 0
for d in mv_recs:
    if not (von_c and bis_c):
        break
    a, b = to_dt(d[von_c]), to_dt(d[bis_c])
    if not a or not b:
        if len(_TO_DT_UNPARSED) < 10:
            _TO_DT_UNPARSED.append((d[von_c], d[bis_c]))
        continue
    span_h = (b - a).total_seconds() / 3600
    if span_h < 0:
        inverted += 1
        continue
    period_spans.append(span_h)
    g = reported_gaps.setdefault(
        d[sid_c], {"reported_periods": 0, "longest": 0.0, "total": 0.0}
    )
    g["reported_periods"] += 1
    g["longest"] = max(g["longest"], span_h)
    g["total"] += span_h
print(f"inverted von>bis ranges: {inverted}  parsed spans: {len(period_spans)}")
if _TO_DT_UNPARSED:
    print(f"unparsed von/bis samples (first {len(_TO_DT_UNPARSED)}): {_TO_DT_UNPARSED}")
for sid, g in reported_gaps.items():
    print(f"station {sid}: {g}")

# COMMAND ----------

# DBTITLE 1,Parameter-code domain -- reported codes vs measurement value columns
observed_value_cols = {
    c for t in MEASUREMENT_TABLES.values() for c in value_cols(bronze_frame(t))
}
reported_codes = {str(d[param_c]) for d in mv_recs} if param_c else set()
codes_unknown = sorted(reported_codes - observed_value_cols)
codes_unused = sorted(observed_value_cols - reported_codes)
print("reported parameter codes not a measurement value column:", codes_unknown)
print("measurement value columns never in a reported period:", codes_unused)

# COMMAND ----------

# DBTITLE 1,Per-station rollup per measurement -- distinct hours + observed -999/blank (one scan each)
station_roll = {}
missing_rates = {}
for m, t in MEASUREMENT_TABLES.items():
    df = bronze_frame(t)
    s, dts = find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")
    vc = value_cols(df)
    exprs = [
        F.min(dts).alias("min_ts"),
        F.max(dts).alias("max_ts"),
        F.countDistinct(dts).alias("distinct_hours"),
        F.count(F.lit(1)).alias("rows"),
    ]
    for c in vc:
        v = safe_num(c)
        exprs += [
            F.sum((v == -999).cast("long")).alias(c + "__999"),
            F.sum((F.col(c).isNull() | (F.trim(F.col(c)) == "")).cast("long")).alias(
                c + "__blank"
            ),
        ]
    g = (
        df.groupBy(F.col(s).cast("string").alias("station"))
        .agg(*exprs)
        .orderBy("station")
        .collect()
    )
    station_roll[m] = [x.asDict() for x in g]
    total_m = sum(x["rows"] for x in g)
    missing_rates[m] = {}
    for c in vc:
        miss = sum(x[c + "__999"] + x[c + "__blank"] for x in g)
        missing_rates[m][c] = (miss, total_m)
        print(f"  {m}.{c:<24} missing_or_-999={miss:>12}  rate={miss / total_m:.4f}")

# COMMAND ----------

# DBTITLE 1,Regime evidence -- observed -999/blank rate by year per measurement (one scan each)
missing_over_time = {}
for m, t in MEASUREMENT_TABLES.items():
    df = bronze_frame(t)
    dts = find_col(df, "MESS_DATUM")
    any_missing = F.lit(False)
    for c in value_cols(df):
        v = safe_num(c)
        any_missing = (
            any_missing | (v == -999) | F.col(c).isNull() | (F.trim(F.col(c)) == "")
        )
    missing_over_time[m] = (
        df.select(
            F.substring(F.col(dts).cast("string"), 1, 4).alias("year"),
            any_missing.alias("miss"),
        )
        .groupBy("year")
        .agg(
            F.avg(F.col("miss").cast("double")).alias("missing_rate"),
            F.count(F.lit(1)).alias("rows"),
        )
        .orderBy("year")
        .collect()
    )
    print(m, [(x["year"], round(x["missing_rate"], 4)) for x in missing_over_time[m]])

# COMMAND ----------

# DBTITLE 1,Reconciliation -- reported periods vs observed -999/blank per station x parameter
reported_pairs = (
    {(str(d[sid_c]), d[param_c]) for d in mv_recs} if (sid_c and param_c) else set()
)
recon = []
for m in MEASUREMENTS:
    df = bronze_frame(MEASUREMENT_TABLES[m])
    vc = value_cols(df)
    for x in station_roll[m]:
        for c in vc:
            recon.append(
                {
                    "station": x["station"],
                    "measurement": m,
                    "parameter": c,
                    "observed_999": x[c + "__999"],
                    "observed_blank": x[c + "__blank"],
                    "rows": x["rows"],
                    "has_reported": (x["station"], c) in reported_pairs,
                }
            )
obs_no_report = sum(1 for r in recon if r["observed_999"] > 0 and not r["has_reported"])
report_no_obs = sum(
    1
    for r in recon
    if r["has_reported"] and r["observed_999"] == 0 and r["observed_blank"] == 0
)
print("station x parameter with -999 observed but NO reported period:", obs_no_report)
print(
    "station x parameter with a reported period but ZERO observed -999/blank:",
    report_no_obs,
)

# COMMAND ----------

# DBTITLE 1,Figure -- station x parameter observed missingness heatmap
figs = []
stations = sorted({r["station"] for r in recon})
params = sorted({r["parameter"] for r in recon})
rate = {}
for r in recon:
    rate[(r["station"], r["parameter"])] = rate.get(
        (r["station"], r["parameter"]), 0
    ) + (r["observed_999"] + r["observed_blank"]) / max(r["rows"], 1)
grid = np.array([[rate.get((st, p), 0.0) for p in params] for st in stations])
if grid.size:
    fig, ax = plt.subplots(
        figsize=(max(6, 0.8 * len(params)), max(3, 0.5 * len(stations)))
    )
    ax.imshow(grid, aspect="auto", cmap="magma")
    ax.set_xticks(range(len(params)))
    ax.set_xticklabels(params, rotation=45, ha="right")
    ax.set_yticks(range(len(stations)))
    ax.set_yticklabels(stations)
    ax.set_title("DWD -- observed missingness rate by station x parameter")
    fig.tight_layout()
    _save_and_show(fig, "dwd_missingness_heatmap.png")
    figs.append(
        (
            "DWD observed missingness rate by station x parameter",
            "dwd_missingness_heatmap.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Figure -- reported missing periods (one faceted figure)
if facet_bars(
    {
        "reported periods per station": sorted(mv_per_station.items()),
        "reported periods per parameter": sorted(mv_per_param.items()),
        "longest reported period (h) per station": [
            (sid, g["longest"]) for sid, g in reported_gaps.items()
        ],
    },
    "DWD missing_value_periods -- reported windows",
    "dwd_reported_missing_periods.png",
    ncols=2,
):
    figs.append(
        (
            "DWD missing_value_periods -- reported windows",
            "dwd_reported_missing_periods.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Figure -- observed -999/blank rate by year, per measurement (faceted)
if lines_grid(
    {
        m: [(x["year"], x["missing_rate"]) for x in rows]
        for m, rows in missing_over_time.items()
        if rows
    },
    "DWD -- observed -999/blank rate by year, per measurement",
    "dwd_missing_rate_by_year.png",
):
    figs.append(
        (
            "DWD observed -999/blank rate by year, per measurement",
            "dwd_missing_rate_by_year.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Figure -- missing rate per value column + distinct hours per station (faceted)
if facet_bars(
    {
        m: [(c, miss / tm) for c, (miss, tm) in cols.items()]
        for m, cols in missing_rates.items()
    },
    "DWD -- missing / -999 rate by value column, per measurement",
    "dwd_missing_rate_by_column.png",
    rot=30,
):
    figs.append(
        (
            "DWD missing / -999 rate by value column, per measurement",
            "dwd_missing_rate_by_column.png",
        )
    )
if facet_bars(
    {
        m: [(x["station"], x["distinct_hours"]) for x in station_roll[m]]
        for m in MEASUREMENTS
    },
    "DWD -- distinct observed hours per station, per measurement",
    "dwd_distinct_hours_per_station.png",
):
    figs.append(
        (
            "DWD distinct observed hours per station, per measurement",
            "dwd_distinct_hours_per_station.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Findings
_worst = {sid: g["longest"] for sid, g in reported_gaps.items()}
_obs_rate = {
    f"{m}.{c}": round(miss / tm, 4)
    for m, cs in missing_rates.items()
    for c, (miss, tm) in cs.items()
}
print("reported periods total:", total, " inverted ranges:", inverted)
print("reported longest gap per station:", _worst)
print("observed -999/blank rate per (measurement.column):", _obs_rate)
print(
    "reconciliation: -999-without-report =",
    obs_no_report,
    " report-without-observed =",
    report_no_obs,
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/dwd.md
_profile = [
    f"- `dwd_missing_value_periods`: {total} rows, columns {MV_COLS}",
    f"- station-id column: `{sid_c}`  parameter column: `{param_c}`  von/bis: `{von_c}`/`{bis_c}`",
    f"- periods per station: {mv_per_station}",
    f"- periods per parameter: {mv_per_param}",
]

_dq = [
    "- REPORTED = a row in dwd_missing_value_periods; OBSERVED = a measurement value that is `-999` or blank. They are not guaranteed to line up.",
    f"- reconciliation: station x parameter with -999 observed but NO reported period: {obs_no_report}",
    f"- reconciliation: station x parameter with a reported period but ZERO observed -999/blank: {report_no_obs}",
]

_domain = [
    "Reported parameter codes vs the measurement value-column names (the DWD parameter code IS the column name):",
    f"- reported codes not a measurement value column: {codes_unknown}",
    f"- measurement value columns never in a reported period: {codes_unused}",
    para(
        "An unknown reported code points to a parameter outside the seven",
        "measurement tables profiled here (a deepening measurement, 05) or a code",
        "drift -- reconcile before treating REPORTED and OBSERVED as one signal.",
    ),
]

_tcons = [
    f"Reported-period span (hours): parsed {len(period_spans)} of {total} rows.",
]
if _TO_DT_UNPARSED:
    _tcons.append(f"- unparsed von/bis samples: {_TO_DT_UNPARSED}")
if period_spans:
    _sp = sorted(period_spans)
    _tcons.append(
        f"- span min={_sp[0]:.1f}, median={_sp[len(_sp) // 2]:.1f}, max={_sp[-1]:.1f} hours"
    )
_tcons.append(
    f"- `von_datum` > `bis_datum` (inverted) in {inverted} rows -- a validity check is needed at Silver."
)
_tcons.append(f"- longest reported missing period per station (hours): {_worst}")

_regime = [
    para(
        "Observed -999/blank rate by year per measurement -- missingness is",
        "time-varying, so no uniform-completeness assumption holds and any",
        "imputation is an explicit, evidenced choice per era.",
    ),
    "",
]
for m, rows in missing_over_time.items():
    _regime.append(f"- {m}: {[(x['year'], round(x['missing_rate'], 4)) for x in rows]}")

_dist = ["Observed -999/blank rate per (measurement.value-column):"]
for k, vrate in _obs_rate.items():
    _dist.append(f"- {k}: {vrate:.4f}")

_dom = ["Per-station observed distinct hours (measurement completeness proxy):"]
for m in MEASUREMENTS:
    _dom.append(
        f"- {m}: " + str({x["station"]: x["distinct_hours"] for x in station_roll[m]})
    )

_silver = [
    "- `-999` and blank are the DWD missing sentinels in the measurement fact -> convert to NULL in Silver.",
    "- Keep `dwd_missing_value_periods` as a reference table keyed by (station, parameter, from_ts, to_ts).",
]
if obs_no_report or report_no_obs:
    _silver.append(
        "- REPORTED and OBSERVED missingness disagree for some station x parameter (counts above) -> do NOT reconcile by deleting rows; flag with a data-quality column."
    )
if inverted:
    _silver.append(
        "- Inverted von>bis reported ranges -> a fix/exclusion rule is required (rule not yet established)."
    )
_silver.append(
    "- Missingness is time-varying (year plots) -> no uniform-completeness assumption; any imputation is an explicit, evidenced choice."
)

_target = para(
    "`dwd_missing_value_periods` is itself a natural label source for a",
    "missingness/outage-prediction use case; the observed -999/blank rate per",
    "(measurement.column) is a denser alternative target for the same question.",
)
_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            "Reported periods and per-station rollups are keyed by (station, parameter) -- split by station id, not by row.",
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            para(
                "The station x parameter reconciliation is 1:1 per pair by construction,",
                f"but the two sides disagree ({obs_no_report} observed-without-report,",
                f"{report_no_obs} reported-without-observed) -- treat REPORTED and OBSERVED",
                "as two signals, not one validated join.",
            ),
        ),
        (
            "Target contamination",
            para(
                _target,
                "The reported period and everything dated inside it must be excluded",
                "from features for predicting that same gap.",
            ),
        ),
        (
            "Temporal / post-event leakage",
            para(
                "A reported period's von/bis window is only known once DWD closes the",
                "gap -- a forecasting model may use only periods with bis_datum strictly",
                "before the prediction point.",
            ),
        ),
        (
            "Proxy leakage",
            "`DatumLetzteAktualisierung`-style fields and the parameter/station keys describe the gap itself -- circular for predicting it.",
        ),
        (
            "Split / entity leakage",
            "Split by station id so a station's reported periods do not leak across train/test.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            "Replay reported periods forward from a base date -- do not use the full set joined to a past date.",
        ),
        (
            "Survivorship / coverage bias",
            f"Reported periods are concentrated per station/parameter ({mv_per_station}) -- a station-level classifier sees a skewed positive rate.",
        ),
        (
            "Missingness leakage",
            "A missing von or bis value may itself mark the gap type -- check before an 'is-missing' feature.",
        ),
        (
            "Duplicate-event leakage",
            "De-duplicate the reported-periods table before counting gaps as independent observations.",
        ),
        (
            "Target / feature temporal misalignment",
            "von_datum (gap start) vs bis_datum (gap end) vs the DWD report date are distinct -- align target and features to one.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            "Not applicable -- no numeric measures in the reported-periods table.",
        ),
        (
            "Data-generation-process leakage",
            "Whether a gap is REPORTED at all is a property of DWD's QC process, not the physical outage -- OBSERVED is the more direct signal.",
        ),
        (
            "Class / label instability",
            "Parameter codes and the reporting practice change across DWD archive versions -- pin the version.",
        ),
        (
            "Label availability lag",
            "Reported periods are back-loaded (a gap is logged after it closes) -- a real-time model cannot assume the row exists at the gap time.",
        ),
        (
            "Source / version / regime change",
            "Observed missingness rate by year (Regime / Version Evidence) shows the reporting regime shifting over the archive span.",
        ),
        (
            "Sample-vs-full divergence",
            "Every statistic (reported-period table, per-station rollups, yearly rates) is a full Spark scan or fully collected small table -- no sampling.",
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Data Quality", "\n".join(_dq)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Temporal Consistency", "\n".join(_tcons)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Distributions", "\n".join(_dist)),
        ("Domain Findings", "\n".join(_dom)),
        ("ML-Readiness Evidence", _ml),
        (
            "EDA Findings",
            "\n".join(
                [
                    f"- reported periods total: {total}, inverted ranges: {inverted}",
                    f"- reported longest gap per station (h): {_worst}",
                    f"- observed -999/blank rate per (measurement.column): {_obs_rate}",
                    f"- reconciliation: -999-without-report={obs_no_report}, report-without-observed={report_no_obs}",
                ]
            ),
        ),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- DWD RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **Purpose:** Cross-table checks across the 12 DWD Bronze tables --
# MAGIC station-id joinability, referential integrity between measurements and
# MAGIC metadata, station overlap across measurements, city consistency, full
# MAGIC relationship-cardinality profiles, cross-measurement timestamp
# MAGIC alignment, point-in-time consistency of measurements vs station
# MAGIC validity windows, and an evidence-based verdict on whether the 7
# MAGIC measurements can be combined. Station-id sets are small and collected;
# MAGIC the (station, timestamp) overlap is derived from one tagged union + one
# MAGIC presence matrix.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "dwd"
NB_KEY = "04_relationships_and_findings"
SECTION_TITLE = "Cross-table relationships & verdict"
MEASUREMENTS = [
    "air_temperature",
    "cloudiness",
    "moisture",
    "precipitation",
    "pressure",
    "sun",
    "wind",
]
MEASUREMENT_TABLES = {m: f"{CATALOG}.{BRONZE_SCHEMA}.dwd_{m}" for m in MEASUREMENTS}
META = {
    "station_geography": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_station_geography",
    "station_name_history": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_station_name_history",
    "parameter_unit": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_parameter_unit",
    "device_instrument": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_device_instrument",
}

# COMMAND ----------

# DBTITLE 1,Distinct station-id sets per table (collected -- small) + measurement row counts
station_set = {}
table_rows = {}
for name, t in {**MEASUREMENT_TABLES, **META}.items():
    df = bronze_frame(t)
    sid = find_col(df, "STATIONS_ID", "Stations_id", "stations_id")
    station_set[name] = {
        str(x[0]) for x in df.select(F.col(sid).cast("string")).distinct().collect()
    }
    print(
        f"{name:<24} distinct stations = {len(station_set[name])}: {sorted(station_set[name])}"
    )
for m, t in MEASUREMENT_TABLES.items():
    table_rows[m] = bronze_frame(t).count()

# COMMAND ----------

# DBTITLE 1,Station overlap + referential integrity (Python set math)
union_stations = set().union(*(station_set[m] for m in MEASUREMENTS))
measure_missing = {m: len(union_stations - station_set[m]) for m in MEASUREMENTS}
print(f"union of stations across measurements = {len(union_stations)}")
print("missing per measurement (vs union):", measure_missing)
ref_integrity = {}
for meta_name in ("station_geography", "station_name_history", "parameter_unit"):
    orphans = len(union_stations - station_set[meta_name])
    unused = len(station_set[meta_name] - union_stations)
    ref_integrity[meta_name] = (orphans, unused)
    print(
        f"{meta_name}: measurement-stations-not-in-metadata={orphans}  metadata-stations-unused={unused}"
    )

# COMMAND ----------

# DBTITLE 1,City <-> station consistency + city coordinates (one union + distinct, collected)
city_map = None
for t in MEASUREMENT_TABLES.values():
    df = bronze_frame(t)
    sid = find_col(df, "STATIONS_ID")
    part = df.select(F.col(sid).cast("string").alias("station_id"), F.col("city"))
    city_map = part if city_map is None else city_map.union(part)
cm = {(x["station_id"], x["city"]) for x in city_map.distinct().collect()}
station_cities = {}
for s, c in cm:
    station_cities.setdefault(s, set()).add(c)
multi_city = {s: sorted(cs) for s, cs in station_cities.items() if len(cs) > 1}
city_counts = {}
for s, cs in station_cities.items():
    for c in cs:
        city_counts[c] = city_counts.get(c, 0) + 1
print("stations mapped to >1 city:", multi_city)
print("stations per city:", city_counts)

# COMMAND ----------

# DBTITLE 1,Relationship cardinality -- measurement station -> each metadata table
geo = bronze_frame(META["station_geography"])
gsid = find_col(geo, "STATIONS_ID", "Stations_id", "stations_id")
meta_card = {}
for meta_name, meta_t in META.items():
    md = bronze_frame(meta_t)
    s = find_col(md, "STATIONS_ID", "Stations_id", "stations_id")
    child_keys = {
        str(x[0]) for x in md.select(F.col(s).cast("string")).distinct().collect()
    }
    cp = cardinality_profile(md, s, child_keys, union_stations)
    meta_card[meta_name] = cp
    kind = "1:1" if cp["max_fanout"] <= 1 else "1:N (fan-out on station_id alone)"
    print(f"measurement -> {meta_name}: {kind}  {cp}")

# COMMAND ----------

# DBTITLE 1,Cross-measurement (station, MESS_DATUM) presence matrix -- one tagged union
u = None
for m, t in MEASUREMENT_TABLES.items():
    df = bronze_frame(t)
    s, d = find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")
    part = df.select(
        F.col(s).cast("string").alias("station"),
        F.col(d).cast("string").alias("ts"),
        F.lit(m).alias("src"),
    )
    u = part if u is None else u.union(part)
p = u.groupBy("station", "ts").agg(
    *[F.max((F.col("src") == m).cast("int")).alias(m) for m in MEASUREMENTS]
)

# COMMAND ----------

# DBTITLE 1,Overlap / cardinality stats -- one agg over the presence matrix
pairs = [(a, b) for i, a in enumerate(MEASUREMENTS) for b in MEASUREMENTS[i + 1 :]]
S = (
    p.agg(
        F.count(F.lit(1)).alias("union_keys"),
        *[F.sum(m).alias("present__" + m) for m in MEASUREMENTS],
        *[F.sum(F.col(a) * F.col(b)).alias(f"pair__{a}__{b}") for a, b in pairs],
    )
    .first()
    .asDict()
)
union_keys = S["union_keys"]
present = {m: S["present__" + m] for m in MEASUREMENTS}
pair_overlap = []
for a, b in pairs:
    shared = S[f"pair__{a}__{b}"]
    pair_overlap.append((a, b, shared, present[a] - shared, present[b] - shared))
    print(
        f"{a:<16} x {b:<16}  shared={shared:>12}  only_{a}={present[a] - shared:>12}  only_{b}={present[b] - shared:>12}"
    )
key_unique = {m: table_rows[m] == present[m] for m in MEASUREMENTS}
for m in MEASUREMENTS:
    print(
        f"{m:<16} rows={table_rows[m]:>12}  distinct (station, ts)={present[m]:>12}  key_unique={key_unique[m]}"
    )

# COMMAND ----------

# DBTITLE 1,Point-in-time consistency -- measurement hours vs the station geography validity window
geo_von = find_col(geo, "von_datum", "Von_Datum", "von")
geo_bis = find_col(geo, "bis_datum", "Bis_Datum", "bis")
pit = {}
if gsid and geo_von:
    win = geo.select(
        F.col(gsid).cast("string").alias("station"),
        F.try_to_timestamp(
            F.regexp_replace(F.col(geo_von).cast("string"), r"\.0$", ""),
            F.lit("yyyyMMdd"),
        ).alias("gv"),
        F.try_to_timestamp(
            F.regexp_replace(F.col(geo_bis).cast("string"), r"\.0$", ""),
            F.lit("yyyyMMdd"),
        ).alias("gb")
        if geo_bis
        else F.lit(None).cast("timestamp").alias("gb"),
    )
    # widest window per station (min von .. max bis, open bis -> now)
    wspan = win.groupBy("station").agg(
        F.min("gv").alias("gv"),
        F.max(F.coalesce("gb", F.current_timestamp())).alias("gb"),
    )
    for m, t in MEASUREMENT_TABLES.items():
        df = bronze_frame(t)
        s, dd = find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")
        j = (
            df.select(
                F.col(s).cast("string").alias("station"), dwd_timestamp(dd).alias("ts")
            )
            .where(F.col("ts").isNotNull())
            .join(wspan, on="station", how="inner")
        )
        r = (
            j.agg(
                F.count(F.lit(1)).alias("comparable"),
                F.sum(
                    ((F.col("ts") < F.col("gv")) | (F.col("ts") > F.col("gb"))).cast(
                        "long"
                    )
                ).alias("outside"),
            )
            .first()
            .asDict()
        )
        comp = r["comparable"] or 0
        pit[m] = {
            "comparable_rows": comp,
            "outside_window": r["outside"] or 0,
            "outside_pct": round((r["outside"] or 0) / comp * 100, 4) if comp else None,
        }
        print(
            f"{m}: measurement hours outside the station geography window -> {pit[m]}"
        )

# COMMAND ----------

# DBTITLE 1,Cross-variable settings and helpers
# Joins across measurements use the recent window only (bounded cost); values equal
# to a candidate special code are set aside so a code never enters a comparison.
RECENT_FROM = "2016-01-01"
SPECIAL = [-999.0, -99.9, -99.0, -9.9, -9.0, -1.0, 990.0, 999.0, 9999.0]
# A candidate code inside a column's plausible range (-1 degC, 990 hPa) is an
# ordinary value there and is kept; only out-of-range candidates are set aside.
PLAUSIBLE_RANGE = {
    "TT_TU": (-70.0, 60.0),
    "TT_STD": (-70.0, 60.0),
    "TF_STD": (-70.0, 60.0),
    "TD_STD": (-70.0, 60.0),
    "RF_TU": (0.0, 101.0),
    "RF_STD": (0.0, 101.0),
    "P": (300.0, 1100.0),
    "P0": (200.0, 1100.0),
    "P_STD": (200.0, 1100.0),
    "V_N": (0.0, 8.0),
    "R1": (0.0, 300.0),
    "RS_IND": (0.0, 1.0),
    "SD_SO": (0.0, 60.0),
    "F": (0.0, 100.0),
    "D": (0.0, 360.0),
}


def codes_for(colname):
    rng = PLAUSIBLE_RANGE.get(colname.upper())
    return [x for x in SPECIAL if rng is None or x < rng[0] or x > rng[1]]


def clean(colname):
    v = safe_num(colname)
    codes = codes_for(colname)
    return F.when(~v.isin(codes), v) if codes else v


def hourly(m, cols, raw=(), since=RECENT_FROM):
    df = bronze_frame(MEASUREMENT_TABLES[m])
    s_, d_ = find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")
    have = [c for c in cols if find_col(df, c)]
    return df.select(
        F.col(s_).cast("string").alias("station"),
        dwd_timestamp(d_).alias("ts"),
        *[clean(find_col(df, c)).alias(c) for c in have],
        *[safe_num(find_col(df, c)).alias(f"raw_{c}") for c in raw if find_col(df, c)],
    ).where(
        (F.col("ts") >= F.lit(since).cast("timestamp"))
        if since
        else F.col("ts").isNotNull()
    )


# COMMAND ----------

# DBTITLE 1,Temperature and humidity agree between the two tables that carry them; dew point never above air temperature
xv = {}
at = hourly("air_temperature", ["TT_TU", "RF_TU"])
mo = hourly("moisture", ["TT_STD", "RF_STD", "TD_STD"])
j = at.join(mo, ["station", "ts"], "inner")
if {"TT_TU", "TT_STD", "RF_TU", "RF_STD", "TD_STD"} <= set(j.columns):
    xv["temp_humidity"] = (
        j.agg(
            F.count(F.lit(1)).alias("joined"),
            F.sum(
                (F.col("TT_TU").isNotNull() & F.col("TT_STD").isNotNull()).cast("long")
            ).alias("temp_pairs"),
            F.avg(F.abs(F.col("TT_TU") - F.col("TT_STD"))).alias("temp_mean_abs_diff"),
            F.sum((F.abs(F.col("TT_TU") - F.col("TT_STD")) <= 0.5).cast("long")).alias(
                "temp_within_half_degree"
            ),
            F.corr("TT_TU", "TT_STD").alias("temp_corr"),
            F.sum(
                (F.col("RF_TU").isNotNull() & F.col("RF_STD").isNotNull()).cast("long")
            ).alias("rh_pairs"),
            F.avg(F.abs(F.col("RF_TU") - F.col("RF_STD"))).alias("rh_mean_abs_diff"),
            F.corr("RF_TU", "RF_STD").alias("rh_corr"),
            F.sum(
                (F.col("TD_STD").isNotNull() & F.col("TT_STD").isNotNull()).cast("long")
            ).alias("dew_pairs"),
            F.sum((F.col("TD_STD") > F.col("TT_STD") + 0.1).cast("long")).alias(
                "dew_above_temp"
            ),
            F.avg(F.col("TT_STD") - F.col("TD_STD")).alias("mean_dew_point_depression"),
        )
        .first()
        .asDict()
    )
print(xv.get("temp_humidity"))

# COMMAND ----------

# DBTITLE 1,Precipitation amount against the precipitation indicator
prec = hourly("precipitation", ["R1", "RS_IND"])
xv["precip"] = None
if {"R1", "RS_IND"} <= set(prec.columns):
    xv["precip"] = [
        x.asDict()
        for x in prec.where(F.col("R1").isNotNull() & F.col("RS_IND").isNotNull())
        .groupBy(
            (F.col("R1") > 0).alias("amount_positive"),
            F.col("RS_IND").alias("indicator"),
        )
        .count()
        .orderBy("amount_positive", "indicator")
        .collect()
    ]
print(xv["precip"])

# COMMAND ----------

# DBTITLE 1,Sunshine duration by cloud cover
sun = hourly("sun", ["SD_SO"])
cl = hourly("cloudiness", ["V_N"], raw=["V_N"])
j2 = sun.join(cl, ["station", "ts"], "inner")
xv["sun_cloud"] = None
if {"SD_SO", "V_N"} <= set(j2.columns):
    rows = (
        j2.where(F.col("SD_SO").isNotNull() & F.col("raw_V_N").isNotNull())
        .groupBy(F.col("raw_V_N").alias("cloud_code"))
        .agg(F.count(F.lit(1)).alias("hours"), F.avg("SD_SO").alias("mean_sun_minutes"))
        .orderBy("cloud_code")
        .collect()
    )
    xv["sun_cloud"] = {
        "by_code": [
            (x["cloud_code"], x["hours"], round(x["mean_sun_minutes"], 2)) for x in rows
        ],
        "corr_clean": j2.stat.corr("SD_SO", "V_N"),
    }
print(xv["sun_cloud"])

# COMMAND ----------

# DBTITLE 1,Wind direction codes against wind speed
# Full history: 990 occurs only in the 1970s to 2000s, outside the recent window.
wd = hourly("wind", ["F", "D"], raw=["D"], since=None)
xv["wind"], xv["wind_by_decade"] = None, None
if {"F", "D"} <= set(wd.columns):
    cls = (
        F.when(F.col("raw_D") == 990, "D=990")
        .when(F.col("raw_D") == 0, "D=0")
        .when(F.col("raw_D") == -999, "D=-999")
        .otherwise("other")
    )
    base = wd.where(F.col("F").isNotNull() & F.col("raw_D").isNotNull())
    xv["wind"] = [
        x.asDict()
        for x in base.groupBy(cls.alias("direction_class"))
        .agg(
            F.count(F.lit(1)).alias("hours"),
            F.avg("F").alias("mean_speed"),
            F.max("F").alias("max_speed"),
            F.expr("percentile_approx(F, 0.5)").alias("median_speed"),
        )
        .orderBy("direction_class")
        .collect()
    ]
    xv["wind_by_decade"] = [
        (int(x["dec"]), x["direction_class"], x["hours"])
        for x in base.where(F.col("raw_D").isin(990.0, 0.0))
        .groupBy(
            (F.floor(F.year("ts") / 10) * 10).alias("dec"), cls.alias("direction_class")
        )
        .agg(F.count(F.lit(1)).alias("hours"))
        .orderBy("dec", "direction_class")
        .collect()
    ]
print(xv["wind"])

# COMMAND ----------


# DBTITLE 1,Station-level means against station elevation and latitude


def _geo_col(*subs):
    return next((c for c in geo.columns if any(x in c.lower() for x in subs)), None)


g_lat, g_lon, g_elev = (
    _geo_col("breit", "latit"),
    _geo_col("laeng", "longit"),
    _geo_col("hoehe", "elev", "height"),
)
st_geo = pd.DataFrame()
if gsid and g_lat and g_lon:
    st_geo = (
        geo.select(
            F.col(gsid).cast("string").alias("station"),
            safe_num(g_lat).alias("lat"),
            safe_num(g_lon).alias("lon"),
            *([safe_num(g_elev).alias("elev")] if g_elev else []),
        )
        .groupBy("station")
        .agg(
            *[F.avg(c).alias(c) for c in ("lat", "lon", *(["elev"] if g_elev else []))]
        )
        .toPandas()
        .set_index("station")
    )
st_means = None
parts = []
for m, cols in (
    ("air_temperature", ["TT_TU"]),
    ("pressure", ["P0", "P"]),
    ("sun", ["SD_SO"]),
    ("wind", ["F"]),
    ("moisture", ["RF_STD"]),
):
    h = hourly(m, cols)
    have = [c for c in cols if c in h.columns]
    if have:
        parts.append(
            h.groupBy("station")
            .agg(*[F.avg(c).alias(c) for c in have])
            .toPandas()
            .set_index("station")
        )
if parts:
    st_means = pd.concat(parts, axis=1)
spatial_stats = {}
if st_means is not None and not st_geo.empty:
    tab = st_geo.join(st_means, how="inner")
    spatial_stats["stations"] = len(tab)
    spatial_stats["corr"] = {}
    for v_ in [c for c in st_means.columns]:
        for g_ in [c for c in st_geo.columns if c != "lon"]:
            pair = tab[[v_, g_]].dropna()
            if len(pair) >= 5:
                spatial_stats["corr"][f"{v_} ~ {g_}"] = round(
                    float(pair[v_].corr(pair[g_])), 3
                )
    if "elev" in tab and "TT_TU" in tab:
        pair = tab[["TT_TU", "elev"]].dropna()
        if len(pair) >= 5:
            slope = np.polyfit(pair["elev"], pair["TT_TU"], 1)[0]
            spatial_stats["temp_per_100m"] = round(float(slope * 100), 2)
    spatial_stats["table"] = tab.round(2).reset_index().to_dict("records")
print({k: v for k, v in spatial_stats.items() if k != "table"})

# COMMAND ----------

# DBTITLE 1,Verdict -- can the 7 measurements be combined downstream?
max_pair_only = max((max(o[3], o[4]) for o in pair_overlap), default=0)
schemas = {
    m: tuple(sorted(bronze_frame(MEASUREMENT_TABLES[m]).columns)) for m in MEASUREMENTS
}
schema_disjoint = len(set(schemas.values())) == len(schemas)
print(f"(station, MESS_DATUM) unique in every measurement : {all(key_unique.values())}")
print(f"largest non-shared timestamp count in any pair    : {max_pair_only}")
print(f"every measurement has a distinct value-column set  : {schema_disjoint}")

# COMMAND ----------

# DBTITLE 1,Figures
figs = []
labels = [f"{a[:4]}x{b[:4]}" for a, b, *_ in pair_overlap]
shared_v = [o[2] for o in pair_overlap]
nonshared_v = [o[3] + o[4] for o in pair_overlap]
xx = np.arange(len(labels))
fig, ax = plt.subplots(figsize=(13, 4))
ax.bar(xx, shared_v, label="shared (station, ts)")
ax.bar(xx, nonshared_v, bottom=shared_v, label="only one side")
ax.set_xticks(xx)
ax.set_xticklabels(labels, rotation=90)
ax.legend()
ax.set_title("DWD -- cross-measurement timestamp overlap per pair")
ax.set_ylabel("(station, ts) keys")
fig.tight_layout()
_save_and_show(fig, "dwd_cross_measurement_overlap.png")
figs.append(
    (
        "DWD cross-measurement (station, timestamp) overlap per pair",
        "dwd_cross_measurement_overlap.png",
    )
)

if barplot(
    [(n, len(station_set[n])) for n in {**MEASUREMENT_TABLES, **META}],
    "DWD -- distinct stations per Bronze table",
    "table",
    "stations",
    rot=40,
    filename="dwd_stations_per_bronze_table.png",
):
    figs.append(
        ("DWD distinct stations per Bronze table", "dwd_stations_per_bronze_table.png")
    )

if barplot(
    list(measure_missing.items()),
    "DWD -- stations absent from each measurement (vs union of all 7)",
    "measurement",
    "missing",
    rot=30,
    filename="dwd_stations_absent_per_measurement.png",
):
    figs.append(
        (
            "DWD stations absent from each measurement",
            "dwd_stations_absent_per_measurement.png",
        )
    )

metas = list(ref_integrity)
x = np.arange(len(metas))
fig, ax = plt.subplots(figsize=(10, 4))
ax.bar(
    x - 0.2,
    [ref_integrity[k][0] for k in metas],
    width=0.4,
    label="orphan measurement stations",
)
ax.bar(
    x + 0.2,
    [ref_integrity[k][1] for k in metas],
    width=0.4,
    label="unused metadata stations",
)
ax.set_xticks(x)
ax.set_xticklabels(metas, rotation=20, ha="right")
ax.legend()
ax.set_title("DWD -- referential integrity: measurements <-> metadata")
ax.set_ylabel("stations")
fig.tight_layout()
_save_and_show(fig, "dwd_referential_integrity.png")
figs.append(
    (
        "DWD referential integrity: measurements <-> metadata",
        "dwd_referential_integrity.png",
    )
)

if barplot(
    sorted(city_counts.items()),
    "DWD -- distinct stations per city",
    "city",
    "stations",
    filename="dwd_stations_per_city.png",
):
    figs.append(("DWD distinct stations per city", "dwd_stations_per_city.png"))

# COMMAND ----------

# DBTITLE 1,Findings
print(
    "distinct stations per table :",
    {n: len(station_set[n]) for n in {**MEASUREMENT_TABLES, **META}},
)
print("stations missing per measurement (vs union):", measure_missing)
print("referential integrity (orphans, unused)   :", ref_integrity)
print("stations mapped to >1 city                 :", multi_city)
print(
    "measurement->metadata max fan-out          :",
    {k: v["max_fanout"] for k, v in meta_card.items()},
)
print("(station, MESS_DATUM) unique in every measurement:", all(key_unique.values()))
print(
    "measurement hours outside station window   :",
    {m: v["outside_pct"] for m, v in pit.items()},
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/dwd.md
_ent = [
    f"Distinct station ids per Bronze table: { {n: len(station_set[n]) for n in {**MEASUREMENT_TABLES, **META}} }",
    f"Union of stations across the 7 measurements: {len(union_stations)} -> {sorted(union_stations)}",
    f"Stations absent from each measurement (vs the union): {measure_missing}",
    f"Stations mapped to >1 city: {multi_city}   |   distinct stations per city: {city_counts}",
]

_rel = [
    "Referential integrity -- measurement stations vs metadata (orphans, unused):",
    *[
        f"- {k}: orphan measurement stations={v[0]}, unused metadata stations={v[1]}"
        for k, v in ref_integrity.items()
    ],
    "",
    "Relationship cardinality -- measurement station -> metadata table:",
]
for k, cp in meta_card.items():
    _rel.append(
        f"- {k}: {cp['child_rows']} rows over {cp['distinct_child_keys']} station keys; "
        f"{cp['matched_parent_keys']} of {cp['parent_keys_total']} measurement stations have a row "
        f"({cp['parent_keys_referenced_pct']}%); rows-per-station p50/p90/p99 "
        f"{cp['child_rows_per_parent_p50']}/{cp['child_rows_per_parent_p90']}/"
        f"{cp['child_rows_per_parent_p99']}, max fan-out {cp['max_fanout']}, orphan keys "
        f"{cp['orphan_child_keys']}."
    )
_rel += [
    "",
    f"(STATIONS_ID, MESS_DATUM) unique within each measurement: {key_unique}",
    "",
    "Cross-measurement (station, MESS_DATUM) overlap per pair (shared, only_a, only_b):",
    *[
        f"- {a} x {b}: shared={sh}, only_{a}={oa}, only_{b}={ob}"
        for a, b, sh, oa, ob in pair_overlap
    ],
    "",
    f"union of (station, ts) across all 7 measurements = {union_keys}",
]

_tcons = [
    para(
        "Measurement hours vs the station's geography validity window (widest",
        "von..bis, open bis -> now). A row outside the window means the station was",
        "producing data for a period its geography record does not cover.",
    ),
    "",
]
if pit:
    for m, v in pit.items():
        _tcons.append(
            f"- {m}: {v['outside_window']}/{v['comparable_rows']} hours outside the window "
            f"({v['outside_pct']}%)."
        )
else:
    _tcons.append("- station_geography has no von/bis columns -- not assessable.")

_spatial = [
    f"`city` <-> station_id: {len(multi_city)} station(s) mapped to >1 city {multi_city or ''}.",
    para(
        "Coordinate-level spatial validity is in section 02. There is no second",
        "coordinate source to cross-check DWD's own against (LIMITATION); the",
        "checkable cross-table consistency is that a station resolves to one city.",
    ),
]

_xvar = [
    para(
        f"Cross-variable checks on the hourly rows from {RECENT_FROM} onward (the wind direction check uses the full history).",
        f"Candidate special codes {SPECIAL} are set aside per column only where they lie outside the column's plausible range {PLAUSIBLE_RANGE}.",
    )
]
th = xv.get("temp_humidity")
if th:
    _xvar.append(
        f"- air_temperature.TT_TU vs moisture.TT_STD: {th['temp_pairs']} hourly pairs, mean absolute difference {th['temp_mean_abs_diff']}, "
        f"{th['temp_within_half_degree']} within 0.5 degree, correlation {th['temp_corr']}."
    )
    _xvar.append(
        f"- air_temperature.RF_TU vs moisture.RF_STD: {th['rh_pairs']} pairs, mean absolute difference {th['rh_mean_abs_diff']}, correlation {th['rh_corr']}."
    )
    _xvar.append(
        f"- dew point above air temperature (moisture.TD_STD > TT_STD + 0.1): {th['dew_above_temp']} of {th['dew_pairs']} pairs; mean dew-point depression {th['mean_dew_point_depression']}."
    )
if xv.get("precip"):
    _xvar.append(
        f"- precipitation R1 > 0 against RS_IND (amount positive, indicator, hours): {xv['precip']}."
    )
if xv.get("sun_cloud"):
    _xvar.append(
        f"- sunshine minutes by cloud-cover code (code, hours, mean minutes): {xv['sun_cloud']['by_code']}; correlation with the codes set aside {xv['sun_cloud']['corr_clean']}."
    )
if xv.get("wind"):
    _xvar.append(
        f"- wind speed by direction class over the full history (990 and -999 only occur outside the recent window): {xv['wind']}."
    )
    _xvar.append(
        f"- hours with direction 990 or 0 by decade (decade, class, hours): {xv['wind_by_decade']}."
    )
if not any(xv.values()):
    _xvar.append("- no pair had the columns needed.")

_sp = []
if spatial_stats:
    _sp.append(
        f"Station means over the same window against station coordinates and elevation, {spatial_stats.get('stations')} stations (mean of the station's geography rows):"
    )
    _sp.append(f"- Pearson correlations: {spatial_stats.get('corr')}")
    if "temp_per_100m" in spatial_stats:
        _sp.append(
            f"- fitted change of mean air temperature per 100 m of elevation: {spatial_stats['temp_per_100m']} degrees."
        )
    _sp.append(
        f"- station table (station, lat, lon, elev, means): {spatial_stats.get('table')}"
    )
else:
    _sp.append(
        "- Station geography columns for latitude / longitude were not found; spatial patterns not computed."
    )

_areas = {
    "Domain understanding": [
        "seven measurement tables for a station network, with station geography, name history, device and parameter metadata",
        f"cross-variable agreement: temperature pairs {th['temp_pairs'] if th else 'n/a'} with mean abs diff {th['temp_mean_abs_diff'] if th else 'n/a'}; dew point above temperature in {th['dew_above_temp'] if th else 'n/a'} pairs",
        f"wind direction classes against speed: {xv.get('wind')}",
    ],
    "Structure and engineering": [
        f"(station, MESS_DATUM) unique in every measurement: {all(key_unique.values())}; largest non-shared timestamp count {max_pair_only}",
        f"value-column sets disjoint across measurements: {schema_disjoint}",
        f"stations mapped to more than one city: {len(multi_city)}",
    ],
    "Temporal": [
        f"measurement hours outside the station geography window: { {m: v['outside_pct'] for m, v in pit.items()} }"
    ],
    "Spatial": [
        f"station means against elevation / latitude: {spatial_stats.get('corr')}",
        f"temperature change per 100 m: {spatial_stats.get('temp_per_100m')}",
    ],
    "Data quality": [
        f"reference integrity (measurement stations not in metadata, metadata stations unused): {ref_integrity}",
    ],
    "Statistical patterns": [
        "seasonal and diurnal profiles are in the measurements notebook; station-to-station spatial gradients above"
    ],
    "Relationships": [
        f"sunshine by cloud code: {xv.get('sun_cloud', {}).get('by_code') if xv.get('sun_cloud') else 'n/a'}",
        f"precipitation amount vs indicator: {xv.get('precip')}",
    ],
    "Analytics use": [
        "a joinable station network across seven parameters on the shared hourly grid"
    ],
    "ML use": [
        "multi-variable hourly features per station are possible on the overlap; special codes and quality flags need handling first"
    ],
    "AI / knowledge use": [
        "station metadata (names, history, instruments) is a small structured reference, no free text"
    ],
}

_verdict = [
    f"- (station, MESS_DATUM) is unique in every measurement: {all(key_unique.values())}  -> pairwise measurement<->measurement joins are 1:1 on the overlap.",
    f"- largest non-shared timestamp count in any measurement pair: {max_pair_only}  -> an inner join to a wide table drops that tail.",
    f"- all 7 measurements have disjoint value-column sets: {schema_disjoint}.",
    "- Verdict: combine into a wide table only where timestamps overlap; keep one model per measurement in Silver, align at Gold.",
]

_silver = [
    "- Shared join key is (STATIONS_ID, MESS_DATUM); unique per measurement -> safe fan-out-free measurement<->measurement joins.",
]
if any(cp["max_fanout"] > 1 for cp in meta_card.values()):
    _silver.append(
        "- station_geography / station_name_history are 1:N on station_id -> join with the von/bis validity window, never station_id alone."
    )
if any(v[0] > 0 for v in ref_integrity.values()):
    _silver.append(
        "- Referential-integrity orphans exist -> left-join + a data-quality flag; do not drop the fact row."
    )
if pit and any(v["outside_window"] for v in pit.values()):
    _silver.append(
        "- Some measurement hours fall outside the station's geography validity window -> extend the window or flag; do not silently drop."
    )
if not multi_city:
    _silver.append(
        "- `city` is consistent 1:1 with station_id here -> safe to carry as a station attribute."
    )
_silver.append(
    "- A cross-measurement wide 'all weather at station S, hour H' table drops rows (overlap numbers above) -> that is a Gold consolidation, not Silver."
)

_no_target = para(
    "No candidate ML target lives across these 7 measurement + metadata tables --",
    "this notebook is a joinability audit, not a labelled-outcome source.",
)
_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            para(
                "(STATIONS_ID, MESS_DATUM) is unique within every measurement",
                f"({all(key_unique.values())}) -- a station-level (not row-level) split is",
                "required for any downstream model combining these tables.",
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            para(
                "measurement -> metadata is 1:1 only for",
                f"{[k for k, v in meta_card.items() if v['max_fanout'] <= 1]};",
                f"{[k for k, v in meta_card.items() if v['max_fanout'] > 1]} fan out and MUST be",
                "joined on the von/bis window, not station_id alone (full profile above).",
            ),
        ),
        ("Target contamination", _no_target),
        (
            "Temporal / post-event leakage",
            para(
                "Point-in-time consistency above quantifies measurement hours outside",
                "the station's geography window -- a metadata attribute joined without",
                "the window can attach a future location to a historical row.",
            ),
        ),
        (
            "Proxy leakage",
            "Station id / city are near-unique site identifiers -- a model given them memorises the station.",
        ),
        (
            "Split / entity leakage",
            "Split by station id across ALL tables at once so a station's rows stay on one side of every join.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            "The validity-window join is the point-in-time mechanism -- an un-windowed metadata join is point-in-time leakage.",
        ),
        (
            "Survivorship / coverage bias",
            para(
                f"Referential-integrity orphans { {k: v[0] for k, v in ref_integrity.items()} }",
                f"and cross-measurement non-overlap (max {max_pair_only}) mean a joined",
                "training set silently drops the under-instrumented stations/hours.",
            ),
        ),
        (
            "Missingness leakage",
            "Whether a station appears in a metadata table correlates with its tenure in the network -- an 'is-known' flag can leak that.",
        ),
        (
            "Duplicate-event leakage",
            "Per-measurement (station, ts) duplicate composition is in section 01 -- de-duplicate before joining or splitting.",
        ),
        (
            "Target / feature temporal misalignment",
            "MESS_DATUM (hour) vs metadata von/bis (day) are different resolutions -- align before pairing.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            f"All 7 measurements have disjoint value-column sets ({schema_disjoint}) -- no direct column-overlap leakage between them.",
        ),
        (
            "Data-generation-process leakage",
            "A parser trailer row in metadata (02) would inject a non-station into every station-keyed join here -- filter it first.",
        ),
        (
            "Class / label instability",
            "parameter_unit / device_instrument codes are DWD enumerations that change between archive versions -- pin the version.",
        ),
        ("Label availability lag", "Not applicable -- joinability audit, no label."),
        (
            "Source / version / regime change",
            "Station coverage and the metadata schema shifted over the archive's multi-decade span (per-decade evidence in 01/03).",
        ),
        (
            "Sample-vs-full divergence",
            "Every statistic here (station sets, overlap counts, cardinality, point-in-time) is a full Spark aggregation or a fully collected small set -- no sampling.",
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Entities / Keys", "\n".join(_ent)),
        ("Relationships", "\n".join(_rel)),
        ("Temporal Consistency", "\n".join(_tcons)),
        ("Spatial Consistency", "\n".join(_spatial)),
        ("Cross-variable Checks", "\n".join(_xvar)),
        ("Spatial Patterns", "\n".join(_sp)),
        ("EDA Findings", "\n".join(_verdict)),
        ("ML-Readiness Evidence", _ml),
        ("Observations by Area", area_block(_areas)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- DWD DEEPENING MEASUREMENTS & MISSING VALUE PERIODS
# MAGIC
# MAGIC **Purpose:** Profile the eight DWD deepening measurement Bronze tables
# MAGIC (dew_point, soil_temperature, visibility, cloud_type, wind_synop,
# MAGIC extreme_wind, weather_phenomena, solar) and the missing_value_periods
# MAGIC metadata table alongside the original seven measurements profiled in
# MAGIC the weather measurements section -- schema, missingness, constant columns,
# MAGIC station coverage, hourly continuity, per-station duplicates, value
# MAGIC distributions, the QN quality-flag domain, per-decade regime evidence,
# MAGIC and the layered modelling-risk checklist. Column semantics (units,
# MAGIC plausibility bounds) are not assumed ahead of a live-schema read.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import Window
from pyspark.sql import functions as F

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


def value_columns(df):
    return [
        c
        for c in df.columns
        if c.upper() not in NON_VALUE
        and not c.upper().startswith("QN")
        and not c.upper().endswith(_NON_VALUE_SUFFIXES)
    ]


# COMMAND ----------

# DBTITLE 1,Profile -- rows, missingness, approx distinct, min/max ts, constant columns (one agg per table)
frames = {m: bronze_frame(t) for m, t in TABLES.items()}
prof = {}
for m in MEASUREMENTS:
    df = frames[m]
    ts_col = find_col(df, "MESS_DATUM")
    extra = {"min_ts": F.min(ts_col), "max_ts": F.max(ts_col)} if ts_col else {}
    prof[m] = profile_frame(df, extra=extra)
    prof[m].update({"min_ts": None, "max_ts": None, **prof[m].pop("extra", {})})
    cols = prof[m]["cols"]
    sid = find_col(df, "STATIONS_ID")
    print(
        "=" * 90,
        f"\n{m}  rows={prof[m]['total']}  {prof[m]['min_ts']}..{prof[m]['max_ts']}  "
        f"stations~={prof[m]['acd'][sid] if sid else 'n/a'}\n",
        "=" * 90,
    )
    for c in cols:
        rate = prof[m]["miss"][c] / prof[m]["total"] if prof[m]["total"] else 0
        print(
            f"  {c:<26} missing={prof[m]['miss'][c]:>12} rate={rate:.4f} approx_distinct={prof[m]['acd'][c]}"
        )
    print("constant columns:", [c for c in cols if prof[m]["acd"][c] <= 1])
totals = {m: prof[m]["total"] for m in MEASUREMENTS}
constant_cols = {m: constant_column_names(frames[m], prof[m]) for m in MEASUREMENTS}

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
    dup_breakdown[m] = dup_key_composition(df, [sid, dts])
    print(f"{m:<18} {dup_breakdown[m]}")

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
    # No pre-.distinct() shuffle -- duplicate (station, hour) rows give lag()
    # gap_h <= 0 (dropped by the `> 0` filter); observed_hours uses countDistinct.
    per_station = (
        df.select(F.col(sid).alias("station"), dwd_timestamp(dts).alias("ts"))
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
    decade = (F.floor(F.year(dwd_timestamp(dts)) / 10) * 10).cast("int")
    probe = [c for c in ([qn] if qn else []) + value_columns(df)]
    regime[m] = population_by_group(
        df.withColumn("__decade", decade).where(F.col("__decade").isNotNull()),
        "__decade",
        probe,
    )
    print(f"{m} by decade:", {d: dv["rows"] for d, dv in sorted(regime[m].items())})

# COMMAND ----------

# DBTITLE 1,missing_value_periods -- small metadata table, collected in Python
mvp = bronze_frame(META_TABLE)
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- ACCUWEATHER (SAMPLES CATALOG SCHEMA)
# MAGIC
# MAGIC **Purpose:** Profile every table of the `samples.accuweather` schema, read
# MAGIC in place (no Bronze table) -- table inventory and naming structure, column
# MAGIC dictionary, missingness, duplicates, composite keys, numeric plausibility
# MAGIC (unit-aware), cap / sentinel values, time columns and horizon, cadence and
# MAGIC completeness against a full grid, location entities and coordinate
# MAGIC consistency, imperial-versus-metric agreement, forecast-versus-historical
# MAGIC overlap, categorical distributions, and the layered modelling-risk
# MAGIC checklist -- as evidence.

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib
from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "accuweather"
NB_KEY = "01_accuweather"
SECTION_TITLE = "AccuWeather tables (Samples: samples.accuweather)"
SRC_CATALOG = "samples"
SRC_SCHEMA = "accuweather"
FQ_SCHEMA = f"{SRC_CATALOG}.{SRC_SCHEMA}"
GLOBAL_BBOX = (-90.0, 90.0, -180.0, 180.0)
FULL_DUP_MAX_ROWS = 50_000_000
NUMERIC_TYPES = ("tinyint", "smallint", "int", "bigint", "float", "double", "decimal")
MAX_NUMERIC_COLS = 120
# Exact column names that identify a location; no substring matching.
LOCATION_NAMES = ("city_name", "location_key", "location_id", "city_id", "station_id")
TIME_PREFERENCE = ("datetime_valid_local", "date")
ISSUE_HINTS = (
    "issue",
    "generated",
    "fetch",
    "created",
    "updated",
    "extract",
    "snapshot",
    "retriev",
)
TZ_HINTS = ("utc", "timezone", "tz", "offset")
CODE_SUFFIXES = ("_code", "_flag", "_icon")
CAP_SHARE = 0.05
# (match kind, text, imperial bounds, metric bounds) -- matched on column names.
BOUNDS = [
    ("startswith", "humidity_relative", (0.0, 100.0), (0.0, 100.0)),
    ("startswith", "cloud_cover", (0.0, 100.0), (0.0, 100.0)),
    ("endswith", "probability", (0.0, 100.0), (0.0, 100.0)),
    ("startswith", "latitude", (-90.0, 90.0), (-90.0, 90.0)),
    ("startswith", "longitude", (-180.0, 180.0), (-180.0, 180.0)),
    ("startswith", "temperature", (-100.0, 140.0), (-75.0, 60.0)),
    ("startswith", "dew_point", (-100.0, 120.0), (-75.0, 50.0)),
    ("startswith", "index_uv", (0.0, 20.0), (0.0, 20.0)),
    ("startswith", "uv_index", (0.0, 20.0), (0.0, 20.0)),
]

# COMMAND ----------


# DBTITLE 1,Helpers


def bounds_for(col, unit):
    c = col.lower()
    for kind, text, imp, met in BOUNDS:
        if (kind == "startswith" and c.startswith(text)) or (
            kind == "endswith" and c.endswith(text)
        ):
            return imp if unit == "imperial" else met
    return None


def short(items, n=12):
    items = list(items)
    if len(items) <= n:
        return items or "none"
    return f"{items[:n]} (+{len(items) - n} more)"


# COMMAND ----------

# DBTITLE 1,Table inventory
tables = sorted(
    r["tableName"]
    for r in spark.sql(f"SHOW TABLES IN {FQ_SCHEMA}").collect()
    if not r["isTemporary"]
)
print(f"{FQ_SCHEMA}: {len(tables)} tables")
print(tables)

# COMMAND ----------

# DBTITLE 1,Table sizes (Delta detail where available)
detail = {}
for t in tables:
    with contextlib.suppress(Exception):
        r = spark.sql(f"DESCRIBE DETAIL {FQ_SCHEMA}.{t}").first().asDict()
        detail[t] = {
            "format": r.get("format"),
            "size_bytes": r.get("sizeInBytes"),
            "files": r.get("numFiles"),
        }
for t in tables:
    print(t, detail.get(t, "no detail"))

# COMMAND ----------

# DBTITLE 1,Column dictionary (types and comments)
cols_info = {}
with contextlib.suppress(Exception):
    rows = spark.sql(
        f"SELECT table_name, column_name, data_type, comment, ordinal_position "
        f"FROM {SRC_CATALOG}.information_schema.columns WHERE table_schema = '{SRC_SCHEMA}' "
        "ORDER BY table_name, ordinal_position"
    ).collect()
    for r in rows:
        cols_info.setdefault(r["table_name"], []).append(r.asDict())
for t in tables:
    print(t, len(cols_info.get(t, [])), "columns in information_schema")

# COMMAND ----------

# DBTITLE 1,Table comments (deduplicated)
comment_tables = {}
with contextlib.suppress(Exception):
    for r in spark.sql(
        f"SELECT table_name, comment FROM {SRC_CATALOG}.information_schema.tables WHERE table_schema = '{SRC_SCHEMA}'"
    ).collect():
        if r["comment"]:
            comment_tables.setdefault(r["comment"], []).append(r["table_name"])
for text, names in comment_tables.items():
    print(f"{len(names)} tables share this comment: {names}\n{text[:600]}")

# COMMAND ----------

# DBTITLE 1,Frames
DFS = {t: spark.table(f"{FQ_SCHEMA}.{t}") for t in tables}
DTYPE = {
    t: {f.name: f.dataType.simpleString() for f in df.schema.fields}
    for t, df in DFS.items()
}
for t in tables:
    print(t, len(DTYPE[t]), "columns")

# COMMAND ----------

# DBTITLE 1,Table naming structure (period / grain / unit system)
STRUCT = {}
for t in tables:
    parts = t.split("_")
    STRUCT[t] = {
        "period": parts[0],
        "unit": parts[-1] if parts[-1] in ("imperial", "metric") else None,
        "grain": "_".join(parts[1:-1])
        if parts[-1] in ("imperial", "metric")
        else "_".join(parts[1:]),
    }
print({t: tuple(s.values()) for t, s in STRUCT.items()})

# COMMAND ----------

# DBTITLE 1,Resolve column roles per table
ROLE = {}
for t, types in DTYPE.items():
    cols = list(types)
    typed = [c for c in cols if types[c] in ("timestamp", "date", "timestamp_ntz")]
    ROLE[t] = {
        "numeric": [c for c in cols if types[c].startswith(NUMERIC_TYPES)][
            :MAX_NUMERIC_COLS
        ],
        "string": [c for c in cols if types[c] == "string"],
        "time_typed": typed,
        "time": next(
            (c for c in TIME_PREFERENCE if c in typed), typed[0] if typed else None
        ),
        "loc": next((c for c in LOCATION_NAMES if c in cols), None),
        "sub": [c for c in ("day_flag",) if c in cols],
        "lat": next((c for c in cols if c.lower() == "latitude"), None),
        "lon": next((c for c in cols if c.lower() == "longitude"), None),
        "country": next((c for c in cols if c.lower() == "country_code"), None),
        "issue": [c for c in cols if any(h in c.lower() for h in ISSUE_HINTS)],
        "tz": [c for c in cols if any(h in c.lower() for h in TZ_HINTS)],
    }
    ROLE[t]["key"] = (
        [ROLE[t]["loc"], ROLE[t]["time"], *ROLE[t]["sub"]]
        if ROLE[t]["loc"] and ROLE[t]["time"]
        else []
    )
    print(t, {k: v for k, v in ROLE[t].items() if k not in ("numeric", "string")})

# COMMAND ----------

# DBTITLE 1,Profile each table -- rows, missingness, approx distinct
prof = {}
for t, df in DFS.items():
    prof[t] = profile_frame(df)
    print(f"{t}: rows={prof[t]['total']} cols={len(prof[t]['cols'])}")

# COMMAND ----------

# DBTITLE 1,Constant columns, all-missing columns and full-row duplicates
for t, df in DFS.items():
    p = prof[t]
    p["constant"] = constant_column_names(df, p)
    p["all_missing"] = sorted(
        c for c in p["cols"] if p["total"] and p["miss"][c] == p["total"]
    )
    p["dups"] = (
        full_row_dup_count(df, p["total"]) if p["total"] <= FULL_DUP_MAX_ROWS else None
    )
    print(
        f"{t}: constant={len(p['constant'])} all-missing={len(p['all_missing'])} full-row duplicates={p['dups']}"
    )

# COMMAND ----------

# DBTITLE 1,Composite key -- location, time and day/night flag
dupk, uniq = {}, {}
for t, df in DFS.items():
    key = ROLE[t]["key"]
    if not key:
        continue
    dupk[t] = dup_key_composition(df, key)
    uniq[t] = exact_uniqueness(df, [ROLE[t]["loc"]])
    print(t, key, dupk[t])

# COMMAND ----------

# DBTITLE 1,Numeric parse yield + moments
num = {}
for t, df in DFS.items():
    num[t] = numeric_scan(df, ROLE[t]["numeric"])
    print(
        t, sum(1 for s in num[t].values() if s["is_numeric"]), "numeric columns parsed"
    )

# COMMAND ----------

# DBTITLE 1,Plausibility against unit-aware bounds
plaus = {}
for t, df in DFS.items():
    plaus[t] = {}
    for c, s in num[t].items():
        b = bounds_for(c, STRUCT[t]["unit"])
        if not b or not s["is_numeric"]:
            continue
        plaus[t][c] = plausibility(df, c, lo=b[0], hi=b[1], sentinels=())
        plaus[t][c]["bounds"] = b
        if plaus[t][c].get("below") or plaus[t][c].get("above"):
            print(
                t,
                c,
                b,
                {a: plaus[t][c].get(a) for a in ("min", "max", "below", "above")},
            )

# COMMAND ----------

# DBTITLE 1,Cap / sentinel-like values (share of rows at the column maximum)
caps = {}
for t, df in DFS.items():
    cols = [
        c
        for c, s in num[t].items()
        if s["is_numeric"]
        and s["max"] is not None
        and s["max"] > 0
        and prof[t]["acd"][c] > 1
    ]
    caps[t] = []
    if not cols:
        continue
    r = (
        df.agg(
            *[
                F.sum(
                    (to_double(c) == F.lit(float(num[t][c]["max"]))).cast("long")
                ).alias(f"m{i}")
                for i, c in enumerate(cols)
            ]
        )
        .first()
        .asDict()
    )
    for i, c in enumerate(cols):
        nn = num[t][c]["non_null"]
        share = (r[f"m{i}"] or 0) / nn if nn else 0.0
        if share >= CAP_SHARE:
            caps[t].append((c, num[t][c]["max"], r[f"m{i}"], round(share, 4)))
    print(t, caps[t])

# COMMAND ----------

# DBTITLE 1,Time columns -- range, distinct values, sub-daily component
tinfo = {}
for t, df in DFS.items():
    tinfo[t] = {}
    for c in ROLE[t]["time_typed"]:
        r = (
            df.agg(
                F.min(qcol(c)).alias("lo"),
                F.max(qcol(c)).alias("hi"),
                F.countDistinct(qcol(c)).alias("distinct"),
                F.sum((F.hour(qcol(c).cast("timestamp")) != 0).cast("long")).alias(
                    "has_time"
                ),
            )
            .first()
            .asDict()
        )
        tinfo[t][c] = {k: str(v) if k in ("lo", "hi") else v for k, v in r.items()}
        print(t, c, tinfo[t][c])

# COMMAND ----------

# DBTITLE 1,Horizon per table (days between first and last time value)
horizon = {}
for t in tables:
    c = ROLE[t]["time"]
    if c and c in tinfo[t]:
        r = (
            DFS[t]
            .agg(
                F.datediff(
                    F.max(qcol(c).cast("timestamp")), F.min(qcol(c).cast("timestamp"))
                ).alias("days")
            )
            .first()
        )
        horizon[t] = r["days"]
print(horizon)

# COMMAND ----------

# DBTITLE 1,Location entities and rows per location
loc_stats = {}
for t, df in DFS.items():
    lk = ROLE[t]["loc"]
    if not lk:
        continue
    loc_stats[t] = (
        df.groupBy(qcol(lk))
        .count()
        .agg(
            F.count(F.lit(1)).alias("locations"),
            F.min("count").alias("min"),
            F.expr("percentile_approx(`count`, 0.5)").alias("p50"),
            F.max("count").alias("max"),
        )
        .first()
        .asDict()
    )
    print(t, lk, loc_stats[t])

# COMMAND ----------

# DBTITLE 1,Cadence per location (and day/night flag)
cadence = {}
for t, df in DFS.items():
    key = ROLE[t]["key"]
    if not key:
        continue
    part = [
        qcol(c).alias(f"p{i}") for i, c in enumerate([ROLE[t]["loc"], *ROLE[t]["sub"]])
    ]
    pnames = [f"p{i}" for i in range(len(part))]
    ts = F.unix_timestamp(qcol(ROLE[t]["time"]).cast("timestamp")).alias("t")
    gaps = (
        df.select(*part, ts)
        .withColumn(
            "gap",
            F.col("t") - F.lag("t").over(Window.partitionBy(*pnames).orderBy("t")),
        )
        .where(F.col("gap").isNotNull())
    )
    cadence[t] = (
        gaps.agg(
            F.expr("percentile_approx(gap, 0.5)").alias("gap_p50"),
            F.expr("percentile_approx(gap, 0.99)").alias("gap_p99"),
            F.max("gap").alias("gap_max"),
            F.sum((F.col("gap") == 0).cast("long")).alias("zero_gaps"),
        )
        .first()
        .asDict()
    )
    cadence[t]["partition"] = [ROLE[t]["loc"], *ROLE[t]["sub"]]
    cadence[t]["time_column"] = ROLE[t]["time"]
    print(t, cadence[t])

# COMMAND ----------

# DBTITLE 1,Completeness within each location's own time range
complete = {}
for t, df in DFS.items():
    key = ROLE[t]["key"]
    step = cadence.get(t, {}).get("gap_p50")
    if not key or not step:
        continue
    names = [key[0], *ROLE[t]["sub"]]
    part = [qcol(c).alias(f"p{i}") for i, c in enumerate(names)]
    pn = [f"p{i}" for i in range(len(names))]
    ts = F.unix_timestamp(qcol(key[1]).cast("timestamp"))
    g = (
        df.select(*part, ts.alias("t"))
        .groupBy(*pn)
        .agg(
            F.min("t").alias("lo"), F.max("t").alias("hi"), F.count(F.lit(1)).alias("n")
        )
        .withColumn(
            "expected",
            ((F.col("hi") - F.col("lo")) / F.lit(float(step))).cast("long") + 1,
        )
    )
    b = g.agg(F.min("lo").alias("glo"), F.max("hi").alias("ghi")).first()
    late, early = F.col("lo") > F.lit(b["glo"]), F.col("hi") < F.lit(b["ghi"])
    r = (
        g.agg(
            F.count(F.lit(1)).alias("groups"),
            F.sum("n").alias("actual"),
            F.sum("expected").alias("expected_own"),
            F.countDistinct("lo").alias("start_instants"),
            F.countDistinct("hi").alias("end_instants"),
            F.min("n").alias("n_min"),
            F.max("n").alias("n_max"),
            F.sum(late.cast("long")).alias("late_start"),
            F.sum(early.cast("long")).alias("early_end"),
        )
        .first()
        .asDict()
    )
    r["interior_missing"] = r["expected_own"] - r["actual"]
    r["range_expected"] = r["groups"] * (int((b["ghi"] - b["glo"]) / step) + 1)
    r["range_missing"] = r["range_expected"] - r["actual"]
    edge = (
        g.where(late | early)
        .select(
            *pn,
            F.to_timestamp(F.from_unixtime("lo")).alias("first"),
            F.to_timestamp(F.from_unixtime("hi")).alias("last"),
            "n",
        )
        .orderBy(*pn)
        .limit(6)
        .collect()
    )
    r["edge_sample"] = [tuple(str(x) for x in row) for row in edge]
    complete[t] = r
    print(t, r)

# COMMAND ----------

# DBTITLE 1,Geography -- coordinate validity
geo = {}
for t, df in DFS.items():
    if ROLE[t]["lat"] and ROLE[t]["lon"]:
        geo[t] = spatial_validity(
            df, ROLE[t]["lat"], ROLE[t]["lon"], bbox=GLOBAL_BBOX, name=t
        )
        print(
            t,
            {
                k: geo[t][k]
                for k in ("present", "missing", "null_island", "outside_bbox")
            },
        )

# COMMAND ----------

# DBTITLE 1,Location attributes across all tables (coordinates, country)
_parts = [
    DFS[t].select(
        qcol(ROLE[t]["loc"]).alias("c"),
        to_double(ROLE[t]["lat"]).alias("la"),
        to_double(ROLE[t]["lon"]).alias("lo"),
        (
            as_str(ROLE[t]["country"])
            if ROLE[t]["country"]
            else F.lit(None).cast("string")
        ).alias("cc"),
    )
    for t in tables
    if ROLE[t]["loc"] and ROLE[t]["lat"] and ROLE[t]["lon"]
]
loc_attr = None
if _parts:
    allc = reduce(lambda a, b: a.unionByName(b), _parts)
    loc_attr = (
        allc.groupBy("c")
        .agg(
            F.countDistinct("la").alias("lat_variants"),
            F.countDistinct("lo").alias("lon_variants"),
            F.countDistinct("cc").alias("country_variants"),
        )
        .agg(
            F.count(F.lit(1)).alias("locations"),
            F.sum(
                ((F.col("lat_variants") > 1) | (F.col("lon_variants") > 1)).cast("long")
            ).alias("coord_conflicts"),
            F.sum((F.col("country_variants") > 1).cast("long")).alias(
                "country_conflicts"
            ),
        )
        .first()
        .asDict()
    )
print(loc_attr)

# COMMAND ----------

# DBTITLE 1,Location sets across tables
city_sets = {
    t: collect_key_set(DFS[t], ROLE[t]["loc"]) for t in tables if ROLE[t]["loc"]
}
ri = []
same_cities = None
if city_sets:
    ref = max(city_sets, key=lambda t: len(city_sets[t]))
    same_cities = all(s == city_sets[ref] for s in city_sets.values())
    for t, s in city_sets.items():
        if s != city_sets[ref]:
            ri.append(
                referential_integrity(s, city_sets[ref], child=f"{t}", parent=f"{ref}")
            )
print("identical location sets in every table:", same_cities, ri)

# COMMAND ----------

# DBTITLE 1,Variant pairs (imperial/metric and forecast/historical)
variants = {}
for t, s in STRUCT.items():
    variants.setdefault((s["period"], s["grain"]), {})[s["unit"]] = t
UNIT_PAIRS = [
    (v["imperial"], v["metric"])
    for v in variants.values()
    if "imperial" in v and "metric" in v
]
periods = {}
for t, s in STRUCT.items():
    periods.setdefault((s["grain"], s["unit"]), {})[s["period"]] = t
PERIOD_PAIRS = [
    (v["forecast"], v["historical"])
    for v in periods.values()
    if "forecast" in v and "historical" in v
]
print("imperial/metric:", UNIT_PAIRS)
print("forecast/historical:", PERIOD_PAIRS)

# COMMAND ----------

# DBTITLE 1,Imperial vs metric -- schema differences
schema_diff = {}
for a, b in UNIT_PAIRS:
    ca, cb = set(DTYPE[a]), set(DTYPE[b])
    schema_diff[(a, b)] = {
        "only_imperial": sorted(ca - cb),
        "only_metric": sorted(cb - ca),
        "dtype_differs": sorted(c for c in ca & cb if DTYPE[a][c] != DTYPE[b][c]),
    }
    print(a, b, schema_diff[(a, b)])

# COMMAND ----------


# DBTITLE 1,Key frames and pair alignment


def key_frame(t, subs):
    r = ROLE[t]
    return (
        DFS[t]
        .select(
            qcol(r["loc"]).alias("k0"),
            qcol(r["time"]).cast("timestamp").alias("k1"),
            *[qcol(s).alias(f"k{2 + i}") for i, s in enumerate(subs)],
        )
        .distinct()
    )


def align_pair(a, b):
    subs = ROLE[a]["sub"] if ROLE[a]["sub"] == ROLE[b]["sub"] else []
    ka, kb = key_frame(a, subs), key_frame(b, subs)
    names = ["k0", "k1", *[f"k{2 + i}" for i in range(len(subs))]]
    only_a, only_b = ka.join(kb, names, "left_anti"), kb.join(ka, names, "left_anti")
    return {
        "subs": subs,
        "names": names,
        "keys_a": ka.count(),
        "keys_b": kb.count(),
        "both": ka.join(kb, names, "inner").count(),
        "only_a": only_a.count(),
        "only_b": only_b.count(),
        "only_a_sample": [
            str(r["k1"]) for r in only_a.orderBy("k1").limit(4).collect()
        ],
        "only_b_sample": [
            str(r["k1"]) for r in only_b.orderBy("k1").limit(4).collect()
        ],
    }


# COMMAND ----------

# DBTITLE 1,Imperial vs metric -- row alignment
unit_align = {}
for a, b in UNIT_PAIRS:
    if ROLE[a]["key"] and ROLE[b]["key"]:
        unit_align[(a, b)] = align_pair(a, b)
        print(a, b, {k: v for k, v in unit_align[(a, b)].items() if k != "names"})

# COMMAND ----------


# DBTITLE 1,Compare shared numeric columns on a joined key set


def compare_pair(a, b, max_cols=60):
    info = align_pair_cache[(a, b)]
    names = info["names"]
    shared = [
        c
        for c in ROLE[a]["numeric"]
        if c in ROLE[b]["numeric"]
        and num[a].get(c, {}).get("is_numeric")
        and num[b].get(c, {}).get("is_numeric")
        and c not in (ROLE[a]["lat"], ROLE[a]["lon"])
    ][:max_cols]
    subs = info["subs"]

    def side(t, tag):
        r = ROLE[t]
        return DFS[t].select(
            qcol(r["loc"]).alias("k0"),
            qcol(r["time"]).cast("timestamp").alias("k1"),
            *[qcol(s).alias(f"k{2 + i}") for i, s in enumerate(subs)],
            *[to_double(c).alias(f"{tag}_{c}") for c in shared],
        )

    j = side(a, "a").join(side(b, "b"), names, "inner")
    if not shared:
        return []
    aggs = []
    for i, c in enumerate(shared):
        both = F.col(f"a_{c}").isNotNull() & F.col(f"b_{c}").isNotNull()
        aggs += [
            F.sum(both.cast("long")).alias(f"n{i}"),
            F.avg(F.col(f"a_{c}")).alias(f"ma{i}"),
            F.avg(F.col(f"b_{c}")).alias(f"mb{i}"),
            F.try_divide(
                F.covar_pop(F.col(f"a_{c}"), F.col(f"b_{c}")),
                F.sqrt(F.var_pop(F.col(f"a_{c}")) * F.var_pop(F.col(f"b_{c}"))),
            ).alias(f"r{i}"),
            F.sum((both & (F.col(f"a_{c}") == F.col(f"b_{c}"))).cast("long")).alias(
                f"e{i}"
            ),
            F.avg(F.when(both, F.abs(F.col(f"a_{c}") - F.col(f"b_{c}")))).alias(
                f"mae{i}"
            ),
            F.covar_pop(F.col(f"a_{c}"), F.col(f"b_{c}")).alias(f"cv{i}"),
            F.var_pop(F.col(f"a_{c}")).alias(f"va{i}"),
        ]
    r = j.agg(*aggs).first().asDict()
    return [
        {
            "col": c,
            "n": r[f"n{i}"],
            "mean_a": r[f"ma{i}"],
            "mean_b": r[f"mb{i}"],
            "corr": r[f"r{i}"],
            "equal": r[f"e{i}"],
            "mae": r[f"mae{i}"],
            "slope": (r[f"cv{i}"] / r[f"va{i}"]) if r[f"va{i}"] else None,
            "intercept": (
                r[f"mb{i}"] - (r[f"cv{i}"] / r[f"va{i}"]) * r[f"ma{i}"]
                if r[f"va{i}"] and r[f"mb{i}"] is not None and r[f"ma{i}"] is not None
                else None
            ),
        }
        for i, c in enumerate(shared)
    ]


align_pair_cache = dict(unit_align)
unit_cols = {}
for pair in UNIT_PAIRS:
    if pair in unit_align:
        unit_cols[pair] = compare_pair(*pair)
        rows = unit_cols[pair]
        same = [x["col"] for x in rows if x["n"] and x["equal"] == x["n"]]
        print(
            pair, f"{len(rows)} shared numeric columns; identical in both: {len(same)}"
        )

# COMMAND ----------

# DBTITLE 1,Forecast vs historical -- key overlap
period_align = {}
for a, b in PERIOD_PAIRS:
    if ROLE[a]["key"] and ROLE[b]["key"]:
        period_align[(a, b)] = align_pair(a, b)
        print(a, b, {k: v for k, v in period_align[(a, b)].items() if k != "names"})

# COMMAND ----------

# DBTITLE 1,Forecast vs historical -- values on the overlap window
align_pair_cache.update(period_align)
period_cols = {}
for pair, info in period_align.items():
    if info["both"] > 0:
        period_cols[pair] = compare_pair(*pair, max_cols=30)
        print(
            pair,
            f"{info['both']} overlapping keys; {len(period_cols[pair])} shared numeric columns",
        )

# COMMAND ----------


# DBTITLE 1,Imperial date column vs metric local time -- hour-shift search


def shift_search(a, b, shifts=range(-14, 15)):
    ta = "temperature" if "temperature" in ROLE[a]["numeric"] else None
    tb = "temperature" if "temperature" in ROLE[b]["numeric"] else None
    if not (ta and tb):
        return None
    has_off = "gmt_offset" in DTYPE[b]
    imp = DFS[a].select(
        qcol(ROLE[a]["loc"]).alias("c"),
        qcol(ROLE[a]["time"]).cast("timestamp").alias("t"),
        to_double(ta).alias("v"),
    )
    met = DFS[b].select(
        qcol(ROLE[b]["loc"]).alias("c"),
        qcol(ROLE[b]["time"]).cast("timestamp").alias("t"),
        to_double(tb).alias("w"),
        *([to_double("gmt_offset").alias("off")] if has_off else []),
    )
    parts = []
    for h in shifts:
        j = imp.withColumn("t", F.col("t") + F.expr(f"INTERVAL {h} HOURS")).join(
            met, ["c", "t"]
        )
        match = (F.abs((F.col("v") - 32) * 5 / 9 - F.col("w")) < 0.3).cast("long")
        parts.append(
            j.select(
                "c",
                F.lit(h).alias("h"),
                match.alias("m"),
                *([F.col("off")] if has_off else []),
            )
        )
    allj = reduce(lambda x, y: x.unionByName(y), parts)
    overall = (
        allj.groupBy("h")
        .agg(F.count(F.lit(1)).alias("joined"), F.sum("m").alias("temp_match"))
        .orderBy(F.desc("temp_match"))
        .limit(5)
        .collect()
    )
    pc = allj.groupBy("c", "h").agg(
        F.sum("m").alias("m"), *([F.first("off").alias("off")] if has_off else [])
    )
    best = (
        pc.withColumn(
            "rk", F.row_number().over(Window.partitionBy("c").orderBy(F.desc("m")))
        )
        .where(F.col("rk") == 1)
        .collect()
    )
    return {
        "top_shifts": [(r["h"], r["joined"], r["temp_match"]) for r in overall],
        "best_per_city": [
            (r["c"], r["h"], r["m"], r["off"] if has_off else None) for r in best
        ],
    }


# COMMAND ----------

# DBTITLE 1,Run the hour-shift search on unit pairs whose time keys differ
shifts_found = {}
for a, b in UNIT_PAIRS:
    differs = ROLE[a]["time"] != ROLE[b]["time"]
    info = unit_align.get((a, b), {})
    if differs or info.get("only_a") or info.get("only_b"):
        res = shift_search(a, b)
        if res:
            shifts_found[(a, b)] = res
            print(a, b, res["top_shifts"])

# COMMAND ----------


# DBTITLE 1,Physical consistency rules (ordering of related columns)


def consistency_rules(cols):
    have = set(cols)
    rules = []
    for c in sorted(have):
        if c.endswith("_min"):
            stem = c[:-4]
            if f"{stem}_max" in have:
                rules.append((f"{stem}_min <= {stem}_max", c, f"{stem}_max"))
                if f"{stem}_avg" in have:
                    rules.append((f"{stem}_min <= {stem}_avg", c, f"{stem}_avg"))
                    rules.append(
                        (f"{stem}_avg <= {stem}_max", f"{stem}_avg", f"{stem}_max")
                    )
    for sfx in ("", "_avg", "_min", "_max"):
        for lo, hi in (
            ("temperature_dew_point", "temperature"),
            ("wind_speed", "wind_gust"),
        ):
            if lo + sfx in have and hi + sfx in have:
                rules.append((f"{lo}{sfx} <= {hi}{sfx}", lo + sfx, hi + sfx))
    return rules


# COMMAND ----------

# DBTITLE 1,Evaluate the consistency rules per table
phys = {}
for t, df in DFS.items():
    cols = [c for c, s in num[t].items() if s["is_numeric"]]
    rules = consistency_rules(cols)
    phys[t] = []
    if not rules:
        continue
    aggs = []
    for i, (_, lo, hi) in enumerate(rules):
        both = to_double(lo).isNotNull() & to_double(hi).isNotNull()
        aggs += [
            F.sum(both.cast("long")).alias(f"n{i}"),
            F.sum((both & (to_double(lo) > to_double(hi) + 1e-6)).cast("long")).alias(
                f"v{i}"
            ),
        ]
    r = df.agg(*aggs).first().asDict()
    phys[t] = [(name, r[f"n{i}"], r[f"v{i}"]) for i, (name, _, _) in enumerate(rules)]
    bad = [x for x in phys[t] if x[2]]
    print(t, f"{len(rules)} rules; violated: {bad}")

# COMMAND ----------

# DBTITLE 1,Temperature by latitude band and hemisphere (metric daily tables)
lat_bands = {}
for t, df in DFS.items():
    s = STRUCT[t]
    col = "temperature_avg"
    if (
        s["unit"] != "metric"
        or s["grain"] != "daily_calendar"
        or col not in ROLE[t]["numeric"]
        or not ROLE[t]["lat"]
    ):
        continue
    la = F.abs(to_double(ROLE[t]["lat"]))
    band = (
        F.when(la < 15, "0-15")
        .when(la < 30, "15-30")
        .when(la < 45, "30-45")
        .otherwise("45+")
    )
    rows = (
        df.groupBy(band.alias("band"))
        .agg(
            F.countDistinct(qcol(ROLE[t]["loc"])).alias("locations"),
            F.avg(to_double(col)).alias("mean"),
            F.sum((to_double(ROLE[t]["lat"]) < 0).cast("long")).alias("south_rows"),
        )
        .orderBy("band")
        .collect()
    )
    corr_lat = df.select(
        to_double(ROLE[t]["lat"]).alias("x"), to_double(col).alias("y")
    ).stat.corr("x", "y")
    lat_bands[t] = {
        "bands": [(r["band"], r["locations"], round(r["mean"], 1)) for r in rows],
        "corr_lat_temp": corr_lat,
        "south_rows": sum(r["south_rows"] for r in rows),
    }
    print(t, lat_bands[t])

# COMMAND ----------

# DBTITLE 1,Day versus night temperature (daynight tables, metric)
daynight = {}
for t, df in DFS.items():
    s = STRUCT[t]
    if (
        s["unit"] != "metric"
        or not ROLE[t]["sub"]
        or "temperature_avg" not in ROLE[t]["numeric"]
    ):
        continue
    flag = ROLE[t]["sub"][0]
    wide = (
        df.select(
            qcol(ROLE[t]["loc"]).alias("c"),
            qcol(ROLE[t]["time"]).alias("dt"),
            qcol(flag).alias("f"),
            to_double("temperature_avg").alias("v"),
        )
        .groupBy("c", "dt")
        .pivot("f", ["d", "n"])
        .agg(F.avg("v"))
    )
    both = wide.where(F.col("d").isNotNull() & F.col("n").isNotNull())
    r = (
        both.agg(
            F.count(F.lit(1)).alias("pairs"),
            F.avg(F.col("d") - F.col("n")).alias("mean_diff"),
            F.sum((F.col("d") > F.col("n")).cast("long")).alias("day_warmer"),
        )
        .first()
        .asDict()
    )
    daynight[t] = r
    print(t, r)

# COMMAND ----------

# DBTITLE 1,Encoding of unavailable values -- single-value columns
const_kinds = {}
for t, df in DFS.items():
    cols = prof[t]["constant"]
    if not cols:
        const_kinds[t] = {}
        continue
    r = (
        df.agg(
            *[
                F.first(as_str(c), ignorenulls=True).alias(f"v{i}")
                for i, c in enumerate(cols)
            ]
        )
        .first()
        .asDict()
    )
    kinds_ = {}
    for i, c in enumerate(cols):
        v = r[f"v{i}"]
        label = (
            "all null"
            if v is None
            else (
                "zero / false"
                if str(v).strip().lower() in ("0", "0.0", "false")
                else f"other value {v}"
            )
        )
        kinds_.setdefault(label, []).append(c)
    const_kinds[t] = {k: len(v) for k, v in kinds_.items()}
    print(t, const_kinds[t])

# COMMAND ----------

# DBTITLE 1,Countries per location
country_mix = None
first_t = next((t for t in tables if ROLE[t]["loc"] and ROLE[t]["country"]), None)
if first_t:
    cc = (
        DFS[first_t]
        .select(
            qcol(ROLE[first_t]["loc"]).alias("c"),
            qcol(ROLE[first_t]["country"]).alias("cc"),
        )
        .distinct()
        .groupBy("cc")
        .count()
        .orderBy(F.desc("count"))
        .collect()
    )
    country_mix = {
        "countries": len(cc),
        "locations": sum(r["count"] for r in cc),
        "top": [(r["cc"], r["count"]) for r in cc[:6]],
    }
    print(country_mix)

# COMMAND ----------

# DBTITLE 1,Categorical and coded columns
cat_dist = {}
for t, df in DFS.items():
    p = prof[t]
    cats = [
        c
        for c in ROLE[t]["string"]
        if 1 < p["acd"][c] <= 40 and c not in ROLE[t]["key"]
    ]
    cats += [
        c
        for c in p["cols"]
        if c.lower().endswith(CODE_SUFFIXES)
        and 1 < p["acd"][c] <= 60
        and c not in cats
        and c != ROLE[t]["country"]
    ]
    cat_dist[t] = {}
    for c in cats[:10]:
        vc = df.groupBy(qcol(c)).count().orderBy(F.desc("count")).limit(25).collect()
        cat_dist[t][c] = [(r[0], r["count"]) for r in vc]
    print(t, list(cat_dist[t]))

# COMMAND ----------

# DBTITLE 1,Figures
figs = []
if facet_bars(
    {
        "rows per table": [(t, prof[t]["total"]) for t in tables],
        "columns per table": [(t, len(prof[t]["cols"])) for t in tables],
    },
    "AccuWeather -- overview",
    "accuweather_overview.png",
    rot=60,
    ncols=1,
    logy=True,
):
    figs.append(("AccuWeather -- overview", "accuweather_overview.png"))
_first_cat = {t: next(iter(v.values())) for t, v in cat_dist.items() if v}
if _first_cat and facet_bars(
    dict(list(_first_cat.items())[:6]),
    "AccuWeather -- first categorical column per table",
    "accuweather_categorical.png",
    rot=45,
    ncols=2,
):
    figs.append(
        (
            "AccuWeather -- first categorical column per table",
            "accuweather_categorical.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Findings
for t in tables:
    print(
        f"{t}: rows={prof[t]['total']}, cols={len(prof[t]['cols'])}, constant={len(prof[t]['constant'])}, "
        f"all-missing={len(prof[t]['all_missing'])}, dups={prof[t]['dups']}, key={ROLE[t]['key']}, "
        f"interior gaps={complete.get(t, {}).get('interior_missing')}"
    )

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/accuweather.md
_profile = [
    "| table | period | grain | unit | rows | cols | size (bytes) | constant cols | all-missing cols |",
    "|---|---|---|---|---|---|---|---|---|",
]
for t in tables:
    d, s = detail.get(t, {}), STRUCT[t]
    _profile.append(
        f"| {t} | {s['period']} | {s['grain']} | {s['unit']} | {prof[t]['total']} | {len(prof[t]['cols'])} | "
        f"{d.get('size_bytes', '-')} | {len(prof[t]['constant'])} | {len(prof[t]['all_missing'])} |"
    )

_prov = [
    para(
        "The tables are read in place from the Databricks Samples catalog",
        f"(`{FQ_SCHEMA}`); there is no Bronze table and no ECRMAP acquisition step.",
    ),
    f"Tables found: {len(tables)} -- {tables}.",
]
if comment_tables:
    _prov.append("Table comments in the catalog (each distinct comment shown once):")
    for text, names in comment_tables.items():
        _prov.append(f"- shared by {len(names)} tables ({short(names, 3)}):")
        _prov += [f"  > {ln.strip()[:400]}" for ln in text.splitlines() if ln.strip()][
            :12
        ]
else:
    _prov.append(
        "- No table comments in the catalog: provenance, licence and real/synthetic status are not documented here (LIMITATION)."
    )
_col_comments = [
    (t, ci["column_name"], ci["comment"])
    for t, lst in cols_info.items()
    for ci in lst
    if ci.get("comment")
]
if _col_comments:
    _prov.append("Column comments in the catalog (first 40):")
    _prov += [f"- `{t}.{c}`: {cm}" for t, c, cm in _col_comments[:40]]
else:
    _prov.append(
        "- No column comments in the catalog: column meanings and units come only from the names."
    )
_prov.append(
    "- Issue / extract-time columns (names containing "
    + ", ".join(ISSUE_HINTS)
    + f"): {short(sorted({c for t in tables for c in ROLE[t]['issue']}))}. "
    + f"Time-zone-like columns: {short(sorted({c for t in tables for c in ROLE[t]['tz']}))}."
)

_struct = [
    para(
        "Table names encode three things: period (`forecast` / `historical`), grain",
        "(`daily_calendar`, `daynight`, `hourly`) and unit system (`imperial` / `metric`).",
    )
]
for t in tables:
    r = ROLE[t]
    _struct.append(
        f"- `{t}`: {len(DTYPE[t])} columns -- numeric {len(r['numeric'])}, string {len(r['string'])}, "
        f"time {r['time_typed'] or 'none'}; location `{r['loc']}`; time key `{r['time']}`; sub-key {r['sub'] or 'none'}."
    )

_dq = [
    "Full-row exact duplicates, constant columns, all-missing columns and composite-key duplicates:"
]
for t in tables:
    dk = dupk.get(t)
    _dq.append(
        f"- `{t}`: full-row duplicates {prof[t]['dups']}; constant columns {len(prof[t]['constant'])} {short(prof[t]['constant'], 8)}; "
        f"all-missing columns {len(prof[t]['all_missing'])} {short(prof[t]['all_missing'], 8)}"
        + (
            f"; key {ROLE[t]['key']} duplicated {dk['dup_groups']} of {dk['distinct_keys']} "
            f"({dk['identical']} identical, {dk['conflicting']} conflicting)"
            if dk
            else ""
        )
        + "."
    )
_dq.append("Partly missing columns (rate above 5%, excluding all-missing):")
for t in tables:
    p = prof[t]
    part = [
        (c, round(p["miss"][c] / p["total"], 3))
        for c in p["cols"]
        if p["total"] and 0.05 < p["miss"][c] / p["total"] < 1.0
    ]
    _dq.append(f"- `{t}`: {short(part, 10)}")
_dq.append(
    "Cap / sentinel-like values (columns with at least 5% of rows at the column maximum):"
)
for t in tables:
    _dq.append(
        f"- `{t}`: (column, max, rows at max, share) {short(caps.get(t, []), 8)}"
    )
_dq.append(
    "Completeness within each location's (and day/night flag's) own time range; a gap inside the range is a real gap, a group that starts later or ends earlier than the table's overall range is an edge effect:"
)
for t, c in complete.items():
    _dq.append(
        f"- `{t}`: {c['groups']} groups; rows {c['actual']} vs {c['expected_own']} expected inside their own ranges (interior gaps {c['interior_missing']}); "
        f"distinct start instants {c['start_instants']}, end instants {c['end_instants']}; rows per group {c['n_min']}..{c['n_max']}; "
        f"groups starting later than the overall range {c['late_start']}, ending earlier {c['early_end']}; "
        f"missing against the overall range {c['range_missing']}; sample (group, first, last, rows) {c['edge_sample'][:3] or 'none'}."
    )

_entities = [
    "Location key `city_name` and its uniqueness per table (distinct / ratio / unique):"
]
for t in tables:
    if t in uniq:
        for c, u in uniq[t].items():
            _entities.append(
                f"- `{t}.{c}`: {u['distinct']} / {u['ratio']} / unique={u['unique']}"
            )
    if t in loc_stats:
        s = loc_stats[t]
        _entities.append(
            f"  rows per location: min {s['min']}, p50 {s['p50']}, max {s['max']} ({s['locations']} locations)."
        )
if loc_attr:
    _entities.append(
        f"- across all tables: {loc_attr['locations']} locations; {loc_attr['coord_conflicts']} with more than one "
        f"coordinate pair and {loc_attr['country_conflicts']} with more than one country code."
    )
if same_cities is not None:
    _entities.append(
        f"- the location set is {'identical' if same_cities else 'NOT identical'} in every table"
        + (f": {ri}" if ri else ".")
    )

_unit = []
for t in tables:
    for c, s in num[t].items():
        if not s["is_numeric"] or prof[t]["acd"][c] <= 1:
            continue
        line = (
            f"- `{t}.{c}`: range {s['min']}..{s['max']}, mean {fmt_num(s['mean'])}, sd {fmt_num(s['sd'])}, "
            f"zero {s['zero']}, negative {s['negative']}"
        )
        if c in plaus[t]:
            p = plaus[t][c]
            line += f"; bounds {p['bounds']} ({STRUCT[t]['unit']}): below {p['below']}, above {p['above']}"
        _unit.append(line + ".")
if not _unit:
    _unit.append("- No numeric column found.")
_unit.append(
    "Bounds are applied only to columns matched by name prefix / suffix and depend on the table's unit system."
)

_domain = []
for t in tables:
    for c, pairs_ in cat_dist[t].items():
        _domain.append(f"- `{t}.{c}`: " + fmt_pairs(pairs_, n=8).replace("\n", " "))
if not _domain:
    _domain.append("- No low-cardinality string column located.")

_geo = []
for t, g in geo.items():
    _geo.append(
        f"- `{t}`: coordinates present {g['present']}, missing {g['missing']}, at 0/0 {g['null_island']}, "
        f"outside the global box {g['outside_bbox']}."
    )
if loc_attr:
    _geo.append(
        f"- coordinates and country code per location across all tables: {loc_attr['coord_conflicts']} locations with conflicting "
        f"coordinates, {loc_attr['country_conflicts']} with conflicting country codes."
    )
_geo.append(
    "A latitude/longitude-swap test is not meaningful against a global box and is not reported."
)

_temporal = []
for t in tables:
    for c, v in tinfo[t].items():
        _temporal.append(
            f"- `{t}.{c}`: {v['lo']} .. {v['hi']}; {v['distinct']} distinct values; rows with a non-midnight time {v['has_time']}."
        )
    if t in horizon:
        _temporal.append(
            f"  span of the time key `{ROLE[t]['time']}`: {horizon[t]} days."
        )
if not _temporal:
    _temporal.append("- No time column located.")

_tcons = []
for t, cd in cadence.items():
    _tcons.append(
        f"- `{t}` per {cd['partition']} over `{cd['time_column']}`: gap p50 {cd['gap_p50']} s, p99 {cd['gap_p99']} s, "
        f"max {cd['gap_max']} s; zero-second gaps {cd['zero_gaps']}."
    )
if not _tcons:
    _tcons.append(
        "- No table had both a location key and a time key; cadence not assessed."
    )

_card = [
    "Row alignment between the two unit systems (same period and grain), on the key (location, time[, day/night flag]):"
]
for (a, b), r in unit_align.items():
    _card.append(
        f"- `{a}` vs `{b}`: keys {r['keys_a']} / {r['keys_b']}; in both {r['both']}; only imperial {r['only_a']} {r['only_a_sample']}; "
        f"only metric {r['only_b']} {r['only_b_sample']}."
    )
_card.append(
    "Rows are related only through the key above; there is no surrogate row identifier."
)

_variants = ["Schema differences between the two unit systems:"]
for (a, b), d in schema_diff.items():
    _variants.append(
        f"- `{a}` vs `{b}`: only in imperial {d['only_imperial']}; only in metric {d['only_metric']}; different type {d['dtype_differs']}."
    )
_variants.append(
    "Agreement of shared numeric columns on the joined keys (column: n, mean imperial, mean metric, corr, identical rows, mean abs diff):"
)
for pair, rows in unit_cols.items():
    same = [x["col"] for x in rows if x["n"] and x["equal"] == x["n"]]
    _variants.append(
        f"- `{pair[0]}` vs `{pair[1]}`: {len(rows)} shared numeric columns; identical in every joined row: {short(same, 20)}."
    )
    differing = [x for x in rows if x["n"] and x["equal"] != x["n"]]
    for x in differing[:15]:
        _variants.append(
            f"  - `{x['col']}`: n {x['n']}, {fmt_num(x['mean_a'])} vs {fmt_num(x['mean_b'])}, corr {fmt_num(x['corr'])}, "
            f"identical {x['equal']}, mean abs diff {fmt_num(x['mae'])}"
        )
    if len(differing) > 15:
        _variants.append(f"  - (+{len(differing) - 15} more differing columns)")
    linear = [
        (x["col"], round(x["slope"], 4), round(x["intercept"], 3))
        for x in differing
        if x["corr"] is not None and x["corr"] > 0.999 and x["slope"] is not None
    ]
    _variants.append(
        f"  - columns that are a linear function of the imperial value, as (column, slope, intercept) of metric on imperial: {short(linear, 30)}"
    )


def _norm_diff(x):
    level = 0.5 * (abs(x["mean_a"] or 0.0) + abs(x["mean_b"] or 0.0))
    return (x["mae"] or 0.0) / level if level else 0.0


_fh = [
    "Forecast versus historical tables (same grain and unit): key overlap and value comparison on the overlap; columns are ranked by mean absolute difference relative to the mean level so that scale does not decide the order:"
]
for (a, b), r in period_align.items():
    _fh.append(
        f"- `{a}` vs `{b}`: keys {r['keys_a']} / {r['keys_b']}; overlapping {r['both']}; only forecast {r['only_a']}; only historical {r['only_b']}."
    )
for pair, rows in period_cols.items():
    _fh.append(
        f"- `{pair[0]}` (forecast) minus `{pair[1]}` (historical) on {period_align[pair]['both']} overlapping keys:"
    )
    ranked = sorted(
        (x for x in rows if x["n"]),
        key=lambda x: -_norm_diff(x),
    )
    shown = (
        ranked[:12] + [x for x in ranked[12:] if x["col"].startswith("temperature")][:8]
    )
    for x in shown:
        _fh.append(
            f"  - `{x['col']}`: n {x['n']}, forecast mean {fmt_num(x['mean_a'])}, historical mean {fmt_num(x['mean_b'])}, "
            f"corr {fmt_num(x['corr'])}, identical {x['equal']}, mean abs diff {fmt_num(x['mae'])} "
            f"(relative to the mean level: {_norm_diff(x):.2f})"
        )
if not period_align:
    _fh.append("- No forecast/historical pair with a usable key.")

_tcons.append(
    "Timestamp alignment of the imperial `date` column to the metric local time (hour shift at which the imperial temperature converted to Celsius matches the metric temperature):"
)
for (a, b), res in shifts_found.items():
    _tcons.append(
        f"- `{a}` vs `{b}`: best shifts (hours, joined rows, matching rows) {res['top_shifts']}."
    )
    counts = {}
    for _, h, _, _ in res["best_per_city"]:
        counts[h] = counts.get(h, 0) + 1
    _tcons.append(f"  best shift per city, number of cities by shift: {counts}.")
    offs = [(h, off) for _, h, _, off in res["best_per_city"] if off is not None]
    if offs:
        scale = 3600.0 if max(abs(o) for _, o in offs) > 100 else 1.0
        same = sum(1 for h, o in offs if h == round(o / scale))
        opp = sum(1 for h, o in offs if h == -round(o / scale) and h != 0)
        zero = sum(1 for h, _ in offs if h == 0)
        _tcons.append(
            f"  against the metric table's `gmt_offset` (unit assumed {'seconds' if scale == 3600.0 else 'hours'}): shift equals the offset for {same} cities, its negative for {opp}, is zero for {zero} of {len(offs)}."
        )
if not shifts_found:
    _tcons.append(
        "- No unit pair needed the check (time keys and row sets already agree)."
    )

_phys = ["Ordering rules between related columns (rows compared / rows violating):"]
for t in tables:
    bad = [x for x in phys.get(t, []) if x[2]]
    _phys.append(
        f"- `{t}`: {len(phys.get(t, []))} rules checked; violated: {short(bad, 8)}"
    )
_phys.append("Temperature by absolute-latitude band (band, locations, mean):")
for t, r in lat_bands.items():
    _phys.append(
        f"- `{t}`: {r['bands']}; correlation of latitude with temperature {fmt_num(r['corr_lat_temp'])}; rows in the southern hemisphere {r['south_rows']}."
    )
_phys.append("Day versus night mean temperature per (location, date):")
for t, r in daynight.items():
    _phys.append(
        f"- `{t}`: {r['pairs']} pairs; day minus night mean {fmt_num(r['mean_diff'])}; day warmer in {r['day_warmer']}."
    )
if country_mix:
    _phys.append(
        f"Locations per country: {country_mix['locations']} locations in {country_mix['countries']} countries; largest {country_mix['top']}."
    )
_phys.append(
    "How unavailable values are encoded (single-value columns by kind, per table):"
)
for t in tables:
    _phys.append(f"- `{t}`: {const_kinds.get(t) or 'none'}")

_regime = [
    para(
        "Forecast versus observation: no table carries an issue / extract time unless one is listed",
        "under Provenance, so a forecast row is identified only by its valid time. Evidence per table:",
    )
]
for t in tables:
    _regime.append(
        f"- `{t}`: time columns {list(tinfo[t]) or 'none'}; issue-like columns {ROLE[t]['issue'] or 'none'}; span {horizon.get(t)} days."
    )

_coverage = [
    "The catalog comment (see Provenance) states the scope of the sample; the data itself carries no statement of it."
]
for t in tables:
    if t in loc_stats:
        _coverage.append(
            f"- `{t}`: {loc_stats[t]['locations']} `{ROLE[t]['loc']}` values."
        )

_dist = []
for t in tables:
    for c, pairs_ in cat_dist[t].items():
        _dist.append(f"- `{t}.{c}`: " + fmt_pairs(pairs_, n=10).replace("\n", " "))

_findings_md = "\n".join(
    f"- `{t}`: rows={prof[t]['total']}, cols={len(prof[t]['cols'])}, dups={prof[t]['dups']}, key={ROLE[t]['key']}, "
    f"key duplicates={dupk.get(t, {}).get('dup_groups')}, interior gaps={complete.get(t, {}).get('interior_missing')}, "
    f"cap-like columns={len(caps.get(t, []))}"
    for t in tables
)

_vocab = []
for a, b in PERIOD_PAIRS:
    for c in set(cat_dist[a]) & set(cat_dist[b]):
        va, vb = {v for v, _ in cat_dist[a][c]}, {v for v, _ in cat_dist[b][c]}
        if va != vb:
            _vocab.append((c, sorted(map(str, va)), sorted(map(str, vb))))
_by_period = {}
for t in tables:
    _by_period.setdefault(STRUCT[t]["period"], []).append(t)

_window = (
    min(v["lo"] for t in tables for v in tinfo[t].values()),
    max(v["hi"] for t in tables for v in tinfo[t].values()),
)

_areas = {
    "Domain understanding": [
        f"weather variables for {len(next(iter(city_sets.values()), []))} locations over {_window[0]} .. {_window[1]}, as forecasts and as observed history, at {len({s['grain'] for s in STRUCT.values()})} time grains and in {len({s['unit'] for s in STRUCT.values() if s['unit']})} unit systems (from the table names and time columns)",
        f"ordering rules between related columns: { {t: len(phys.get(t, [])) for t in tables} } checked; violated: { {t: [x[0] for x in phys.get(t, []) if x[2]] for t in tables if any(x[2] for x in phys.get(t, []))} or 'none' }",
        f"temperature by absolute-latitude band (band, locations, mean): { {t: r['bands'] for t, r in lat_bands.items()} }; correlation with latitude { {t: fmt_num(r['corr_lat_temp']) for t, r in lat_bands.items()} }",
        f"day versus night: { {t: (r['pairs'], r['day_warmer']) for t, r in daynight.items()} } (pairs, day warmer)",
    ],
    "Structure and engineering": [
        f"{len(tables)} tables in {len({detail.get(t, {}).get('format') for t in tables})} storage format(s) {sorted({str(detail.get(t, {}).get('format')) for t in tables})}; grains {sorted({s['grain'] for s in STRUCT.values()})}, periods {sorted({s['period'] for s in STRUCT.values()})}, unit systems {sorted({str(s['unit']) for s in STRUCT.values()})}",
        f"imperial and metric variants: identical keys in {sum(1 for r in unit_align.values() if not r['only_a'] and not r['only_b'])} of {len(unit_align)} pairs; column type differences {sum(len(d['dtype_differs']) for d in schema_diff.values())}",
        f"unavailable values are encoded as: { {t: const_kinds.get(t) for t in tables if STRUCT[t]['unit'] == 'metric'} }",
        f"time columns are named differently across variants for: {[(a, b) for a, b in UNIT_PAIRS if ROLE[a]['time'] != ROLE[b]['time']] or 'none'}",
        f"columns holding descriptive text: {sorted({c for t in tables for c in DTYPE[t] if any(h in c for h in ('desc', 'phrase', 'text'))})}",
    ],
    "Temporal": [
        f"forecast span {[horizon.get(t) for t in _by_period.get('forecast', [])][:3]} days, historical span {[horizon.get(t) for t in _by_period.get('historical', [])][:3]} days; one calendar window, no seasonality",
        f"cadence per group is regular (max gap {sorted({c['gap_max'] for c in cadence.values()})} s); interior gaps {sum(c['interior_missing'] for c in complete.values())} across all tables",
        f"groups that start late or end early (edge effects): { {t: (c['late_start'], c['early_end']) for t, c in complete.items() if c['late_start'] or c['early_end']} }",
        "no issue time on any forecast table; local-time keys"
        + (
            f"; imperial-date alignment: {shifts_found and {k: v['top_shifts'][:2] for k, v in shifts_found.items()}}"
            if shifts_found
            else ""
        ),
    ],
    "Spatial": [
        f"{loc_attr['locations'] if loc_attr else 'n/a'} locations with fixed coordinates in every table; countries {country_mix['countries'] if country_mix else 'n/a'}",
        f"latitude bands: { {t: r['bands'] for t, r in lat_bands.items()} }",
        f"hemisphere: southern-hemisphere rows { {t: r['south_rows'] for t, r in lat_bands.items()} }",
    ],
    "Data quality": [
        f"key duplicates { {t: d['dup_groups'] for t, d in dupk.items() if d['dup_groups']} or 'none' }; full-row duplicates { {t: prof[t]['dups'] for t in tables if prof[t]['dups']} or 'none' }",
        f"all-missing columns { {t: len(prof[t]['all_missing']) for t in tables if prof[t]['all_missing']} }",
        f"cap-like columns: { {t: [c[0] for c in caps[t]] for t in tables if caps.get(t)} }",
        "imperial vs metric completeness differs in shared columns: see Unit-System Variants",
    ],
    "Statistical patterns": [
        f"linear unit relations between variants (corr above 0.999) confirmed in {sum(1 for rows in unit_cols.values() for x in rows if x['corr'] is not None and x['corr'] > 0.999 and x['equal'] != x['n'])} column comparisons",
        "forecast vs history agreement on the overlap window: see Forecast vs Historical (relative differences)",
    ],
    "Relationships": [
        f"location set identical across tables: {same_cities}",
        f"forecast/historical key overlap: { {f'{a}|{b}': r['both'] for (a, b), r in period_align.items()} }",
        f"code vocabularies that differ between forecast and historical: {_vocab or 'none'}",
    ],
    "Analytics use": [
        f"dimensions: location ({loc_stats[next(iter(loc_stats))]['locations'] if loc_stats else 'n/a'}), time (three grains), day/night flag, period, unit system; measures: {min(len(ROLE[t]['numeric']) for t in tables)}-{max(len(ROLE[t]['numeric']) for t in tables)} numeric columns per table",
        "the same measures exist at hourly, day/night and daily grain, so the grains can be compared with each other",
    ],
    "ML use": [
        f"forecast vs historical value pairs exist only on the overlap window ({ {f'{a}|{b}': r['both'] for (a, b), r in period_align.items()} }), which bounds any forecast-error study",
        f"no target is defined in the source; issue-time columns: {sorted({c for t in tables for c in ROLE[t]['issue']}) or 'none'}, so the moment a forecast value became available cannot be established",
    ],
    "AI / knowledge use": [
        f"catalog comment describes the sample and lists intended uses; column comments: {'none' if not _col_comments else len(_col_comments)}",
        f"coded columns with numeric codes: { {t: [c for c in cat_dist[t] if c.endswith(CODE_SUFFIXES)] for t in tables if any(c.endswith(CODE_SUFFIXES) for c in cat_dist[t])} }",
        f"location reference set: {loc_attr['locations'] if loc_attr else 'n/a'} cities with country codes and coordinates",
    ],
}

_silver = [
    "- Read from the Samples catalog; no Bronze table exists for these tables.",
    "- The set of tables, keys and time semantics above decide how many Silver structures are legitimately needed; nothing is assumed here.",
    "- The imperial/metric alignment and schema-difference results decide whether the two unit systems are separate records or one record in two units.",
    "- The forecast/historical overlap results decide how a forecast row and an observed row for the same key can be related.",
    "- Cap-like columns and all-missing columns need an explicit keep / null / drop rule.",
]

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            f"Composite keys and their duplicate counts: {dupk}. Table grains follow the table names (daily_calendar, daynight, hourly).",
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            f"Rows per location: { {t: (s['min'], s['p50'], s['max']) for t, s in loc_stats.items()} }. Joining daynight to daily on (location, date) doubles rows unless the day/night flag is used.",
        ),
        (
            "Target contamination",
            "No target is defined by the source; forecast columns are predictions, so using them as features for their own observed counterpart is circular.",
        ),
        (
            "Temporal / post-event leakage",
            f"Issue-time columns: { {t: ROLE[t]['issue'] for t in tables if ROLE[t]['issue']} or 'none' }. Without one, a forecast row cannot be tied to the moment it was issued.",
        ),
        (
            "Proxy leakage",
            "Location keys and names are near-identifiers; a model given them memorises the place.",
        ),
        (
            "Split / entity leakage",
            "Split by location and by contiguous time; rows of one location and neighbouring times are strongly dependent.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            f"Forecast/historical overlap: { {f'{a} vs {b}': r['both'] for (a, b), r in period_align.items()} }. Where the windows overlap, a historical row can leak into a forecast evaluation.",
        ),
        (
            "Survivorship / coverage bias",
            f"Locations per table: { {t: s['locations'] for t, s in loc_stats.items()} }; the sample is stated to be a reduced selection.",
        ),
        (
            "Missingness leakage",
            f"All-missing columns per table: { {t: len(prof[t]['all_missing']) for t in tables} }; partly missing columns are listed in Data Quality and can track weather condition.",
        ),
        (
            "Duplicate-event leakage",
            f"Full-row duplicates: { {t: prof[t]['dups'] for t in tables} }; composite-key duplicates: { {t: d['dup_groups'] for t, d in dupk.items()} }.",
        ),
        (
            "Target / feature temporal misalignment",
            f"Time-zone-like columns: { {t: ROLE[t]['tz'] for t in tables if ROLE[t]['tz']} or 'none' }; the local-time key is not assumed to be UTC.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            f"Imperial and metric tables hold the same measures in two units; identical-column counts: { {f'{a}|{b}': sum(1 for x in rows if x['n'] and x['equal'] == x['n']) for (a, b), rows in unit_cols.items()} }. Using both as features double-counts.",
        ),
        (
            "Data-generation-process leakage",
            "Provider-generated forecasts embed the provider's model; the catalog comment describes the data as an AccuWeather sample.",
        ),
        (
            "Class / label instability",
            "Condition text / icon and weather codes are provider taxonomies and can change between provider versions.",
        ),
        (
            "Label availability lag",
            "Observations arrive after the fact; forecasts are available before -- which side each table is on is in the table name.",
        ),
        (
            "Source / version / regime change",
            "No provider version field is assumed; look for constant version-like columns in the constant-column list.",
        ),
        (
            "Sample-vs-full divergence",
            "Every statistic is a full Spark aggregation over the table read, except approximate distinct counts and percentiles, which are labelled; the catalog comment states these tables are a reduced portion of the provider's data.",
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Provenance / Source Evidence", "\n".join(_prov)),
        ("Structural Integrity", "\n".join(_struct)),
        ("Data Quality", "\n".join(_dq)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Unit & Semantic Validation", "\n".join(_unit)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Geography", "\n".join(_geo)),
        ("Temporal Semantics", "\n".join(_temporal)),
        ("Temporal Consistency", "\n".join(_tcons)),
        ("Relationship Cardinality", "\n".join(_card)),
        ("Unit-System Variants", "\n".join(_variants)),
        ("Forecast vs Historical", "\n".join(_fh)),
        ("Physical Consistency and Spatial Patterns", "\n".join(_phys)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Observations by Area", area_block(_areas)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- WEATHER (DAILY TEMPERATURE HISTORY, SAMPLES)
# MAGIC
# MAGIC **Purpose:** Profile the `weather` dataset read in place from the
# MAGIC Databricks Samples Volume (no Bronze table) -- file inventory and README
# MAGIC provenance, schema and header check, missingness, duplicates, date parse
# MAGIC yield and range, calendar continuity against an independent daily grid,
# MAGIC rows per date, temperature plausibility and unit, seasonality, day-to-day
# MAGIC change and outliers, geography and station evidence, and the layered
# MAGIC modelling-risk checklist -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "weather_seattle"
NB_KEY = "01_weather_seattle"
SECTION_TITLE = "Daily temperature history (Samples: weather)"
ROOT = f"{SAMPLES_VOLUME_ROOT}/weather"
DATE_HINTS = ("date", "day", "time", "timestamp")
TEMP_HINTS = ("temp", "tmax", "tmin", "high", "low")
US_FORMATS = (
    "yyyy-MM-dd",
    "M/d/yyyy",
    "MM/dd/yyyy",
    "yyyy/MM/dd",
    "M/d/yy",
    "d-MMM-yyyy",
    "MMM d, yyyy",
    "yyyyMMdd",
)
# Fahrenheit bound wide enough for any mid-latitude station; a value outside it
# is a unit or parse problem, not weather.
TEMP_BOUNDS_F = (-40.0, 130.0)
JUMP_F = 25.0

# COMMAND ----------

# DBTITLE 1,Discover files
entries, kinds, frames = discover_files(ROOT)

# COMMAND ----------

# DBTITLE 1,Support files (README) -- provenance evidence
support = print_support_files(kinds)

# COMMAND ----------

# DBTITLE 1,Frames to profile
DFS = {k: v["df"] for k, v in frames.items()}
DATA_COLS = {k: [c for c in df.columns if c != "__file"] for k, df in DFS.items()}
ROLE = {}
for k, cols in DATA_COLS.items():
    ROLE[k] = {
        "date": next((c for c in cols if any(h == c.lower() for h in DATE_HINTS)), None)
        or next((c for c in cols if any(h in c.lower() for h in DATE_HINTS)), None),
        "temp": [c for c in cols if any(h in c.lower() for h in TEMP_HINTS)],
    }
    print(k, cols, ROLE[k])

# COMMAND ----------

# DBTITLE 1,Structural check -- was a header applied?
struct = {}
for k, cols in DATA_COLS.items():
    unnamed = [c for c in cols if c.lower().startswith(("_c", "unnamed"))]
    struct[k] = {
        "cols": len(cols),
        "unnamed": unnamed,
        "date_col": ROLE[k]["date"],
        "temp_cols": ROLE[k]["temp"],
        "broken": bool(unnamed) or ROLE[k]["date"] is None,
    }
    print(k, struct[k])

# COMMAND ----------

# DBTITLE 1,Profile each frame -- rows, missingness, approx distinct
prof = profile_frames_without_file(DFS)

# COMMAND ----------

# DBTITLE 1,Constant columns + full-row duplicates
constant_and_duplicates(DFS, prof)

# COMMAND ----------

# DBTITLE 1,Date parse yield, range and granularity
tsem = {}
for k, df in DFS.items():
    dc = ROLE[k]["date"]
    if dc:
        tsem[k] = timestamp_semantics(
            df.drop("__file"),
            dc,
            formats=US_FORMATS + GERMAN_TS_FORMATS,
            valid_from="1900-01-01",
            tz="unspecified",
        )
        for ln in tsem[k]["lines"]:
            print(f"{k}: {ln}")

# COMMAND ----------

# DBTITLE 1,Calendar continuity against an independent daily grid
cont = {}
for k, df in DFS.items():
    dc = ROLE[k]["date"]
    if not dc:
        continue
    day = F.to_date(parse_ts_multi(dc, US_FORMATS + GERMAN_TS_FORMATS))
    days = df.select(day.alias("d")).where(F.col("d").isNotNull())
    r = days.agg(
        F.min("d").alias("lo"),
        F.max("d").alias("hi"),
        F.countDistinct("d").alias("distinct_days"),
    ).first()
    expected = (r["hi"] - r["lo"]).days + 1 if r["lo"] else 0
    grid = spark.range(expected).select(
        F.date_add(F.lit(r["lo"]), F.col("id").cast("int")).alias("d")
    )
    missing = grid.join(days.distinct(), "d", "left_anti")
    miss_n = missing.count()
    cont[k] = {
        "first": str(r["lo"]),
        "last": str(r["hi"]),
        "distinct_days": r["distinct_days"],
        "expected_days": expected,
        "missing_days": miss_n,
        "missing_sample": [
            str(x["d"]) for x in missing.orderBy("d").limit(15).collect()
        ],
    }
    print(k, cont[k])

# COMMAND ----------

# DBTITLE 1,Rows per date
per_date = {}
for k, df in DFS.items():
    dc = ROLE[k]["date"]
    if not dc:
        continue
    day = F.to_date(parse_ts_multi(dc, US_FORMATS + GERMAN_TS_FORMATS))
    g = df.select(day.alias("d")).where(F.col("d").isNotNull()).groupBy("d").count()
    dist = (
        g.groupBy(F.col("count").alias("rows_per_date"))
        .agg(F.count(F.lit(1)).alias("dates"))
        .orderBy("rows_per_date")
        .collect()
    )
    per_date[k] = [(int(r["rows_per_date"]), r["dates"]) for r in dist]
    print(k, "rows-per-date -> number of dates:", per_date[k])

# COMMAND ----------

# DBTITLE 1,Temperature parse yield, range and unit plausibility
num, plaus = {}, {}
for k, df in DFS.items():
    num[k] = numeric_scan(df, ROLE[k]["temp"])
    plaus[k] = {}
    for c in ROLE[k]["temp"]:
        s = num[k][c]
        print(
            f"{k}.{c}: yield={s['yield']} range={s['min']}..{s['max']} mean={fmt_num(s['mean'])} sd={fmt_num(s['sd'])}"
        )
        if s["is_numeric"]:
            plaus[k][c] = plausibility(
                df, c, lo=TEMP_BOUNDS_F[0], hi=TEMP_BOUNDS_F[1], sentinels=()
            )
            print("   ", {a: plaus[k][c].get(a) for a in ("below", "above", "zero")})

# COMMAND ----------

# DBTITLE 1,Quantiles per temperature column
quant = {}
for k, df in DFS.items():
    quant[k] = {}
    for c in ROLE[k]["temp"]:
        if num[k][c]["is_numeric"]:
            quant[k][c] = quantiles(df, c)
            print(k, c, quant[k][c])

# COMMAND ----------

# DBTITLE 1,Seasonality -- mean temperature by calendar month
season = {}
for k, df in DFS.items():
    dc, tcs = ROLE[k]["date"], [c for c in ROLE[k]["temp"] if num[k][c]["is_numeric"]]
    if not dc or not tcs:
        continue
    day = F.to_date(parse_ts_multi(dc, US_FORMATS + GERMAN_TS_FORMATS))
    rows = (
        df.select(F.month(day).alias("m"), to_double(tcs[0]).alias("t"))
        .where(F.col("m").isNotNull())
        .groupBy("m")
        .agg(F.avg("t").alias("mean"), F.count(F.lit(1)).alias("rows"))
        .orderBy("m")
        .collect()
    )
    season[k] = [(int(r["m"]), r["mean"], r["rows"]) for r in rows]
    print(k, tcs[0], [(m, round(v, 1)) for m, v, _ in season[k]])

# COMMAND ----------

# DBTITLE 1,Rows per year and month coverage
coverage = {}
for k, df in DFS.items():
    dc = ROLE[k]["date"]
    if not dc:
        continue
    day = F.to_date(parse_ts_multi(dc, US_FORMATS + GERMAN_TS_FORMATS))
    rows = (
        df.select(F.date_format(day, "yyyy-MM").alias("ym"))
        .where(F.col("ym").isNotNull())
        .groupBy("ym")
        .count()
        .orderBy("ym")
        .collect()
    )
    coverage[k] = [(r["ym"], r["count"]) for r in rows]
    print(
        k, "months present:", len(coverage[k]), coverage[k][:3], "...", coverage[k][-3:]
    )

# COMMAND ----------

# DBTITLE 1,Day-to-day change, outliers and lag-1 autocorrelation
dyn = {}
for k, df in DFS.items():
    dc, tcs = ROLE[k]["date"], [c for c in ROLE[k]["temp"] if num[k][c]["is_numeric"]]
    if not dc or not tcs:
        continue
    day = F.to_date(parse_ts_multi(dc, US_FORMATS + GERMAN_TS_FORMATS))
    t = to_double(tcs[0])
    daily = (
        df.select(day.alias("d"), t.alias("t"))
        .where(F.col("d").isNotNull())
        .groupBy("d")
        .agg(F.avg("t").alias("t"))
    )
    w = Window.orderBy("d")
    lg = daily.withColumn("prev", F.lag("t").over(w)).where(F.col("prev").isNotNull())
    r = (
        lg.agg(
            F.sum((F.abs(F.col("t") - F.col("prev")) > JUMP_F).cast("long")).alias(
                "jumps"
            ),
            F.max(F.abs(F.col("t") - F.col("prev"))).alias("max_jump"),
            F.count(F.lit(1)).alias("pairs"),
        )
        .first()
        .asDict()
    )
    r["autocorr"] = lg.stat.corr("t", "prev")
    s = num[k][tcs[0]]
    z = (F.abs(t - F.lit(s["mean"])) / F.lit(s["sd"])) if s["sd"] else F.lit(0.0)
    r["z_gt_4"] = df.agg(F.sum((z > 4).cast("long"))).first()[0]
    dyn[k] = r
    print(k, dyn[k])

# COMMAND ----------

# DBTITLE 1,Max / min rows per date (when two rows per date)
pair_check = {}
for k, df in DFS.items():
    dc, tcs = ROLE[k]["date"], [c for c in ROLE[k]["temp"] if num[k][c]["is_numeric"]]
    if not dc or not tcs or not per_date.get(k):
        continue
    if [n for n, _ in per_date[k]] != [2]:
        continue
    day = F.to_date(parse_ts_multi(dc, US_FORMATS + GERMAN_TS_FORMATS))
    t = to_double(tcs[0])
    g = (
        df.select(day.alias("d"), t.alias("t"))
        .groupBy("d")
        .agg(F.max("t").alias("hi"), F.min("t").alias("lo"))
    )
    r = (
        g.agg(
            F.avg(F.col("hi") - F.col("lo")).alias("mean_range"),
            F.min(F.col("hi") - F.col("lo")).alias("min_range"),
            F.max(F.col("hi") - F.col("lo")).alias("max_range"),
            F.sum((F.col("hi") == F.col("lo")).cast("long")).alias("equal_pair"),
        )
        .first()
        .asDict()
    )
    pair_check[k] = r
    print(k, r)

# COMMAND ----------

# DBTITLE 1,Per source file -- rows, dates, temperature
FMTS = US_FORMATS + GERMAN_TS_FORMATS
MAX_FILES = 6
per_file, daily_by_file = {}, {}
for k, df in DFS.items():
    dc = ROLE[k]["date"]
    tcs = [c for c in ROLE[k]["temp"] if num[k][c]["is_numeric"]]
    if not dc or not tcs or len(frames[k]["paths"]) < 2:
        continue
    day = F.to_date(parse_ts_multi(dc, FMTS))
    t = to_double(tcs[0])
    base = df.select(F.col("__file").alias("f"), day.alias("d"), t.alias("t"))
    rows = (
        base.groupBy("f")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("d").alias("dates"),
            F.min("d").alias("first"),
            F.max("d").alias("last"),
            F.avg("t").alias("mean"),
            F.min("t").alias("lo"),
            F.max("t").alias("hi"),
        )
        .orderBy("f")
        .limit(MAX_FILES)
        .collect()
    )
    per_file[k] = [r.asDict() for r in rows]
    daily_by_file[k] = base
    for r in per_file[k]:
        print(k, r)

# COMMAND ----------

# DBTITLE 1,Cross-file date alignment and ordering (two files)
align, wide_frames = {}, {}
for k, rows in per_file.items():
    if len(rows) != 2:
        continue
    fa, fb = rows[0]["f"], rows[1]["f"]
    wide = (
        daily_by_file[k]
        .groupBy("d")
        .pivot("f", [fa, fb])
        .agg(F.avg("t"))
        .select("d", F.col(f"`{fa}`").alias("a"), F.col(f"`{fb}`").alias("b"))
    )
    wide_frames[k] = wide
    both = F.col("a").isNotNull() & F.col("b").isNotNull()
    r = (
        wide.agg(
            F.sum(both.cast("long")).alias("both"),
            F.sum((F.col("a").isNotNull() & F.col("b").isNull()).cast("long")).alias(
                "only_a"
            ),
            F.sum((F.col("a").isNull() & F.col("b").isNotNull()).cast("long")).alias(
                "only_b"
            ),
            F.avg(F.when(both, F.col("a") - F.col("b"))).alias("mean_diff"),
            F.min(F.when(both, F.col("a") - F.col("b"))).alias("min_diff"),
            F.max(F.when(both, F.col("a") - F.col("b"))).alias("max_diff"),
            F.sum((both & (F.col("a") < F.col("b"))).cast("long")).alias("a_lt_b"),
            F.sum((both & (F.col("a") == F.col("b"))).cast("long")).alias("a_eq_b"),
        )
        .first()
        .asDict()
    )
    r["a"], r["b"] = fa, fb
    align[k] = r
    print(k, r)

# COMMAND ----------

# DBTITLE 1,Per source file -- day-to-day change and lag-1 autocorrelation
dyn_file = {}
for k, rows in per_file.items():
    dyn_file[k] = {}
    for fr in rows:
        daily = (
            daily_by_file[k]
            .where((F.col("f") == fr["f"]) & F.col("d").isNotNull())
            .groupBy("d")
            .agg(F.avg("t").alias("t"))
        )
        lg = daily.withColumn("prev", F.lag("t").over(Window.orderBy("d"))).where(
            F.col("prev").isNotNull()
        )
        r = (
            lg.agg(
                F.sum((F.abs(F.col("t") - F.col("prev")) > JUMP_F).cast("long")).alias(
                    "jumps"
                ),
                F.max(F.abs(F.col("t") - F.col("prev"))).alias("max_jump"),
                F.count(F.lit(1)).alias("pairs"),
            )
            .first()
            .asDict()
        )
        r["autocorr"] = lg.stat.corr("t", "prev")
        dyn_file[k][fr["f"]] = r
        print(k, fr["f"], r)

# COMMAND ----------

# DBTITLE 1,Per source file -- seasonality by month
season_file = {}
for k, base in daily_by_file.items():
    rows = (
        base.where(F.col("d").isNotNull())
        .groupBy("f", F.month("d").alias("m"))
        .agg(F.avg("t").alias("mean"))
        .orderBy("f", "m")
        .collect()
    )
    season_file[k] = {}
    for r in rows:
        season_file[k].setdefault(r["f"], []).append((int(r["m"]), r["mean"]))
    print(k, {f: [(m, round(v, 1)) for m, v in vs] for f, vs in season_file[k].items()})

# COMMAND ----------

# DBTITLE 1,Per source file -- yearly means, record values and monthly outliers
clim = {}
for k, base in daily_by_file.items():
    b = base.where(F.col("d").isNotNull())
    yearly = (
        b.groupBy("f", F.year("d").alias("y"))
        .agg(F.avg("t").alias("mean"), F.count(F.lit(1)).alias("rows"))
        .orderBy("f", "y")
        .collect()
    )
    mw = Window.partitionBy("f", F.month("d"))
    z = (F.col("t") - F.avg("t").over(mw)) / F.stddev("t").over(mw)
    outl = {
        r["f"]: r["n"]
        for r in b.select("f", z.alias("z"))
        .groupBy("f")
        .agg(F.sum((F.abs("z") > 3).cast("long")).alias("n"))
        .collect()
    }
    clim[k] = {"yearly": {}, "records": {}, "month_outliers": outl}
    for r in yearly:
        clim[k]["yearly"].setdefault(r["f"], []).append(
            (int(r["y"]), round(r["mean"], 2), r["rows"])
        )
    for fr in per_file[k]:
        one = b.where(F.col("f") == fr["f"])
        hi = [
            (str(r["d"]), r["t"]) for r in one.orderBy(F.desc("t")).limit(3).collect()
        ]
        lo = [(str(r["d"]), r["t"]) for r in one.orderBy("t").limit(3).collect()]
        clim[k]["records"][fr["f"]] = {"highest": hi, "lowest": lo}
    print(k, clim[k])

# COMMAND ----------

# DBTITLE 1,Daily high-low range by month and correlation
diurnal = {}
for k, wide in wide_frames.items():
    both = wide.where(F.col("a").isNotNull() & F.col("b").isNotNull())
    rows = (
        both.groupBy(F.month("d").alias("m"))
        .agg(F.avg(F.col("a") - F.col("b")).alias("range"))
        .orderBy("m")
        .collect()
    )
    diurnal[k] = {
        "corr": both.stat.corr("a", "b"),
        "monthly_range": [(int(r["m"]), round(r["range"], 1)) for r in rows],
    }
    print(k, diurnal[k])

# COMMAND ----------

# DBTITLE 1,Histograms (binned in Spark)
hist = {}
for k, df in DFS.items():
    hist[k] = {}
    for c in ROLE[k]["temp"]:
        s = num[k][c]
        if s["is_numeric"]:
            hist[k][c] = hist_counts(df, c, s["min"], s["max"])

# COMMAND ----------

# DBTITLE 1,Figures
figs = []
first = next(iter(DFS))
if season.get(first) and facet_bars(
    {"mean temperature by month": [(str(m), v) for m, v, _ in season[first]]},
    "Weather -- seasonal profile",
    "weather_seattle_seasonal.png",
    rot=0,
    ncols=1,
):
    figs.append(("Weather -- seasonal profile", "weather_seattle_seasonal.png"))
if hist.get(first) and facet_bars(
    hist[first],
    "Weather -- temperature distribution",
    "weather_seattle_distribution.png",
    rot=60,
    ncols=2,
):
    figs.append(
        ("Weather -- temperature distribution", "weather_seattle_distribution.png")
    )
if coverage.get(first) and lineplot(
    coverage[first],
    "Weather -- rows per month",
    "month",
    ylabel="rows",
    filename="weather_seattle_rows_per_month.png",
):
    figs.append(("Weather -- rows per month", "weather_seattle_rows_per_month.png"))

# COMMAND ----------

# DBTITLE 1,Findings
for k in DFS:
    print(
        f"{k}: rows={prof[k]['total']}, cols={len(prof[k]['cols'])}, constant={prof[k]['constant']}, "
        f"dups={prof[k]['dups']}, continuity={cont.get(k)}, rows-per-date={per_date.get(k)}"
    )

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/weather_seattle.md
_profile = [
    "| frame | separator | source files | rows | cols | constant columns |",
    "|---|---|---|---|---|---|",
]
for k in DFS:
    _profile.append(
        f"| {k} | {frames[k]['sep']} | {len(frames[k]['paths'])} | {prof[k]['total']} | "
        f"{len(prof[k]['cols'])} | {', '.join(prof[k]['constant']) or '-'} |"
    )

_prov = [
    para(
        "The dataset is read in place from the Databricks Samples Volume;",
        "there is no Bronze table and no ECRMAP acquisition step.",
    ),
    f"Files under `{ROOT}`: {len(entries)} ({ {k: len(v) for k, v in kinds.items()} }).",
]
for name, lines in support.items():
    _prov.append(f"README-type file `{name}` (first lines):")
    _prov += [f"  > {ln[:160]}" for ln in lines[:30] if ln.strip()]
if not support:
    _prov.append("- No README / licence file in the directory (LIMITATION).")

_struct = []
for k, s in struct.items():
    _struct.append(
        f"- {k}: {s['cols']} columns; unnamed={s['unnamed'] or 'none'}; date column={s['date_col']}; "
        f"temperature columns={s['temp_cols'] or 'none'} -> header {'NOT applied / date column missing' if s['broken'] else 'applied'}."
    )

_dq = ["Full-row exact duplicates and missingness per frame:"]
for k in DFS:
    p = prof[k]
    _dq.append(f"- {k}: full-row duplicates {p['dups']}.")
    _dq.append(
        "  missingness: "
        + ", ".join(
            f"{c}={p['miss'][c] / p['total'] if p['total'] else 0:.4f}"
            for c in p["cols"]
        )
    )

_entities = [
    para(
        "The grain is one temperature observation per date (a daily series); there is no",
        "station, city or location column to identify the observing site.",
    )
]
for k in DFS:
    _entities.append(
        f"- {k}: approx distinct per column: "
        + ", ".join(f"{c}={prof[k]['acd'][c]}" for c in prof[k]["cols"])
        + f"; rows per date (rows-per-date -> number of dates): {per_date.get(k)}."
    )

_unit = []
for k in DFS:
    for c in ROLE[k]["temp"]:
        s = num[k][c]
        line = (
            f"- {k}.`{c}`: numeric yield {s['yield']:.1%}, range {s['min']}..{s['max']}, mean {fmt_num(s['mean'])}, "
            f"sd {fmt_num(s['sd'])}"
        )
        if c in plaus[k]:
            line += f"; outside {TEMP_BOUNDS_F} (Fahrenheit assumption): below {plaus[k][c]['below']}, above {plaus[k][c]['above']}"
        _unit.append(line + ".")
_unit.append(
    para(
        "The unit is not in the data. The README (if any) states it; a range consistent with",
        "Fahrenheit is a plausibility check, not proof.",
    )
)
if pair_check:
    for k, r in pair_check.items():
        _unit.append(
            f"- {k}: with two rows per date, per-date high-low range mean {fmt_num(r['mean_range'])}, "
            f"min {r['min_range']}, max {r['max_range']}, dates where both rows are equal {r['equal_pair']}. "
            + (
                "The two source files name the roles (see the per-file comparison)."
                if k in align
                else "The data does not label which row is the maximum and which the minimum."
            )
        )

for k, rows in per_file.items():
    for fr in rows:
        _unit.append(
            f"- {k} / `{fr['f']}`: {fr['rows']} rows, {fr['dates']} distinct dates, temperature range {fmt_num(fr['lo'])}..{fmt_num(fr['hi'])}, mean {fmt_num(fr['mean'])}."
        )
for k, r in align.items():
    _unit.append(
        para(
            f"- {k}: `{r['a']}` minus `{r['b']}` on the {r['both']} dates present in both:",
            f"mean {fmt_num(r['mean_diff'])}, min {fmt_num(r['min_diff'])}, max {fmt_num(r['max_diff'])};",
            f"`{r['a']}` below `{r['b']}` on {r['a_lt_b']} dates, equal on {r['a_eq_b']}.",
            "File names carry the only high/low label; if the names say high and low, a below-count above zero is an inversion.",
        )
    )

_domain = [
    "No categorical column is expected. Columns with 2..60 approx distinct values:"
]
for k in DFS:
    _domain.append(
        f"- {k}: {[c for c in prof[k]['cols'] if 1 < prof[k]['acd'][c] <= 60] or 'none'}"
    )

_temporal = []
for k in DFS:
    if k in tsem:
        _temporal += [f"- {k}: {ln}" for ln in tsem[k]["lines"]]
    if k in cont:
        c = cont[k]
        _temporal.append(
            f"- {k}: {c['first']} .. {c['last']}; {c['distinct_days']} distinct days vs {c['expected_days']} expected; "
            f"{c['missing_days']} missing days (first: {c['missing_sample'][:8]})."
        )
for k, rows in per_file.items():
    for fr in rows:
        _temporal.append(
            f"- {k} / `{fr['f']}`: {fr['first']} .. {fr['last']}; {fr['dates']} distinct dates."
        )
for k, r in align.items():
    _temporal.append(
        f"- {k}: dates in both files {r['both']}; only in `{r['a']}` {r['only_a']}; only in `{r['b']}` {r['only_b']}."
    )
if not _temporal:
    _temporal.append("- No date column located; temporal semantics not assessed.")

_tcons = []
for k, r in dyn.items():
    _tcons.append(
        f"- {k}: day-to-day changes over {JUMP_F} degrees: {r['jumps']} of {r['pairs']}; largest {fmt_num(r['max_jump'])}; "
        f"lag-1 autocorrelation of the pooled daily mean (rows sharing a date averaged across files) {fmt_num(r['autocorr'])}; values more than 4 sd from the pooled mean: {r['z_gt_4']}."
    )
for k, files in dyn_file.items():
    for f, r in files.items():
        _tcons.append(
            f"- {k} / `{f}`: day-to-day changes over {JUMP_F} degrees: {r['jumps']} of {r['pairs']}; largest {fmt_num(r['max_jump'])}; "
            f"lag-1 autocorrelation {fmt_num(r['autocorr'])}."
        )
if not _tcons:
    _tcons.append(
        "- Day-to-day consistency not assessed (no parsed date or numeric temperature)."
    )

_card = [
    para(
        "Single-entity daily series: there is no second table and no key to relate to.",
        "The only relationship is date -> rows (see rows per date above).",
    )
]

_regime = [
    "Seasonal profile by calendar month; the pooled line averages every row of the frame (both files together), so per-file lines are the ones to read:"
]
for k, rows in season.items():
    _regime.append(f"- {k} (pooled): " + ", ".join(f"m{m}={v:.1f}" for m, v, _ in rows))
for k, files in season_file.items():
    for f, vs in files.items():
        _regime.append(f"- {k} / `{f}`: " + ", ".join(f"m{m}={v:.1f}" for m, v in vs))
if not season:
    _regime.append("- not computed.")
_regime.append(
    para(
        "A physically plausible temperate-climate series shows a single summer maximum and",
        "winter minimum; a flat or multi-peaked profile would indicate a different meaning",
        "for the value column.",
    )
)


def _by_value(pair):
    return pair[1]


_clim = []
for k, c in clim.items():
    for f, yrs in c["yearly"].items():
        _clim.append(f"- {k} / `{f}` mean by year (year, mean, rows): {yrs}.")
    for f, rec in c["records"].items():
        _clim.append(f"- {k} / `{f}` highest {rec['highest']}, lowest {rec['lowest']}.")
    _clim.append(
        f"- {k} days more than 3 sd from their own month's mean, per file: {c['month_outliers']}."
    )
for k, d in diurnal.items():
    _clim.append(
        f"- {k} correlation of the two files' values on the same date: {fmt_num(d['corr'])}; mean difference by month (month, degrees): {d['monthly_range']}."
    )
if not _clim:
    _clim.append(
        "- Not computed (needs two or more source files with a parsed date and numeric temperature)."
    )

_cycle = {
    k: {
        f: (min(vs, key=_by_value)[0], max(vs, key=_by_value)[0]) for f, vs in v.items()
    }
    for k, v in season_file.items()
}

_areas = {
    "Domain understanding": [
        f"README-type files: {list(support) or 'none'}; describes daily high and low temperatures for one US city, Fahrenheit, from a national weather service",
        f"files found: { {k: [f['f'] for f in v] for k, v in per_file.items()} }; high-below-low dates: { {k: a['a_lt_b'] for k, a in align.items()} }",
        f"yearly means: { {k: c['yearly'] for k, c in clim.items()} }",
    ],
    "Structure and engineering": [
        f"{len(entries)} files ({ {k: len(v) for k, v in kinds.items()} }); the data files carry no extension and are read as delimited text with a header",
        f"schema: { {k: cols for k, cols in DATA_COLS.items()} }",
        "no station, city or role column: the file name is the only carrier of the high / low role",
    ],
    "Temporal": [
        f"{ {k: (c['first'], c['last'], c['distinct_days'], c['missing_days']) for k, c in cont.items()} } (first, last, distinct days, missing days)",
        f"lag-1 autocorrelation per file: { {k: {f: round(r['autocorr'], 3) for f, r in v.items()} for k, v in dyn_file.items()} }",
    ],
    "Spatial": [
        "no coordinate or station column; the location comes only from the README text"
    ],
    "Data quality": [
        f"full-row duplicates { {k: prof[k]['dups'] for k in DFS} }; missing values { {k: sum(prof[k]['miss'].values()) for k in DFS} }",
        f"days more than 3 sd from their month's mean: { {k: c['month_outliers'] for k, c in clim.items()} }",
    ],
    "Statistical patterns": [
        f"seasonal cycle per file (coldest month, warmest month): {_cycle}",
        f"quantiles: { {k: q for k, q in quant.items()} }",
    ],
    "Relationships": [
        f"high vs low on the same date: { {k: (round(d['corr'], 3) if d['corr'] is not None else None) for k, d in diurnal.items()} }",
        f"mean high-low difference by month: { {k: d['monthly_range'] for k, d in diurnal.items()} }",
    ],
    "Analytics use": [
        "one measure (temperature) on one date dimension with a two-valued role given by the file name; no other dimension is available"
    ],
    "ML use": [
        f"a single-series regression target; strong day-to-day persistence: { {k: {f: round(r['autocorr'], 3) for f, r in v.items()} for k, v in dyn_file.items()} }; no covariates in the source"
    ],
    "AI / knowledge use": [
        "the only descriptive text is the README (source, period, attribute definitions); no text or label columns"
    ],
}

_coverage = [
    para(
        "One site, one short window; the data does not carry the site name, the station",
        "identifier, or the observation time of day. Coverage statements rest on the README.",
    )
]
for k, rows in coverage.items():
    _coverage.append(
        f"- {k}: {len(rows)} calendar months present ({rows[0][0]} .. {rows[-1][0]}); rows per month min "
        f"{min(n for _, n in rows)}, max {max(n for _, n in rows)}."
    )

_dist = []
for k in DFS:
    for c, q in quant[k].items():
        _dist.append(
            f"- {k}.`{c}` quantiles (p1/p25/p50/p75/p99): "
            + ", ".join(fmt_num(v) for v in q.values())
        )

_findings_md = "\n".join(
    f"- {k}: rows={prof[k]['total']}, cols={len(prof[k]['cols'])}, dups={prof[k]['dups']}, "
    f"missing days={cont.get(k, {}).get('missing_days')}, rows per date={per_date.get(k)}"
    for k in DFS
)

_silver = [
    "- Read from the Samples Volume; no Bronze table exists for this dataset.",
    "- Date needs an explicit format rule (formats matched are listed under Temporal Semantics); temperature needs a typed cast with a stated unit.",
    "- The data has no station or location column: any location label must be supplied and flagged as derived from the README, not from the data.",
]
if any(per_date.get(k) and [n for n, _ in per_date[k]] == [2] for k in DFS):
    _silver.append(
        "- Two rows per date and no role column: the only high/low label is the source file name, so `__file` must be carried into Silver to keep the role."
    )

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            f"One row per temperature observation; rows per date: { {k: per_date.get(k) for k in DFS} }. If two rows share a date the grain is (date, unlabelled high/low), not (date).",
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            "Joining to any daily source on date fans out by the rows-per-date count above; aggregate to one row per date first.",
        ),
        (
            "Target contamination",
            f"Temperature is the natural target; the columns found are {DATA_COLS}, so other features would have to be supplied from elsewhere.",
        ),
        (
            "Temporal / post-event leakage",
            "Daily series -- the lag-1 autocorrelation is in Temporal Consistency; where it is high, a random split leaks neighbouring days, so split by contiguous date range.",
        ),
        (
            "Proxy leakage",
            "The calendar month is a proxy for temperature to the extent the seasonal profile above shows a clear cycle.",
        ),
        (
            "Split / entity leakage",
            "Single entity; split by time only.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            "Not applicable -- no reference table.",
        ),
        (
            "Survivorship / coverage bias",
            f"One site and a short window; months present: { {k: len(v) for k, v in coverage.items()} }. Statements about other places or years cannot be drawn.",
        ),
        (
            "Missingness leakage",
            f"Missing calendar days: { {k: cont[k]['missing_days'] for k in cont} } -- gaps are structural, not random, if they cluster.",
        ),
        (
            "Duplicate-event leakage",
            f"Full-row duplicates: { {k: prof[k]['dups'] for k in DFS} }; a repeated (date, value) row is not evidence of two observations.",
        ),
        (
            "Target / feature temporal misalignment",
            "The observation time of day is not recorded, so alignment with any sub-daily source is not possible without an assumption.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            "Unit is not in the data; treat as Fahrenheit only if the README says so. Two unlabelled rows per date must not be treated as both the target.",
        ),
        (
            "Data-generation-process leakage",
            "Values are stated in the README to come from a national weather service record; the extraction rule (which observation) is not in the data.",
        ),
        (
            "Class / label instability",
            "Regression target -- no classes.",
        ),
        (
            "Label availability lag",
            "Not applicable -- observations are historical.",
        ),
        (
            "Source / version / regime change",
            "Single static extract; no version indicator; station moves or instrument changes are not recorded.",
        ),
        (
            "Sample-vs-full divergence",
            "Every statistic is a full Spark aggregation over the files read; the extract's own selection rule is undocumented in the data.",
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Provenance / Source Evidence", "\n".join(_prov)),
        ("Structural Integrity", "\n".join(_struct)),
        ("Data Quality", "\n".join(_dq)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Unit & Semantic Validation", "\n".join(_unit)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Temporal Semantics", "\n".join(_temporal)),
        ("Temporal Consistency", "\n".join(_tcons)),
        ("Relationship Cardinality", "\n".join(_card)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Climatology and Extremes", "\n".join(_clim)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Observations by Area", area_block(_areas)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
