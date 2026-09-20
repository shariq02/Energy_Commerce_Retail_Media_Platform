# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- WEATHER (DAILY TEMPERATURE HISTORY, SAMPLES)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the `weather` dataset read in place from the
# MAGIC Databricks Samples Volume (no Bronze table) -- file inventory and README
# MAGIC provenance, schema and header check, missingness, duplicates, date parse
# MAGIC yield and range, calendar continuity against an independent daily grid,
# MAGIC rows per date, temperature plausibility and unit, seasonality, day-to-day
# MAGIC change and outliers, geography and station evidence, and the layered
# MAGIC modelling-risk checklist -- as evidence.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# MAGIC %run ../_samples_eda_common

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

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Discover files
entries, kinds, frames = load_volume_frames(ROOT)
print(f"{ROOT}: {len(entries)} files")
for e in entries:
    print(f"  {file_kind(e['name']):<10} {e['size']:>12}  {e['path']}")
print({k: len(v) for k, v in kinds.items()})
print({k: (v["sep"], len(v["paths"])) for k, v in frames.items()})

# COMMAND ----------

# DBTITLE 1,Support files (README) -- provenance evidence
support = read_support_text(kinds)
for name, lines in support.items():
    print("=" * 90, f"\n{name}")
    for ln in lines[:40]:
        print("  " + ln[:160])
if not support:
    print("no README / licence file in the directory")

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
prof = {}
for k, df in DFS.items():
    prof[k] = profile_frame(df.drop("__file"))
    p = prof[k]
    print("=" * 90, f"\n{k}  rows={p['total']}  cols={len(p['cols'])}")
    for c in p["cols"]:
        print(
            f"  {c[:40]:<40} missing={p['miss'][c]:>8} "
            f"rate={p['miss'][c] / p['total'] if p['total'] else 0:.4f} "
            f"approx_distinct={p['acd'][c]}"
        )

# COMMAND ----------

# DBTITLE 1,Constant columns + full-row duplicates
for k, df in DFS.items():
    d = df.drop("__file")
    prof[k]["constant"] = constant_cols(d, prof[k])
    prof[k]["dups"] = full_row_dup_count(d, prof[k]["total"])
    print(f"{k}: constant={prof[k]['constant']}  full-row duplicates={prof[k]['dups']}")

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
            "The data does not label which row is the maximum and which the minimum."
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
if not _temporal:
    _temporal.append("- No date column located; temporal semantics not assessed.")

_tcons = []
for k, r in dyn.items():
    _tcons.append(
        f"- {k}: day-to-day changes over {JUMP_F} degrees: {r['jumps']} of {r['pairs']}; largest {fmt_num(r['max_jump'])}; "
        f"lag-1 autocorrelation of the daily mean {fmt_num(r['autocorr'])}; values more than 4 sd from the mean: {r['z_gt_4']}."
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

_regime = ["Seasonal profile (mean of the first temperature column by calendar month):"]
for k, rows in season.items():
    _regime.append(f"- {k}: " + ", ".join(f"m{m}={v:.1f}" for m, v, _ in rows))
if not season:
    _regime.append("- not computed.")
_regime.append(
    para(
        "A physically plausible temperate-climate series shows a single summer maximum and",
        "winter minimum; a flat or multi-peaked profile would indicate a different meaning",
        "for the value column.",
    )
)

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
        "- Two rows per date with no label: a high/low role cannot be assigned from the data alone."
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
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)