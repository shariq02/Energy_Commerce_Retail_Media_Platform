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
require_frames(frames, ROOT, kinds)
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
