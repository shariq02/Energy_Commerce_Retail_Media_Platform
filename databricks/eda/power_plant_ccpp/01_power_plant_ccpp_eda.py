# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- POWER PLANT (COMBINED-CYCLE SENSOR READINGS, SAMPLES)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the `power-plant` dataset read in place from the
# MAGIC Databricks Samples Volume (no Bronze table) -- file inventory and README
# MAGIC provenance, schema and header check, missingness, constant columns,
# MAGIC full-row and cross-file duplicates, numeric parse yield, physical
# MAGIC plausibility of each sensor column, distributions, pairwise correlation
# MAGIC and mirror columns, per-file drift, temporal semantics, and the layered
# MAGIC modelling-risk checklist -- as evidence.

# COMMAND ----------

# DBTITLE 1,Imports
import itertools

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# MAGIC %run ../_samples_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "power_plant_ccpp"
NB_KEY = "01_power_plant_ccpp"
SECTION_TITLE = "Combined-cycle power plant sensor readings (Samples: power-plant)"
ROOT = f"{SAMPLES_VOLUME_ROOT}/power-plant"
# Column names and physical bounds expected from the dataset's public
# description -- the header found in the files confirms or refutes them.
EXPECTED = {
    "AT": ("ambient temperature (degC)", -20.0, 50.0),
    "V": ("exhaust vacuum (cm Hg)", 20.0, 90.0),
    "AP": ("ambient pressure (mbar)", 900.0, 1100.0),
    "RH": ("relative humidity (%)", 0.0, 100.0),
    "PE": ("net hourly electrical output (MW)", 400.0, 520.0),
}
TIME_HINTS = ("date", "time", "timestamp", "hour", "year", "month", "day")

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

# COMMAND ----------

# DBTITLE 1,Frames to profile
DFS = {k: v["df"] for k, v in frames.items()}
DATA_COLS = {k: [c for c in df.columns if c != "__file"] for k, df in DFS.items()}
for k, df in DFS.items():
    print(k, DATA_COLS[k])

# COMMAND ----------

# DBTITLE 1,Structural check -- was a header applied?
struct = {}
for k, cols in DATA_COLS.items():
    unnamed = [c for c in cols if c.lower().startswith(("_c", "unnamed"))]
    numeric_named = [
        c for c in cols if c.replace(".", "", 1).replace("-", "", 1).isdigit()
    ]
    expected_hit = [c for c in cols if c.upper() in EXPECTED]
    struct[k] = {
        "cols": len(cols),
        "unnamed": unnamed,
        "numeric_named": numeric_named,
        "expected_hit": expected_hit,
        "broken": len(unnamed) > 0 or len(numeric_named) > 0,
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

# DBTITLE 1,Rows per source file
per_file = {}
for k, df in DFS.items():
    rows = df.groupBy("__file").count().orderBy("__file").collect()
    per_file[k] = [(r["__file"], r["count"]) for r in rows]
    print(k, per_file[k])

# COMMAND ----------

# DBTITLE 1,Constant columns + full-row duplicates
for k, df in DFS.items():
    d = df.drop("__file")
    prof[k]["constant"] = constant_cols(d, prof[k])
    prof[k]["dups"] = full_row_dup_count(d, prof[k]["total"])
    print(f"{k}: constant={prof[k]['constant']}  full-row duplicates={prof[k]['dups']}")

# COMMAND ----------

# DBTITLE 1,Cross-file duplicate rows
cross = {}
for k, df in DFS.items():
    cols = DATA_COLS[k]
    h = df.select("__file", F.xxhash64(*[qcol(c) for c in cols]).alias("__h"))
    g = h.groupBy("__h").agg(
        F.countDistinct("__file").alias("files"), F.count(F.lit(1)).alias("n")
    )
    r = g.agg(
        F.sum((F.col("files") > 1).cast("long")).alias("in_many_files"),
        F.sum(F.when(F.col("n") > 1, F.col("n") - 1).otherwise(0)).alias("surplus"),
        F.count(F.lit(1)).alias("distinct_rows"),
    ).first()
    cross[k] = r.asDict()
    print(k, cross[k])

# COMMAND ----------

# DBTITLE 1,Per-file distinct rows and file-set identity
file_sets = {}
for k, df in DFS.items():
    h = df.select("__file", F.xxhash64(*[qcol(c) for c in DATA_COLS[k]]).alias("__h"))
    per = h.groupBy("__file", "__h").count()
    by_file = (
        per.groupBy("__file")
        .agg(
            F.count(F.lit(1)).alias("distinct_rows"),
            F.sum(F.col("count") - 1).alias("repeat_rows"),
        )
        .orderBy("__file")
        .collect()
    )
    spread = (
        per.groupBy("__h")
        .agg(F.count(F.lit(1)).alias("files"))
        .groupBy("files")
        .count()
        .orderBy("files")
        .collect()
    )
    file_sets[k] = {
        "by_file": [
            (r["__file"], r["distinct_rows"], r["repeat_rows"]) for r in by_file
        ],
        "rows_by_file_count": [(r["files"], r["count"]) for r in spread],
    }
    print(k, file_sets[k])

# COMMAND ----------

# DBTITLE 1,Numeric parse yield + moments
num = {}
for k, df in DFS.items():
    num[k] = numeric_scan(df, DATA_COLS[k])
    for c, s in num[k].items():
        print(
            f"{k}.{c:<12} numeric={s['is_numeric']} yield={s['yield']} "
            f"min={s['min']} max={s['max']} mean={fmt_num(s['mean'])} sd={fmt_num(s['sd'])}"
        )

# COMMAND ----------

# DBTITLE 1,Physical plausibility against the expected bounds
plaus = {}
for k, df in DFS.items():
    plaus[k] = {}
    for c in DATA_COLS[k]:
        if c.upper() not in EXPECTED or not num[k][c]["is_numeric"]:
            continue
        label, lo, hi = EXPECTED[c.upper()]
        plaus[k][c] = plausibility(df, c, lo=lo, hi=hi, sentinels=())
        plaus[k][c]["label"] = label
        p = plaus[k][c]
        print(
            f"{k}.{c} ({label}): {p['min']}..{p['max']} mean={fmt_num(p['mean'])} "
            f"below {lo}={p['below']} above {hi}={p['above']}"
        )

# COMMAND ----------

# DBTITLE 1,Rows beyond the expected bounds, per source file
beyond = {}
for k, df in DFS.items():
    beyond[k] = {}
    for c, p in plaus[k].items():
        lo, hi = EXPECTED[c.upper()][1], EXPECTED[c.upper()][2]
        v = to_double(c)
        rows = (
            df.groupBy("__file")
            .agg(
                F.sum((v > hi).cast("long")).alias("above"),
                F.sum((v < lo).cast("long")).alias("below"),
                F.max(v).alias("max"),
            )
            .orderBy("__file")
            .collect()
        )
        if any((r["above"] or 0) or (r["below"] or 0) for r in rows):
            beyond[k][c] = [
                (r["__file"], r["above"], r["below"], r["max"]) for r in rows
            ]
            print(k, c, beyond[k][c])

# COMMAND ----------

# DBTITLE 1,Quantiles per numeric column
quant = {}
for k, df in DFS.items():
    quant[k] = {}
    for c in DATA_COLS[k]:
        if num[k][c]["is_numeric"]:
            quant[k][c] = quantiles(df, c)
            print(k, c, quant[k][c])

# COMMAND ----------

# DBTITLE 1,Pairwise correlation
corr = {}
for k, df in DFS.items():
    ncols = [c for c in DATA_COLS[k] if num[k][c]["is_numeric"]]
    numdf = df.select(*[to_double(c).alias(c) for c in ncols])
    corr[k] = {}
    for a, b in itertools.combinations(ncols, 2):
        corr[k][(a, b)] = numdf.stat.corr(a, b)
    ranked = sorted(corr[k].items(), key=lambda kv: -abs(kv[1] or 0))
    print(k, [(p, round(v, 3)) for p, v in ranked[:10]])

# COMMAND ----------

# DBTITLE 1,Mirror / duplicate columns
mirrors = {}
for k in DFS:
    stats = {c: s for c, s in num[k].items() if s["is_numeric"] and s["sd"]}
    mirrors[k] = mirror_columns(stats)
    print(k, mirrors[k])

# COMMAND ----------

# DBTITLE 1,Per-file drift -- mean of each numeric column by source file
drift = {}
for k, df in DFS.items():
    ncols = [c for c in DATA_COLS[k] if num[k][c]["is_numeric"]]
    if not ncols:
        continue
    rows = (
        df.groupBy("__file")
        .agg(*[F.avg(to_double(c)).alias(c) for c in ncols])
        .orderBy("__file")
        .collect()
    )
    drift[k] = [r.asDict() for r in rows]
    for r in drift[k]:
        print(
            k, {a: (round(b, 3) if isinstance(b, float) else b) for a, b in r.items()}
        )

# COMMAND ----------

# DBTITLE 1,Distribution shape and IQR outliers
shape, iqr_out = {}, {}
for k, df in DFS.items():
    cols = [c for c in DATA_COLS[k] if num[k][c]["is_numeric"] and quant[k].get(c)]
    aggs = []
    for i, c in enumerate(cols):
        v = to_double(c)
        q1, q3 = quant[k][c][0.25], quant[k][c][0.75]
        fence = 1.5 * (q3 - q1)
        aggs += [
            F.skewness(v).alias(f"s{i}"),
            F.kurtosis(v).alias(f"k{i}"),
            F.sum(((v < q1 - fence) | (v > q3 + fence)).cast("long")).alias(f"o{i}"),
        ]
    r = df.agg(*aggs).first().asDict() if aggs else {}
    shape[k] = {c: (r[f"s{i}"], r[f"k{i}"]) for i, c in enumerate(cols)}
    iqr_out[k] = {c: r[f"o{i}"] for i, c in enumerate(cols)}
    print(k, shape[k], iqr_out[k])

# COMMAND ----------

# DBTITLE 1,Output vs strongest inputs -- decile trend on distinct rows
trend = {}
for k, df in DFS.items():
    cols = [c for c in DATA_COLS[k] if num[k][c]["is_numeric"]]
    tgt = next((c for c in cols if c.upper() == "PE"), None)
    if not tgt:
        continue
    pair_r = {
        c: abs(corr[k].get((c, tgt)) or corr[k].get((tgt, c)) or 0.0)
        for c in cols
        if c != tgt
    }
    drivers = sorted(pair_r, key=lambda c: -pair_r[c])[:2]
    base = df.drop("__file").distinct()
    trend[k] = {}
    for drv in drivers:
        d = base.select(
            to_double(drv).alias("x"), to_double(tgt).alias("y")
        ).withColumn("q", F.ntile(10).over(Window.orderBy("x")))
        rows = (
            d.groupBy("q")
            .agg(F.avg("x").alias("x"), F.avg("y").alias("y"))
            .orderBy("q")
            .collect()
        )
        ys = [r["y"] for r in rows]
        steps = [b - a for a, b in itertools.pairwise(ys)]
        trend[k][drv] = {
            "target": tgt,
            "deciles": [(round(r["x"], 2), round(r["y"], 2)) for r in rows],
            "monotone": all(x > 0 for x in steps) or all(x < 0 for x in steps),
        }
        print(k, drv, trend[k][drv])

# COMMAND ----------

# DBTITLE 1,Temporal semantics -- is there any time axis?
temporal = {}
for k, cols in DATA_COLS.items():
    tcols = hint_cols(cols, TIME_HINTS)
    temporal[k] = {"time_like_columns": tcols}
    print(k, "time-like columns:", tcols or "none")

# COMMAND ----------

# DBTITLE 1,Histograms (binned in Spark)
hist = {}
for k, df in DFS.items():
    hist[k] = {}
    for c in DATA_COLS[k]:
        s = num[k][c]
        if s["is_numeric"]:
            hist[k][c] = hist_counts(df, c, s["min"], s["max"])

# COMMAND ----------

# DBTITLE 1,Figures
figs = []
first = next(iter(DFS))
if facet_bars(
    {"rows per source file": per_file[first]},
    "Power plant -- rows per source file",
    "power_plant_ccpp_overview.png",
    rot=30,
    ncols=1,
):
    figs.append(
        ("Power plant -- rows per source file", "power_plant_ccpp_overview.png")
    )
if facet_bars(
    {c: hist[first][c] for c in hist[first]},
    "Power plant -- value distributions",
    "power_plant_ccpp_distributions.png",
    rot=60,
    ncols=3,
):
    figs.append(
        ("Power plant -- value distributions", "power_plant_ccpp_distributions.png")
    )

# COMMAND ----------

# DBTITLE 1,Findings
for k in DFS:
    print(
        f"{k}: rows={prof[k]['total']}, cols={len(prof[k]['cols'])}, "
        f"constant={prof[k]['constant']}, dups={prof[k]['dups']}, "
        f"cross-file={cross[k]}, mirrors={mirrors[k]}"
    )

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/power_plant_ccpp.md
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
    _prov += [
        f"  > {ln[:160] if not any(w in ln.lower() for w in ('licen', 'cite', 'citation', 'publish')) else ln}"
        for ln in lines[:40]
        if ln.strip()
    ]
if not support:
    _prov.append("- No README / licence file found in the directory (LIMITATION).")

_struct = []
for k, s in struct.items():
    _struct.append(
        f"- {k}: {s['cols']} columns; unnamed={s['unnamed'] or 'none'}; "
        f"digit-named={s['numeric_named'] or 'none'}; expected columns present="
        f"{s['expected_hit'] or 'none'} -> header "
        f"{'NOT applied' if s['broken'] else 'applied'}."
    )
_struct.append(
    para(
        "Expected columns come from the dataset's public description",
        f"({', '.join(EXPECTED)}); this section confirms or refutes them against the files.",
    )
)

_dq = ["Full-row exact duplicates per frame and duplicates across source files:"]
for k in DFS:
    c = cross[k]
    _dq.append(
        f"- {k}: full-row duplicates {prof[k]['dups']}; distinct rows {c['distinct_rows']}; "
        f"rows present in more than one file {c['in_many_files']}; surplus duplicate rows {c['surplus']}."
    )
for k, fs in file_sets.items():
    _dq.append(
        f"- {k}: per file (file, distinct rows, repeated rows within the file): {fs['by_file']}; "
        f"number of distinct rows by how many files contain them (files, rows): {fs['rows_by_file_count']}."
    )
    n_files = len(frames[k]["paths"])
    full = dict(fs["rows_by_file_count"]).get(n_files, 0)
    total_distinct = sum(n for _, n in fs["rows_by_file_count"])
    _dq.append(
        f"- {k}: {full} of {total_distinct} distinct rows appear in all {n_files} files"
        + (
            " -> the files hold the same set of rows."
            if full == total_distinct
            else " -> the files hold different row sets."
        )
    )
_dq.append("Missingness (rate per column):")
for k in DFS:
    p = prof[k]
    _dq.append(
        "- "
        + k
        + ": "
        + ", ".join(
            f"{c}={p['miss'][c] / p['total'] if p['total'] else 0:.4f}"
            for c in p["cols"]
        )
    )

_entities = [
    para(
        "No entity or record identifier column is expected in a sensor-reading table;",
        "each row is one hourly operating observation. Approx distinct per column:",
    )
]
for k in DFS:
    _entities.append(
        f"- {k}: "
        + ", ".join(f"{c}={prof[k]['acd'][c]}" for c in prof[k]["cols"])
        + f"; rows={prof[k]['total']} -> no column is unique per row, so there is no natural key."
    )

_unit = []
for k in DFS:
    for c in DATA_COLS[k]:
        s = num[k][c]
        line = (
            f"- {k}.`{c}`: numeric yield {s['yield']:.1%}, range {s['min']}..{s['max']}, "
            f"mean {fmt_num(s['mean'])}, sd {fmt_num(s['sd'])}, zero {s['zero']}, negative {s['negative']}"
        )
        if c in plaus[k]:
            p = plaus[k][c]
            line += (
                f"; expected {p['label']} bounds {EXPECTED[c.upper()][1]}..{EXPECTED[c.upper()][2]}: "
                f"below {p['below']}, above {p['above']}"
            )
        _unit.append(line + ".")
for k, cols_ in beyond.items():
    for c, rows in cols_.items():
        _unit.append(
            f"- {k}.`{c}` rows beyond the expected bounds per file (file, above, below, max): {rows}."
        )
if not _unit:
    _unit.append("- No numeric column found.")

_domain = [
    para(
        "No categorical column is expected. Columns with 2..60 approx distinct values:",
    )
]
for k in DFS:
    cats = [c for c in prof[k]["cols"] if 1 < prof[k]["acd"][c] <= 60]
    _domain.append(f"- {k}: {cats or 'none'}")

_temporal = []
for k, t in temporal.items():
    if t["time_like_columns"]:
        _temporal.append(f"- {k}: time-like columns found: {t['time_like_columns']}.")
    else:
        _temporal.append(
            f"- {k}: no date / time / timestamp column -- rows are observations without a "
            "time axis, so hourly ordering, gaps and seasonality cannot be assessed."
        )

_tcons = [
    para(
        "No time axis exists, so temporal consistency (ordering, gaps, duplicates per",
        "timestamp) is not applicable; row order in the files carries no meaning that",
        "can be verified here.",
    )
]

_card = ["Relationship between source files:"]
for k in DFS:
    _card.append(
        f"- {k}: {len(frames[k]['paths'])} file(s); rows per file {per_file[k]}; "
        f"rows appearing in more than one file: {cross[k]['in_many_files']}."
    )
_card.append(
    para(
        "If the files are independent partitions the counts above sum to the frame total",
        "and no row repeats across files; a non-zero cross-file count means the files overlap.",
    )
)

_value = ["Pairwise Pearson correlation (all numeric columns):"]
for k in DFS:
    ranked = sorted(corr[k].items(), key=lambda kv: -abs(kv[1] or 0))
    _value += [f"- {k}: {a} vs {b}: {v:.3f}" for (a, b), v in ranked if v is not None]
    _value.append(f"- {k}: mirror / duplicate column pairs: {mirrors[k] or 'none'}")

_regime = [
    para(
        "Per-file column means (a large shift between files indicates that the files are",
        "different sub-populations or fold-shuffles rather than one homogeneous set):",
    )
]
for k, rows in drift.items():
    for r in rows:
        _regime.append(
            f"- {k} / {r['__file']}: "
            + ", ".join(
                f"{a}={b:.3f}" for a, b in r.items() if a != "__file" and b is not None
            )
        )
if not drift:
    _regime.append("- No numeric column to compare across files.")

_coverage = [
    para(
        "Coverage cannot be established from the files alone: the operating period,",
        "the plant identity and the sampling rule are not recorded in the data.",
        "Any statement about them rests on the README excerpt in the provenance section.",
    ),
    para(
        "One plant, one operating regime is implied; there is no site, unit or load-mode",
        "column, so a model trained here cannot be checked for transfer to other plants.",
    ),
]

_dist = []
for k in DFS:
    for c, q in quant[k].items():
        _dist.append(
            f"- {k}.`{c}` quantiles (p1/p25/p50/p75/p99): "
            + ", ".join(fmt_num(v) for v in q.values())
        )

for k in DFS:
    for c, (sk, ku) in shape[k].items():
        _dist.append(
            f"- {k}.`{c}`: skewness {fmt_num(sk)}, excess kurtosis {fmt_num(ku)}, rows outside 1.5 x IQR: {iqr_out[k][c]}."
        )
for k, drivers in trend.items():
    for drv, t in drivers.items():
        _value.append(
            f"- {k}: mean `{t['target']}` by decile of `{drv}` (decile mean of `{drv}`, mean `{t['target']}`) on distinct rows: {t['deciles']}; "
            f"monotone across deciles: {t['monotone']}."
        )

_areas = {
    "Domain understanding": [
        f"README-type files: {list(support) or 'none'}; the cited titles concern predicting the electrical output of a combined-cycle plant",
        f"column names match the expected sensor set {sorted(EXPECTED)} in {sorted(struct)} frames",
        f"physical plausibility: {sum(1 for k in DFS for c in plaus[k] if plaus[k][c]['below'] or plaus[k][c]['above'])} column(s) with values beyond the expected bounds",
    ],
    "Structure and engineering": [
        f"{len(entries)} files ({ {k: len(v) for k, v in kinds.items()} }), read as {[v['sep'] for v in frames.values()]}-separated text with the header applied",
        f"rows per file {per_file}",
        "no key, no time column, no categorical column: every column is a numeric measurement",
        f"per-file distinct rows and repeats: { {k: fs['by_file'] for k, fs in file_sets.items()} }",
    ],
    "Temporal": [
        "no date / time column"
        if not any(t["time_like_columns"] for t in temporal.values())
        else f"time-like columns: {temporal}"
    ],
    "Spatial": ["no location or coordinate column"],
    "Data quality": [
        f"full-row duplicates {{k: prof[k]['dups'] for k in DFS}}, cross-file identical files {[(k, cross[k]['in_many_files']) for k in DFS]}",
        f"values beyond expected bounds: { {k: sorted(v) for k, v in beyond.items() if v} or 'none' }",
        f"IQR outliers: {iqr_out}",
    ],
    "Statistical patterns": [
        f"skewness / excess kurtosis: { {k: {c: (round(a, 2), round(b, 2)) for c, (a, b) in v.items()} for k, v in shape.items()} }",
        f"strongest correlations: { {k: [(p, round(v, 3)) for p, v in sorted(corr[k].items(), key=lambda kv: -abs(kv[1] or 0))[:3]] for k in DFS} }",
    ],
    "Relationships": [
        f"decile trends of the output against its strongest inputs: { {k: {d: t['monotone'] for d, t in v.items()} for k, v in trend.items()} or 'not computed' }",
        f"mirror / duplicate column pairs: {mirrors}",
    ],
    "Analytics use": [
        "five continuous measures per row and no dimension to group by, so the data supports distribution and relationship analysis, not slicing by entity or period"
    ],
    "ML use": [
        f"an output-like column ({ {k: v[next(iter(v))]['target'] for k, v in trend.items() if v} or 'none identified' }) and its strongest inputs with monotone decile trend: { {k: [d for d, t in v.items() if t['monotone']] for k, v in trend.items()} }",
        "repeated rows across files are the main leakage hazard if the files are split apart",
    ],
    "AI / knowledge use": [
        "no text, label or taxonomy column; the only descriptive content is the README and the two cited papers"
    ],
}

_findings_md = "\n".join(
    f"- {k}: rows={prof[k]['total']}, cols={len(prof[k]['cols'])}, constant={prof[k]['constant']}, "
    f"dups={prof[k]['dups']}, cross-file duplicates={cross[k]['in_many_files']}, "
    f"distinct rows={cross[k]['distinct_rows']}, columns beyond expected bounds={sorted(beyond[k]) or 'none'}"
    for k in DFS
)

_silver = [
    "- Read from the Samples Volume; no Bronze table exists for this dataset.",
    "- All columns arrive as strings when read as delimited text -- numeric casts need an explicit rule; quarantine values that fail.",
    "- No natural key: a Silver key would have to be derived (file + row position) and flagged derived, or the table kept keyless.",
    "- No time axis: any hourly / time-series treatment is unsupported by this data.",
]
if any(cross[k]["in_many_files"] for k in DFS):
    _silver.append(
        "- Rows repeat across files -> decide the de-duplication rule before use; the per-file identity result in Data Quality shows whether the files are copies of one row set."
    )
if any(beyond[k] for k in DFS):
    _silver.append(
        "- Values beyond the expected physical bounds exist (see Unit & Semantic Validation) -> decide whether to keep, cap or quarantine them."
    )

_dup_map = {k: prof[k]["dups"] for k in DFS}

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            f"One row per operating observation; frames={ {k: prof[k]['total'] for k in DFS} }. No identifier or timestamp, so the grain cannot be verified beyond row count.",
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            "No join key exists; joining this table to any other source would have to be a contextual (weather-regime) join, not a keyed one.",
        ),
        (
            "Target contamination",
            "The output column (net electrical output) is the natural target; the other sensor columns are physically upstream of it, so they are legitimate features, not contaminated ones.",
        ),
        (
            "Temporal / post-event leakage",
            "No time axis -- temporal leakage cannot arise from ordering, but a random split also cannot be shown to respect time.",
        ),
        (
            "Proxy leakage",
            "No identifier columns; humidity and temperature are correlated (see the correlation section) so one can proxy the other.",
        ),
        (
            "Split / entity leakage",
            f"Files may be shuffled folds of one series; overlap across files: { {k: cross[k]['in_many_files'] for k in DFS} }. Split by file only if files are shown to be disjoint.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            "Not applicable -- no reference table or validity window.",
        ),
        (
            "Survivorship / coverage bias",
            "Single plant, undocumented operating window; no coverage statement is verifiable from the data.",
        ),
        (
            "Missingness leakage",
            "Missingness per column is reported in Data Quality; near-zero missingness means an is-missing feature carries no signal.",
        ),
        (
            "Duplicate-event leakage",
            f"Full-row duplicates {_dup_map} and cross-file duplicates above: duplicated rows placed on both sides of a split inflate scores.",
        ),
        (
            "Target / feature temporal misalignment",
            "Features and target are one simultaneous reading; no lag structure is represented.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            f"Units are not in the data (only in the README); mirror pairs found: { {k: mirrors[k] for k in DFS} }. Strongly correlated sensors are near-collinear.",
        ),
        (
            "Data-generation-process leakage",
            "The dataset is a curated public research extract; the sampling and any shuffling are undocumented in the files.",
        ),
        (
            "Class / label instability",
            "Regression target -- no classes.",
        ),
        (
            "Label availability lag",
            "Not applicable -- the output is measured at the same instant as the inputs.",
        ),
        (
            "Source / version / regime change",
            "Single static extract; no version or regime indicator, and no evidence in the data of a change in operating regime.",
        ),
        (
            "Sample-vs-full divergence",
            "Every statistic is a full Spark aggregation over the files read; whether the files themselves are a sample of a longer record is not stated in the data.",
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
        ("Value Consistency", "\n".join(_value)),
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
