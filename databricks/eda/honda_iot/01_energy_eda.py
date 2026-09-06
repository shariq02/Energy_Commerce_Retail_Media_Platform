# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- HONDA IOT ENERGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the six Honda IoT energy Bronze tables (electricity
# MAGIC / heating / cooling, each P and W) -- schema, missingness, constant
# MAGIC columns, per-frequency continuity measured against an independent
# MAGIC calendar, value ranges and sign, duplicate keys (identical vs
# MAGIC conflicting), stuck runs and outliers, exact-copy / sign-mirror columns
# MAGIC across all tables, the P<->W relationship, and the layered modelling-risk
# MAGIC checklist -- as evidence for Silver design.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "honda_iot"
NB_KEY = "01_energy"
SECTION_TITLE = "Energy tables (electricity / heating / cooling, each P and W)"
ENERGY = [
    "electricity_p",
    "electricity_w",
    "heating_p",
    "heating_w",
    "cooling_p",
    "cooling_w",
]
TABLES = {e: f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_{e}" for e in ENERGY}
KEY_COLS = ["frequency", "datetime_utc"]
VALUE_EXCLUDE = {"frequency", "datetime_utc"}
FREQ_SECONDS = {"1min": 60, "15min": 900, "1h": 3600}
# The dataset name says datetime_utc is UTC; W tables carry a running energy
# meter (kWh, monotone up), P tables an instantaneous power / flow (kW, signed).
TS_TZ = "UTC"

# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Profile -- rows, missingness, approx distinct, constant columns (one agg per table)
frames = {e: spark.table(t) for e, t in TABLES.items()}
prof = {}
for e in ENERGY:
    df = frames[e]
    cols = df.columns
    exprs = [F.count(F.lit(1)).alias("__rows")]
    for c in cols:
        miss = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
        exprs += [
            F.sum(miss.cast("long")).alias(c + "__m"),
            F.approx_count_distinct(c).alias(c + "__d"),
        ]
    r = df.agg(*exprs).first().asDict()
    prof[e] = {
        "cols": cols,
        "total": r["__rows"],
        "acd": {c: r[c + "__d"] for c in cols},
        "miss": {c: r[c + "__m"] for c in cols},
    }
    print("=" * 88, f"\n{e}  rows={r['__rows']}  ->  {cols}")
    for c in cols:
        print(
            f"  {c:<28} missing={r[c + '__m']:>12} "
            f"rate={r[c + '__m'] / r['__rows']:.4f} approx_distinct={r[c + '__d']}"
        )
totals = {e: prof[e]["total"] for e in ENERGY}
VCOLS = {e: [c for c in prof[e]["cols"] if c not in VALUE_EXCLUDE] for e in ENERGY}

# COMMAND ----------

# DBTITLE 1,Confirm constant columns exactly
constant_cols = {}
for e in ENERGY:
    cands = [c for c in prof[e]["cols"] if prof[e]["acd"][c] <= 1]
    if cands:
        r = frames[e].agg(*[F.countDistinct(F.col(c)).alias(c) for c in cands]).first()
        constant_cols[e] = sorted(c for c in cands if (r[c] or 0) <= 1)
    else:
        constant_cols[e] = []
    prof[e]["constant"] = constant_cols[e]
    print(f"{e}: constant columns (exact) = {constant_cols[e]}")

# COMMAND ----------

# DBTITLE 1,Rows per frequency + timestamp span (one groupBy per table)
freq_cov = {}
for e in ENERGY:
    d = (
        frames[e]
        .groupBy("frequency")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.min("datetime_utc").alias("min_ts"),
            F.max("datetime_utc").alias("max_ts"),
            F.countDistinct("datetime_utc").alias("distinct_ts"),
        )
        .orderBy("frequency")
        .collect()
    )
    freq_cov[e] = {x["frequency"]: x.asDict() for x in d}
    print(f"{e}:", [x.asDict() for x in d])
freq_rows = {e: [(k, v["rows"]) for k, v in freq_cov[e].items()] for e in ENERGY}

# COMMAND ----------

# DBTITLE 1,Duplicate (frequency, datetime_utc) key -- identical vs conflicting (one groupBy per table)
dup = {}
for e in ENERGY:
    dup[e] = dup_key_composition(frames[e], KEY_COLS)
    print(f"{e:<16} {dup[e]}")

# COMMAND ----------

# DBTITLE 1,Value columns -- range, sign, sentinels, percentiles (one agg per table)
value_stats = {}
for e in ENERGY:
    df = frames[e]
    exprs = []
    for c in VCOLS[e]:
        v = safe_num(c)
        exprs += [
            F.min(v).alias(c + "_min"),
            F.max(v).alias(c + "_max"),
            F.avg(v).alias(c + "_avg"),
            F.stddev(v).alias(c + "_sd"),
            F.percentile_approx(v, [0.01, 0.25, 0.5, 0.75, 0.99]).alias(c + "_p"),
            F.sum((v < 0).cast("long")).alias(c + "_negative"),
            F.sum((v == 0).cast("long")).alias(c + "_zero"),
            F.sum(
                (F.col(c).isNotNull() & (F.trim(F.col(c)) != "") & v.isNull()).cast(
                    "long"
                )
            ).alias(c + "_non_numeric"),
        ]
    value_stats[e] = df.agg(*exprs).first().asDict()
    for c in VCOLS[e]:
        print(
            f"{e}.{c:<12}",
            {
                k[len(c) + 1 :]: value_stats[e][k]
                for k in value_stats[e]
                if k.startswith(c + "_")
            },
        )

# COMMAND ----------

# DBTITLE 1,Exact-copy / sign-mirror columns across ALL tables (circular-feature detector)
all_stats = {
    f"{e}.{c}": {
        "mean": value_stats[e][c + "_avg"],
        "sd": value_stats[e][c + "_sd"],
        "min": value_stats[e][c + "_min"],
        "max": value_stats[e][c + "_max"],
    }
    for e in ENERGY
    for c in VCOLS[e]
    if value_stats[e][c + "_sd"] is not None
}
mirrors = mirror_columns(all_stats)
print("mirror / duplicate column pairs:")
for a, b, why in mirrors:
    print(f"  {a}  <->  {b}  : {why}")

# COMMAND ----------

# DBTITLE 1,5-sigma outliers + stuck runs (two passes per table)
outliers = {}
stuck = {}
for e in ENERGY:
    df = frames[e]
    vs = value_stats[e]
    oor_exprs = []
    for c in VCOLS[e]:
        v = safe_num(c)
        m, sd = vs[c + "_avg"], vs[c + "_sd"]
        oor_exprs.append(
            F.sum((F.abs(v - F.lit(m)) > 5 * F.lit(sd)).cast("long")).alias(c)
            if sd
            else F.lit(0).alias(c)
        )
    outliers[e] = df.agg(*oor_exprs).first().asDict()
    w = Window.partitionBy("frequency").orderBy("datetime_utc")
    df_1h = df.where(F.col("frequency") == "1h")
    for c in VCOLS[e]:
        v = safe_num(c)
        df_1h = df_1h.withColumn(
            f"{c}_stuck",
            (
                v.isNotNull() & (v == F.lag(v, 1).over(w)) & (v == F.lag(v, 9).over(w))
            ).cast("long"),
        )
    stuck[e] = (
        df_1h.agg(*[F.sum(F.col(f"{c}_stuck")).alias(c) for c in VCOLS[e]])
        .first()
        .asDict()
    )
    print(f"{e}: 5sigma_outliers={outliers[e]}  stuck>=10run(1h)={stuck[e]}")

# COMMAND ----------

# DBTITLE 1,Temporal continuity -- coverage vs an INDEPENDENT per-frequency calendar
continuity = {}
for e in ENERGY:
    cg = continuity_grid(
        frames[e], "datetime_utc", FREQ_SECONDS, entity_col="frequency"
    )
    continuity[e] = cg["per_entity"]
    print(f"{e}: {cg['per_entity']}")

# COMMAND ----------

# DBTITLE 1,P <-> W value relationship per metric (one join + one agg per metric)
pw_rel = {}
pw_scatter = {}
for metric in ("electricity", "heating", "cooling"):
    p, w = frames[f"{metric}_p"], frames[f"{metric}_w"]
    shared = [c for c in p.columns if c not in VALUE_EXCLUDE and c in w.columns]
    if not shared:
        continue
    j = p.select(*KEY_COLS, *[safe_num(c).alias(f"p_{c}") for c in shared]).join(
        w.select(*KEY_COLS, *[safe_num(c).alias(f"w_{c}") for c in shared]),
        on=KEY_COLS,
        how="inner",
    )
    pw_rel[metric] = (
        j.agg(
            F.count(F.lit(1)).alias("matched"),
            *[F.corr(f"p_{c}", f"w_{c}").alias(f"corr_{c}") for c in shared],
        )
        .first()
        .asDict()
    )
    pw_scatter[metric] = (
        shared[0],
        j.select(f"p_{shared[0]}", f"w_{shared[0]}")
        .where(
            F.col(f"p_{shared[0]}").isNotNull() & F.col(f"w_{shared[0]}").isNotNull()
        )
        .sample(0.1, seed=42)
        .limit(20_000)
        .toPandas(),
    )
    print(f"{metric}: {pw_rel[metric]}")

schema_parity = {
    m: frames[f"{m}_p"].columns == frames[f"{m}_w"].columns
    for m in ("electricity", "heating", "cooling")
}
print("P/W schema parity:", schema_parity)

# COMMAND ----------

# DBTITLE 1,Categorical domain -- `frequency` against the known resolution set
freq_domain = {
    e: categorical_domain(
        frames[e], "frequency", FREQ_SECONDS.keys(), name=f"{e}.frequency"
    )
    for e in ENERGY
}
for e, d in freq_domain.items():
    if d["unexpected_count"]:
        print(
            f"{e}: UNEXPECTED frequency label(s) {d['unexpected']} -- ingestion defect"
        )

# COMMAND ----------

# DBTITLE 1,Temporal consistency -- the W meter must be non-decreasing (cumulative energy)
w_monotonic = {}
for e in ENERGY:
    if not e.endswith("_w"):
        continue
    w_monotonic[e] = {}
    for c in VCOLS[e]:
        res = monotonic_series_check(
            frames[e], c, "datetime_utc", partition_col="frequency"
        )
        w_monotonic[e][c] = res
        print(
            f"{e}.{c}: {res['decreasing_steps']}/{res['comparable_steps']} steps decrease "
            f"({res['decreasing_pct']}%), largest drop {res['largest_drop']}"
        )

# COMMAND ----------

# DBTITLE 1,Physical consistency -- implied power from dW/dt should match the P table
# For each metric, take the per-step increment of the cumulative W meter, convert
# to an average power over the step, and compare it row-for-row with the P value
# at the same (frequency, datetime_utc). A large residual share means P and W do
# not describe the same physical quantity (a unit or labelling error).
pw_identity = {}
for metric in ("electricity", "heating", "cooling"):
    p, w = frames[f"{metric}_p"], frames[f"{metric}_w"]
    shared = [c for c in p.columns if c not in VALUE_EXCLUDE and c in w.columns]
    if not shared:
        continue
    c0 = shared[0]
    win = Window.partitionBy("frequency").orderBy("datetime_utc")
    step_s = F.lit(None).cast("double")
    for lbl, secs in FREQ_SECONDS.items():
        step_s = F.when(F.col("frequency") == lbl, F.lit(float(secs))).otherwise(step_s)
    wc = safe_num(c0)
    wd = w.select(
        *KEY_COLS,
        ((wc - F.lag(wc).over(win)) * 3600.0 / step_s).alias("implied_power"),
    )
    j = wd.join(
        p.select(*KEY_COLS, safe_num(c0).alias("p_val")),
        on=KEY_COLS,
        how="inner",
    ).where(F.col("implied_power").isNotNull() & F.col("p_val").isNotNull())
    pw_identity[metric] = {
        "column": c0,
        **additive_identity_check(
            j, "implied_power", ["p_val"], rel_tol=0.1, abs_floor=0.1
        ),
    }
    print(f"{metric}: dW/dt vs P -- {pw_identity[metric]}")

# COMMAND ----------

# DBTITLE 1,Regime evidence -- first half vs second half of the series (recalibration / sensor swap)
spans = [
    (fc["min_ts"], fc["max_ts"])
    for e in ENERGY
    for fc in freq_cov[e].values()
    if fc.get("min_ts") and fc.get("max_ts")
]
regime = {}
# freq_cov min/max come from F.min/F.max on a Bronze STRING column -> they are
# strings, not datetimes; iso_midpoint parses them.
mid = (
    iso_midpoint(min(s[0] for s in spans), max(s[1] for s in spans)) if spans else None
)
if mid:
    for e in ENERGY:
        shift = regime_population_shift(frames[e], "datetime_utc", VCOLS[e], mid)
        regime[e] = {"cut": mid, **shift}
        print(
            f"{e}: split at {mid} -> pre={shift['pre_rows']} post={shift['post_rows']}; "
            f"flipped={shift['flipped']}"
        )

# COMMAND ----------

# DBTITLE 1,Samples for figures (sampled + first-N chronological)
value_pdf = {
    e: frames[e]
    .select(*[safe_num(c).alias(c) for c in VCOLS[e]])
    .sample(0.1, seed=42)
    .limit(150_000)
    .toPandas()
    for e in ENERGY
}
ts_pdf = {
    e: frames[e]
    .where(F.col("frequency") == "1h")
    .select("datetime_utc", *[safe_num(c).alias(c) for c in VCOLS[e]])
    .orderBy("datetime_utc")
    .limit(2000)
    .toPandas()
    for e in ENERGY
}

# COMMAND ----------

# DBTITLE 1,Figure -- rows per frequency + coverage % + duplicate groups
figs = []
if facet_bars(
    {e: freq_rows[e] for e in ENERGY},
    "Honda energy -- rows per frequency, by table",
    "honda_energy_rows_per_frequency.png",
    rot=0,
):
    figs.append(
        (
            "Honda energy -- rows per frequency, by table",
            "honda_energy_rows_per_frequency.png",
        )
    )
if facet_bars(
    {
        e: [
            (fq, continuity[e][fq]["coverage_pct"])
            for fq in FREQ_SECONDS
            if fq in continuity[e]
        ]
        for e in ENERGY
    },
    "Honda energy -- coverage % vs independent calendar, by frequency",
    "honda_energy_coverage_pct.png",
    rot=0,
):
    figs.append(
        ("Honda energy -- coverage % by frequency", "honda_energy_coverage_pct.png")
    )
if barplot(
    [(e, dup[e]["dup_groups"]) for e in ENERGY],
    "Honda energy -- duplicate (frequency, datetime_utc) groups",
    "table",
    "dup groups",
    rot=30,
    filename="honda_energy_duplicate_groups.png",
):
    figs.append(
        (
            "Honda energy -- duplicate (frequency, datetime_utc) groups",
            "honda_energy_duplicate_groups.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Figure -- longest gap (steps) per table x frequency
gap_ok = False
_gap_rows = [
    (fq, [continuity[e].get(fq, {}).get("longest_gap_steps", 0) or 0 for e in ENERGY])
    for fq in FREQ_SECONDS
]
if any(any(v) for _, v in _gap_rows):
    x = np.arange(len(ENERGY))
    fig, ax = plt.subplots(figsize=(11, 4))
    for i, (fq, vals) in enumerate(_gap_rows):
        ax.bar(x + i * 0.27, vals, width=0.27, label=fq)
    ax.set_xticks(x + 0.27)
    ax.set_xticklabels(ENERGY, rotation=30, ha="right")
    ax.legend()
    ax.set_title("Honda energy -- longest gap (missing steps) per table x frequency")
    ax.set_ylabel("steps")
    fig.tight_layout()
    _save_and_show(fig, "honda_energy_longest_gap_per_table.png")
    gap_ok = True
if gap_ok:
    figs.append(
        (
            "Honda energy -- longest gap (missing steps) per table x frequency",
            "honda_energy_longest_gap_per_table.png",
        )
    )
else:
    print("  no gaps in any series -- longest-gap figure not written")

# COMMAND ----------

# DBTITLE 1,Figure -- value distributions + hourly window + P vs W scatter
if facet_hists(
    {f"{e}.{c}": value_pdf[e][c].dropna().tolist() for e in ENERGY for c in VCOLS[e]},
    "Honda energy -- value distribution per table.column (sampled)",
    "honda_energy_value_distributions.png",
    ncols=4,
):
    figs.append(
        (
            "Honda energy -- value distribution per table.column",
            "honda_energy_value_distributions.png",
        )
    )


def _hourly_draw(tp, cols):
    def draw(ax):
        for c in cols:
            ax.plot(range(len(tp)), tp[c], label=c, linewidth=0.8)
        ax.legend(fontsize=6)

    return draw


if _facet_grid(
    [(e, _hourly_draw(ts_pdf[e], VCOLS[e])) for e in ENERGY if not ts_pdf[e].empty],
    "Honda energy -- first 2000 hourly points, by table",
    "honda_energy_first_hourly_points.png",
):
    figs.append(
        (
            "Honda energy -- first 2000 hourly points, by table",
            "honda_energy_first_hourly_points.png",
        )
    )


def _scatter_draw(col, pdf):
    def draw(ax):
        ax.scatter(pdf[f"p_{col}"], pdf[f"w_{col}"], s=6, alpha=0.3)
        ax.set_xlabel(f"P.{col}", fontsize=7)
        ax.set_ylabel(f"W.{col}", fontsize=7)

    return draw


if _facet_grid(
    [
        (metric, _scatter_draw(col, pdf))
        for metric, (col, pdf) in pw_scatter.items()
        if not pdf.empty
    ],
    "Honda energy -- P vs W per metric (sampled)",
    "honda_energy_p_vs_w_scatter.png",
):
    figs.append(
        ("Honda energy -- P vs W per metric", "honda_energy_p_vs_w_scatter.png")
    )

# COMMAND ----------

# DBTITLE 1,Findings
print("dup composition:", dup)
print("continuity:", continuity)
print("5-sigma outliers:", outliers)
print("mirror columns:", mirrors)
print("P<->W relationship:", pw_rel)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/honda_iot.md
_profile = [
    "| table | rows | cols | frequencies | constant cols |",
    "|---|---|---|---|---|",
]
for e in ENERGY:
    _profile.append(
        f"| {e} | {totals[e]} | {len(prof[e]['cols'])} | "
        f"{ {fr: fc['rows'] for fr, fc in freq_cov[e].items()} } | "
        f"{', '.join(prof[e]['constant']) or '-'} |"
    )
_profile.append("")
_profile.append("Value columns per table: " + str({e: VCOLS[e] for e in ENERGY}))

_dq = ["| table | dup key groups | identical | conflicting |", "|---|---|---|---|"]
for e in ENERGY:
    b = dup[e]
    _dq.append(f"| {e} | {b['dup_groups']} | {b['identical']} | {b['conflicting']} |")
_dq.append("")
_dq.append(f"5-sigma outlier rows per value column: {outliers}")
_dq.append(f"Stuck-run rows (value == value 1 and 9 steps back, 1h): {stuck}")

_unit = [
    (
        "Value columns cast to double; P tables are instantaneous power/flow (kW, may be "
        "signed by convention), W tables a cumulative energy meter (kWh, should be monotone):"
    ),
]
for e in ENERGY:
    for c in VCOLS[e]:
        vs = value_stats[e]
        _unit.append(
            f"- {e}.`{c}`: range {vs[c + '_min']}..{vs[c + '_max']}, mean {vs[c + '_avg']}, "
            f"sd {vs[c + '_sd']}, negative rows {vs[c + '_negative']}, zero {vs[c + '_zero']}, "
            f"non-numeric {vs[c + '_non_numeric']}"
        )
_unit.append("")
_unit.append("Exact-copy / sign-mirror column pairs (circular-feature risk):")
if mirrors:
    for a, b, why in mirrors:
        _unit.append(f"- `{a}` <-> `{b}`: {why}")
else:
    _unit.append("- none detected across the six tables.")

_temporal = [
    (
        f"datetime_utc is treated as {TS_TZ} per the dataset name. Coverage below is measured "
        "against an INDEPENDENT calendar (expected = span / step + 1), not the observed distinct "
        "count -- so 100% means genuinely gap-free, not tautological."
    ),
    "",
    "| table / frequency | observed | expected | coverage % | longest gap (steps) | on-step % |",
    "|---|---|---|---|---|---|",
]
for e in ENERGY:
    for fq in FREQ_SECONDS:
        r = continuity[e].get(fq)
        if not r:
            continue
        _temporal.append(
            f"| {e} / {fq} | {r['observed']} | {r['expected']} | {r['coverage_pct']} | "
            f"{r['longest_gap_steps']} | {r['on_step_pct']} |"
        )

_rel = [
    "P<->W value relationship per metric (inner join on (frequency, datetime_utc), Pearson corr):"
]
for metric, a in pw_rel.items():
    _rel.append(f"- {metric}: {a}")
_rel.append(f"P/W schema parity: {schema_parity}")

_coverage = [
    f"Rows per (table, frequency): { {e: {k: v['rows'] for k, v in freq_cov[e].items()} for e in ENERGY} }.",
    (
        "The 1min partition dominates every table (~50x the 1h partition). A model must not pool "
        "frequencies -- they are three resolutions of the same signal, and a random split would put "
        "near-duplicate 1min/15min/1h rows of the same hour on both sides."
    ),
    "Single building, single sensor set -- no entity dimension; the only split axis is time.",
]

_domain = ["`frequency` values vs the known resolution set (1min / 15min / 1h):"]
for e in ENERGY:
    d = freq_domain[e]
    _domain.append(
        f"- {e}: unexpected={d['unexpected'] or 'none'}, "
        f"unused={d['unused_allowed'] or 'none'}."
    )
if any(freq_domain[e]["unexpected_count"] for e in ENERGY):
    _domain.append(
        "-> an unexpected `frequency` label is an INGESTION defect (a bad partition value), "
        "not a source finding."
    )

_tcons = [
    para(
        "The W tables are cumulative energy meters -- the value must be",
        "non-decreasing when ordered by datetime_utc within a frequency. A",
        "decreasing step is either a meter reset/rollover or a data error.",
    ),
    "",
]
for e, cols in w_monotonic.items():
    for c, res in cols.items():
        _tcons.append(
            f"- {e}.`{c}`: {res['decreasing_steps']} of {res['comparable_steps']} steps "
            f"decrease ({res['decreasing_pct']}%); largest drop {res['largest_drop']}."
        )
if not w_monotonic:
    _tcons.append("- No `*_w` cumulative-meter table in scope.")

_pcons = [
    para(
        "Physical identity check: the per-step increment of the W meter, converted",
        "to an average power over the step, should equal the P value at the same",
        "(frequency, datetime_utc). Residual = implied_power - P.",
    ),
    "",
]
for metric, res in pw_identity.items():
    _pcons.append(
        f"- {metric} (`{res['column']}`): {res['violations']}/{res['comparable_rows']} rows "
        f"exceed {int(res['rel_tol'] * 100)}% relative residual ({res['violation_pct']}%); "
        f"residual p01/p50/p99 {res['residual_p01_p50_p99']}, max abs {res['max_abs_residual']}."
    )
if not pw_identity:
    _pcons.append("- No P/W column pair shared a name for the check.")

_regime = [
    para(
        "Rows split at the series midpoint -- a large null-rate or distinct-count",
        "move on one side points to a sensor swap / outage window rather than a",
        "physical change. This measures coverage, not signal level.",
    ),
    "",
]
for e in ENERGY:
    rg = regime.get(e)
    if not rg:
        _regime.append(f"- {e}: no datetime span -- not assessable.")
        continue
    _regime.append(
        f"- {e} (cut {rg['cut']}): pre={rg['pre_rows']}, post={rg['post_rows']}; "
        f"columns with a >=50pt null-rate move: {rg['flipped'] or 'none'}."
    )

_conflict = any(dup[e]["conflicting"] for e in ENERGY)
_neg = {
    f"{e}.{c}": value_stats[e][c + "_negative"]
    for e in ENERGY
    for c in VCOLS[e]
    if value_stats[e][c + "_negative"]
}

_silver = [
    "- Type conversion: datetime_utc -> timestamp (UTC); value columns -> double.",
    (
        "- `frequency` (1min / 15min / 1h) is a real physical resolution -> keep it in the grain; "
        "do not blend frequencies."
    ),
    "- Identical (frequency, datetime_utc) repeats can be de-duplicated.",
]
if _conflict:
    _silver.append(
        "- Conflicting (frequency, datetime_utc) duplicates exist -> a deterministic "
        "conflict-resolution rule is required before Silver."
    )
if mirrors:
    _silver.append(
        f"- Exact-copy / sign-mirror columns exist ({[(a, b) for a, b, _ in mirrors]}) -> keep "
        "ONE per pair, or an SCD/lineage note explaining the derivation; never expose both as "
        "independent features."
    )
if _neg:
    _silver.append(
        f"- Negative values in energy columns ({_neg}) -> confirm the sign convention "
        "(generation as negative? measurement error?) before Silver casting."
    )
_silver.append(
    "- Series are not necessarily dense (coverage / gaps above) -> observed points only; "
    "resampling is a downstream choice."
)
_silver.append("- Stuck-sensor runs and 5-sigma spikes -> data-quality flag, keep raw.")

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                f"One row per (frequency, datetime_utc) per table. Joining P+W per metric is 1:1 on that "
                f"key (schema parity {schema_parity}); pooling frequencies changes the grain."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                "P<->W join is 1:1 on (frequency, datetime_utc) -- no fan-out. A cross-metric wide join "
                "(electricity+heating+cooling at one timestamp) is also 1:1 on the intersection but drops "
                "the non-overlapping tail (see 03)."
            ),
        ),
        (
            "Target contamination",
            (
                "If a value column is the forecast target, the same column at the target timestamp (and "
                "the cumulative W meter, which encodes the future increment) must be excluded from features."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "The W tables are cumulative meters -- W[t] already contains energy that flows after the "
                "prediction cutoff if the cutoff sits mid-interval; use first-difference (per-interval "
                "energy), not the raw meter, and only points strictly before the cutoff. Meter "
                "monotonicity is measured in Temporal Consistency; the dW/dt-vs-P identity in Physical "
                "Consistency."
            ),
        ),
        (
            "Proxy leakage",
            (
                "P and W of the same metric are near-redundant (corr above); a heat/cool total is close to "
                "the sum of its components -- a 'feature' that is an arithmetic function of the target leaks."
            ),
        ),
        (
            "Split / entity leakage",
            (
                "Single building, no entity id -- split by contiguous date range only, and never mix "
                "frequencies within one split (1min/15min/1h rows of the same hour are near-duplicates)."
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "No slowly-changing attributes here; a diurnal / seasonal profile used as a feature must be "
                "computed only from data before the prediction point, never over the full history."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                "Continuity above shows the real gap profile. The 1min partition dominates; any statistic "
                "pooled across frequencies is really a 1min statistic."
            ),
        ),
        (
            "Missingness leakage",
            (
                "Whether a value is present may correlate with sensor downtime windows -- check before "
                "adding an 'is-missing' feature that a naive model could exploit."
            ),
        ),
        (
            "Duplicate-event leakage",
            (
                f"Duplicate key groups per table: { {e: dup[e]['dup_groups'] for e in ENERGY} } "
                f"(conflicting: { {e: dup[e]['conflicting'] for e in ENERGY} }) -- de-duplicate before "
                "counting observations or splitting."
            ),
        ),
        (
            "Target / feature temporal misalignment",
            (
                "P (instantaneous, timestamped at the instant) and W (cumulative, timestamped at interval "
                "end) are not aligned to the same instant -- align both to one convention before pairing."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            (
                f"Mirror/copy columns: {[(a, b) for a, b, _ in mirrors] or 'none'}. Negative energy: "
                f"{_neg or 'none'}. P<->W is a physical relationship, not independent signal -- using one "
                "to predict the other is circular."
            ),
        ),
        (
            "Data-generation-process leakage",
            (
                "The W meter reset/rollover behaviour and any gap-filling done upstream are part of the "
                "data-generation process -- a feature that spikes at a meter reset encodes the process, "
                "not the building's energy use."
            ),
        ),
        (
            "Class / label instability",
            "Not applicable -- all targets here are continuous.",
        ),
        (
            "Label availability lag",
            (
                "Meter readings are available at interval end; a nowcast at time t cannot use the interval "
                "[t, t+step] reading."
            ),
        ),
        (
            "Source / version / regime change",
            (
                "A single deployment; watch for sensor swaps or recalibration (a step change in level with "
                "no physical cause) when the series is extended."
            ),
        ),
        (
            "Sample-vs-full divergence",
            (
                "value_pdf is a 10% sample capped at 150k rows, the hourly figure uses the first 2000 "
                "chronological points, the P-vs-W scatter a 10% sample capped at 20k -- none are "
                "representative; use the full-table value_stats / outliers / continuity aggregates for "
                "any feature-quality decision."
            ),
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
        ("Unit & Semantic Validation", "\n".join(_unit)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Temporal Semantics", "\n".join(_temporal)),
        ("Temporal Consistency", "\n".join(_tcons)),
        ("Physical Consistency", "\n".join(_pcons)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Relationships", "\n".join(_rel)),
        (
            "EDA Findings",
            "\n".join(
                [
                    f"- dup composition: {dup}",
                    "- continuity (coverage % / longest gap steps): "
                    + str(
                        {
                            e: {
                                fq: (
                                    continuity[e][fq]["coverage_pct"],
                                    continuity[e][fq]["longest_gap_steps"],
                                )
                                for fq in continuity[e]
                            }
                            for e in ENERGY
                        }
                    ),
                    f"- 5-sigma outliers: {outliers}",
                    f"- mirror/duplicate columns: {[(a, b, w) for a, b, w in mirrors]}",
                    f"- P<->W relationship: {pw_rel}",
                ]
            ),
        ),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
