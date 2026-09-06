# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- REDISPATCH MEASURES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the redispatch_measures Bronze table -- schema,
# MAGIC missingness, constant columns, full-row duplicates, categorical
# MAGIC distributions (reason, direction, grid operator, energy carrier),
# MAGIC stricter numeric-column detection (parse yield, not "looks numeric"),
# MAGIC power/energy value plausibility, measure start/end temporal semantics,
# MAGIC observed coverage window, and the layered modelling-risk checklist --
# MAGIC as evidence for Silver design.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "redispatch"
NB_KEY = "01_redispatch"
SECTION_TITLE = "Redispatch measures (redispatch_measures)"
TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.redispatch_measures"
# A column whose name contains one of these is a candidate date/time column.
DATE_COL_HINTS = ("datum", "date")
# Power / energy value columns; name carries the unit (MW instantaneous, MWH
# energy). BETROFFENE_ANLAGE and *_UENB are text, not numeric.
AMOUNT_HINTS = ("_mw", "_mwh", "leistung", "arbeit")

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
    miss = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
    exprs += [
        F.sum(miss.cast("long")).alias(c + "__m"),
        F.approx_count_distinct(c).alias(c + "__d"),
    ]
r = df.agg(*exprs).first().asDict()
total = r["__rows"]
acd = {c: r[c + "__d"] for c in COLS}
miss = {c: r[c + "__m"] for c in COLS}
constant_cands = [c for c in COLS if acd[c] <= 1]
constant_cols = (
    sorted(
        c
        for c in constant_cands
        if (df.agg(F.countDistinct(F.col(c)).alias(c)).first()[c] or 0) <= 1
    )
    if constant_cands
    else []
)
distinct_rows = df.distinct().count()
print(f"rows={total}  columns={len(COLS)}  ->  {COLS}")
for c in COLS:
    print(
        f"  {c:<24} missing={miss[c]:>8} rate={miss[c] / total:.4f} approx_distinct={acd[c]}"
    )
print("constant columns (exact):", constant_cols)
print("exact full-row duplicates:", total - distinct_rows)

# COMMAND ----------

# DBTITLE 1,Low-cardinality categorical distributions (one groupBy per flagged column)
cat_cols = [c for c in COLS if 1 < acd[c] <= 60]
categorical_dist = {}
for c in cat_cols:
    vc = df.groupBy(c).count().orderBy(F.desc("count")).limit(50).collect()
    categorical_dist[c] = [(x[c], x["count"]) for x in vc]
print("categorical columns:", cat_cols)

# COMMAND ----------

# DBTITLE 1,Numeric columns -- parse YIELD, not "looks numeric" (comma-decimal aware)
amount_cols = [c for c in COLS if any(h in c.lower() for h in AMOUNT_HINTS)]
# also probe any other high-cardinality non-date column, but only KEEP it as
# numeric if the parse yield is high -- this is what stops a free-text
# affected-plant column being reported as "0 of N numeric values parsed".
probe_cols = sorted(
    set(amount_cols)
    | {
        c
        for c in COLS
        if acd[c] > 60
        and not any(h in c.lower() for h in DATE_COL_HINTS)
        and c not in cat_cols
    }
)
numeric = {}
text_flagged = []
for c in probe_cols:
    npar = numeric_parseability(df, c)
    numeric[c] = npar
    if npar["is_numeric"]:
        pl = plausibility(df, c, lo=None, sentinels=())
        numeric[c]["plausibility"] = pl
        print(
            f"{c}: NUMERIC yield={npar['yield']} range=({pl['min']},{pl['max']}) "
            f"negative={pl['negative']} zero={pl['zero']}"
        )
    else:
        text_flagged.append(c)
        print(f"{c}: TEXT (numeric yield {npar['yield']:.1%}) -- not a numeric column")

# COMMAND ----------

# DBTITLE 1,Temporal semantics -- measure start / end date columns
dcols = [
    c
    for c in COLS
    if any(h in c.lower() for h in DATE_COL_HINTS) and not c.lower().endswith("_nv")
]
temporal = {}
for c in dcols:
    ts = timestamp_semantics(df, c, valid_from="2010-01-01", tz="Europe/Berlin")
    temporal[c] = ts
    for ln in ts["lines"]:
        print(f"  {c}: {ln}")

# COMMAND ----------

# DBTITLE 1,Inverted start/end ranges + observed coverage window
begin_c = next((c for c in dcols if "beginn" in c.lower() or "von" in c.lower()), None)
end_c = next((c for c in dcols if "ende" in c.lower() or "bis" in c.lower()), None)
inverted = None
if begin_c and end_c:
    b = parse_ts_multi(begin_c)
    e = parse_ts_multi(end_c)
    inverted = df.where(b.isNotNull() & e.isNotNull() & (e < b)).count()
    print(f"rows with {end_c} < {begin_c}: {inverted}")

# COMMAND ----------

# DBTITLE 1,Categorical / domain validation -- RICHTUNG against the known direction set
_dir_col = next((c for c in COLS if "richtung" in c.lower()), None)
KNOWN_RICHTUNG = {
    "Wirkleistungseinspeisung reduzieren",
    "Wirkleistungseinspeisung erhoehen",
    "Wirkleistungseinspeisung erhöhen",
    "Einspeisung reduzieren",
    "Einspeisung erhoehen",
}
richtung_domain = (
    categorical_domain(df, _dir_col, KNOWN_RICHTUNG, name=_dir_col)
    if _dir_col
    else None
)
print("RICHTUNG domain:", richtung_domain)

# COMMAND ----------

# DBTITLE 1,Value consistency -- mean <= max power, non-negative energy, energy ~ mean power x duration
mean_c = next((c for c in COLS if "mittler" in c.lower() and "mw" in c.lower()), None)
max_c = next((c for c in COLS if "maximal" in c.lower() and "mw" in c.lower()), None)
work_c = next(
    (c for c in COLS if "arbeit" in c.lower() or c.lower().endswith("_mwh")), None
)
value_order = []
if mean_c and max_c:
    value_order.append(numeric_order_check(df, mean_c, max_c))
if mean_c:
    pl = plausibility(df, mean_c, lo=0.0, sentinels=())
    value_order.append(
        {
            "label": f"{mean_c} >= 0",
            "violations": pl["negative"],
            "comparable_rows": None,
        }
    )
for res in value_order:
    print(res)

# energy ~ mean power x duration, only if begin/end timestamps can be built from
# date + time columns.
begin_uhr = next(
    (c for c in COLS if "beginn" in c.lower() and "uhr" in c.lower()), None
)
end_uhr = next((c for c in COLS if "ende" in c.lower() and "uhr" in c.lower()), None)
work_identity = None
if mean_c and work_c and begin_c and end_c and begin_uhr and end_uhr:
    bt = F.to_timestamp(
        F.concat_ws(" ", F.col(begin_c).cast("string"), F.col(begin_uhr).cast("string"))
    )
    et = F.to_timestamp(
        F.concat_ws(" ", F.col(end_c).cast("string"), F.col(end_uhr).cast("string"))
    )
    j = df.select(
        (F.col(mean_c).cast("double")).alias("mean_mw"),
        (F.col(work_c).cast("double")).alias("work_mwh"),
        ((et.cast("long") - bt.cast("long")) / 3600.0).alias("dur_h"),
    ).where(F.col("dur_h").isNotNull() & (F.col("dur_h") > 0))
    j = j.withColumn("implied_mwh", F.col("mean_mw") * F.col("dur_h"))
    work_identity = additive_identity_check(
        j, "work_mwh", ["implied_mwh"], rel_tol=0.05, abs_floor=1.0
    )
    print("energy vs mean-power x duration:", work_identity)

# COMMAND ----------

# DBTITLE 1,Regime evidence -- Redispatch 2.0 split (2021-10-01) on the measure start date
REGIME_CUT = "2021-10-01"
regime = None
if begin_c:
    regime = {
        "cut": REGIME_CUT,
        **regime_population_shift(df, begin_c, COLS, REGIME_CUT),
    }
    print(
        f"Redispatch 2.0 split @ {REGIME_CUT}: pre={regime['pre_rows']} post={regime['post_rows']} "
        f"undated={regime['undated_rows']}; flipped={regime['flipped']}"
    )
    vocab_shift = population_by_group(
        df.withColumn(
            "__regime",
            F.when(
                parse_ts_multi(begin_c) < F.lit(REGIME_CUT).cast("timestamp"), "pre"
            ).otherwise("post"),
        ),
        "__regime",
        [c for c in cat_cols],
    )
    print("category vocabulary per regime:", vocab_shift)
else:
    vocab_shift = {}

# COMMAND ----------

# DBTITLE 1,Figure -- overview (direction + reason distributions)
figs = []
_reason_c = next((c for c in cat_cols if "grund" in c.lower()), None)
_dir_c = next((c for c in cat_cols if "richtung" in c.lower()), None)
if facet_bars(
    {
        "direction (RICHTUNG)": categorical_dist.get(_dir_c, [])
        if _dir_c
        else (next(iter(categorical_dist.values()), [])),
        "measure reason (GRUND_DER_MASSNAHME)": categorical_dist.get(_reason_c, [])
        if _reason_c
        else [],
    },
    "Redispatch measures -- overview",
    "redispatch_overview.png",
    rot=45,
    ncols=2,
):
    figs.append(("Redispatch measures -- overview", "redispatch_overview.png"))

# COMMAND ----------

# DBTITLE 1,Rows per year (first date column) -- plausible years only
if dcols:
    c0 = dcols[0]
    yr = F.year(parse_ts_multi(c0))
    g = (
        df.select(yr.alias("y"))
        .where(yr.between(2010, F.year(F.current_timestamp())))
        .groupBy("y")
        .count()
        .orderBy("y")
        .collect()
    )
    year_pairs = [(x["y"], x["count"]) for x in g]
    print("rows per year:", year_pairs)
    if barplot(
        year_pairs,
        f"Redispatch -- rows per year ({c0})",
        "year",
        "rows",
        filename="redispatch_rows_per_year.png",
    ):
        figs.append(
            (f"Redispatch -- rows per year ({c0})", "redispatch_rows_per_year.png")
        )
else:
    year_pairs = []

# COMMAND ----------

# DBTITLE 1,Findings
print("constant columns:", constant_cols)
print("numeric columns:", [c for c in numeric if numeric[c]["is_numeric"]])
print("text columns wrongly numeric-shaped by name:", text_flagged)
print("rows per year:", year_pairs)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/redispatch.md
_prof = ["| column | missing | rate | approx_distinct |", "|---|---|---|---|"]
for c in COLS:
    _prof.append(f"| {c} | {miss[c]} | {miss[c] / total:.4f} | {acd[c]} |")
_prof += ["", f"Rows: {total}. Constant columns (exact): {constant_cols or 'none'}."]

_dq = [f"Exact full-row duplicates: {total - distinct_rows}."]
if inverted:
    _dq.append(f"Rows where the end time precedes the start time: {inverted}.")

_unit = ["Power/energy columns (unit in the name: MW instantaneous, MWH energy):"]
for c in sorted(numeric):
    npar = numeric[c]
    if npar["is_numeric"]:
        pl = npar["plausibility"]
        _unit.append(
            f"- `{c}`: numeric (parse yield {npar['yield']:.1%}), range "
            f"{pl['min']}..{pl['max']}, negative {pl['negative']}, zero {pl['zero']} "
            "-- confirm the decimal separator (German comma) on the Silver cast."
        )
    else:
        _unit.append(
            f"- `{c}`: parses as a number only {npar['yield']:.1%} of the time -> this is a "
            "TEXT column (e.g. an affected-plant name / operator code), NOT a numeric column "
            "with unparsed values."
        )

_temporal = ["Measure start / end date columns (multi-format parse):"]
for c, ts in temporal.items():
    _temporal.append(
        f"- `{c}`: parse yield {ts['yield']:.1%} of {ts['present']}, range "
        f"{ts['min_ts']}..{ts['max_ts']}, before 2010={ts['before_valid']}, "
        f"future-dated={ts['future']}, formats={ts['per_format']}."
    )
if begin_c and end_c:
    _temporal.append(
        f"- `{end_c}` < `{begin_c}` in {inverted} rows -> a validity-window check is needed at Silver."
    )
_temporal.append(
    para(
        "Source timezone is Europe/Berlin wall-clock; start/end date and time are separate",
        "columns (BEGINN_DATUM + BEGINN_UHRZEIT) -- combine and convert to UTC on a documented",
        "rule before any join to an hourly grid.",
    )
)

_domain = []
if richtung_domain:
    _domain.append(
        f"`{_dir_col}` vs the known direction set: "
        f"unexpected={richtung_domain['unexpected'] or 'none'}, "
        f"unused={richtung_domain['unused_allowed'] or 'none'}."
    )
    _domain.append(
        para(
            "The known-direction list is best-effort (BNetzA wording varies with umlaut encoding /",
            "regime) -- an 'unexpected' value here is more likely a label-encoding difference than a",
            "bad value; inspect before treating it as a finding.",
        )
    )
else:
    _domain.append("- No direction column located by name.")

_vcons = ["Value-consistency checks across the power / energy columns:"]
for res in value_order:
    if res.get("comparable_rows"):
        _vcons.append(
            f"- {res['label']}: {res['violations']}/{res['comparable_rows']} violations "
            f"({res.get('violation_pct')}%)."
        )
    else:
        _vcons.append(f"- {res['label']}: {res['violations']} violations.")
if work_identity:
    _vcons.append(
        f"- `{work_identity['identity']}` (implied = mean power x duration): "
        f"{work_identity['violations']}/{work_identity['comparable_rows']} rows exceed "
        f"{int(work_identity['rel_tol'] * 100)}% relative residual ({work_identity['violation_pct']}%); "
        f"residual p01/p50/p99 {work_identity['residual_p01_p50_p99']}."
    )
else:
    _vcons.append(
        "- energy vs mean-power x duration: NOT tested -- needs BEGINN/ENDE date + time combined "
        "into a timestamp (a Silver step); Bronze keeps date and time in separate columns."
    )
if not value_order and not work_identity:
    _vcons = ["- No power/energy columns located by name for a consistency check."]

_regime = [
    para(
        f"Rows split on the measure start date at {REGIME_CUT} -- the NABEG / Redispatch 2.0",
        "boundary. Per-column null-rate and category vocabulary each side.",
    ),
    "",
]
if regime:
    _regime.append(
        f"- pre={regime['pre_rows']}, post={regime['post_rows']}, undated={regime['undated_rows']}; "
        f"columns flipping populated/empty: {regime['flipped'] or 'none'}."
    )
    for c in regime["flipped"][:10]:
        v = regime["per_column"][c]
        _regime.append(
            f"  - `{c}`: null-rate pre={v['pre_null_rate']} post={v['post_null_rate']}."
        )
    _regime.append("")
    _regime.append("Category vocabulary per regime (distinct value counts):")
    for g, gv in vocab_shift.items():
        _regime.append(
            f"- {g} ({gv['rows']} rows): "
            + ", ".join(f"{c}={d['distinct']}" for c, d in gv["columns"].items())
        )
    _regime.append(
        para(
            "This is the measurable form of the Redispatch 2.0 regime change -- if columns flip or",
            "vocabularies differ, do not pool pre- and post-2021 rows without a regime indicator.",
        )
    )
else:
    _regime.append("- no start-date column -- regime split not assessable.")

_coverage = [
    f"Observed rows per year: {year_pairs}.",
    para(
        "Redispatch reporting moved to the 'NABEG 2.0 / Redispatch 2.0' regime in Oct 2021;",
        "if the observed window ends before then, this Bronze table is the OLD-regime extract",
        "and does not cover current redispatch -- a hard boundary for any model that assumes",
        "continuity.",
    ),
]
for c in cat_cols:
    if "uenb" in c.lower() or "anlage" in c.lower():
        _coverage.append(
            f"`{c}` has {acd[c]} distinct values -- {'grid-operator combinations' if 'uenb' in c.lower() else 'affected plants'}; "
            "a few dominate (see Distributions), so a per-entity model faces a long tail."
        )

_dist = []
for c, pairs in categorical_dist.items():
    _dist.append(f"- `{c}`: " + fmt_pairs(pairs, n=15))
for c in sorted(numeric):
    if numeric[c]["is_numeric"]:
        pl = numeric[c]["plausibility"]
        _dist.append(
            f"- `{c}`: min={pl['min']}, max={pl['max']}, mean={pl['mean']}, sd={pl['sd']}, "
            f"parsed {numeric[c]['parsed']} of {numeric[c]['non_null']} non-null."
        )

_findings = []
if constant_cols:
    _findings.append(f"- Constant columns: {constant_cols}.")
if total - distinct_rows:
    _findings.append(f"- {total - distinct_rows} exact duplicate rows.")
if text_flagged:
    _findings.append(
        f"- Columns that are TEXT despite a numeric-looking name/shape: {text_flagged} "
        "(reported here so they are not mistaken for broken numeric columns)."
    )
if inverted:
    _findings.append(f"- {inverted} rows have end time before start time.")
if year_pairs:
    _findings.append(
        f"- Coverage window: {year_pairs[0][0]}..{year_pairs[-1][0]} "
        f"(no rows after {year_pairs[-1][0]})."
    )
_findings_md = (
    "\n".join(_findings) if _findings else "No material data-quality issues found."
)

_silver = []
if constant_cols:
    _silver.append(f"- Drop constant columns {constant_cols} from Silver.")
if total - distinct_rows:
    _silver.append("- Apply distinct on load to drop exact duplicate rows.")
_silver += [
    (
        "- Cast the MW / MWH columns to double with an explicit German-comma decimal rule; "
        "quarantine values that fail."
    ),
    (
        "- Keep BETROFFENE_ANLAGE / *_UENB as text; reconcile the affected-plant name to a "
        "power_plant_list / MaStR unit id at Silver (fuzzy match -- there is no shared key)."
    ),
    (
        "- Silver grain: one row per redispatch measure/event; combine BEGINN_DATUM+UHRZEIT and "
        "ENDE_DATUM+UHRZEIT into UTC start/end timestamps and validate start <= end."
    ),
]

_ri = [
    para(
        "There is NO shared key between redispatch_measures and power_plant_list / MaStR --",
        "BETROFFENE_ANLAGE is a free-text plant name. A name-based reconciliation is a Silver",
        "task with its own match-rate and ambiguity to measure; this notebook only establishes",
        "that no join key exists in Bronze.",
    ),
]

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                "One row per redispatch measure. No entity id in Bronze -- the affected plant is a name "
                "string. A per-plant or per-operator model must first resolve that name (Silver)."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                "A future join to plant attributes on a resolved plant id is 1:N (many measures per plant) "
                "-- safe if aggregated to the measure, a cartesian risk if a plant name matches several "
                "plant-list rows."
            ),
        ),
        (
            "Target contamination",
            (
                "Candidate targets: the reason categorical, or the MW/MWH volume/duration. The realised "
                "end time, realised duration and realised energy MUST NOT be features for predicting "
                "whether/when a measure starts -- they are known only after the measure."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "Only information available at or before the measure's START time may be a feature; the "
                "ENDE_* columns and GESAMTE_ARBEIT_MWH are post-event."
            ),
        ),
        (
            "Proxy leakage",
            (
                "ANWEISENDER_UENB / ANFORDERNDER_UENB encode which TSO acted -- for some use cases that is "
                "a near-label (the reason a measure exists), not an independent feature."
            ),
        ),
        (
            "Split / entity leakage",
            (
                "Split by the affected plant / requesting TSO (once resolved) or by contiguous date range "
                "-- measures for the same plant in the same congestion event are highly correlated."
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "If plant attributes are joined in, use the plant's state AS OF the measure date, not its "
                "current state (a plant may have been decommissioned or repowered since)."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                f"Coverage window {year_pairs[0][0] if year_pairs else '?'}..{year_pairs[-1][0] if year_pairs else '?'}. "
                "This is the pre-Redispatch-2.0 regime; a model trained on it does not describe current "
                "redispatch practice."
            ),
        ),
        (
            "Missingness leakage",
            (
                "GRUND_DER_MASSNAHME / PRIMAERENERGIEART / ANFORDERNDER_UENB have some missing values "
                "-- whether a field is filled can correlate with the measure type; check before an "
                "'is-missing' feature."
            ),
        ),
        (
            "Duplicate-event leakage",
            (
                f"{total - distinct_rows} exact duplicate rows -- de-duplicate before counting measures as "
                "independent observations or splitting."
            ),
        ),
        (
            "Target / feature temporal misalignment",
            (
                "Start date/time and end date/time are separate columns -- a target defined on the start "
                "instant paired with features cut at the end instant (or vice versa) misaligns them."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            (
                "MITTLERE_LEISTUNG_MW <= MAXIMALE_LEISTUNG_MW and GESAMTE_ARBEIT_MWH ~ mean power x "
                "duration -- these three are near-collinear; using all as independent features is "
                "double-counting."
            ),
        ),
        (
            "Data-generation-process leakage",
            (
                "coverage_regime and ZEITZONE_* describe how the extract was produced, not the physical "
                "measure -- constant here, safe to drop, but a mixed-regime future extract would make "
                "coverage_regime a process feature."
            ),
        ),
        (
            "Class / label instability",
            (
                "GRUND_DER_MASSNAHME categories and the RICHTUNG labels are BNetzA reporting codes that "
                "changed with the Redispatch 2.0 regime -- a class defined by a raw label is only stable "
                "within one regime."
            ),
        ),
        (
            "Label availability lag",
            (
                "Redispatch measures are reported to BNetzA with a lag (weeks to months); a real-time "
                "model cannot assume the measure record exists at the measure time."
            ),
        ),
        (
            "Source / version / regime change",
            (
                "PRIMARY concern: the Oct-2021 Redispatch 2.0 switch changed reporting scope, thresholds "
                "and the actor model. Regime / Version Evidence above measures the null-rate and "
                "category-vocabulary shift across the 2021-10-01 cut -- do not pool pre- and post-2021 "
                "data without a regime indicator."
            ),
        ),
        (
            "Sample-vs-full divergence",
            "Every statistic here is a full Spark aggregation or `.distinct().count()` -- no sampling.",
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_prof)),
        ("Data Quality", "\n".join(_dq)),
        ("Unit & Semantic Validation", "\n".join(_unit)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Value Consistency", "\n".join(_vcons)),
        ("Temporal Semantics", "\n".join(_temporal)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Referential Integrity", "\n".join(_ri)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
