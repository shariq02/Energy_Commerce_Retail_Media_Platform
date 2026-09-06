# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- HONDA IOT WEATHER
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile honda_iot_weather -- schema, missingness, constant
# MAGIC columns, per-frequency continuity against an independent calendar, value
# MAGIC plausibility (Ta range, Igm >= 0), stuck runs, duplicate keys (identical
# MAGIC vs conflicting), and the layered modelling-risk checklist -- as evidence
# MAGIC for Silver design and for the energy<->weather join in notebook 03.

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
SOURCE = "honda_iot"
NB_KEY = "02_weather"
SECTION_TITLE = "Weather table (honda_iot_weather)"
TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_weather"
FREQ_SECONDS = {"1min": 60, "15min": 900, "1h": 3600}
# Ta = air temperature degC ; Igm = global irradiance W/m2.
PLAUSIBLE = {"Ta": (-40.0, 50.0), "Igm": (0.0, 1500.0)}
TS_TZ = "UTC"

# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Profile -- rows, missingness, approx distinct, value ranges (one agg)
df = spark.table(TABLE)
COLS = df.columns
VCOLS = [c for c in COLS if c not in ("frequency", "datetime_utc")]
exprs = [F.count(F.lit(1)).alias("__rows")]
for c in COLS:
    miss = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
    exprs += [
        F.sum(miss.cast("long")).alias(c + "__m"),
        F.approx_count_distinct(c).alias(c + "__d"),
    ]
for c in VCOLS:
    v = safe_num(c)
    b = PLAUSIBLE.get(c.split("_")[-1])
    exprs += [
        F.min(v).alias(c + "_min"),
        F.max(v).alias(c + "_max"),
        F.avg(v).alias(c + "_avg"),
        F.stddev(v).alias(c + "_sd"),
        F.percentile_approx(v, [0.01, 0.5, 0.99]).alias(c + "_p"),
        F.sum((v == 0).cast("long")).alias(c + "_zero"),
        F.sum((v < 0).cast("long")).alias(c + "_negative"),
        F.sum(
            (F.col(c).isNotNull() & (F.trim(F.col(c)) != "") & v.isNull()).cast("long")
        ).alias(c + "_non_numeric"),
        *(
            [F.sum(((v < b[0]) | (v > b[1])).cast("long")).alias(c + "_out_of_range")]
            if b
            else []
        ),
    ]
S = df.agg(*exprs).first().asDict()
total = S["__rows"]
constant_cands = [c for c in COLS if S[c + "__d"] <= 1]
constant_cols = (
    sorted(
        c
        for c in constant_cands
        if (df.agg(F.countDistinct(F.col(c)).alias(c)).first()[c] or 0) <= 1
    )
    if constant_cands
    else []
)
print(f"rows={total}  columns={len(COLS)}  ->  {COLS}")
for c in COLS:
    print(
        f"  {c:<32} missing={S[c + '__m']:>10} "
        f"rate={S[c + '__m'] / total:.4f} approx_distinct={S[c + '__d']}"
    )
print("constant columns (exact):", constant_cols)
for c in VCOLS:
    print(f"  {c:<32}", {k[len(c) + 1 :]: S[k] for k in S if k.startswith(c + "_")})

# COMMAND ----------

# DBTITLE 1,Rows per frequency (one groupBy)
d = (
    df.groupBy("frequency")
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.min("datetime_utc").alias("min_ts"),
        F.max("datetime_utc").alias("max_ts"),
        F.countDistinct("datetime_utc").alias("distinct_ts"),
    )
    .orderBy("frequency")
    .collect()
)
freq_rows = [(x["frequency"], x["rows"]) for x in d]
freq_cov = {x["frequency"]: x.asDict() for x in d}
print([x.asDict() for x in d])

# COMMAND ----------

# DBTITLE 1,Duplicate (frequency, datetime_utc) key -- identical vs conflicting (one groupBy)
dk = df.groupBy("frequency", "datetime_utc").agg(
    F.count(F.lit(1)).alias("n"),
    F.countDistinct(F.hash(*[F.col(c) for c in COLS])).alias("row_variants"),
)
dq = (
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
print("duplicate key composition:", dq)

# COMMAND ----------

# DBTITLE 1,Temporal continuity -- coverage vs an INDEPENDENT per-frequency calendar
cg = continuity_grid(df, "datetime_utc", FREQ_SECONDS, entity_col="frequency")
continuity = cg["per_entity"]
for fq, r in continuity.items():
    print(fq, r)

# COMMAND ----------

# DBTITLE 1,Categorical domain (`frequency`) + regime split (first half vs second half)
freq_domain = categorical_domain(df, "frequency", FREQ_SECONDS.keys(), name="frequency")
if freq_domain["unexpected_count"]:
    print(
        f"UNEXPECTED frequency label(s): {freq_domain['unexpected']} -- ingestion defect"
    )

_span = [
    (v["min_ts"], v["max_ts"])
    for v in freq_cov.values()
    if v.get("min_ts") and v.get("max_ts")
]
regime = None
if _span:
    lo = min(s[0] for s in _span)
    hi = max(s[1] for s in _span)
    mid = (lo + (hi - lo) / 2).replace(microsecond=0).isoformat()
    regime = {"cut": mid, **regime_population_shift(df, "datetime_utc", VCOLS, mid)}
    print(
        f"regime split at {mid}: {regime['pre_rows']} / {regime['post_rows']}, flipped={regime['flipped']}"
    )

# COMMAND ----------

# DBTITLE 1,Stuck-run detection per value column (one windowed pass)
w2 = Window.partitionBy("frequency").orderBy("datetime_utc")
df_1h = df.where(F.col("frequency") == "1h")
for c in VCOLS:
    v = safe_num(c)
    df_1h = df_1h.withColumn(
        f"{c}_stuck",
        (
            v.isNotNull() & (v == F.lag(v, 1).over(w2)) & (v == F.lag(v, 11).over(w2))
        ).cast("long"),
    )
stuck = (
    df_1h.agg(*[F.sum(F.col(f"{c}_stuck")).alias(c) for c in VCOLS]).first().asDict()
)
print("stuck>=12-run (1h):", stuck)

# COMMAND ----------

# DBTITLE 1,Samples for figures + full-table diurnal profile
value_pdf = (
    df.select(*[safe_num(c).alias(c) for c in VCOLS])
    .sample(0.1, seed=42)
    .limit(150_000)
    .toPandas()
)
ts_pdf = (
    df.where(F.col("frequency") == "1h")
    .select("datetime_utc", *[safe_num(c).alias(c) for c in VCOLS])
    .orderBy("datetime_utc")
    .limit(3000)
    .toPandas()
)
hourly = (
    df.where(F.col("frequency") == "1h")
    .groupBy(F.hour(F.to_timestamp("datetime_utc")).alias("hod"))
    .agg(*[F.avg(safe_num(c)).alias(c) for c in VCOLS])
    .orderBy("hod")
    .collect()
)

# COMMAND ----------

# DBTITLE 1,Figure -- frequency overview (rows / coverage % / longest gap)
figs = []
if facet_bars(
    {
        "rows per frequency": freq_rows,
        "coverage % (independent calendar)": [
            (fq, continuity[fq]["coverage_pct"])
            for fq in FREQ_SECONDS
            if fq in continuity
        ],
        "longest gap (steps)": [
            (fq, continuity[fq]["longest_gap_steps"])
            for fq in FREQ_SECONDS
            if fq in continuity
        ],
    },
    "Honda weather -- frequency overview",
    "honda_weather_frequency.png",
    rot=0,
):
    figs.append(("Honda weather -- frequency overview", "honda_weather_frequency.png"))

# COMMAND ----------

# DBTITLE 1,Figure -- value distributions, hourly window, diurnal profile
if facet_hists(
    {c: value_pdf[c].dropna().tolist() for c in VCOLS},
    "Honda weather -- value distribution per column (sampled)",
    "honda_weather_value_distributions.png",
):
    figs.append(
        (
            "Honda weather -- value distribution per column",
            "honda_weather_value_distributions.png",
        )
    )
if not ts_pdf.empty:
    fig, axes = plt.subplots(len(VCOLS), 1, figsize=(12, 3 * len(VCOLS)), squeeze=False)
    for i, c in enumerate(VCOLS):
        axes[i][0].plot(range(len(ts_pdf)), ts_pdf[c], linewidth=0.8)
        axes[i][0].set_title(f"Honda weather -- {c} (first 3000 hourly points)")
    fig.tight_layout()
    _save_and_show(fig, "honda_weather_hourly_window.png")
    figs.append(
        (
            "Honda weather -- first 3000 hourly points per column",
            "honda_weather_hourly_window.png",
        )
    )


def _diurnal_draw(c):
    def draw(ax):
        ax.plot([h["hod"] for h in hourly], [h[c] for h in hourly], marker="o")
        ax.set_xlabel("hour", fontsize=7)

    return draw


if _facet_grid(
    [(c, _diurnal_draw(c)) for c in VCOLS],
    "Honda weather -- mean value by hour of day",
    "honda_weather_diurnal_profiles.png",
    ncols=2,
):
    figs.append(
        (
            "Honda weather -- mean value by hour of day",
            "honda_weather_diurnal_profiles.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Findings
print("constant columns:", constant_cols)
print("duplicate key composition:", dq)
print("continuity:", continuity)
print("stuck runs:", stuck)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/honda_iot.md
_prof = ["| column | missing | rate | approx_distinct |", "|---|---|---|---|"]
for c in COLS:
    _prof.append(
        f"| {c} | {S[c + '__m']} | {S[c + '__m'] / total:.4f} | {S[c + '__d']} |"
    )
_prof.append("")
_prof.append(f"Rows: {total}. Constant columns (exact): {constant_cols or 'none'}.")

_dist = [
    "| column | min | p01 | p50 | p99 | max | mean | sd | zero | negative | non_numeric |",
    "|---|---|---|---|---|---|---|---|---|---|---|",
]
for c in VCOLS:
    p = S[c + "_p"] or [None, None, None]
    _dist.append(
        f"| {c} | {S[c + '_min']} | {p[0]} | {p[1]} | {p[2]} | {S[c + '_max']} | "
        f"{S[c + '_avg']} | {S[c + '_sd']} | {S[c + '_zero']} | {S[c + '_negative']} | "
        f"{S[c + '_non_numeric']} |"
    )

_oor = {c: S.get(c + "_out_of_range") for c in VCOLS if c + "_out_of_range" in S}

_unit = [
    para(
        "Value columns: Ta = air temperature (degC), Igm = global irradiance (W/m2).",
        f"Plausible bounds {PLAUSIBLE}.",
    ),
]
for c in VCOLS:
    _unit.append(
        f"- {c}: range {S[c + '_min']}..{S[c + '_max']}, mean {S[c + '_avg']}, "
        f"out-of-plausible-range rows {S.get(c + '_out_of_range')}, negative {S[c + '_negative']}, "
        f"non-numeric {S[c + '_non_numeric']}"
    )
_unit.append(
    para(
        "Igm at night is 0 by physics -- a large zero count is expected, not missingness;",
        "a negative Igm or an Igm far above ~1200 W/m2 in Germany is a sensor fault.",
    )
)

_temporal = [
    para(
        f"datetime_utc is treated as {TS_TZ} per the dataset name.",
        "Coverage below is measured against an INDEPENDENT calendar",
        "(expected = span / step + 1), so 100% means genuinely gap-free.",
    ),
    "",
    "| frequency | observed | expected | coverage % | longest gap (steps) | on-step % |",
    "|---|---|---|---|---|---|",
]
for fq in FREQ_SECONDS:
    r = continuity.get(fq)
    if r:
        _temporal.append(
            f"| {fq} | {r['observed']} | {r['expected']} | {r['coverage_pct']} | "
            f"{r['longest_gap_steps']} | {r['on_step_pct']} |"
        )

_dq = [f"Duplicate (frequency, datetime_utc) key composition: {dq}."]
if dq.get("conflicting", 0) > 0:
    _dq.append(
        para(
            "Conflicting duplicate keys exist (same key, differing non-key values) --",
            "Silver load needs a deterministic de-duplication rule for this table.",
        )
    )
elif dq.get("dup_groups", 0) > 0:
    _dq.append(
        "Duplicate keys are all fully identical rows -- a plain distinct suffices."
    )
else:
    _dq.append("(frequency, datetime_utc) is unique in Bronze.")
_dq.append(f"Stuck-run rows (value == value 1 and 11 steps back, 1h): {stuck}.")

_domain = [
    para(
        "`frequency` values vs the known resolution set (1min / 15min / 1h):",
        f"unexpected={freq_domain['unexpected'] or 'none'},",
        f"unused={freq_domain['unused_allowed'] or 'none'}.",
    ),
]
if freq_domain["unexpected_count"]:
    _domain.append(
        "-> an unexpected label is an INGESTION defect (a bad partition value), not a source finding."
    )

_regime = [
    para(
        "Rows split at the series midpoint. A large null-rate or distinct-count",
        "move on one side points to a sensor outage window, not a physical change",
        "(this measures coverage, not signal level).",
    ),
]
if regime:
    _regime.append(
        f"- cut {regime['cut']}: pre={regime['pre_rows']}, post={regime['post_rows']}; "
        f"columns with a >=50pt null-rate move: {regime['flipped'] or 'none'}."
    )
    for c in regime["flipped"]:
        v = regime["per_column"][c]
        _regime.append(
            f"  - `{c}`: null-rate pre={v['pre_null_rate']} post={v['post_null_rate']}."
        )
else:
    _regime.append("- no datetime span -- not assessable.")

_coverage = [
    f"Rows per frequency: { {k: v['rows'] for k, v in freq_cov.items()} }.",
    para(
        "The 1min partition dominates. Any statistic pooled across frequencies is really a",
        "1min statistic. Single weather source (no station id) -- the only split axis is time.",
    ),
    para(
        "This weather series is the feature side of the energy<->weather join (03); if it does",
        "not cover the full energy span, an inner join silently truncates the training window.",
    ),
]

_findings = []
for fq, r in continuity.items():
    if r["coverage_pct"] is not None and r["coverage_pct"] < 99.0:
        _findings.append(
            f"- {fq}: {r['coverage_pct']}% coverage, longest gap {r['longest_gap_steps']} steps."
        )
if constant_cols:
    _findings.append(f"- Constant columns carry no signal: {constant_cols}.")
if _oor and any(_oor.values()):
    _findings.append(f"- Out-of-plausible-range values present: {_oor}.")
if any(stuck.values()):
    _findings.append(f"- Sensor stuck-runs detected (1h): {stuck}.")
_findings_md = (
    "\n".join(_findings) if _findings else "No material data-quality issues found."
)

_silver = [
    "- Type conversion: datetime_utc -> timestamp (UTC); value columns -> double."
]
if dq.get("conflicting", 0) > 0:
    _silver.append(
        "- De-duplicate (frequency, datetime_utc) with a deterministic rule before Silver."
    )
if constant_cols:
    _silver.append(f"- Constant columns {constant_cols} can be dropped from Silver.")
if _oor and any(_oor.values()):
    _silver.append(
        "- Apply plausibility bounds; quarantine out-of-range weather readings rather than "
        "silently nulling them."
    )
_silver.append(
    "- Keep `frequency` in the grain; do not resample or blend resolutions at Silver."
)

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                "One row per (frequency, datetime_utc). Joining to the energy tables (03) must be on the "
                "same key at the same frequency; resampling drifts the grain."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                "Single weather source -> energy<->weather is at most 1:1 per (frequency, timestamp); see "
                "03 for the confirmed match rate."
            ),
        ),
        (
            "Target contamination",
            (
                "Weather is a feature, not a target here -- but if a weather variable becomes a target, its "
                "own future values and any smoothed/rolling version must be excluded from features."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "A weather reading timestamped at interval end already summarises that interval -- for a "
                "nowcast at time t use only readings strictly before t."
            ),
        ),
        (
            "Proxy leakage",
            (
                "Igm is a near-deterministic function of time-of-day and season -- a model given both Igm "
                "and a fine-grained clock feature is partly memorising the calendar."
            ),
        ),
        (
            "Split / entity leakage",
            "No entity dimension -- split by contiguous date range; never mix frequencies within a split.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "A diurnal / seasonal climatology used as a feature must be built only from data before the "
                "prediction point, not averaged over the full series."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                "Continuity above shows the real gaps; a sensor-outage window is a real absence, not zero. "
                "If the weather span is shorter than the energy span, the joined training set is truncated."
            ),
        ),
        (
            "Missingness leakage",
            (
                "Ta/Igm missing together (2.4% each in the first profile) likely marks a station outage -- "
                "an 'is-missing' flag can leak the outage timing."
            ),
        ),
        (
            "Duplicate-event leakage",
            f"Duplicate key composition {dq} -- de-duplicate before treating a reading as one observation.",
        ),
        (
            "Target / feature temporal misalignment",
            (
                "Weather at interval end vs energy P at the instant vs energy W cumulative -- align all to "
                "one timestamp convention before joining."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            (
                "Ta in degC, Igm in W/m2 -- confirm before combining with any external weather source in a "
                "different unit. No mirror columns in this single-column-pair table."
            ),
        ),
        (
            "Data-generation-process leakage",
            (
                "Any upstream gap-filling / interpolation of the weather series is part of the "
                "data-generation process; an interpolated stretch will look unnaturally smooth."
            ),
        ),
        ("Class / label instability", "Not applicable -- continuous variables only."),
        (
            "Label availability lag",
            (
                "Not applicable to weather-as-feature; if forecast weather is used instead of observed, its "
                "publication lag and horizon must be respected."
            ),
        ),
        (
            "Source / version / regime change",
            (
                "A single sensor deployment; a recalibration or sensor swap would show as a step change "
                "with no physical cause."
            ),
        ),
        (
            "Sample-vs-full divergence",
            (
                "value_pdf is a 10% sample capped at 150k rows and the hourly-window figure uses the first "
                "3000 points -- the diurnal-profile figure IS a full-table groupBy average and is safe; use "
                "the full-table S aggregate for any threshold decision."
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
        ("Unit & Semantic Validation", "\n".join(_unit)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Temporal Semantics", "\n".join(_temporal)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
