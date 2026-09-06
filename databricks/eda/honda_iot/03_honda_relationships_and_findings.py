# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- HONDA IOT RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Cross-table checks across the 7 Honda IoT Bronze tables --
# MAGIC (frequency, datetime_utc) key uniqueness, pairwise overlap, full 7-way
# MAGIC join yield, the energy<->weather match rate and the shared time window,
# MAGIC and the layered modelling-risk checklist. All key-overlap analysis is
# MAGIC derived from one tagged union + one presence matrix rather than repeated
# MAGIC pairwise joins.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "honda_iot"
NB_KEY = "03_relationships_and_findings"
SECTION_TITLE = "Cross-table relationships (7 Honda IoT tables)"
DATASETS = [
    "electricity_p",
    "electricity_w",
    "heating_p",
    "heating_w",
    "cooling_p",
    "cooling_w",
    "weather",
]
ENERGY = [d for d in DATASETS if d != "weather"]
TABLES = {d: f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_{d}" for d in DATASETS}

# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Tagged union of (frequency, datetime_utc, source) across all 7 tables
u = None
for d, t in TABLES.items():
    part = spark.table(t).select("frequency", "datetime_utc", F.lit(d).alias("src"))
    u = part if u is None else u.unionByName(part)

# COMMAND ----------

# DBTITLE 1,Row counts + time span per (dataset, frequency)
grid = (
    u.groupBy("src", "frequency")
    .agg(
        F.min("datetime_utc").alias("min_ts"),
        F.max("datetime_utc").alias("max_ts"),
        F.countDistinct("datetime_utc").alias("distinct_ts"),
        F.count(F.lit(1)).alias("rows"),
    )
    .collect()
)
table_rows = {}
span = {}
for x in grid:
    table_rows[x["src"]] = table_rows.get(x["src"], 0) + x["rows"]
    lo, hi = span.get(x["src"], (x["min_ts"], x["max_ts"]))
    span[x["src"]] = (min(lo, x["min_ts"]), max(hi, x["max_ts"]))
    print(x.asDict())

# COMMAND ----------

# DBTITLE 1,Key-presence matrix -- one groupBy over the union
p = u.groupBy("frequency", "datetime_utc").agg(
    *[F.max((F.col("src") == d).cast("int")).alias(d) for d in DATASETS]
)

# COMMAND ----------

# DBTITLE 1,Overlap / referential stats -- one agg over the presence matrix
pairs = [(a, b) for i, a in enumerate(DATASETS) for b in DATASETS[i + 1 :]]
stats = (
    p.agg(
        F.count(F.lit(1)).alias("union_keys"),
        *[F.sum(d).alias("present__" + d) for d in DATASETS],
        F.sum(F.least(*[F.col(d) for d in DATASETS])).alias("all_seven"),
        *[F.sum(F.col(a) * F.col(b)).alias(f"pair__{a}__{b}") for a, b in pairs],
        *[F.sum(F.col(e) * F.col("weather")).alias(f"ew__{e}") for e in ENERGY],
    )
    .first()
    .asDict()
)

union_ct = stats["union_keys"]
present = {d: stats["present__" + d] for d in DATASETS}
grid_missing = {d: union_ct - present[d] for d in DATASETS}
overlap = {(a, b): stats[f"pair__{a}__{b}"] for a, b in pairs}
join_yield = {e: (present[e], stats[f"ew__{e}"]) for e in ENERGY}
key_unique = {d: table_rows.get(d, 0) == present[d] for d in DATASETS}
seven_ct = stats["all_seven"]

print(f"union keys={union_ct}  keys in all 7={seven_ct}")
for d in DATASETS:
    print(
        f"  {d:<16} present={present[d]:>10}  missing vs union={grid_missing[d]:>10}  "
        f"rows={table_rows.get(d)}  key_unique={key_unique[d]}"
    )
_ewr = {e: round(m / t * 100, 1) for e, (t, m) in join_yield.items() if t}
print("energy<->weather match rate:", _ewr)

# COMMAND ----------

# DBTITLE 1,Cross-source temporal overlap (energy vs weather span)
overlap_win = cross_source_overlap(
    {
        "energy": span.get("electricity_p", (None, None)),
        "weather": span.get("weather", (None, None)),
    }
)
print("shared window energy x weather:", overlap_win)

# COMMAND ----------

# DBTITLE 1,Figure -- pairwise overlap heatmap
figs = []
n = len(DATASETS)
m = np.zeros((n, n))
for i, a in enumerate(DATASETS):
    m[i, i] = present[a]
    for j, b in enumerate(DATASETS):
        if (a, b) in overlap:
            m[i, j] = m[j, i] = overlap[(a, b)]
fig, ax = plt.subplots(figsize=(8, 7))
im = ax.imshow(np.log10(m + 1), cmap="viridis")
fig.colorbar(im, ax=ax, label="log10(shared keys + 1)")
ax.set_xticks(range(n))
ax.set_xticklabels(DATASETS, rotation=45, ha="right")
ax.set_yticks(range(n))
ax.set_yticklabels(DATASETS)
ax.set_title("Honda -- pairwise (frequency, datetime_utc) overlap")
fig.tight_layout()
_save_and_show(fig, "honda_overlap_matrix.png")
figs.append(
    ("Honda pairwise (frequency, datetime_utc) overlap", "honda_overlap_matrix.png")
)

# COMMAND ----------

# DBTITLE 1,Figure -- missing keys vs union + energy<->weather join yield
if barplot(
    list(grid_missing.items()),
    "Honda -- (frequency, datetime_utc) keys missing vs union",
    "dataset",
    "missing keys",
    rot=30,
    filename="honda_keys_missing_vs_union.png",
):
    figs.append(
        (
            "Honda -- keys missing from each table vs the union",
            "honda_keys_missing_vs_union.png",
        )
    )
x = np.arange(len(ENERGY))
fig, ax = plt.subplots(figsize=(11, 4))
ax.bar(x - 0.2, [join_yield[e][0] for e in ENERGY], width=0.4, label="energy keys")
ax.bar(
    x + 0.2, [join_yield[e][1] for e in ENERGY], width=0.4, label="matched to weather"
)
ax.set_xticks(x)
ax.set_xticklabels(ENERGY, rotation=30, ha="right")
ax.legend()
ax.set_title("Honda -- energy<->weather join yield on (frequency, datetime_utc)")
ax.set_ylabel("keys")
fig.tight_layout()
_save_and_show(fig, "honda_energy_weather_join_yield.png")
figs.append(
    ("Honda -- energy <-> weather join yield", "honda_energy_weather_join_yield.png")
)

# COMMAND ----------

# DBTITLE 1,Findings
print("key unique per table       :", key_unique)
print("keys missing vs union      :", grid_missing)
print("energy<->weather join yield :", join_yield)
print("keys shared by all 7        :", seven_ct, "of", union_ct)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/honda_iot.md
_ek = [
    "| table | rows | distinct keys present | missing vs union | key unique |",
    "|---|---|---|---|---|",
]
for d_ in DATASETS:
    _ek.append(
        f"| honda_iot_{d_} | {table_rows.get(d_)} | {present[d_]} | "
        f"{grid_missing[d_]} | {key_unique[d_]} |"
    )
_ek += [
    "",
    para(
        f"Union of (frequency, datetime_utc) keys: {union_ct}.",
        f"Keys shared by all 7 tables: {seven_ct} ({seven_ct / union_ct * 100:.1f}%).",
    ),
]

_rel = [
    "Energy<->weather match rate on (frequency, datetime_utc):",
    "",
    "| energy table | energy keys | matched to weather | % |",
    "|---|---|---|---|",
]
for e_ in ENERGY:
    tk, mk = join_yield[e_]
    _rel.append(
        f"| honda_iot_{e_} | {tk} | {mk} | {mk / tk * 100:.1f} |"
        if tk
        else f"| honda_iot_{e_} | 0 | 0 | |"
    )
_rel += ["", "Pairwise shared-key counts:", "", "| pair | shared keys |", "|---|---|"]
for (a_, b_), v_ in overlap.items():
    _rel.append(f"| {a_} + {b_} | {v_} |")
_rel += [
    "",
    "Relationship cardinality (energy stream -> weather, on (frequency, datetime_utc)):",
    "",
    "| energy table | child rows | distinct child keys | matched to weather | orphan keys | max fan-out |",
    "|---|---|---|---|---|---|",
]
for e_ in ENERGY:
    tk, mk = join_yield[e_]
    _rel.append(
        f"| honda_iot_{e_} | {table_rows.get(e_)} | {present[e_]} | {mk} | {present[e_] - mk} | "
        f"{'1 (key unique)' if key_unique[e_] else '>1 -- de-dup first'} |"
    )
_rel.append(
    para(
        "Because (frequency, datetime_utc) is unique in every table, every join here is 1:1 --",
        "the only cardinality risk is row LOSS on an inner join, quantified by the orphan-keys",
        "column, not row multiplication.",
    )
)

_coverage = [
    para(
        "Time span per dataset:",
        str({d: (str(span[d][0]), str(span[d][1])) for d in DATASETS if d in span}),
    ),
    para(
        "Energy x weather shared window:",
        str(overlap_win["common_window"]),
        "-- an energy+weather model can only train inside this window; an inner join outside",
        "it silently drops rows.",
    ),
    para(
        f"{union_ct - seven_ct} of {union_ct} keys are absent from at least one table",
        f"(per-table gaps: {grid_missing}). This is a CROSS-table completeness gap -- a single",
        "table can still be internally dense (see 01/02 continuity) while differing from another",
        "by a handful of timestamps.",
    ),
]

_verdict = [
    f"(frequency, datetime_utc) is unique in every Honda table: {all(key_unique.values())}.",
    f"{seven_ct} of {union_ct} keys ({seven_ct / union_ct * 100:.1f}%) are in all 7 tables.",
    f"Energy<->weather match rate: {_ewr}.",
    para(
        "A shared 1:1 key exists. A wide 'all Honda metrics at (frequency, datetime_utc)' table",
        "is feasible on the intersection but drops the non-overlapping tail; the natural Silver",
        "grain is one fact per dataset (or per metric joining P+W), the wide table is Gold.",
    ),
]

_findings = []
if not all(key_unique.values()):
    _findings.append(
        f"- Duplicate (frequency, datetime_utc) keys in: "
        f"{[d_ for d_ in DATASETS if not key_unique[d_]]}."
    )
if seven_ct < union_ct:
    _findings.append(
        f"- {union_ct - seven_ct} keys missing from at least one table (gaps: {grid_missing})."
    )
if any(v < 100 for v in _ewr.values()):
    _findings.append(
        f"- Energy<->weather join is lossy for: {[e_ for e_, v in _ewr.items() if v < 100]}."
    )
_findings_md = (
    "\n".join(_findings)
    if _findings
    else "All 7 tables align 1:1 on the key with full coverage."
)

_silver = []
if not all(key_unique.values()):
    _silver.append(
        "- De-duplicate per-table on (frequency, datetime_utc) before any Silver join."
    )
_silver.append(
    "- Silver grain: one fact table per dataset keyed on (frequency, datetime_utc); "
    "join P+W per metric where a combined metric fact is needed."
)
if any(v < 100 for v in _ewr.values()):
    _silver.append(
        "- Use outer joins (not inner) when combining energy and weather; carry an explicit "
        "unmatched flag."
    )

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                f"(frequency, datetime_utc) is unique in every table ({all(key_unique.values())}); all "
                "pairwise and 7-way joins hold that grain. Resampling to a common frequency would change it."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                "1:1 on the shared key -- no fan-out. The risk is the opposite: an inner 7-way join keeps "
                f"only {seven_ct} of {union_ct} keys."
            ),
        ),
        (
            "Target contamination",
            (
                "No target across these tables -- see 01/02. A wide feature row must not include the target "
                "metric's own value at the target timestamp."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "Energy and weather share the timestamp grid -- same-timestamp weather is a legitimate "
                "feature for same-timestamp energy, but no later energy/weather value may feed an earlier "
                "prediction."
            ),
        ),
        (
            "Proxy leakage",
            (
                "P and W of one metric are near-redundant; heat/cool total ~ sum of components -- a wide "
                "join makes these collinear features trivially available."
            ),
        ),
        (
            "Split / entity leakage",
            (
                "Split by contiguous date range across ALL tables at once, never per-table by row, so a "
                "timestamp's energy and weather stay on one side."
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            "No slowly-changing attributes; a climatology feature must be built only from pre-cutoff data.",
        ),
        (
            "Survivorship / coverage bias",
            (
                f"Energy x weather shared window {overlap_win['common_window']} -- training outside it is "
                "impossible; the join yield table shows where each energy stream loses weather coverage."
            ),
        ),
        (
            "Missingness leakage",
            (
                "A missing key in one table at a timestamp present in others marks a per-stream outage -- an "
                "'is-present' flag per stream can leak the outage timing."
            ),
        ),
        (
            "Duplicate-event leakage",
            (
                f"Key uniqueness per table: {key_unique} -- de-duplicate any table that is not unique before "
                "joining or splitting."
            ),
        ),
        (
            "Target / feature temporal misalignment",
            (
                "P (instant), W (cumulative, interval-end) and weather (interval-end) use different "
                "timestamp conventions -- align to one before building a wide row."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            "See 01 -- P<->W and component<->total relationships are physical identities, not signal.",
        ),
        (
            "Data-generation-process leakage",
            (
                "Any upstream alignment / gap-filling that made the 7 grids match is part of the "
                "data-generation process; the residual mismatch here is what survived it."
            ),
        ),
        ("Class / label instability", "Not applicable -- continuous metrics only."),
        (
            "Label availability lag",
            (
                "Interval-end readings are not available until the interval closes -- a nowcast cannot use "
                "the interval it is predicting."
            ),
        ),
        (
            "Source / version / regime change",
            (
                "One deployment; a sensor swap on any stream would show as a step change and a shift in that "
                "stream's coverage window."
            ),
        ),
        (
            "Sample-vs-full divergence",
            (
                "Every number here (key presence, pairwise overlap, join yield, spans) is a full Spark "
                "aggregation over the tagged-union presence matrix -- no sampling."
            ),
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    [
        ("Entities / Keys", "\n".join(_ek)),
        ("Relationships", "\n".join(_rel)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("EDA Findings", _findings_md + "\n\n" + "\n".join(_verdict)),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
