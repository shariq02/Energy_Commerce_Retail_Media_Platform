# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- MASTR CHANGE HISTORY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the three MaStR deletion / change-log Bronze
# MAGIC tables (geloeschte_deaktivierte_einheiten,
# MAGIC geloeschte_deaktivierte_marktakteure,
# MAGIC einheiten_aenderung_netzbetreiberzuordnungen) -- schema, missingness,
# MAGIC constant columns, exact key cardinality, full-row duplicates, and a
# MAGIC proper multi-format parse of every change-event date (each format's
# MAGIC contribution, unparsed samples, and implausible / future-dated rows
# MAGIC reported explicitly). These tables are the only record of units and
# MAGIC actors that left the register -- evidence for Silver design and for the
# MAGIC survivorship-bias read of the current-state tables.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "mastr"
NB_KEY = "04_change_history"
SECTION_TITLE = (
    "Change history (geloeschte_deaktivierte_* / "
    "einheiten_aenderung_netzbetreiberzuordnungen)"
)
DATASETS = [
    "geloeschte_deaktivierte_einheiten",
    "geloeschte_deaktivierte_marktakteure",
    "einheiten_aenderung_netzbetreiberzuordnungen",
]
TABLES = {d: f"{CATALOG}.{BRONZE_SCHEMA}.mastr_{d}" for d in DATASETS}

# A date column's name contains one of these. "aenderung" alone is NOT here --
# it also matches ArtDerAenderung ("type of change"), a catalog code column
# whose values (3085, 3086, ...) would be mis-read as calendar years.
DATE_COL_HINTS = ("datum", "date", "zeitpunkt")
# `*_nv` ("nicht vorhanden") columns are boolean availability flags, never dates.
NON_DATE_SUFFIXES = ("_nv",)
OWN_KEY_PREFERENCE = ("EinheitMastrNummer", "MarktakteurMastrNummer", "MastrNummer")
# A change registered against a unit cannot predate MaStR's precursor register;
# anything before this or after "now" is a parse artefact, not a real event.
VALID_FROM = "2000-01-01"

# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Profile each table -- rows, missingness, approx distinct, constant columns (one agg per table)
frames = {d: spark.table(t) for d, t in TABLES.items()}
prof = {}
for name, df in frames.items():
    cols = df.columns
    exprs = [F.count(F.lit(1)).alias("__rows")]
    for c in cols:
        miss = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
        exprs += [
            F.sum(miss.cast("long")).alias(c + "__m"),
            F.approx_count_distinct(c).alias(c + "__d"),
        ]
    r = df.agg(*exprs).first().asDict()
    total = r["__rows"]
    acd = {c: r[c + "__d"] for c in cols}
    miss = {c: r[c + "__m"] for c in cols}
    prof[name] = {"cols": cols, "total": total, "acd": acd, "miss": miss}
    print("=" * 90, f"\n{name}  rows={total}  cols={len(cols)}")
    for c in cols:
        rate = miss[c] / total if total else 0
        print(
            f"  {c:<44} missing={miss[c]:>10} rate={rate:.4f} approx_distinct={acd[c]}"
        )

# COMMAND ----------

# DBTITLE 1,Confirm constant columns exactly
constant_cols = {}
for name, df in frames.items():
    cands = [c for c in prof[name]["cols"] if prof[name]["acd"][c] <= 1]
    if cands:
        r = (
            df.agg(*[F.countDistinct(F.col(c)).alias(c) for c in cands])
            .first()
            .asDict()
        )
        constant_cols[name] = sorted(c for c in cands if (r[c] or 0) <= 1)
    else:
        constant_cols[name] = []
    prof[name]["constant"] = constant_cols[name]
    print(f"{name}: constant columns (exact) = {constant_cols[name]}")

# COMMAND ----------

# DBTITLE 1,Key columns -- EXACT distinct count (a unit/actor recurs across change rows)
key_report = {}
own_key = {}
for name, df in frames.items():
    cands = key_like_cols(df.columns)
    uniq = exact_uniqueness(df, cands)
    k, _info = pick_entity_key(uniq, cands, prefer=OWN_KEY_PREFERENCE)
    own_key[name] = k
    key_report[name] = uniq
    print(f"{name}: referenced-entity key = {k}")
    for c, u in uniq.items():
        print(
            f"    {c:<34} distinct={u['distinct']:>10} ratio={u['ratio']} unique={u['unique']}"
        )

# COMMAND ----------

# DBTITLE 1,Full-row duplicates per table
dup_counts = {}
for name, df in frames.items():
    dup_counts[name] = prof[name]["total"] - df.distinct().count()
    print(f"{name}: exact full-row duplicates = {dup_counts[name]}")

# COMMAND ----------

# DBTITLE 1,Temporal semantics -- multi-format parse of every date-like column
temporal = {}
for name, df in frames.items():
    dcols = [
        c
        for c in prof[name]["cols"]
        if any(h in c.lower() for h in DATE_COL_HINTS)
        and not c.lower().endswith(NON_DATE_SUFFIXES)
    ]
    per_col = {}
    for c in dcols:
        ts = timestamp_semantics(df, c, valid_from=VALID_FROM, tz="Europe/Berlin")
        per_col[c] = ts
        for ln in ts["lines"]:
            print(f"  {name}.{c}: {ln}")
    temporal[name] = per_col

# COMMAND ----------

# DBTITLE 1,Rows per year -- ONLY plausible years; unparsed / implausible counted separately
rows_per_year = {}
for name, df in frames.items():
    dcols = list(temporal[name].keys())
    per_col = {}
    for c in dcols:
        parsed = parse_ts_multi(c)
        yr = F.year(parsed)
        plausible = yr.between(2000, F.year(F.current_timestamp()) + 1)
        g = (
            df.select(yr.alias("yr"), plausible.alias("ok"))
            .where(F.col("ok"))
            .groupBy("yr")
            .count()
            .orderBy("yr")
            .collect()
        )
        bad = (
            df.select(parsed.alias("p"))
            .where(
                F.col("p").isNull()
                | ~F.year("p").between(2000, F.year(F.current_timestamp()) + 1)
            )
            .count()
        )
        per_col[c] = {
            "by_year": [(x["yr"], x["count"]) for x in g],
            "unparsed_or_implausible": bad,
        }
        print(f"{name}.{c}: {per_col[c]}")
    rows_per_year[name] = per_col

# COMMAND ----------

# DBTITLE 1,Figure -- rows per table + duplicates
figs = []
if facet_bars(
    {
        "rows per table": [(d, prof[d]["total"]) for d in DATASETS],
        "full-row duplicates": [(d, dup_counts[d]) for d in DATASETS],
    },
    "MaStR change history -- overview",
    "mastr_change_history_overview.png",
    rot=30,
    ncols=2,
):
    figs.append(
        ("MaStR change history -- overview", "mastr_change_history_overview.png")
    )

# COMMAND ----------

# DBTITLE 1,Figure -- rows per plausible year (first date column per table)
if facet_bars(
    {
        d: (
            next(iter(rows_per_year[d].values()))["by_year"] if rows_per_year[d] else []
        )
        for d in DATASETS
    },
    "MaStR change history -- rows per year (plausible years only; parse artefacts excluded)",
    "mastr_change_history_temporal.png",
    rot=45,
    ncols=3,
):
    figs.append(
        (
            "MaStR change history -- rows per year (plausible years only)",
            "mastr_change_history_temporal.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Findings
findings_lines = []
for d in DATASETS:
    tcols = list(temporal[d].keys())
    findings_lines.append(
        f"{d}: rows={prof[d]['total']}, cols={len(prof[d]['cols'])}, "
        f"constant={prof[d]['constant']}, duplicates={dup_counts[d]}, "
        f"entity_key={own_key[d]}, date columns={tcols}"
    )
print("\n".join(findings_lines))

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/mastr.md
_profile = ["| table | rows | cols | constant columns |", "|---|---|---|---|"]
for d in DATASETS:
    _profile.append(
        f"| {d} | {prof[d]['total']} | {len(prof[d]['cols'])} | "
        f"{', '.join(prof[d]['constant']) or '-'} |"
    )

_dq = ["Full-row exact duplicates per table:"]
for d in DATASETS:
    _dq.append(f"- {d}: {dup_counts[d]}")

_entities = [
    "Referenced-entity key cardinality (column: distinct / ratio-to-rows / unique):"
]
for d in DATASETS:
    _entities.append(
        f"- {d}: entity key = `{own_key[d]}` (a unit/actor recurs across change rows)"
    )
    for c, u in key_report[d].items():
        _entities.append(
            f"  - `{c}`: {u['distinct']} / {u['ratio']} / unique={u['unique']}"
        )

_temporal_md = ["Every date-like column parsed under multiple ISO + German formats:"]
for d in DATASETS:
    for c, ts in temporal[d].items():
        _temporal_md.append(
            f"- `{d}.{c}`: parse yield {ts['yield']:.1%} of {ts['present']}; "
            f"formats {ts['per_format']}; range {ts['min_ts']}..{ts['max_ts']}; "
            f"before {VALID_FROM}={ts['before_valid']}; future-dated={ts['future']}."
        )
        if ts["unparsed_sample"]:
            _temporal_md.append(f"  - unparsed samples: {ts['unparsed_sample']}")
        rpy = rows_per_year[d][c]
        _temporal_md.append(
            f"  - rows per plausible year: {rpy['by_year']}; "
            f"unparsed or implausible-year rows (excluded from the figure): "
            f"{rpy['unparsed_or_implausible']}."
        )
_temporal_md.append(
    "Source timezone is Europe/Berlin wall-clock; the registration and effective dates are "
    "distinct columns where present -- do not treat them as interchangeable."
)

_coverage = [
    f"Row counts: { {d: prof[d]['total'] for d in DATASETS} }.",
    (
        "These tables ARE the survivorship record: a unit in geloeschte_deaktivierte_einheiten "
        "(or an actor in geloeschte_deaktivierte_marktakteure) has left the current-state register. "
        "Any population built only from the current-state tables (01/03) is missing exactly this "
        "set. The rows-per-year trend is dominated by the most recent export year -- MaStR back-"
        "loads deregistration records, so the apparent surge is a registration-lag artefact, not a "
        "real spike in decommissioning."
    ),
]

_findings_md = "\n".join(f"- {ln}" for ln in findings_lines)

_silver = [
    (
        "- Append-only change-event logs -> model as a Silver change/event fact, never overwrite "
        "the current-state generation-unit / market-actor tables with them."
    ),
    (
        "- Parse each date column with the explicit format list; quarantine values that fail to "
        "parse or fall outside 2000..now (the figure already excludes them)."
    ),
]
if any(dup_counts.values()):
    _silver.append("- Exact duplicate rows exist -> de-duplicate on load.")
_silver.append(
    "- einheiten_aenderung_netzbetreiberzuordnungen carries BOTH a registration date and an "
    "effective date -> keep both; the effective date is the event time, the registration date "
    "is when it became knowable."
)

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                "One row per change EVENT, keyed by the referenced unit/actor MastrNummer -- NOT one row "
                "per entity. A unit can have several change rows; any per-entity feature must aggregate "
                "them, and a naive join to a current-state table drifts the grain to (unit, event)."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                "Joining these logs to the current-state unit/actor tables is 1:N per entity -> aggregate "
                "to the entity or keep as a separate fact; joining as 1:1 duplicates static attributes."
            ),
        ),
        (
            "Target contamination",
            (
                "These tables ARE the natural label source for a decommission / deactivation / operator-"
                "reassignment target. The event row (and everything dated on/after it) must be excluded "
                "from the feature set for predicting that same event."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "A change is only knowable from its registration date onward -- a forecasting feature may "
                "use only change rows whose registration date is strictly before the prediction cutoff."
            ),
        ),
        (
            "Proxy leakage",
            (
                "`DatumLetzteAktualisierung` and any 'reason' text describe the deregistration itself; "
                "using them to predict deregistration is circular."
            ),
        ),
        (
            "Split / entity leakage",
            (
                "Split by the referenced MastrNummer so an entity's full change history sits on one side; "
                "a row-level split leaks a unit's later events into training."
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "To build a correct as-of snapshot, replay these events forward from a base date -- do "
                "not use the current-state table joined to a past date."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                "See Coverage & Sampling Bias -- these rows are the churned population that the current-"
                "state tables omit; a negative class drawn only from current-state units is biased."
            ),
        ),
        (
            "Missingness leakage",
            (
                "A missing effective date vs a present one may itself signal the change type; check "
                "before adding an 'is-missing' feature."
            ),
        ),
        (
            "Duplicate-event leakage",
            (
                f"Full-row duplicates: {dict(dup_counts)} -- a duplicated deregistration row double-counts "
                "an event and can split across train/test."
            ),
        ),
        (
            "Target / feature temporal misalignment",
            (
                "Effective date != registration date. A target defined on the effective date paired with "
                "features cut at the registration date (or vice versa) misaligns label and features."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            "Not applicable -- no numeric measures in these tables.",
        ),
        (
            "Data-generation-process leakage",
            (
                "The back-loaded rows-per-year pattern is a property of MaStR's export process, not of "
                "the physical decommissioning rate -- a year feature would encode the export cadence."
            ),
        ),
        (
            "Class / label instability",
            (
                "The change-type / reason codes are MaStR enumerations that evolve between releases -- "
                "pin the catalog version (05)."
            ),
        ),
        (
            "Label availability lag",
            (
                "The gap between effective date and registration date is the label lag -- quantify it per "
                "table before choosing a prediction horizon; a same-day label is not available same-day."
            ),
        ),
        (
            "Source / version / regime change",
            (
                "Post-2019 MaStR deregistration workflow differs from the migrated legacy records; a "
                "pre/post-migration indicator is warranted."
            ),
        ),
        (
            "Sample-vs-full divergence",
            "Every statistic is a full Spark aggregation -- no `.sample()` / `.limit()`.",
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
        ("Entities / Keys", "\n".join(_entities)),
        ("Temporal Semantics", "\n".join(_temporal_md)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)