# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- MASTR EEG SUPPORT & AUTHORISATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the seven MaStR EEG-support and
# MAGIC authorisation-adjacent Bronze tables (anlagen_eeg_wind,
# MAGIC anlagen_eeg_biomasse, anlagen_eeg_wasser,
# MAGIC anlagen_eeg_geothermie_gsgk, anlagen_kwk, einheiten_genehmigung,
# MAGIC ertuechtigungen) -- schema, missingness, constant columns, exact key
# MAGIC cardinality (own key + the foreign key back to a generation unit),
# MAGIC full-row duplicates, tariff/date validation, categorical distributions,
# MAGIC and the layered modelling-risk checklist -- as evidence for Silver design.

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
NB_KEY = "02_eeg_support_and_authorisation"
SECTION_TITLE = (
    "EEG support & authorisation (anlagen_eeg_* / anlagen_kwk / "
    "einheiten_genehmigung / ertuechtigungen)"
)
DATASETS = [
    "anlagen_eeg_wind",
    "anlagen_eeg_biomasse",
    "anlagen_eeg_wasser",
    "anlagen_eeg_geothermie_gsgk",
    "anlagen_kwk",
    "einheiten_genehmigung",
    "ertuechtigungen",
]
TABLES = {d: f"{CATALOG}.{BRONZE_SCHEMA}.mastr_{d}" for d in DATASETS}

OWN_KEY_PREFERENCE = {
    "anlagen_eeg_wind": ("EegMaStRNummer",),
    "anlagen_eeg_biomasse": ("EegMaStRNummer",),
    "anlagen_eeg_wasser": ("EegMaStRNummer",),
    "anlagen_eeg_geothermie_gsgk": ("EegMaStRNummer",),
    "anlagen_kwk": ("KwkMastrNummer", "KwkMaStRNummer"),
    "einheiten_genehmigung": ("GenMastrNummer",),
    "ertuechtigungen": ("ErtuechtigungMastrNummer", "EegMastrNummer"),
}
AMOUNT_HINTS = (
    "anzulegenderwert",
    "zuschlag",
    "installierteleistung",
    "leistung",
    "wert",
)
DATE_HINTS = ("datum", "date")

# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Profile each table -- rows, missingness, approx distinct (one agg per table)
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
            f"  {c:<40} missing={miss[c]:>10} rate={rate:.4f} approx_distinct={acd[c]}"
        )

# COMMAND ----------

# DBTITLE 1,Confirm constant columns exactly
for name, df in frames.items():
    cands = [c for c in prof[name]["cols"] if prof[name]["acd"][c] <= 1]
    if cands:
        r = (
            df.agg(*[F.countDistinct(F.col(c)).alias(c) for c in cands])
            .first()
            .asDict()
        )
        prof[name]["constant"] = sorted(c for c in cands if (r[c] or 0) <= 1)
    else:
        prof[name]["constant"] = []
    print(f"{name}: constant columns (exact) = {prof[name]['constant']}")

# COMMAND ----------

# DBTITLE 1,Keys -- EXACT distinct; own key + foreign key back to a generation unit
key_report = {}
own_key = {}
fk_cols = {}
for name, df in frames.items():
    cands = key_like_cols(df.columns)
    uniq = exact_uniqueness(df, cands)
    k, _ = pick_entity_key(uniq, cands, prefer=OWN_KEY_PREFERENCE.get(name, ()))
    own_key[name] = k
    key_report[name] = uniq
    fk_cols[name] = [c for c in cands if c != k]
    print(f"{name}: own_key={k}  foreign_keys={fk_cols[name]}")
    for c, u in uniq.items():
        print(
            f"    {c:<32} distinct={u['distinct']:>10} ratio={u['ratio']} unique={u['unique']}"
        )

# COMMAND ----------

# DBTITLE 1,Full-row duplicates per table
dup_counts = {}
for name, df in frames.items():
    dup_counts[name] = prof[name]["total"] - df.distinct().count()
    print(f"{name}: exact full-row duplicates = {dup_counts[name]}")

# COMMAND ----------

# DBTITLE 1,Unit & semantic validation -- tariff/amount columns + scheme dates
sem = {}
for name, df in frames.items():
    cols = df.columns
    amt = next((c for c in cols if any(h in c.lower() for h in AMOUNT_HINTS)), None)
    dcols = [
        c
        for c in cols
        if any(h in c.lower() for h in DATE_HINTS) and not c.lower().endswith("_nv")
    ]
    entry = {}
    if amt:
        npar = numeric_parseability(df, amt)
        pl = plausibility(df, amt, lo=0.0, sentinels=())
        entry["amount"] = {"column": amt, "parse": npar, "plausibility": pl}
        print(
            f"{name}.{amt}: parse_yield={npar['yield']} range=({pl['min']},{pl['max']}) "
            f"negative={pl['negative']} zero={pl['zero']}"
        )
    entry["dates"] = {}
    for c in dcols[:4]:
        # Commissioning dates predate MaStR by decades (hydro from ~1900);
        # registration / last-update dates cannot predate the register (2019).
        vf = "1900-01-01" if "inbetriebnahme" in c.lower() else "2018-01-01"
        ts = timestamp_semantics(df, c, valid_from=vf, tz="Europe/Berlin")
        entry["dates"][c] = ts
        for ln in ts["lines"]:
            print(f"  {name}.{c}: {ln}")
    sem[name] = entry

# COMMAND ----------

# DBTITLE 1,Low-cardinality (categorical) column value counts
categorical_dist = {}
for name, df in frames.items():
    cat_cols = [c for c in prof[name]["cols"] if 1 < prof[name]["acd"][c] <= 50]
    dists = {}
    for c in cat_cols:
        vc = df.groupBy(c).count().orderBy(F.desc("count")).limit(50).collect()
        dists[c] = [(x[c], x["count"]) for x in vc]
    categorical_dist[name] = dists
    print(f"{name} categorical columns: {cat_cols}")

# COMMAND ----------

# DBTITLE 1,Coverage bias -- rows per table
row_counts = {d: prof[d]["total"] for d in DATASETS}
cov = coverage_bias(row_counts)
print("rows per table:", row_counts, "coverage bias:", cov)

# COMMAND ----------

# DBTITLE 1,Figure -- rows / columns / duplicates / exact own-key ratio
best_ratio = {
    d: (key_report[d][own_key[d]]["ratio"] if own_key[d] in key_report[d] else 0.0)
    for d in DATASETS
}
figs = []
if facet_bars(
    {
        "rows per table": [(d, prof[d]["total"]) for d in DATASETS],
        "columns per table": [(d, len(prof[d]["cols"])) for d in DATASETS],
        "full-row duplicates": [(d, dup_counts[d]) for d in DATASETS],
        "exact own-key uniqueness ratio": [(d, best_ratio[d]) for d in DATASETS],
    },
    "MaStR EEG support & authorisation -- overview",
    "mastr_eeg_support_overview.png",
    rot=30,
    ncols=2,
):
    figs.append(
        (
            "MaStR EEG support & authorisation -- overview",
            "mastr_eeg_support_overview.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Figure -- first categorical column per table
if facet_bars(
    {
        d: (next(iter(categorical_dist[d].values())) if categorical_dist[d] else [])
        for d in DATASETS
    },
    "MaStR EEG support & authorisation -- first categorical column per table",
    "mastr_eeg_support_categorical.png",
    rot=45,
    ncols=3,
):
    figs.append(
        (
            "MaStR EEG support & authorisation -- first categorical column per table",
            "mastr_eeg_support_categorical.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Findings
findings_lines = []
for d in DATASETS:
    findings_lines.append(
        f"{d}: rows={prof[d]['total']}, cols={len(prof[d]['cols'])}, "
        f"constant={prof[d]['constant']}, duplicates={dup_counts[d]}, "
        f"own_key={own_key[d]} (ratio {best_ratio[d]}), fk_cols={fk_cols[d]}"
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
    (
        "Exact key cardinality (column: distinct / ratio-to-rows / unique). Own key is near-unique; "
        "a lower-cardinality `*MastrNummer` is the foreign key back to the generation unit:"
    )
]
for d in DATASETS:
    _entities.append(f"- {d}: own_key=`{own_key[d]}`, foreign_key(s)={fk_cols[d]}")
    for c, u in key_report[d].items():
        _entities.append(
            f"  - `{c}`: {u['distinct']} / {u['ratio']} / unique={u['unique']}"
        )

_unit = []
for d in DATASETS:
    e = sem[d]
    if "amount" in e:
        a = e["amount"]
        _unit.append(
            f"- {d}.`{a['column']}`: parse yield {a['parse']['yield']:.1%}, "
            f"observed {a['plausibility']['min']}..{a['plausibility']['max']}, "
            f"negative={a['plausibility']['negative']}, zero={a['plausibility']['zero']} "
            "-- confirm the unit (ct/kWh vs EUR/MWh vs kW) against the source layout."
        )
    for c, ts in e.get("dates", {}).items():
        _unit.append(
            f"- {d}.`{c}` (date): parse yield {ts['yield']:.1%}, "
            f"range {ts['min_ts']}..{ts['max_ts']}, implausibly-early={ts['before_valid']}, "
            f"future-dated={ts['future']}, formats={ts['per_format']}."
        )
if not _unit:
    _unit.append("- No amount or date column located by name in these tables.")

_temporal = [
    (
        "Scheme / authorisation dates are Europe/Berlin wall-clock. `Registrierungsdatum` is when "
        "the record entered MaStR; the tariff/authorisation effective date is a separate column -- "
        "the effective date is the event, the registration date is when it became knowable."
    )
]
for d in DATASETS:
    for c, ts in sem[d].get("dates", {}).items():
        _temporal.append(
            f"- {d}.`{c}`: {ts['min_ts']} .. {ts['max_ts']} (future-dated: {ts['future']})."
        )

_ri = [
    (
        "- The 1:1 vs 1:N cardinality of each `anlagen_eeg_* / anlagen_kwk / einheiten_genehmigung / "
        "ertuechtigungen` record against its generation unit is confirmed in "
        "`06_mastr_relationships_and_findings.py` with a row-level fan-out probe; here only the own "
        "key and the FK column name are established."
    ),
    (
        "- `ertuechtigungen` (repowering/upgrade) is expected 1:N against a unit -- a unit can be "
        "upgraded more than once."
    ),
]

_coverage = [
    f"Rows per table: {row_counts}.",
    (
        f"Concentration: Gini {cov['gini']}, top-10% share {cov['top10pct_share']}. anlagen_kwk and "
        "anlagen_eeg_wind dominate; anlagen_eeg_geothermie_gsgk / ertuechtigungen are 10^2 rows. A "
        "support record exists only for units that entered a scheme -- units outside EEG/KWK have "
        "no row here, so an EEG-tariff feature is structurally missing for a non-random subset."
    ),
]

_dist = []
for d in DATASETS:
    for c, pairs in categorical_dist[d].items():
        _dist.append(f"- {d}.`{c}`: " + fmt_pairs(pairs, n=15))

_findings_md = "\n".join(f"- {ln}" for ln in findings_lines)

_silver = [
    (
        "- Own `*MastrNummer` is the Bronze->Silver grain key per table; the lower-cardinality "
        "`*MastrNummer` is the join back to the generation unit (verified in 06)."
    ),
    "- Constant columns carry no information and can be dropped at Silver.",
    (
        "- Cast amount columns with an explicit unit; quarantine values that fail numeric parsing "
        "or are negative."
    ),
]
if any(dup_counts.values()):
    _silver.append("- Exact duplicate rows exist -> de-duplicate on load.")
_silver.append(
    "- EEG-support / KWK-bonus / authorisation records are 1:1 or 1:N against a unit depending "
    "on scheme changes -> use the cardinality confirmed in 06, not an assumed 1:1 join."
)

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                "One row per support/authorisation record. Own key near-unique; the FK to the generation "
                "unit is lower-cardinality, so unit -> support is potentially 1:N (confirmed in 06). "
                "Joining to the unit table without checking drifts the grain."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                f"Foreign keys per table: {fk_cols}. A unit with multiple EEG/KWK/ertuechtigung records "
                "joined as 1:1 multiplies its attributes -- 06 measures the fan-out."
            ),
        ),
        (
            "Target contamination",
            (
                "`einheiten_genehmigung` (authorisation outcome) and `ertuechtigungen` (upgrade events) "
                "are plausible targets; the EEG tariff/support level may be a CONSEQUENCE of the "
                "authorisation, so it must not be a feature for predicting the authorisation."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "A tariff/authorisation attribute is only known from its effective date; the registration "
                "date is later still. Use only records dated strictly before the prediction cutoff."
            ),
        ),
        (
            "Proxy leakage",
            (
                "The EEG scheme number itself, and the amount column, can uniquely identify a unit's "
                "support decision -- a proxy for the very outcome being modelled."
            ),
        ),
        (
            "Split / entity leakage",
            (
                "Split by the generation-unit MastrNummer (root of the join chain), not by row in these "
                "tables, so a unit's support and authorisation records stay together."
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "Tariff levels change with scheme vintage; a record superseded by a later one must not be "
                "used as the unit's tariff at an earlier date."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                "A support record exists only for scheme participants -- see Coverage & Sampling Bias; "
                "absence of a row is informative, not random."
            ),
        ),
        (
            "Missingness leakage",
            (
                "Whether an amount/date field is populated correlates with scheme, vintage and carrier -- "
                "check before adding an 'is-missing' feature."
            ),
        ),
        (
            "Duplicate-event leakage",
            f"Full-row duplicates: {dict(dup_counts)} -- de-duplicate before counting support records.",
        ),
        (
            "Target / feature temporal misalignment",
            (
                "Effective date, registration date and (for authorisation) decision date are distinct -- "
                "align target and features to one as-of date."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            (
                "Amount-column unit unconfirmed; `InstallierteLeistung` here overlaps the capacity column "
                "in the unit table -- using both is double-counting."
            ),
        ),
        (
            "Data-generation-process leakage",
            (
                "`Registrierungsdatum` and any MaStR status columns describe record handling, not the "
                "physical scheme -- they can leak the timing of the label."
            ),
        ),
        (
            "Class / label instability",
            "Support-scheme category codes change with each EEG amendment; pin the catalog release (05).",
        ),
        (
            "Label availability lag",
            (
                "Authorisation and tariff decisions are registered after they are made -- the "
                "effective-to-registration gap is the label lag."
            ),
        ),
        (
            "Source / version / regime change",
            (
                "EEG 2000/2004/2009/2012/2014/2017/2021/2023 each changed the support mechanism (fixed "
                "feed-in -> auction) -- a scheme-vintage indicator is essential before pooling records."
            ),
        ),
        (
            "Sample-vs-full divergence",
            "Every statistic is a full Spark aggregation or `.distinct().count()` -- no sampling.",
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
        ("Unit & Semantic Validation", "\n".join(_unit)),
        ("Temporal Semantics", "\n".join(_temporal)),
        ("Referential Integrity", "\n".join(_ri)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)