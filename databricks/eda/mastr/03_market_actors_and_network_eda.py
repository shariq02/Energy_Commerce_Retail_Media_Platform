# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- MASTR MARKET ACTORS & NETWORK
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the six MaStR market-actor and network Bronze
# MAGIC tables (marktakteure, marktakteure_und_rollen, netzanschlusspunkte,
# MAGIC netze, lokationen, bilanzierungsgebiete) -- schema, missingness, exact
# MAGIC key cardinality, full-row duplicates, the lokationen delimited
# MAGIC MaStR-Nummer link arrays, a structural column-alignment check on the
# MAGIC wide marktakteure export, categorical distributions, and the layered
# MAGIC modelling-risk checklist -- as evidence for Silver design.

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
NB_KEY = "03_market_actors_and_network"
SECTION_TITLE = (
    "Market actors & network (marktakteure* / netzanschlusspunkte / netze / "
    "lokationen / bilanzierungsgebiete)"
)
DATASETS = [
    "marktakteure",
    "marktakteure_und_rollen",
    "netzanschlusspunkte",
    "netze",
    "lokationen",
    "bilanzierungsgebiete",
]
TABLES = {d: f"{CATALOG}.{BRONZE_SCHEMA}.mastr_{d}" for d in DATASETS}

OWN_KEY_PREFERENCE = {
    "marktakteure": ("MastrNummer",),
    "marktakteure_und_rollen": ("MastrNummer",),
    "netzanschlusspunkte": ("NetzanschlusspunktMastrNummer",),
    "netze": ("MastrNummer",),
    "lokationen": ("MastrNummer",),
}
# marktakteure free-text vs code columns: a name column that reads as mostly
# numeric, or a salutation column carrying long free text, means the CSV parse
# shifted fields (embedded delimiter / unquoted newline in the wide export).
NAME_COL_HINTS = ("nachname", "firmenname", "name")
CODE_COL_HINTS = ("personenart", "marktfunktion", "anrede", "titel")

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

# DBTITLE 1,Keys -- EXACT distinct count
key_report = {}
own_key = {}
for name, df in frames.items():
    cands = key_like_cols(df.columns)
    uniq = exact_uniqueness(df, cands)
    k, _ = pick_entity_key(uniq, cands, prefer=OWN_KEY_PREFERENCE.get(name, ()))
    own_key[name] = k
    key_report[name] = uniq
    print(f"{name}: own_key={k}")
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

# DBTITLE 1,Structural column-alignment check on marktakteure (wide export, shift-prone)
ma = frames["marktakteure"]
name_cols = [c for c in ma.columns if any(h in c.lower() for h in NAME_COL_HINTS)]
code_cols = [c for c in ma.columns if any(h in c.lower() for h in CODE_COL_HINTS)]
shift_report = {"name_cols": {}, "code_cols": {}}
for c in name_cols:
    npar = numeric_parseability(ma, c, decimal_comma=False)
    shift_report["name_cols"][c] = {
        "numeric_yield": npar["yield"],
        "non_null": npar["non_null"],
        "suspect": npar["non_null"] > 0 and npar["yield"] > 0.3,
    }
    print(f"marktakteure.{c}: numeric_yield={npar['yield']} (name column should be ~0)")
for c in code_cols:
    ln = ma.agg(
        F.max(F.length(F.col(c).cast("string"))).alias("mx"),
        F.avg(F.length(F.col(c).cast("string"))).alias("av"),
    ).first()
    shift_report["code_cols"][c] = {
        "max_len": int(ln["mx"]) if ln["mx"] else 0,
        "avg_len": round(ln["av"], 2) if ln["av"] else 0,
        "suspect": bool(ln["mx"] and ln["mx"] > 40),
    }
    print(f"marktakteure.{c}: max_len={ln['mx']} (code column should be short)")
shift_suspected = any(v["suspect"] for v in shift_report["name_cols"].values()) or any(
    v["suspect"] for v in shift_report["code_cols"].values()
)
print("marktakteure column-shift suspected:", shift_suspected)

# COMMAND ----------

# DBTITLE 1,lokationen link-array columns (delimited MaStR-Nummer lists, not a join table)
lok = frames.get("lokationen")
lok_link_cols = (
    [c for c in lok.columns if "mastrnummern" in c.lower()] if lok is not None else []
)
lok_link_stats = {}
if lok is not None and lok_link_cols:
    exprs = []
    for c in lok_link_cols:
        non_empty = F.col(c).isNotNull() & (F.trim(F.col(c)) != "")
        n_items = F.size(F.split(F.col(c), r"[,; ]+"))
        exprs += [
            F.sum(non_empty.cast("long")).alias(c + "__present"),
            F.max(F.when(non_empty, n_items)).alias(c + "__maxlen"),
            F.avg(F.when(non_empty, n_items)).alias(c + "__avglen"),
        ]
    r = lok.agg(*exprs).first().asDict()
    for c in lok_link_cols:
        lok_link_stats[c] = {
            "present": r[c + "__present"],
            "max_list_len": int(r[c + "__maxlen"]) if r[c + "__maxlen"] else 0,
            "avg_list_len": round(r[c + "__avglen"], 2) if r[c + "__avglen"] else 0,
        }
    print("lokationen link-array stats:", lok_link_stats)

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
    "MaStR market actors & network -- overview",
    "mastr_market_network_overview.png",
    rot=30,
    ncols=2,
):
    figs.append(
        (
            "MaStR market actors & network -- overview",
            "mastr_market_network_overview.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Figure -- lokationen link-array presence + first categorical column per table
if lok_link_stats and barplot(
    [(c, v["present"]) for c, v in sorted(lok_link_stats.items())],
    "MaStR lokationen -- rows with a non-empty MastrNummer link array",
    "column",
    "rows",
    rot=30,
    filename="mastr_lokationen_link_arrays.png",
):
    figs.append(
        (
            "MaStR lokationen -- rows with a non-empty MastrNummer link array",
            "mastr_lokationen_link_arrays.png",
        )
    )
if facet_bars(
    {
        d: (next(iter(categorical_dist[d].values())) if categorical_dist[d] else [])
        for d in DATASETS
    },
    "MaStR market actors & network -- first categorical column per table",
    "mastr_market_network_categorical.png",
    rot=45,
    ncols=3,
):
    figs.append(
        (
            "MaStR market actors & network -- first categorical column per table",
            "mastr_market_network_categorical.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Findings
findings_lines = []
for d in DATASETS:
    findings_lines.append(
        f"{d}: rows={prof[d]['total']}, cols={len(prof[d]['cols'])}, "
        f"constant={prof[d]['constant']}, duplicates={dup_counts[d]}, "
        f"own_key={own_key[d]} (ratio {best_ratio[d]})"
    )
print("\n".join(findings_lines))
print("column-shift suspected on marktakteure:", shift_suspected)

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

_struct = [
    (
        "Structural column-alignment check on `marktakteure` (5-6M rows, 60+ columns -- the wide "
        "export is prone to field shift from an unquoted delimiter or embedded newline):"
    ),
]
for c, v in shift_report["name_cols"].items():
    _struct.append(
        f"- name column `{c}`: {v['numeric_yield']:.1%} of non-null values parse as a number "
        f"(a real surname column is ~0%) -> {'MISALIGNED' if v['suspect'] else 'ok'}."
    )
for c, v in shift_report["code_cols"].items():
    _struct.append(
        f"- code column `{c}`: max value length {v['max_len']} chars "
        f"(a code/salutation is short) -> {'MISALIGNED' if v['suspect'] else 'ok'}."
    )
if shift_suspected:
    _struct.append(
        "-> This is an INGESTION defect, not a source-data finding: values have shifted across "
        "columns. Fix in `databricks/bronze/06_load_mastr_bronze.py` (multiLine + explicit "
        "quote/escape for marktakteure), re-stage the raw export and re-run Bronze before "
        "trusting any marktakteure column."
    )
else:
    _struct.append("-> No column shift detected in the current Bronze table.")

_entities = [
    "Exact `*MastrNummer` key cardinality (column: distinct / ratio-to-rows / unique):"
]
for d in DATASETS:
    _entities.append(f"- {d}: own_key=`{own_key[d]}`")
    for c, u in key_report[d].items():
        _entities.append(
            f"  - `{c}`: {u['distinct']} / {u['ratio']} / unique={u['unique']}"
        )

_rel = [
    (
        "lokationen references related Einheiten / Netzanschlusspunkte as delimited MaStR-Nummer "
        "list columns, not a normalised join table:"
    ),
    f"- link-array columns: {lok_link_cols}",
]
for c, v in lok_link_stats.items():
    _rel.append(
        f"- `{c}`: {v['present']} non-empty rows; list length up to {v['max_list_len']} "
        f"(avg {v['avg_list_len']}) -> exploding this is a M:N bridge."
    )
_rel.append(
    "netzanschlusspunkte -> lokationen / netze / marktakteure and marktakteure_und_rollen -> "
    "marktakteure are checked with orphan + fan-out probes in 06."
)

_coverage = [
    f"Rows per table: {row_counts}.",
    (
        "marktakteure / netzanschlusspunkte / lokationen are 5-7M rows; netze / bilanzierungsgebiete "
        "are ~10^3. Most market actors and locations are never referenced by a generation unit -- a "
        "unit-rooted model touches a small, non-random slice, and joining the full actor table adds "
        "mostly-unused rows."
    ),
]

_dist = []
for d in DATASETS:
    for c, pairs in categorical_dist[d].items():
        _dist.append(f"- {d}.`{c}`: " + fmt_pairs(pairs, n=15))

_findings_md = "\n".join(f"- {ln}" for ln in findings_lines)

_silver = [
    "- Each table's near-unique `*MastrNummer` is the Bronze->Silver grain key.",
    "- Constant columns carry no information and can be dropped at Silver.",
]
if any(dup_counts.values()):
    _silver.append("- Exact duplicate rows exist -> de-duplicate on load.")
if shift_suspected:
    _silver.append(
        "- BLOCKED: marktakteure columns are misaligned in Bronze -> fix the loader and re-run "
        "before modelling any actor attribute."
    )
if lok_link_cols:
    _silver.append(
        "- lokationen's delimited MaStR-Nummer link columns must be split/exploded into a "
        "proper M:N bridge table at Silver before any join to Einheiten or Netzanschlusspunkte."
    )
_silver.append(
    "- marktakteure and marktakteure_und_rollen are a candidate 1:N role-assignment split -- "
    "confirm on the shared MastrNummer key (06) before merging."
)

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                "marktakteure / netze / lokationen are one row per entity; marktakteure_und_rollen is "
                "one row per (actor, role) and drifts the grain; lokationen link arrays explode to M:N."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                f"lokationen link lists reach length {max((v['max_list_len'] for v in lok_link_stats.values()), default=0)} "
                "-> a bridge explode multiplies rows; verify exploded vs pre-explode counts. 06 measures "
                "the actor/location fan-out into netzanschlusspunkte."
            ),
        ),
        (
            "Target contamination",
            (
                "No target in these dimension tables; if a role-classification target is built from "
                "marktakteure_und_rollen, actor attributes assigned because of the role must be excluded."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "These are current-state dimensions with no validity windows -- an attribute joined to a "
                "dated fact is assumed constant over all time, which is not verifiable here."
            ),
        ),
        (
            "Proxy leakage",
            (
                "Actor name / Betriebsnummer / grid-operator id are near-unique and can memorise a "
                "specific actor's outcome."
            ),
        ),
        (
            "Split / entity leakage",
            (
                "Split by actor or by grid operator (whichever is the modelled entity), not by row in a "
                "joined child table."
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "No point-in-time reconstruction possible from these snapshots; a time-varying operator "
                "assignment must come from einheiten_aenderung_netzbetreiberzuordnungen (04), not here."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                "Deregistered actors move to geloeschte_deaktivierte_marktakteure (04); this table is "
                "current actors only."
            ),
        ),
        (
            "Missingness leakage",
            (
                "Whether address / name / Betriebsnummer fields are populated correlates with actor type "
                "(natural person vs company) and registration era."
            ),
        ),
        (
            "Duplicate-event leakage",
            (
                f"Full-row duplicates: {dict(dup_counts)} -- marktakteure_und_rollen carries the most; "
                "de-duplicate before treating a role assignment as one observation."
            ),
        ),
        (
            "Target / feature temporal misalignment",
            "Not applicable within these tables (no dates); relevant once joined to dated facts.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            "Not applicable -- no numeric measures.",
        ),
        (
            "Data-generation-process leakage",
            (
                "If marktakteure columns are misaligned (Structural Integrity above), EVERY downstream "
                "feature from this table is corrupted by the parse, not by reality."
            ),
        ),
        (
            "Class / label instability",
            "Marktrolle / Marktfunktion code sets change with regulation; pin the catalog version (05).",
        ),
        ("Label availability lag", "Not applicable in this notebook (no event dates)."),
        (
            "Source / version / regime change",
            "Actor records migrated in 2019 differ in completeness from natively-registered ones.",
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
        ("Structural Integrity", "\n".join(_struct)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Relationships", "\n".join(_rel)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)