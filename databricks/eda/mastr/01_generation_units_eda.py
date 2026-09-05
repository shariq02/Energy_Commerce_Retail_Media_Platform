# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- MASTR GENERATION UNITS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the six MaStR generation-unit Bronze tables
# MAGIC (einheiten_wind, einheiten_biomasse, einheiten_wasser,
# MAGIC einheiten_verbrennung, einheiten_kernkraft,
# MAGIC einheiten_geothermie_gsgk) -- schema, missingness, constant columns,
# MAGIC exact primary-key cardinality, full-row duplicates, capacity/coordinate/
# MAGIC commissioning-date validation, categorical distributions, and the
# MAGIC layered modelling-risk checklist -- as evidence for Silver design.

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
NB_KEY = "01_generation_units"
SECTION_TITLE = "Generation units (einheiten_wind ... einheiten_geothermie_gsgk)"
DATASETS = [
    "einheiten_wind",
    "einheiten_biomasse",
    "einheiten_wasser",
    "einheiten_verbrennung",
    "einheiten_kernkraft",
    "einheiten_geothermie_gsgk",
]
TABLES = {d: f"{CATALOG}.{BRONZE_SCHEMA}.mastr_{d}" for d in DATASETS}

# The own-entity key for a generation unit is always EinheitMastrNummer; the
# other *MastrNummer columns are foreign keys (operator, location, EEG, KWK,
# authorisation). Listed as the tie-break preference for pick_entity_key --
# selection is still driven by the exact distinct/row ratio.
OWN_KEY_PREFERENCE = ("EinheitMastrNummer", "MastrNummer")

# Substrings used to locate the semantic columns dynamically -- the exact
# German field name differs slightly between carrier tables.
CAPACITY_HINTS = ("nettonennleistung", "bruttoleistung", "nennleistung")
LAT_HINTS = ("breitengrad", "breite")
LON_HINTS = ("laengengrad", "laenge", "längengrad")
COMMISSION_HINTS = ("inbetriebnahmedatum",)
STATUS_HINTS = ("einheitbetriebsstatus", "betriebsstatus")

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
    # approx_count_distinct <= 1 can misfire near the boundary; confirm constant
    # columns exactly below before reporting them.
    constant_candidates = [c for c in cols if acd[c] <= 1]
    prof[name] = {
        "cols": cols,
        "total": total,
        "acd": acd,
        "miss": miss,
        "constant_candidates": constant_candidates,
    }
    print("=" * 90, f"\n{name}  rows={total}  cols={len(cols)}")
    for c in cols:
        rate = miss[c] / total if total else 0
        print(
            f"  {c:<40} missing={miss[c]:>10} rate={rate:.4f} approx_distinct={acd[c]}"
        )

# COMMAND ----------

# DBTITLE 1,Confirm constant columns exactly (approx_count_distinct is only a screen)
constant_cols = {}
for name, df in frames.items():
    cands = prof[name]["constant_candidates"]
    if not cands:
        constant_cols[name] = []
        continue
    r = df.agg(*[F.countDistinct(F.col(c)).alias(c) for c in cands]).first().asDict()
    constant_cols[name] = sorted(c for c in cands if (r[c] or 0) <= 1)
    prof[name]["constant"] = constant_cols[name]
    print(f"{name}: constant columns (exact) = {constant_cols[name]}")
for name in DATASETS:
    prof[name].setdefault("constant", constant_cols.get(name, []))

# COMMAND ----------

# DBTITLE 1,Primary-key detection -- EXACT distinct count, not the HLL estimate
key_report = {}
own_key = {}
for name, df in frames.items():
    cands = key_like_cols(prof[name]["cols"])
    uniq = exact_uniqueness(df, cands)
    k, info = pick_entity_key(uniq, cands, prefer=OWN_KEY_PREFERENCE)
    own_key[name] = k
    key_report[name] = uniq
    print(f"{name}: own_key={k}  {info}")
    for c, u in uniq.items():
        print(
            f"    {c:<34} distinct={u['distinct']:>10} ratio={u['ratio']} unique={u['unique']}"
        )

# COMMAND ----------

# DBTITLE 1,Full-row duplicates per table
dup_counts = {}
for name, df in frames.items():
    total = prof[name]["total"]
    dup_counts[name] = total - df.distinct().count()
    print(f"{name}: exact full-row duplicates = {dup_counts[name]}")

# COMMAND ----------

# DBTITLE 1,Unit & semantic validation -- capacity, coordinates, commissioning date
sem = {}
for name, df in frames.items():
    cols = df.columns
    cap_col = next(
        (c for c in cols if any(h in c.lower() for h in CAPACITY_HINTS)), None
    )
    lat_col = next((c for c in cols if any(h in c.lower() for h in LAT_HINTS)), None)
    lon_col = next((c for c in cols if any(h in c.lower() for h in LON_HINTS)), None)
    com_col = next(
        (c for c in cols if any(h in c.lower() for h in COMMISSION_HINTS)), None
    )
    entry = {}
    if cap_col:
        npar = numeric_parseability(df, cap_col)
        # MaStR capacity is kW; 0 is implausible for a registered unit, and a
        # single unit above ~2 GW would be a national-scale outlier.
        # Filter to rows where capacity can be safely cast (some rows have text like turbine types)
        # Use regexp to identify numeric-like values (digits, commas, dots, minus)
        df_numeric = df.filter(
            F.col(cap_col).isNull()
            | (F.trim(F.col(cap_col).cast("string")) == "")
            | F.col(cap_col).cast("string").rlike(r"^[0-9.,\-]+$")
        )
        pl = plausibility(df_numeric, cap_col, lo=0.0, hi=2_000_000.0, sentinels=())
        entry["capacity"] = {"column": cap_col, "parse": npar, "plausibility": pl}
        print(
            f"{name}.{cap_col}: parse_yield={npar['yield']} range=({pl['min']}, {pl['max']}) "
            f"zero={pl['zero']} negative={pl['negative']} above_2GW={pl.get('above')}"
        )
    if lat_col and lon_col:
        sp = spatial_validity(df, lat_col, lon_col, name=f"{name}.{lat_col}/{lon_col}")
        entry["spatial"] = sp
        print(
            f"{name} coords {lat_col}/{lon_col}: present={sp['present']} missing={sp['missing']} "
            f"outside_DE={sp['outside_bbox']} null_island={sp['null_island']} "
            f"looks_swapped={sp['looks_swapped']}"
        )
    if com_col:
        ts = timestamp_semantics(
            df, com_col, valid_from="1900-01-01", tz="Europe/Berlin"
        )
        entry["commission"] = {
            "column": com_col,
            **{
                k: ts[k]
                for k in (
                    "yield",
                    "min_ts",
                    "max_ts",
                    "before_valid",
                    "future",
                    "per_format",
                )
            },
        }
        for ln in ts["lines"]:
            print(f"  {name}: {ln}")
    sem[name] = entry

# COMMAND ----------

# DBTITLE 1,Operating-status distribution -- survivorship read
status_dist = {}
for name, df in frames.items():
    sc = next(
        (c for c in df.columns if any(h in c.lower() for h in STATUS_HINTS)), None
    )
    if not sc:
        continue
    vc = df.groupBy(sc).count().orderBy(F.desc("count")).limit(20).collect()
    status_dist[name] = {"column": sc, "values": [(x[sc], x["count"]) for x in vc]}
    print(f"{name}.{sc}: {status_dist[name]['values']}")

# COMMAND ----------

# DBTITLE 1,Low-cardinality (categorical) column value counts (one groupBy per flagged column)
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

# DBTITLE 1,Coverage bias -- rows per carrier table
carrier_counts = {d: prof[d]["total"] for d in DATASETS}
cov = coverage_bias(carrier_counts)
print("rows per carrier table:", carrier_counts)
print("coverage bias:", cov)

# COMMAND ----------

# DBTITLE 1,Figure -- rows / columns / duplicates / exact PK ratio per table
best_ratio = {
    d: max((u["ratio"] for u in key_report[d].values()), default=0.0) for d in DATASETS
}
figs = []
if facet_bars(
    {
        "rows per table": [(d, prof[d]["total"]) for d in DATASETS],
        "columns per table": [(d, len(prof[d]["cols"])) for d in DATASETS],
        "full-row duplicates": [(d, dup_counts[d]) for d in DATASETS],
        "exact PK uniqueness ratio (best *MastrNummer col)": [
            (d, best_ratio[d]) for d in DATASETS
        ],
    },
    "MaStR generation units -- overview",
    "mastr_generation_units_overview.png",
    rot=30,
    ncols=2,
):
    figs.append(
        ("MaStR generation units -- overview", "mastr_generation_units_overview.png")
    )

# COMMAND ----------

# DBTITLE 1,Figure -- first categorical column per table
if facet_bars(
    {
        d: (next(iter(categorical_dist[d].values())) if categorical_dist[d] else [])
        for d in DATASETS
    },
    "MaStR generation units -- first categorical column per table",
    "mastr_generation_units_categorical.png",
    rot=45,
    ncols=3,
):
    figs.append(
        (
            "MaStR generation units -- first categorical column per table",
            "mastr_generation_units_categorical.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Findings
findings_lines = []
for d in DATASETS:
    findings_lines.append(
        f"{d}: rows={prof[d]['total']}, cols={len(prof[d]['cols'])}, "
        f"constant={prof[d].get('constant', [])}, duplicates={dup_counts[d]}, "
        f"own_key={own_key[d]} (ratio {best_ratio[d]})"
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
    "Exact `*MastrNummer` key cardinality (column: distinct / ratio-to-rows / unique):"
]
for d in DATASETS:
    _entities.append(f"- {d}: own_key = `{own_key[d]}`")
    for c, u in key_report[d].items():
        _entities.append(
            f"  - `{c}`: {u['distinct']} / {u['ratio']} / unique={u['unique']}"
            + (f" (nulls={u['nulls']})" if u["nulls"] else "")
        )

_unit = []
for d in DATASETS:
    e = sem[d]
    if "capacity" in e:
        cap, npar, pl = (
            e["capacity"]["column"],
            e["capacity"]["parse"],
            e["capacity"]["plausibility"],
        )
        _unit.append(
            f"- {d}.`{cap}` (capacity, kW): parse yield {npar['yield']:.1%}, "
            f"observed {pl['min']}..{pl['max']}, zero={pl['zero']}, negative={pl['negative']}, "
            f">2 GW={pl.get('above')} -- confirm the unit (kW vs MW) against the source layout."
        )
    if "spatial" in e:
        sp = e["spatial"]
        _unit.append(
            f"- {d} coordinates: present={sp['present']}, missing={sp['missing']}, "
            f"outside the Germany bounding box={sp['outside_bbox']}, (0,0)={sp['null_island']}, "
            f"lat/lon possibly swapped={sp['looks_swapped']}."
        )
    if "commission" in e:
        cm = e["commission"]
        _unit.append(
            f"- {d}.`{cm['column']}` (commissioning date): parse yield {cm['yield']:.1%}, "
            f"range {cm['min_ts']}..{cm['max_ts']}, pre-1900={cm['before_valid']}, "
            f"future-dated={cm['future']}, formats={cm['per_format']}."
        )
if not _unit:
    _unit.append(
        "- No capacity / coordinate / commissioning-date column was located by name."
    )

_temporal = []
for d in DATASETS:
    if "commission" in sem[d]:
        cm = sem[d]["commission"]
        _temporal.append(
            f"- {d}.`{cm['column']}`: {cm['min_ts']} .. {cm['max_ts']} "
            f"(Europe/Berlin wall-clock in source; convert to UTC on a documented rule). "
            f"Future-dated rows: {cm['future']} -- planned commissioning dates are recorded "
            "ahead of time, so this column is only 'known' up to its own value."
        )
if not _temporal:
    _temporal.append(
        "- No commissioning-date column located; temporal semantics not assessed here."
    )

_coverage = [
    f"Rows per carrier table: {carrier_counts}.",
    (
        f"Concentration (Gini {cov['gini']}, top-10% share {cov['top10pct_share']}, "
        f"max/min ratio {cov['max_min_ratio']}) -- einheiten_verbrennung and einheiten_wind "
        "dominate; einheiten_kernkraft has a handful of rows."
    ),
]
for d, sd in status_dist.items():
    _coverage.append(f"Operating status {d}.`{sd['column']}`: {sd['values'][:8]}")
_coverage.append(
    "This table is a current-state snapshot: a unit decommissioned before the export date "
    "may be absent entirely (it moves to geloeschte_deaktivierte_einheiten). Any population "
    "built only from this table is survivor-biased toward still-registered units."
)

_ri = [
    (
        "- Cross-carrier and generation-unit <-> support/authorisation/change referential integrity "
        "is assessed in `06_mastr_relationships_and_findings.py` against an explicit join spec; this "
        "notebook only establishes each table's own-entity key."
    )
]

_dist = []
for d in DATASETS:
    for c, pairs in categorical_dist[d].items():
        _dist.append(f"- {d}.`{c}`: " + fmt_pairs(pairs, n=15))

_findings_md = "\n".join(f"- {ln}" for ln in findings_lines)

_no_exact_pk = [
    d for d in DATASETS if own_key[d] and not key_report[d][own_key[d]]["unique"]
]

_silver = [
    (
        "- `EinheitMastrNummer` is the Bronze->Silver grain key for every carrier table "
        f"(exact ratios above; not unique in: {_no_exact_pk or 'none'} -> investigate duplicate "
        "unit numbers there before treating it as a primary key)."
    ),
    "- Constant columns above carry no information and can be dropped at Silver.",
]
if any(dup_counts.values()):
    _silver.append(
        "- Exact duplicate rows exist in at least one table -> de-duplicate on load."
    )
_silver += [
    (
        "- Capacity columns are kW in MaStR -- cast with an explicit unit and quarantine values "
        "that fail numeric parsing or fall outside a plausible range."
    ),
    (
        "- Coordinates that fall outside Germany / sit at (0,0) are a quarantine class, not a "
        "silent NULL."
    ),
    (
        "- Generation-unit tables are one physical unit per row; a unit appears in >1 carrier "
        "table only on a carrier-type change -> reconcile at Gold, confirmed in 06, not assumed absent."
    ),
]

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                f"One row per generation unit, keyed by `EinheitMastrNummer` (exact ratios above). "
                f"Not unique in: {_no_exact_pk or 'none'} -- any grain assumption must be re-checked "
                "there. Downstream joins to EEG-support / KWK / change-history can drift the grain to "
                "one-row-per-(unit, scheme) or one-row-per-(unit, event)."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                "The foreign-key `*MastrNummer` columns (operator, location, EEG, KWK) are lower-"
                "cardinality than the row count -> joining a child table on them without confirming "
                "1:1 fans out unit attributes. Cardinality is confirmed in 06."
            ),
        ),
        (
            "Target contamination",
            (
                "No target in this table. If a decommission/status target is built from "
                "`EinheitBetriebsstatus` (or the change-history tables), the current-state attributes "
                "here reflect the unit's state AT EXPORT, which is after many candidate decommission "
                "dates -- a point-in-time reconstruction is required."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "Commissioning date is the only time column; a unit's row is only valid from its "
                "commissioning date onward, and planned future dates exist (see Temporal Semantics)."
            ),
        ),
        (
            "Proxy leakage",
            (
                "`Kraftwerksnummer` / `Weic` / operator identity can act as near-unique proxies for a "
                "specific unit and leak its outcome into a supposedly generalising model."
            ),
        ),
        (
            "Split / entity leakage",
            (
                "Split by `EinheitMastrNummer` (or by operator `AnlagenbetreiberMastrNummer` when "
                "modelling operator behaviour) -- a row-level split leaks nothing here because attributes "
                "are static, but it also provides no independence and hides duplicate-row leakage."
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "This is a single current-state snapshot with no validity-period columns -- there is no "
                "way to reconstruct a unit's attributes at an earlier date from this table alone; a "
                "time-varying feature must come from the change-history tables, not from here."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                f"Current-state only: units decommissioned/deleted before the export are not present "
                f"(see Coverage & Sampling Bias). Rows per carrier: {carrier_counts}."
            ),
        ),
        (
            "Missingness leakage",
            (
                "Several columns are constant/blank per carrier (see Profile). Whether a field is "
                "populated can itself correlate with carrier type, registration era, or operator -- "
                "an 'is-missing' feature must be checked for this before use."
            ),
        ),
        (
            "Duplicate-event leakage",
            (
                f"Full-row duplicates per table: {dict(dup_counts)} -- de-duplicate before counting "
                "units, or a duplicated unit lands on both sides of a split."
            ),
        ),
        (
            "Target / feature temporal misalignment",
            (
                "Not applicable within this table (no target, one time column); becomes relevant when "
                "joined to dated support/change records."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            (
                "Capacity unit (kW vs MW) is unconfirmed -- see Unit & Semantic Validation. Net vs "
                "gross capacity columns are near-collinear; using both as independent features is "
                "double-counting."
            ),
        ),
        (
            "Data-generation-process leakage",
            (
                "`NetzbetreiberpruefungStatus` and `EinheitSystemstatus` describe MaStR's own "
                "registration/validation workflow, not the physical unit -- they encode how/when the "
                "record was processed and can leak label timing."
            ),
        ),
        (
            "Class / label instability",
            (
                "Categorical code columns (e.g. status, technology) are MaStR enumerations that change "
                "between register releases -- pin the catalog version (see 05) before treating a code as "
                "a stable class."
            ),
        ),
        (
            "Label availability lag",
            (
                "A decommission is registered in MaStR after it happens, sometimes with a long lag -- "
                "the change-history tables (04) carry the registration date; do not assume a status is "
                "known at the physical event time."
            ),
        ),
        (
            "Source / version / regime change",
            (
                "MaStR replaced the older EEG/Anlagenregister in 2019; pre-2019 units were bulk-migrated "
                "and their attribute completeness differs from natively-registered units -- a "
                "registration-era indicator is warranted."
            ),
        ),
        (
            "Sample-vs-full divergence",
            (
                "Every statistic here is a full Spark aggregation or `.distinct().count()` / "
                "`countDistinct` -- no `.sample()` / `.limit()` feeds a reported number."
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
