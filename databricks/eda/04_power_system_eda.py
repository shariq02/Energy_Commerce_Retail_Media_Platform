# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- POWER SYSTEM (MASTR, REDISPATCH, POWER PLANT LISTS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** exploratory analysis of the power system data in one notebook. Each section profiles one group of
# MAGIC tables and writes its findings to `src/schemas/profiling/<source>.md` (one section key per group).

# COMMAND ----------

# MAGIC %run ./_eda_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- MASTR GENERATION UNITS
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
frames = {d: bronze_frame(t) for d, t in TABLES.items()}
prof = profile_each_frame(frames)

# COMMAND ----------

# DBTITLE 1,Confirm constant columns exactly (approx_count_distinct is only a screen)
constant_cols = {}
for name, df in frames.items():
    constant_cols[name] = constant_column_names(df, prof[name])
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
    dup_counts[name] = full_row_dup_count(df, total)
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
categorical_dist = categorical_distributions(frames, prof)

# COMMAND ----------

# DBTITLE 1,Coverage bias -- rows per carrier table
carrier_counts = {d: prof[d]["total"] for d in DATASETS}
cov = coverage_bias(carrier_counts)
print("rows per carrier table:", carrier_counts)
print("coverage bias:", cov)

# COMMAND ----------

# DBTITLE 1,Cross-table spatial consistency -- internal geographic agreement (no external shapefile)
# Only the einheiten_* tables carry coordinates; lokationen / netzanschlusspunkte
# carry none, so there is no second table to reconcile lat/lon against. What IS
# checkable without external data: a unit's own geographic attributes must agree
# with each other, and units sharing a location must sit in the same place.
REGION_HINTS = ("bundesland",)
AGS_HINTS = ("gemeindeschluessel", "gemeindeschl")
PLZ_HINTS = ("postleitzahl", "plz")
LOK_HINTS = ("lokationmastrnummer",)
spatial_consistency = {}
for name, df in frames.items():
    cols = df.columns
    reg = next((c for c in cols if any(h in c.lower() for h in REGION_HINTS)), None)
    ags = next((c for c in cols if any(h in c.lower() for h in AGS_HINTS)), None)
    plz = next((c for c in cols if any(h in c.lower() for h in PLZ_HINTS)), None)
    lok = next((c for c in cols if any(h in c.lower() for h in LOK_HINTS)), None)
    lat_col = next((c for c in cols if any(h in c.lower() for h in LAT_HINTS)), None)
    lon_col = next((c for c in cols if any(h in c.lower() for h in LON_HINTS)), None)
    checks = []
    if reg and ags:
        checks.append(
            (
                "Bundesland code vs municipality-key (AGS) prefix",
                group_attribute_spread(df, reg, ags, transform="prefix2"),
            )
        )
    if ags and plz:
        checks.append(
            (
                "municipality-key (AGS) vs postcode prefix",
                group_attribute_spread(df, ags, plz, transform="prefix2"),
            )
        )
    if lok and lat_col and lon_col:
        dfc = df.withColumn(
            "__coordkey",
            F.concat_ws(
                "|",
                F.round(safe_num(lat_col), 2),
                F.round(safe_num(lon_col), 2),
            ),
        )
        checks.append(
            (
                "units sharing a LokationMaStRNummer vs their coordinates (2dp)",
                group_attribute_spread(dfc, lok, "__coordkey"),
            )
        )
    spatial_consistency[name] = checks
    for lbl, res in checks:
        print(
            f"{name}: {lbl} -- {res['inconsistent_groups']}/{res['groups']} groups "
            f"inconsistent (max distinct in one group = "
            f"{res['max_distinct_values_in_a_group']})"
        )

# COMMAND ----------

# DBTITLE 1,Regime / version evidence -- measured field population across the 2019 migration
# MaStR bulk-migrated the legacy Anlagenregister in early 2019, then took native
# registrations afterwards. Split on the registration date and measure, per
# column, how differently the two cohorts are populated -- evidence for a
# migration-era indicator, not a narrative warning.
REGIME_CUT = "2019-06-01"
REG_DATE_HINTS = ("registrierungsdatum",)
regime = {}
for name, df in frames.items():
    rdc = next(
        (c for c in df.columns if any(h in c.lower() for h in REG_DATE_HINTS)), None
    )
    if not rdc:
        regime[name] = None
        continue
    shift = regime_population_shift(df, rdc, df.columns, REGIME_CUT)
    regime[name] = {"date_col": rdc, **shift}
    print(
        f"{name}: split on `{rdc}` @ {REGIME_CUT} -> pre={shift['pre_rows']} "
        f"post={shift['post_rows']} undated={shift['undated_rows']}; "
        f"columns flipping populated/empty across the cut: {shift['flipped']}"
    )

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
        "is assessed in the MaStR relationships section of this notebook against an explicit join spec; "
        "this section only establishes each table's own-entity key."
    )
]

_spatial = [
    para(
        "Only the einheiten_* tables carry latitude/longitude -- lokationen,",
        "netzanschlusspunkte and netze carry no coordinates, so a cross-table",
        "lat/lon reconciliation is NOT possible (limitation, not a finding).",
        "The checks below are internal geographic agreement, no external",
        "shapefile: each is `inconsistent groups / total groups`.",
    ),
    "",
]
for d in DATASETS:
    if not spatial_consistency.get(d):
        continue
    for lbl, res in spatial_consistency[d]:
        _spatial.append(
            f"- {d}: {lbl} -- {res['inconsistent_groups']}/{res['groups']} "
            f"inconsistent (worst group holds "
            f"{res['max_distinct_values_in_a_group']} distinct values)."
        )
if len(_spatial) <= 2:
    _spatial.append("- No table exposed a checkable pair of geographic columns.")
_spatial.append(
    para(
        "A non-zero inconsistent count is a source-data finding (a unit whose",
        "stated region, municipality key and coordinates disagree); it does not",
        "prove which attribute is wrong.",
    )
)

_regime = [
    para(
        f"Rows split on the registration date at {REGIME_CUT} (the MaStR legacy",
        "migration wave vs native registrations). Per-column null-rate and",
        "distinct-value count on each side; `flipped` = null-rate moves >= 50",
        "points across the cut.",
    ),
    "",
]
for d in DATASETS:
    rg = regime.get(d)
    if not rg:
        _regime.append(f"- {d}: no registration-date column -- not assessable.")
        continue
    _regime.append(
        f"- {d} (`{rg['date_col']}`): pre={rg['pre_rows']}, post={rg['post_rows']}, "
        f"undated={rg['undated_rows']}."
    )
    if rg["flipped"]:
        _regime.append(
            f"  - columns populated on one side of the cut only: {rg['flipped']}"
        )
        for c in rg["flipped"][:10]:
            v = rg["per_column"][c]
            _regime.append(
                f"    - `{c}`: null-rate pre={v['pre_null_rate']} post={v['post_null_rate']}; "
                f"distinct pre={v['pre_distinct']} post={v['post_distinct']}."
            )
    else:
        _regime.append("  - no column flips populated/empty across the cut.")
_regime.append(
    para(
        "This is a source/version-regime property: pre-migration rows are a",
        "different data-generating process from native ones. Carry a",
        "migration-era flag on any feature drawn from these tables; this notebook",
        "quantifies the gap, it does not choose the feature.",
    )
)

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
                "MaStR replaced the older EEG/Anlagenregister in 2019; pre-2019 units were bulk-migrated. "
                "Regime / Version Evidence above measures the population gap across the "
                f"{REGIME_CUT} cut per column -- a registration-era indicator is warranted where columns "
                "flip populated/empty."
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
        ("Spatial Consistency", "\n".join(_spatial)),
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- MASTR EEG SUPPORT & AUTHORISATION
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

# DBTITLE 1,Profile each table -- rows, missingness, approx distinct (one agg per table)
frames = {d: bronze_frame(t) for d, t in TABLES.items()}
prof = profile_each_frame(frames)

# COMMAND ----------

# DBTITLE 1,Confirm constant columns exactly
for name, df in frames.items():
    prof[name]["constant"] = constant_column_names(df, prof[name])
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
dup_counts = full_row_duplicate_counts(frames, prof)

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
categorical_dist = categorical_distributions(frames, prof)

# COMMAND ----------

# DBTITLE 1,Coverage bias -- rows per table
row_counts = {d: prof[d]["total"] for d in DATASETS}
cov = coverage_bias(row_counts)
print("rows per table:", row_counts, "coverage bias:", cov)

# COMMAND ----------


# DBTITLE 1,Point-in-time / temporal consistency -- event ordering within each table
# Expected ordering of the dated milestones on one record:
#   application  <=  decision/effective  <=  registration  <=  last-update
# and the scheme commissioning date must not post-date registration. Each pair
# is only checked when BOTH columns exist in the table.
def _first_col(cols, *subs):
    return next(
        (
            c
            for c in cols
            if any(s in c.lower() for s in subs) and not c.lower().endswith("_nv")
        ),
        None,
    )


order_checks = {}
for name, df in frames.items():
    cols = df.columns
    application = _first_col(cols, "antragstellung")
    commissioning = _first_col(cols, "inbetriebnahme")
    decision = "Datum" if "Datum" in cols else _first_col(cols, "zuschlag")
    registration = _first_col(cols, "registrierungsdatum")
    last_update = _first_col(cols, "datumletzteaktualisierung")
    pairs = [
        (application, decision, "application <= decision"),
        (application, registration, "application <= registration"),
        (decision, registration, "decision/effective <= registration"),
        (commissioning, registration, "commissioning <= registration"),
        (registration, last_update, "registration <= last-update"),
    ]
    checks = []
    for earlier, later, lbl in pairs:
        if earlier and later and earlier != later:
            res = date_order_check(df, earlier, later, label=f"{name}: {lbl}")
            checks.append(res)
            print(
                f"{name}: {lbl} ({earlier} -> {later}) -- {res['violations']}/"
                f"{res['comparable_rows']} out of order ({res['violation_pct']}%)"
            )
    order_checks[name] = checks

# COMMAND ----------

# DBTITLE 1,Regime / version evidence -- field population across EEG scheme eras
# Split each table on its registration date at the MaStR migration boundary and
# measure per-column null-rate / distinct-value counts on each side.
REGIME_CUT = "2019-06-01"
regime = {}
for name, df in frames.items():
    rdc = _first_col(df.columns, "registrierungsdatum")
    if not rdc:
        regime[name] = None
        continue
    shift = regime_population_shift(df, rdc, df.columns, REGIME_CUT)
    regime[name] = {"date_col": rdc, **shift}
    print(
        f"{name}: split on `{rdc}` @ {REGIME_CUT} -> pre={shift['pre_rows']} "
        f"post={shift['post_rows']} undated={shift['undated_rows']}; flipped={shift['flipped']}"
    )

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
        "the MaStR relationships section with a row-level fan-out probe; here only the own "
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

_order = [
    para(
        "Event-ordering checks within each record (a pair is checked only when",
        "both columns exist). A violation is a source-data finding: the record's",
        "own milestone dates contradict each other.",
    ),
    "",
]
_any_order = False
for d in DATASETS:
    for res in order_checks.get(d, []):
        _any_order = True
        _order.append(
            f"- {res['label']} (`{res['earlier']}` -> `{res['later']}`): "
            f"{res['violations']}/{res['comparable_rows']} out of order "
            f"({res['violation_pct']}%)."
        )
if not _any_order:
    _order.append("- No table exposed two comparable milestone-date columns.")

_regime = [
    para(
        f"Rows split on the registration date at {REGIME_CUT} (MaStR migration",
        "wave vs native). Per-column null-rate + distinct count each side;",
        "`flipped` = null-rate moves >= 50 points across the cut.",
    ),
    "",
]
for d in DATASETS:
    rg = regime.get(d)
    if not rg:
        _regime.append(f"- {d}: no registration-date column -- not assessable.")
        continue
    _regime.append(
        f"- {d} (`{rg['date_col']}`): pre={rg['pre_rows']}, post={rg['post_rows']}, "
        f"undated={rg['undated_rows']}; flipped columns: {rg['flipped'] or 'none'}."
    )
    for c in rg["flipped"][:8]:
        v = rg["per_column"][c]
        _regime.append(
            f"  - `{c}`: null-rate pre={v['pre_null_rate']} post={v['post_null_rate']}; "
            f"distinct pre={v['pre_distinct']} post={v['post_distinct']}."
        )
_regime.append(
    para(
        "EEG 2000/2004/2009/2012/2014/2017/2021/2023 each changed the support",
        "mechanism; the migration cut above is the measurable discontinuity in",
        "this export. A scheme-vintage indicator is warranted -- not chosen here.",
    )
)

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
                "feed-in -> auction). Regime / Version Evidence above measures the population "
                f"discontinuity at the {REGIME_CUT} migration cut -- a scheme-vintage indicator is "
                "warranted where columns flip."
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
        ("Temporal Consistency", "\n".join(_order)),
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- MASTR MARKET ACTORS & NETWORK
# MAGIC
# MAGIC **Purpose:** Profile the six MaStR market-actor and network Bronze
# MAGIC tables (marktakteure, marktakteure_und_rollen, netzanschlusspunkte,
# MAGIC netze, lokationen, bilanzierungsgebiete) -- schema, missingness, exact
# MAGIC key cardinality, full-row duplicates, the lokationen delimited
# MAGIC MaStR-Nummer link arrays, a structural column-alignment check on the
# MAGIC wide marktakteure export, categorical distributions, and the layered
# MAGIC modelling-risk checklist -- as evidence for Silver design.

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

# DBTITLE 1,Profile each table -- rows, missingness, approx distinct (one agg per table)
frames = {d: bronze_frame(t) for d, t in TABLES.items()}
prof = profile_each_frame(frames)

# COMMAND ----------

# DBTITLE 1,Confirm constant columns exactly
for name, df in frames.items():
    prof[name]["constant"] = constant_column_names(df, prof[name])
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
dup_counts = full_row_duplicate_counts(frames, prof)

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
categorical_dist = categorical_distributions(frames, prof)

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
        "columns. Fix in `databricks/bronze/01_load_energy_commerce_bronze.py` (multiLine + explicit "
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- MASTR CHANGE HISTORY
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

# DBTITLE 1,Profile each table -- rows, missingness, approx distinct, constant columns (one agg per table)
frames = {d: bronze_frame(t) for d, t in TABLES.items()}
prof = {}
for name, df in frames.items():
    prof[name] = profile_frame(df)
    cols, total = prof[name]["cols"], prof[name]["total"]
    acd, miss = prof[name]["acd"], prof[name]["miss"]
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
    constant_cols[name] = constant_column_names(df, prof[name])
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
dup_counts = full_row_duplicate_counts(frames, prof)

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

# DBTITLE 1,Categorical columns -- values and counts
cat_info = {}
for name, df in frames.items():
    skip = set(key_report[name]) | set(temporal[name])
    cats = [
        c
        for c in prof[name]["cols"]
        if 1 < prof[name]["acd"][c] <= 60 and c not in skip
    ]
    cat_info[name] = {
        c: [
            (x[0], x["count"])
            for x in df.groupBy(F.col(c))
            .count()
            .orderBy(F.desc("count"))
            .limit(30)
            .collect()
        ]
        for c in cats
    }
    print(name, {c: len(v) for c, v in cat_info[name].items()})

# COMMAND ----------

# DBTITLE 1,Bulk-update concentration -- days, instants and months of each date column
bulk = {}
for name, df in frames.items():
    bulk[name] = {}
    for c in temporal[name]:
        ts = parse_ts_multi(c)
        base = (
            df.select(ts.alias("ts"))
            .where(F.col("ts").isNotNull())
            .withColumn("day", F.to_date("ts"))
        )
        per_day = base.groupBy("day").count()
        top = per_day.orderBy(F.desc("count")).limit(30).collect()
        total = base.count()
        month_rows = (
            base.groupBy(F.date_format("ts", "yyyy-MM").alias("m"))
            .count()
            .orderBy(F.desc("m"))
            .limit(14)
            .collect()
        )
        same_ts = (
            base.groupBy("ts")
            .count()
            .agg(
                F.max("count").alias("max_same"), F.count(F.lit(1)).alias("distinct_ts")
            )
            .first()
        )
        top_counts = [x["count"] for x in top]
        bulk[name][c] = {
            "rows": total,
            "distinct_days": per_day.count(),
            "distinct_instants": same_ts["distinct_ts"],
            "max_rows_sharing_one_instant": same_ts["max_same"],
            "top_days": [(str(x["day"]), x["count"]) for x in top[:10]],
            "share_top1_day": round(sum(top_counts[:1]) / total, 4) if total else None,
            "share_top5_days": round(sum(top_counts[:5]) / total, 4) if total else None,
            "share_top30_days": round(sum(top_counts[:30]) / total, 4)
            if total
            else None,
            "latest_months": [(x["m"], x["count"]) for x in month_rows],
        }
        print(name, c, bulk[name][c])

# COMMAND ----------

# DBTITLE 1,Change types by year of the first date column
type_by_year = {}
for name, df in frames.items():
    dcols = list(temporal[name])
    if not dcols or not cat_info[name]:
        continue
    yr = F.year(parse_ts_multi(dcols[0]))
    type_by_year[name] = {"date_column": dcols[0], "columns": {}}
    for c, vals in cat_info[name].items():
        if len(vals) > 12:
            continue
        rows = (
            df.groupBy(yr.alias("y"), F.col(c).alias("v"))
            .count()
            .where(F.col("y").between(2000, 2030))
            .orderBy("y", F.desc("count"))
            .collect()
        )
        per = {}
        for x in rows:
            per.setdefault(int(x["y"]), []).append((x["v"], x["count"]))
        type_by_year[name]["columns"][c] = per
    print(name, list(type_by_year[name]["columns"]))

# COMMAND ----------

# DBTITLE 1,Repeated changes per unit and the lag between registration and effective date
repeat_info = {}
for name, df in frames.items():
    k = own_key[name]
    if not k or key_report[name][k]["unique"]:
        continue
    g = df.groupBy(F.col(k)).agg(F.count(F.lit(1)).alias("n"))
    sizes = (
        g.groupBy("n")
        .agg(F.count(F.lit(1)).alias("keys"))
        .orderBy("n")
        .limit(15)
        .collect()
    )
    repeat_info[name] = {"key": k, "rows_per_key": [(x["n"], x["keys"]) for x in sizes]}
    reg_c = next((c for c in temporal[name] if "registrierung" in c.lower()), None)
    eff_c = next(
        (
            c
            for c in temporal[name]
            if "aenderungsdatum" in c.lower() and "registrierung" not in c.lower()
        ),
        None,
    )
    if reg_c and eff_c:
        lag = (
            F.datediff(
                F.to_date(parse_ts_multi(reg_c)), F.to_date(parse_ts_multi(eff_c))
            )
        ).alias("lag")
        d = df.select(lag).where(F.col("lag").isNotNull())
        r = (
            d.agg(
                F.count(F.lit(1)).alias("n"),
                F.sum((F.col("lag") < 0).cast("long")).alias(
                    "registered_before_effective"
                ),
                F.sum((F.col("lag") == 0).cast("long")).alias("same_day"),
                F.sum((F.col("lag") > 365).cast("long")).alias("over_a_year"),
                F.expr("percentile_approx(lag, array(0.1, 0.5, 0.9, 0.99))").alias(
                    "p10_50_90_99_days"
                ),
                F.max("lag").alias("max_days"),
            )
            .first()
            .asDict()
        )
        repeat_info[name].update(
            {"registration_column": reg_c, "effective_column": eff_c, "lag_days": r}
        )
    print(name, repeat_info[name])

# COMMAND ----------

# DBTITLE 1,Overlap of the referenced units between the change tables
overlap = None
ku = own_key.get("geloeschte_deaktivierte_einheiten")
kc = own_key.get("einheiten_aenderung_netzbetreiberzuordnungen")
if ku and kc and ku == kc:
    a = (
        frames["geloeschte_deaktivierte_einheiten"]
        .select(F.col(ku).alias("k"))
        .distinct()
    )
    b = (
        frames["einheiten_aenderung_netzbetreiberzuordnungen"]
        .select(F.col(kc).alias("k"))
        .distinct()
    )
    overlap = {
        "deleted_units": a.count(),
        "units_with_operator_change": b.count(),
        "in_both": a.join(b, "k", "left_semi").count(),
    }
    print(overlap)

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

_bulk = [
    "Concentration of each date column (rows sharing days and instants); a value repeated on few days signals a batch operation:"
]
for name, cols in bulk.items():
    for c, b in cols.items():
        _bulk.append(
            f"- `{name}.{c}`: {b['rows']} rows on {b['distinct_days']} days and {b['distinct_instants']} distinct instants (most rows sharing one instant: {b['max_rows_sharing_one_instant']}); "
            f"top day holds {b['share_top1_day']}, top 5 days {b['share_top5_days']}, top 30 days {b['share_top30_days']} of the rows."
        )
        _bulk.append(f"  - ten largest days (day, rows): {b['top_days']}")
        _bulk.append(f"  - latest months (month, rows): {b['latest_months']}")
_bulk.append(
    para(
        "A date that is spread over many instants and days looks like a per-record timestamp; a large share on a",
        "few days looks like a batch or migration. Which one applies to the year totals is stated by these counts;",
        "the reason for a batch is not in the data.",
    )
)

_types = []
for name, ci in cat_info.items():
    for c, vals in ci.items():
        _types.append(f"- `{name}.{c}` (value, rows): {vals[:15]}")
for name, tb in type_by_year.items():
    for c, per in tb["columns"].items():
        _types.append(
            f"- `{name}.{c}` by year of `{tb['date_column']}` (year: (value, rows) ...): {per}"
        )
if not _types:
    _types.append("- No low-cardinality category column in these tables.")

_rep = []
for name, ri in repeat_info.items():
    _rep.append(
        f"- `{name}`: rows per `{ri['key']}` (rows, keys): {ri['rows_per_key']}."
    )
    if "lag_days" in ri:
        l_ = ri["lag_days"]
        _rep.append(
            f"  - registration (`{ri['registration_column']}`) minus effective date (`{ri['effective_column']}`), days: p10/p50/p90/p99 {l_['p10_50_90_99_days']}, max {l_['max_days']}; "
            f"registered before the effective date {l_['registered_before_effective']} of {l_['n']}, same day {l_['same_day']}, more than a year late {l_['over_a_year']}."
        )
if overlap:
    _rep.append(
        f"- units in the deletion table and in the operator-change table (deleted units, units with an operator change, in both): {overlap}."
    )
if not _rep:
    _rep.append("- No table with a repeated entity key.")

_coverage = [
    f"Row counts: { {d: prof[d]['total'] for d in DATASETS} }.",
    (
        "These tables ARE the survivorship record: a unit in geloeschte_deaktivierte_einheiten "
        "(or an actor in geloeschte_deaktivierte_marktakteure) has left the current-state register. "
        "Any population built only from the current-state tables (01/03) is missing exactly this set."
    ),
    (
        "The date column in the two deletion tables is "
        f"{ {n: list(temporal[n]) for n in ('geloeschte_deaktivierte_einheiten', 'geloeschte_deaktivierte_marktakteure')} }; "
        "if that is a last-update date, the year of a row is the year of the record's last edit, not of its "
        "deletion. How concentrated the dates are is measured in Bulk Updates; the cause is not decided here."
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

_areas = {
    "Domain understanding": [
        "logs of units and market actors that left the register (deleted or deactivated) and of grid-operator assignment changes for units",
        f"categorical columns: { {n: list(ci) for n, ci in cat_info.items()} }",
        f"date columns: { {n: list(temporal[n]) for n in DATASETS} }",
    ],
    "Structure and engineering": [
        f"3 Bronze tables, {sum(prof[d]['total'] for d in DATASETS)} rows; entity keys {own_key}",
        f"units in both the deletion and the operator-change tables: {overlap}",
    ],
    "Temporal": [
        f"rows per year: { {n: {c: v['by_year'] for c, v in cols.items()} for n, cols in rows_per_year.items()} }",
        f"concentration on the top 5 days: { {n: {c: b['share_top5_days'] for c, b in cols.items()} for n, cols in bulk.items()} }",
    ],
    "Spatial": ["no location columns"],
    "Data quality": [
        f"duplicates {dup_counts}; unparsed or implausible dates { {n: {c: v['unparsed_or_implausible'] for c, v in cols.items()} for n, cols in rows_per_year.items()} }"
    ],
    "Statistical patterns": [
        f"latest months by table: { {n: {c: b['latest_months'][:3] for c, b in cols.items()} for n, cols in bulk.items()} }"
    ],
    "Relationships": [
        f"repeated entity keys: { {n: ri['rows_per_key'][:4] for n, ri in repeat_info.items()} }",
        f"registration lag: { {n: ri.get('lag_days', {}).get('p10_50_90_99_days') for n, ri in repeat_info.items()} }",
    ],
    "Analytics use": [
        "survivorship: units and actors that left the register, and how long an assignment change takes to be registered"
    ],
    "ML use": ["no target; the logs are event streams keyed by unit or actor"],
    "AI / knowledge use": [
        "catalog codes for change types (see Change Types); no free text"
    ],
}

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Data Quality", "\n".join(_dq)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Temporal Semantics", "\n".join(_temporal_md)),
        ("Bulk Updates", "\n".join(_bulk)),
        ("Change Types and Regimes", "\n".join(_types)),
        ("Repeated Changes and Lags", "\n".join(_rep)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Observations by Area", area_block(_areas)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- MASTR REFERENCE CATALOGS
# MAGIC
# MAGIC **Purpose:** Profile the six MaStR static reference / code-lookup Bronze
# MAGIC tables (einheitentypen, katalogkategorien, katalogwerte, lokationstypen,
# MAGIC marktfunktionen, marktrollen) -- schema, full-row duplicates, the
# MAGIC katalogkategorien <-> katalogwerte lookup consistency, and how these
# MAGIC code sets constrain the categorical columns profiled in 01-04. Small
# MAGIC tables, collected and reconciled in Python.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "mastr"
NB_KEY = "05_reference_catalogs"
SECTION_TITLE = (
    "Reference catalogs (einheitentypen / katalog* / lokationstypen / "
    "marktfunktionen / marktrollen)"
)
DATASETS = [
    "einheitentypen",
    "katalogkategorien",
    "katalogwerte",
    "lokationstypen",
    "marktfunktionen",
    "marktrollen",
]
TABLES = {d: f"{CATALOG}.{BRONZE_SCHEMA}.mastr_{d}" for d in DATASETS}

# COMMAND ----------

# DBTITLE 1,Collect the 6 catalog tables (small) and profile in Python
meta = {}
for name, t in TABLES.items():
    df = bronze_frame(t)
    recs = [x.asDict() for x in df.collect()]
    cols = df.columns
    total = len(recs)
    dups = total - len({tuple(sorted(d.items())) for d in recs})
    consts = [c for c in cols if len({d[c] for d in recs}) <= 1]
    meta[name] = {
        "cols": cols,
        "recs": recs,
        "total": total,
        "dups": dups,
        "consts": consts,
    }
    print(
        "=" * 90, f"\n{name}  rows={total}  cols={cols}  dups={dups}  constant={consts}"
    )
    for c in cols:
        missing = sum(1 for d in recs if d[c] is None or str(d[c]).strip() == "")
        print(f"  {c:<30} missing={missing:>6}  distinct={len({d[c] for d in recs})}")
    for d in recs[:20]:
        print("  ", d)

# COMMAND ----------

# DBTITLE 1,katalogkategorien <-> katalogwerte lookup consistency
kk, kw = meta["katalogkategorien"], meta["katalogwerte"]
kk_id = find_col(
    bronze_frame(TABLES["katalogkategorien"]),
    "Id",
    "KatalogKategorieId",
    "kategorie_id",
)
kw_fk = next((c for c in kw["cols"] if "kategorie" in c.lower()), None)
kk_ids = {d[kk_id] for d in kk["recs"]} if kk_id else set()
kw_fks = {d[kw_fk] for d in kw["recs"]} if kw_fk else set()
kat_ri = referential_integrity(
    kw_fks, kk_ids, child=f"katalogwerte.{kw_fk}", parent=f"katalogkategorien.{kk_id}"
)
print(f"catalog lookup key: {kk_id} <-> {kw_fk}")
print("referential integrity:", kat_ri)
for ln in ri_interpretation(kat_ri):
    print("  ", ln)

# COMMAND ----------

# DBTITLE 1,Figure -- rows per catalog table
figs = []
if barplot(
    [(n, meta[n]["total"]) for n in DATASETS],
    "MaStR reference catalogs -- rows per table",
    "table",
    "rows",
    rot=30,
    filename="mastr_reference_catalogs_overview.png",
):
    figs.append(
        (
            "MaStR reference catalogs -- rows per table",
            "mastr_reference_catalogs_overview.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/mastr.md
_profile = [
    "| table | rows | cols | full-row dups | constant columns |",
    "|---|---|---|---|---|",
]
for name in DATASETS:
    x = meta[name]
    _profile.append(
        f"| {name} | {x['total']} | {len(x['cols'])} | {x['dups']} | "
        f"{', '.join(x['consts']) or '-'} |"
    )

_dq = [
    f"Full-row exact duplicates per table: { {n: meta[n]['dups'] for n in DATASETS} }"
]

_recon = [
    f"katalogkategorien key `{kk_id}` <-> katalogwerte FK `{kw_fk}`:",
    (
        f"- {kat_ri['orphans']} katalogwerte FK values with no matching category "
        f"(match rate {kat_ri['match_rate']})."
    ),
    (
        f"- {kat_ri['unused_parent']} categories never referenced by a katalogwerte row: "
        f"{sorted(str(x) for x in (kk_ids - kw_fks))[:15]}."
    ),
]
for ln in ri_interpretation(kat_ri):
    _recon.append(f"- {ln}")

_domain = [
    (
        "These six tables are the authoritative code sets for the categorical columns profiled in "
        "01-04. Before Silver, each low-cardinality code column found there (status, technology, "
        "energy carrier, market role, ...) must be reconciled against the matching catalog here -- "
        "an unknown code is a quarantine class, not a silent NULL. That reconciliation is a Silver "
        "task; this notebook only confirms the catalogs are internally consistent."
    ),
]

_findings_md = "\n".join(
    f"- {name}: {meta[name]['dups']} full-row duplicate(s), "
    f"{len(meta[name]['cols'])} column(s)"
    for name in DATASETS
)

_silver = [
    (
        "- All six tables are static lookups -> Silver reference dimensions, SCD type 1 (overwrite "
        "on reload) unless a future MaStR release adds a validity-period column."
    ),
    (
        "- Pin the MaStR catalog release used -- code meanings change between register versions, so "
        "a code decoded against the wrong catalog vintage is a silent label error downstream."
    ),
]
if any(meta[n]["dups"] for n in DATASETS):
    _silver.append("- Exact duplicate rows exist -> de-duplicate on load.")
if kat_ri["orphans"]:
    _silver.append(
        "- katalogwerte has FK values with no category row -> confirm the FK column semantics "
        "against the live schema before enforcing the foreign key."
    )

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                "Code dimensions with no observation grain -- a model joins a label in, it does not train "
                "on these rows."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                f"katalogkategorien <-> katalogwerte is a clean code lookup ({kat_ri['match_rate']} match). "
                "A decode join fans out only if a code appears in more than one category -- not observed."
            ),
        ),
        (
            "Target contamination",
            "Not applicable -- static reference data, no outcome.",
        ),
        ("Temporal / post-event leakage", "Not applicable -- no temporal ordering."),
        (
            "Proxy leakage",
            (
                "A decoded label is a deterministic function of the code -- using both the raw code and "
                "its decoded label as separate features is redundant, not leakage, but wastes capacity."
            ),
        ),
        ("Split / entity leakage", "Not applicable -- no entities to split."),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "Real risk: decoding a historical record with today's catalog assigns a meaning the code "
                "did not have then. Keep the catalog version alongside the decoded value."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                f"{kat_ri['unused_parent']} categories are never used by any katalogwerte row -- harmless "
                "for a dimension, but a one-hot over the full catalog carries always-zero columns."
            ),
        ),
        ("Missingness leakage", "Not applicable -- fully populated small tables."),
        (
            "Duplicate-event leakage",
            (
                f"Full-row duplicates: { {n: meta[n]['dups'] for n in DATASETS} } -- de-duplicate so a "
                "decode join stays 1:1."
            ),
        ),
        ("Target / feature temporal misalignment", "Not applicable -- no dates."),
        (
            "Unit / sign / circular-feature leakage",
            "Not applicable -- no numeric measures.",
        ),
        (
            "Data-generation-process leakage",
            (
                "The catalogs describe MaStR's own classification scheme -- a code encodes how MaStR "
                "categorises a thing, not an independent physical property."
            ),
        ),
        (
            "Class / label instability",
            (
                "PRIMARY concern here: MaStR revises these code sets between releases (adds/retires/"
                "renames). A class defined by a raw code is only stable within one catalog vintage."
            ),
        ),
        ("Label availability lag", "Not applicable -- reference data."),
        (
            "Source / version / regime change",
            (
                "Each MaStR release ships an updated catalog -- pin and diff it across releases before "
                "pooling records decoded under different vintages."
            ),
        ),
        (
            "Sample-vs-full divergence",
            "All six tables are fully collected (small) -- no sampling.",
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
        ("Referential Integrity", "\n".join(_recon)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- MASTR RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **Purpose:** cross-table checks over the 28 MaStR Bronze tables. Keys come from
# MAGIC an exact distinct-count. Each join follows an explicit relationship spec (columns
# MAGIC matched case-insensitively) and reports orphan rate, unused parents and the
# MAGIC cardinality profile. It also reconciles the coded columns of 01-04 with the
# MAGIC reference catalogs (05), checks event-date order on `EinheitMastrNummer`, and
# MAGIC compares coordinates across carriers.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "mastr"
NB_KEY = "06_relationships_and_findings"
SECTION_TITLE = "Cross-table relationships & verdict"

GENERATION_UNITS = [
    "einheiten_wind",
    "einheiten_biomasse",
    "einheiten_wasser",
    "einheiten_verbrennung",
    "einheiten_kernkraft",
    "einheiten_geothermie_gsgk",
]
EEG_SUPPORT = [
    "anlagen_eeg_wind",
    "anlagen_eeg_biomasse",
    "anlagen_eeg_wasser",
    "anlagen_eeg_geothermie_gsgk",
    "anlagen_kwk",
    "einheiten_genehmigung",
    "ertuechtigungen",
]
MARKET_NETWORK = [
    "marktakteure",
    "marktakteure_und_rollen",
    "netzanschlusspunkte",
    "netze",
    "lokationen",
    "bilanzierungsgebiete",
]
CHANGE_HISTORY = [
    "geloeschte_deaktivierte_einheiten",
    "geloeschte_deaktivierte_marktakteure",
    "einheiten_aenderung_netzbetreiberzuordnungen",
]
REFERENCE = [
    "einheitentypen",
    "katalogkategorien",
    "katalogwerte",
    "lokationstypen",
    "marktfunktionen",
    "marktrollen",
]
ANALYTICAL = GENERATION_UNITS + EEG_SUPPORT + MARKET_NETWORK + CHANGE_HISTORY
ALL_DATASETS = ANALYTICAL + REFERENCE
TABLES = {d: f"{CATALOG}.{BRONZE_SCHEMA}.mastr_{d}" for d in ALL_DATASETS}

# Own-entity key tie-break preference per table family. pick_entity_key still
# ranks primarily by the exact distinct/row ratio; this only breaks ties and
# names the semantically correct key when several columns are near-unique.
OWN_KEY_PREFERENCE = {
    **{d: ("EinheitMastrNummer",) for d in GENERATION_UNITS},
    "anlagen_eeg_wind": ("EegMaStRNummer",),
    "anlagen_eeg_biomasse": ("EegMaStRNummer",),
    "anlagen_eeg_wasser": ("EegMaStRNummer",),
    "anlagen_eeg_geothermie_gsgk": ("EegMaStRNummer",),
    "anlagen_kwk": ("KwkMastrNummer", "KwkMaStRNummer"),
    "einheiten_genehmigung": ("GenMastrNummer",),
    "ertuechtigungen": ("ErtuechtigungMastrNummer", "EegMastrNummer"),
    "marktakteure": ("MastrNummer",),
    "marktakteure_und_rollen": ("MastrNummer",),
    "netzanschlusspunkte": ("NetzanschlusspunktMastrNummer",),
    "netze": ("MastrNummer",),
    "lokationen": ("MastrNummer",),
    "geloeschte_deaktivierte_einheiten": ("EinheitMastrNummer",),
    "geloeschte_deaktivierte_marktakteure": ("MarktakteurMastrNummer",),
    "einheiten_aenderung_netzbetreiberzuordnungen": ("EinheitMastrNummer",),
}

# Explicit relationship spec: (child_table, child_key_suffix, parent_role,
# parent_key_suffix). parent_role "generation_units" / "marktakteure" /
# "lokationen" / "netze" resolves to the union of that key across its tables.
# Suffixes are matched case-insensitively against column names.
JOIN_SPEC = [
    # EEG / KWK / authorisation -> generation unit (shared scheme number)
    ("anlagen_eeg_wind", "EegMaStRNummer", "gen_eeg", "EegMaStRNummer"),
    ("anlagen_eeg_biomasse", "EegMaStRNummer", "gen_eeg", "EegMaStRNummer"),
    ("anlagen_eeg_wasser", "EegMaStRNummer", "gen_eeg", "EegMaStRNummer"),
    ("anlagen_eeg_geothermie_gsgk", "EegMaStRNummer", "gen_eeg", "EegMaStRNummer"),
    ("anlagen_kwk", "KwkMastrNummer", "gen_kwk", "KwkMaStRNummer"),
    ("einheiten_genehmigung", "GenMastrNummer", "gen_gen", "GenMastrNummer"),
    ("ertuechtigungen", "EegMastrNummer", "gen_eeg", "EegMaStRNummer"),
    # change history -> generation unit / market actor (shared entity number)
    (
        "geloeschte_deaktivierte_einheiten",
        "EinheitMastrNummer",
        "gen_einheit",
        "EinheitMastrNummer",
    ),
    (
        "einheiten_aenderung_netzbetreiberzuordnungen",
        "EinheitMastrNummer",
        "gen_einheit",
        "EinheitMastrNummer",
    ),
    (
        "geloeschte_deaktivierte_marktakteure",
        "MarktakteurMastrNummer",
        "marktakteure",
        "MastrNummer",
    ),
    # market actor / network wiring
    (
        "marktakteure_und_rollen",
        "MarktakteurMastrNummer",
        "marktakteure",
        "MastrNummer",
    ),
    ("netzanschlusspunkte", "LokationMaStRNummer", "lokationen", "MastrNummer"),
    ("netzanschlusspunkte", "NetzMaStRNummer", "netze", "MastrNummer"),
    ("netzanschlusspunkte", "NetzbetreiberMaStRNummer", "marktakteure", "MastrNummer"),
    # generation unit -> operator / location
    ("einheiten_wind", "AnlagenbetreiberMastrNummer", "marktakteure", "MastrNummer"),
    ("einheiten_verbrennung", "LokationMaStRNummer", "lokationen", "MastrNummer"),
]

# How each child table is expected to relate to the LIVE generation-unit /
# market-actor population, so a high orphan rate is read correctly:
#   subset        - child keys should almost all resolve to a live parent row;
#                   a non-trivial orphan rate is a real data-quality signal.
#   disjoint      - child keys should NOT be in the live tables by design; a
#                   deregistered unit/actor is removed from the live export, so
#                   ~100% "orphan" is the expected, correct result.
#   partial-scope - the live parent set is incomplete here because the solar and
#                   storage object types are deferred from staging, so a log
#                   spanning every technology shows "orphans" that are really
#                   just out-of-scope units, not broken keys.
RELATIONSHIP_EXPECTATION = {
    "geloeschte_deaktivierte_einheiten": "disjoint",
    "geloeschte_deaktivierte_marktakteure": "disjoint",
    "einheiten_aenderung_netzbetreiberzuordnungen": "partial-scope",
    "ertuechtigungen": "partial-scope",
}

# COMMAND ----------

# DBTITLE 1,Helpers -- resolve a *MastrNummer column by suffix, case-insensitive
frames = {d: bronze_frame(t) for d, t in TABLES.items()}
row_count = {d: frames[d].count() for d in ALL_DATASETS}


def resolve_key(table, suffix):
    low = suffix.lower()
    for c in frames[table].columns:
        if c.lower() == low or c.lower().endswith(low):
            return c
    return None


# COMMAND ----------

# DBTITLE 1,Own-entity key per table -- EXACT distinct count
own_key = {}
own_uniq = {}
for d in ALL_DATASETS:
    df = frames[d]
    cands = key_like_cols(df.columns) or [
        c for c in df.columns if c.lower().endswith("id")
    ]
    uniq = exact_uniqueness(df, cands)
    k, info = pick_entity_key(uniq, cands, prefer=OWN_KEY_PREFERENCE.get(d, ()))
    own_key[d] = k
    own_uniq[d] = uniq
    print(f"{d:<48} own_key={k}  rows={row_count[d]:>10}  {info}")

# COMMAND ----------

# DBTITLE 1,Own-key value set per table (collected -- key sets are far smaller than row counts)
key_set = {}
for d in ALL_DATASETS:
    key_set[d] = collect_key_set(frames[d], own_key[d]) if own_key[d] else set()
    print(f"{d:<48} distinct own keys = {len(key_set[d])}")

# COMMAND ----------

# DBTITLE 1,Parent-role key unions (by the join column, not the own key)
PARENT_ROLES = {
    "gen_eeg": (GENERATION_UNITS, "EegMaStRNummer"),
    "gen_kwk": (GENERATION_UNITS, "KwkMaStRNummer"),
    "gen_gen": (GENERATION_UNITS, "GenMastrNummer"),
    "gen_einheit": (GENERATION_UNITS, "EinheitMastrNummer"),
    "marktakteure": (["marktakteure"], "MastrNummer"),
    "lokationen": (["lokationen"], "MastrNummer"),
    "netze": (["netze"], "MastrNummer"),
}
parent_union = {}
for role, (tables, suffix) in PARENT_ROLES.items():
    vals = set()
    for t in tables:
        col = resolve_key(t, suffix)
        if col:
            vals |= collect_key_set(frames[t], col)
    parent_union[role] = vals
    print(
        f"parent role {role:<14} ({suffix}) -> {len(vals)} distinct keys from {tables}"
    )

gen_einheit_union = parent_union["gen_einheit"]
print(
    f"generation-unit population (EinheitMastrNummer union) = {len(gen_einheit_union)}"
)

# COMMAND ----------

# DBTITLE 1,Referential integrity per explicit relationship (+ row-level fan-out probe)
ri_results = []
for child, child_suffix, role, _parent_suffix in JOIN_SPEC:
    ccol = resolve_key(child, child_suffix)
    if ccol is None:
        ri_results.append(
            {
                "child": child,
                "child_col": None,
                "role": role,
                "skipped": "child key column not found",
            }
        )
        print(f"SKIP  {child}.{child_suffix} -> {role}: child column not found")
        continue
    child_vals = collect_key_set(frames[child], ccol)
    ri = referential_integrity(
        child_vals, parent_union[role], child=f"{child}.{ccol}", parent=role
    )
    # Full relationship cardinality (not just the orphan rate): child rows,
    # distinct child keys, how many parent entities are actually referenced, and
    # the child-rows-per-parent distribution + max fan-out.
    cp = cardinality_profile(frames[child], ccol, child_vals, parent_union[role])
    ri.update(cp)
    ri["child_max_rows_per_key"] = cp["max_fanout"]
    ri["child_avg_rows_per_key"] = (
        round(cp["child_rows"] / cp["distinct_child_keys"], 3)
        if cp["distinct_child_keys"]
        else None
    )
    ri["expectation"] = RELATIONSHIP_EXPECTATION.get(child, "subset")
    # A real integrity problem = orphans where the child was expected to be a
    # subset of the parent. "disjoint" / "partial-scope" orphans are expected.
    ri["orphan_problem"] = ri["expectation"] == "subset" and ri["orphans"] > 0
    ri_results.append(ri)
    print(
        f"{child}.{ccol} -> {role} [{ri['expectation']}]: match_rate={ri['match_rate']} "
        f"orphans={ri['orphans']} child_rows={cp['child_rows']} "
        f"distinct_child_keys={cp['distinct_child_keys']} "
        f"parents_referenced={cp['matched_parent_keys']}/{cp['parent_keys_total']} "
        f"child_rows_per_parent p50/p90/p99={cp['child_rows_per_parent_p50']}/"
        f"{cp['child_rows_per_parent_p90']}/{cp['child_rows_per_parent_p99']} "
        f"max_fanout={cp['max_fanout']}"
    )

# COMMAND ----------

# DBTITLE 1,katalogkategorien <-> katalogwerte (reference lookup consistency)
kk = frames["katalogkategorien"]
kw = frames["katalogwerte"]
kk_id = find_col(kk, "Id", "KatalogKategorieId", "kategorie_id")
kw_fk = next((c for c in kw.columns if "kategorie" in c.lower()), None)
kat_ri = None
if kk_id and kw_fk:
    kat_ri = referential_integrity(
        collect_key_set(kw, kw_fk),
        collect_key_set(kk, kk_id),
        child=f"katalogwerte.{kw_fk}",
        parent=f"katalogkategorien.{kk_id}",
    )
    print("catalog lookup:", kat_ri)

# COMMAND ----------

# DBTITLE 1,Catalog / domain reconciliation -- code columns in 01-04 vs the reference catalogs (05)
# Systematic check: every low-cardinality integer-coded column across the
# analytical tables is reconciled against mastr_katalogwerte. The export has no
# explicit column -> category binding, so a column is scoped to a category only
# when its name matches one; otherwise it falls back to global value-id
# membership (which proves a code is used SOMEWHERE in MaStR, not that it is
# valid for this column). An unknown code is a real source-data finding.
catalog = load_mastr_catalog(spark, CATALOG, BRONZE_SCHEMA)
print(
    f"catalog: {catalog['n_values']} value-ids across {catalog['n_categories']} categories"
)
ANALYTICAL_FOR_CODES = GENERATION_UNITS + EEG_SUPPORT + CHANGE_HISTORY
code_reco = {}
for d in ANALYTICAL_FOR_CODES:
    df = frames[d]
    cols = [
        c
        for c in df.columns
        if not c.lower().endswith(("mastrnummer", "mastrnr", "_nv", "id"))
        and "datum" not in c.lower()
        and "nummer" not in c.lower()
    ]
    if not cols:
        code_reco[d] = []
        continue
    acd = df.agg(*[F.approx_count_distinct(c).alias(c) for c in cols]).first().asDict()
    flagged = [c for c in cols if 2 <= (acd[c] or 0) <= 60]
    res = []
    for c in flagged:
        r = reconcile_codes(df, c, catalog, category_hint=c)
        if r.get("coded"):
            res.append(r)
    code_reco[d] = res
    for r in res:
        tag = "ok" if r["unknown_count"] == 0 else "UNKNOWN"
        print(
            f"{d}.{r['column']} [{tag}] category={r['matched_category']} "
            f"({r['checked_against']}) unknown={r['unknown_count']} vals "
            f"({r['unknown_rows']} rows): {r['unknown_values'][:8]}"
        )

# COMMAND ----------

# DBTITLE 1,Point-in-time / temporal consistency -- cross-table event ordering on the same unit
# Commissioning (einheiten_*) must not post-date a later lifecycle date recorded
# for the same EinheitMastrNummer in another table. Only pairs linked by a shared
# key are checkable -- a full unit lifecycle cannot be ordered because the dates
# live in different tables with partial overlap.
comm_parts = []
for d in GENERATION_UNITS:
    ek = resolve_key(d, "EinheitMastrNummer")
    ic = next(
        (c for c in frames[d].columns if c.lower() == "inbetriebnahmedatum"), None
    )
    if ek and ic:
        comm_parts.append(
            frames[d].select(
                F.col(ek).cast("string").alias("unit"),
                F.col(ic).cast("string").alias("commissioning"),
            )
        )
commissioning_df = comm_parts[0] if comm_parts else None
for p in comm_parts[1:]:
    commissioning_df = commissioning_df.unionByName(p)

xtab_order = []
XTAB_TARGETS = [
    (
        "geloeschte_deaktivierte_einheiten",
        "DatumLetzteAktualisierung",
        "commissioning <= deregistration last-update",
    ),
    (
        "einheiten_aenderung_netzbetreiberzuordnungen",
        "Netzbetreiberzuordnungsaenderungsdatum",
        "commissioning <= net-operator-change effective date",
    ),
]
if commissioning_df is not None:
    for t, dcol, lbl in XTAB_TARGETS:
        lk = resolve_key(t, "EinheitMastrNummer")
        rc = next((c for c in frames[t].columns if c.lower() == dcol.lower()), None)
        if not (lk and rc):
            continue
        j = commissioning_df.join(
            frames[t].select(
                F.col(lk).cast("string").alias("unit"),
                F.col(rc).cast("string").alias("later"),
            ),
            on="unit",
            how="inner",
        )
        res = date_order_check(j, "commissioning", "later", label=lbl)
        xtab_order.append(res)
        print(
            f"cross-table: {lbl} -- {res['violations']}/{res['comparable_rows']} "
            f"out of order ({res['violation_pct']}%)"
        )

# within-table: the net-operator change effective date vs its own registration date
ea = frames["einheiten_aenderung_netzbetreiberzuordnungen"]
eff = next(
    (c for c in ea.columns if c.lower() == "netzbetreiberzuordnungsaenderungsdatum"),
    None,
)
regd = next(
    (
        c
        for c in ea.columns
        if c.lower() == "registrierungsdatumnetzbetreiberzuordnungsaenderung"
    ),
    None,
)
if eff and regd:
    res = date_order_check(
        ea, eff, regd, label="net-operator change: effective <= registration"
    )
    xtab_order.append(res)
    print(
        f"within-table: {res['label']} -- {res['violations']}/{res['comparable_rows']} "
        f"({res['violation_pct']}%)"
    )

# COMMAND ----------

# DBTITLE 1,Cross-carrier spatial consistency -- a shared Lokation should resolve to one place
# Cross-table lat/lon reconciliation is not possible: only the einheiten_* tables
# carry coordinates (lokationen / netzanschlusspunkte / netze carry none). The
# one cross-table check available: units in DIFFERENT carrier tables that share a
# LokationMaStRNummer should have near-identical coordinates.
LAT_HINTS = ("breitengrad", "breite")
LON_HINTS = ("laengengrad", "laenge", "längengrad")
coord_parts = []
for d in GENERATION_UNITS:
    cols = frames[d].columns
    lk = resolve_key(d, "LokationMaStRNummer")
    la = next((c for c in cols if any(h in c.lower() for h in LAT_HINTS)), None)
    lo = next((c for c in cols if any(h in c.lower() for h in LON_HINTS)), None)
    if lk and la and lo:
        coord_parts.append(
            frames[d].select(
                F.col(lk).cast("string").alias("lok"),
                F.concat_ws(
                    "|",
                    F.round(safe_num(la), 2),
                    F.round(safe_num(lo), 2),
                ).alias("coordkey"),
            )
        )
cross_coord = None
if coord_parts:
    cdf = coord_parts[0]
    for p in coord_parts[1:]:
        cdf = cdf.unionByName(p)
    cross_coord = group_attribute_spread(cdf, "lok", "coordkey")
    print("cross-carrier LokationMaStRNummer -> coordinate spread:", cross_coord)

# COMMAND ----------

# DBTITLE 1,Figure -- rows per Bronze table
figs = []
if barplot(
    [(d, row_count[d]) for d in ALL_DATASETS],
    "MaStR -- rows per Bronze table (all 28)",
    "table",
    "rows",
    rot=90,
    figsize=(16, 5),
    filename="mastr_rows_per_table.png",
):
    figs.append(("MaStR rows per Bronze table (all 28)", "mastr_rows_per_table.png"))

# COMMAND ----------

# DBTITLE 1,Figure -- orphan rate per explicit relationship
# Only the "subset" relationships belong on an orphan-rate chart -- for
# "disjoint" / "partial-scope" a high rate is expected, not a defect.
orphan_pairs = [
    (
        f"{r['child'].split('.')[0]}->{r['parent']}",
        round((1 - r["match_rate"]) * 100, 2),  # match_rate can be 0.0 -- do not `or`
    )
    for r in ri_results
    if r.get("expectation") == "subset" and r.get("match_rate") is not None
]
if barplot(
    orphan_pairs,
    "MaStR -- orphan-key rate, subset relationships only (% of child keys with no live parent)",
    "relationship",
    "orphan %",
    rot=60,
    figsize=(14, 5),
    filename="mastr_eeg_fk_orphans.png",
):
    figs.append(
        (
            "MaStR -- orphan-key rate per explicit relationship",
            "mastr_eeg_fk_orphans.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Findings
print("own key per table:", own_key)
print("relationships checked:", len([r for r in ri_results if "match_rate" in r]))
_bad = [r for r in ri_results if r.get("orphans")]
print("relationships with orphan keys:", [(r["child"], r["orphans"]) for r in _bad])

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/mastr.md
_ent = [
    "Own-entity key column per table (exact distinct / ratio-to-rows / unique):",
]
for d in ALL_DATASETS:
    k = own_key[d]
    u = own_uniq[d].get(k) if k else None
    _ent.append(
        f"- {d}: `{k}`"
        + (
            f" -- {u['distinct']} / {u['ratio']} / unique={u['unique']}"
            if u
            else " -- none"
        )
    )
_ent.append(
    f"Generation-unit population (union of EinheitMastrNummer): {len(gen_einheit_union)}"
)

_rel = [
    para(
        "Referential integrity per EXPLICIT relationship (child column -> parent role).",
        "`expectation`: subset = child should resolve to a live parent;",
        "disjoint = child should NOT be in the live tables by design (deregistered",
        "entities); partial-scope = the live parent set is incomplete because",
        "solar/storage object types are deferred from staging.",
    ),
    "",
]
for r in ri_results:
    if "match_rate" not in r:
        _rel.append(f"- {r['child']} -> {r['role']}: SKIPPED ({r.get('skipped')})")
        continue
    _rel.append(
        f"- `{r['child']}` -> {r['parent']} [{r['expectation']}]: "
        f"match_rate={r['match_rate']}, orphans={r['orphans']}/{r['child_distinct']}, "
        f"child_rows={r['child_rows']}, distinct_child_keys={r['distinct_child_keys']}, "
        f"parents_referenced={r['matched_parent_keys']}/{r['parent_keys_total']} "
        f"({r['parent_keys_referenced_pct']}%), "
        f"child_rows_per_parent p50/p90/p99="
        f"{r['child_rows_per_parent_p50']}/{r['child_rows_per_parent_p90']}/"
        f"{r['child_rows_per_parent_p99']}, max_fanout={r['max_fanout']}"
    )
_rel.append("")
_rel.append(
    "Interpretation of the SUBSET relationships with a real (unexpected) orphan rate:"
)
_real_problems = [r for r in ri_results if r.get("orphan_problem")]
if not _real_problems:
    _rel.append("- none -- every subset relationship resolves cleanly.")
for r in _real_problems:
    for ln in ri_interpretation(r):
        _rel.append(f"- {r['child']}: {ln}")
_expected_orphans = [
    r for r in ri_results if r.get("orphans") and not r.get("orphan_problem")
]
if _expected_orphans:
    _rel.append("")
    _rel.append("Expected high-orphan relationships (not a defect):")
    for r in _expected_orphans:
        _rel.append(
            f"- `{r['child']}` [{r['expectation']}]: {1 - (r['match_rate'] or 0):.1%} of "
            f"child keys have no live parent -- "
            + (
                "deregistered entities are removed from the live export; join to the live "
                "tables only to confirm the entity is gone."
                if r["expectation"] == "disjoint"
                else "the log spans solar/storage units that are deferred from staging; the "
                "orphan rate here reflects staging scope, not a key mismatch."
            )
        )
if kat_ri:
    _rel.append("")
    _rel.append(
        f"katalogwerte -> katalogkategorien: {kat_ri['orphans']} orphan FK values, "
        f"{kat_ri['unused_parent']} unreferenced categories."
    )

_catalog = [
    para(
        "Every low-cardinality integer-coded column across the generation-unit,",
        "EEG/authorisation and change-history tables, reconciled against",
        f"mastr_katalogwerte ({catalog['n_values']} value-ids,",
        f"{catalog['n_categories']} categories). LIMITATION: the export carries no",
        "column -> category binding, so a column is scoped to a category only when",
        "its name matches; otherwise the check is global value-id membership,",
        "which cannot prove a code is valid FOR THAT column.",
    ),
    "",
]
_code_hits = 0
for d in ANALYTICAL_FOR_CODES:
    for r in code_reco.get(d, []):
        if r["unknown_count"] == 0:
            continue
        _code_hits += 1
        _catalog.append(
            f"- `{d}.{r['column']}` [{r['checked_against']}"
            + (f", category `{r['matched_category']}`" if r["matched_category"] else "")
            + f"]: {r['unknown_count']} unknown code(s) over {r['unknown_rows']} rows "
            f"-- {r['unknown_values'][:12]}"
        )
if _code_hits == 0:
    _catalog.append(
        "- No unknown codes: every coded column resolves fully against the catalog "
        "(within the matching limitation above)."
    )
_catalog.append("")
_catalog.append(
    "Columns matched to a named category (scoped check, strongest evidence):"
)
_scoped = [
    f"`{d}.{r['column']}` -> `{r['matched_category']}`"
    for d in ANALYTICAL_FOR_CODES
    for r in code_reco.get(d, [])
    if r["matched_category"]
]
_catalog.append("- " + (", ".join(_scoped) if _scoped else "none matched by name."))

_tcons = [
    para(
        "Cross-table event ordering on the same EinheitMastrNummer. A violation",
        "is a source-data finding (the unit's dates in two tables contradict).",
        "Only key-linked pairs are checkable.",
    ),
    "",
]
if not xtab_order:
    _tcons.append("- No key-linked date pair was available to check.")
for res in xtab_order:
    _tcons.append(
        f"- {res['label']} (`{res['earlier']}` -> `{res['later']}`): "
        f"{res['violations']}/{res['comparable_rows']} out of order "
        f"({res['violation_pct']}%)."
    )

_spatial = [
    para(
        "Cross-table lat/lon reconciliation is NOT possible -- only the einheiten_*",
        "tables carry coordinates (limitation, not a finding). Per-table internal",
        "geographic agreement is in section 01. The one cross-table check:",
    ),
]
if cross_coord:
    _spatial.append(
        f"- units in different carrier tables sharing a LokationMaStRNummer: "
        f"{cross_coord['inconsistent_groups']}/{cross_coord['groups']} shared "
        f"locations resolve to more than one coordinate (2dp); worst holds "
        f"{cross_coord['max_distinct_values_in_a_group']} distinct coordinates."
    )
else:
    _spatial.append(
        "- no carrier table exposed both a LokationMaStRNummer and coordinates."
    )

_coverage = [
    f"Rows per Bronze table: { {d: row_count[d] for d in ALL_DATASETS} }.",
    (
        "marktakteure / netzanschlusspunkte / lokationen are ~5-7M rows each; the generation-unit "
        "and support tables are 10^2-10^5. A model rooted at a generation unit touches a tiny "
        "slice of the market-actor and location tables -- most of those rows are never referenced."
    ),
]

_verdict = []
_fanout = [r for r in ri_results if (r.get("child_max_rows_per_key") or 0) > 1]
_verdict.append(
    f"- Subset relationships with a real orphan problem: "
    f"{[r['child'] for r in _real_problems] or 'none'}."
)
_verdict.append(
    f"- Expected-disjoint / partial-scope relationships (high orphan rate is correct): "
    f"{[r['child'] for r in _expected_orphans] or 'none'}."
)
if _fanout:
    _verdict.append(
        "- Child-side fan-out (one child key on multiple rows) in: "
        + ", ".join(
            f"{r['child']} (max {r['child_max_rows_per_key']})" for r in _fanout
        )
        + " -- these are 1:N and must not be joined as 1:1."
    )
_verdict.append(
    "- Verdict: each table's own-entity `*MastrNummer` (exact ratio above) is the Silver grain "
    "key; cross-table joins use the explicit column pairs above, resolved case-insensitively, "
    "and must be re-validated per MaStR release."
)

_silver = [
    (
        "- Silver join key: the explicit child `*MastrNummer` -> parent `*MastrNummer` pairs above "
        "(NOT inferred from column-name similarity, and NOT case-sensitive)."
    ),
]
if _real_problems:
    _silver.append(
        "- Subset relationships with real orphans "
        f"({[r['child'] for r in _real_problems]}) -> LEFT join with an explicit "
        "unmatched flag; never inner."
    )
if _expected_orphans:
    _silver.append(
        "- Deregistered-entity and deferred-scope logs are disjoint from the live tables "
        "by design -- do not 'fix' their orphan rate by dropping rows; carry them as "
        "separate history facts."
    )
if _fanout:
    _silver.append(
        "- 1:N relationships identified above -> aggregate the child to the parent grain, or "
        "keep it as a separate fact; never fold it into the parent's attribute row."
    )

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                "Root any cross-table model at one entity grain (generation unit = `EinheitMastrNummer`, "
                "or market actor = `MastrNummer`); every join in JOIN_SPEC either holds that grain (1:1) "
                "or drifts it (1:N, flagged above)."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                f"Full cardinality profile per relationship (Referential Integrity section): 1:N in "
                f"{[r['child'] for r in _fanout] or 'none'}, with child-rows-per-parent p50/p90/p99 and "
                "max fan-out quantified. lokationen link arrays (03) are M:N and explode further -- "
                "verify exploded row counts against pre-explosion counts."
            ),
        ),
        (
            "Target contamination",
            (
                "No target across these tables; a decommission/authorisation target drawn from 04/02 must "
                "not be enriched with attributes recorded as a consequence of that same event."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "Key-set membership plus cross-table date ordering (Temporal Consistency section): "
                "commissioning-vs-later-lifecycle-date violations are quantified on the shared "
                "EinheitMastrNummer. Full per-feature date guards still live in 02/04."
            ),
        ),
        (
            "Proxy leakage",
            (
                "Operator identity (`AnlagenbetreiberMastrNummer`), grid connection point, and location "
                "MastrNummer are high-cardinality near-keys that can memorise a specific unit's outcome."
            ),
        ),
        (
            "Split / entity leakage",
            (
                "Split at the ROOT entity of the join chain (unit or actor MastrNummer) so a unit's rows "
                "across all joined tables stay on one side; a per-table row split leaks across joins."
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "The current-state tables carry no validity windows; joining them to a dated event as if "
                "their attributes were true at the event date is point-in-time leakage."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                "The generation-unit population here is what survived to the export; the change-history "
                "tables are the only record of units that left. A joined training set that starts from "
                "current-state units silently excludes the churned population."
            ),
        ),
        (
            "Missingness leakage",
            (
                "Orphan rate per relationship (above) is itself informative -- whether a unit has an EEG "
                "or KWK record correlates with carrier type and support era; an 'is-linked' flag can leak."
            ),
        ),
        (
            "Duplicate-event leakage",
            (
                "Child-side fan-out counts above show which tables repeat a key across rows; de-duplicate "
                "or aggregate before joining so one entity is not counted multiple times across a split."
            ),
        ),
        (
            "Target / feature temporal misalignment",
            (
                "Not resolvable from key sets alone; requires the per-table date columns (02/04) aligned "
                "to a single as-of date."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            "Not applicable at the key-graph level (no numeric measures joined here).",
        ),
        (
            "Data-generation-process leakage",
            (
                "MaStR's own processing columns (Systemstatus, Netzbetreiberpruefung, migration flags) "
                "propagate through every join and encode record-handling, not physical reality."
            ),
        ),
        (
            "Class / label instability",
            (
                "Catalog codes referenced across tables are version-dependent (05) -- pin the release. "
                "The Catalog / Domain Reconciliation section reports any code in 01-04 not resolvable "
                "against the current catalog vintage."
            ),
        ),
        (
            "Label availability lag",
            (
                "Change events are registered after the fact; a clean key match here does not tell you "
                "the event was known at its physical date."
            ),
        ),
        (
            "Source / version / regime change",
            (
                "The 2019 MaStR migration means pre-2019 units carry migrated keys with different "
                "completeness; a registration-era flag should ride along any cross-table feature."
            ),
        ),
        (
            "Sample-vs-full divergence",
            "Every number here is a full distinct/count or a fully collected key set -- no sampling.",
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Entities / Keys", "\n".join(_ent)),
        ("Referential Integrity", "\n".join(_rel)),
        ("Catalog / Domain Reconciliation", "\n".join(_catalog)),
        ("Temporal Consistency", "\n".join(_tcons)),
        ("Spatial Consistency", "\n".join(_spatial)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("EDA Findings", "\n".join(_verdict)),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- REDISPATCH MEASURES
# MAGIC
# MAGIC **Purpose:** Profile the redispatch_measures Bronze table -- schema,
# MAGIC missingness, constant columns, full-row duplicates, categorical
# MAGIC distributions (reason, direction, grid operator, energy carrier),
# MAGIC stricter numeric-column detection (parse yield, not "looks numeric"),
# MAGIC power/energy value plausibility, measure start/end temporal semantics,
# MAGIC observed coverage window, and the layered modelling-risk checklist --
# MAGIC as evidence for Silver design.

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

# DBTITLE 1,Profile -- rows, missingness, approx distinct, constant columns (one agg)
df = spark.table(TABLE)
COLS = df.columns
P = profile_frame(df)
total = P["total"]
acd = P["acd"]
miss = P["miss"]
constant_cols = constant_column_names(df, P)
distinct_rows = total - full_row_dup_count(df, total)
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


# energy ~ mean power x duration. Start / end are built from the date column
# (multi-format parse) plus the clock time (HH:MM read with a pattern), so the
# check is evaluated on every row whose two instants can be built.
def clock_seconds(colname):
    s_ = F.col(colname).cast("string")
    hh = F.regexp_extract(s_, r"^\s*(\d{1,2}):(\d{2})", 1)
    mm = F.regexp_extract(s_, r"^\s*(\d{1,2}):(\d{2})", 2)
    return F.when(F.length(hh) > 0, hh.cast("int") * 3600 + mm.cast("int") * 60)


begin_uhr = next(
    (c for c in COLS if "beginn" in c.lower() and "uhr" in c.lower()), None
)
end_uhr = next((c for c in COLS if "ende" in c.lower() and "uhr" in c.lower()), None)
work_identity, duration_stats = None, None
if mean_c and work_c and begin_c and end_c:
    b_ts = parse_ts_multi(begin_c).cast("long") + (
        F.coalesce(clock_seconds(begin_uhr), F.lit(0)) if begin_uhr else F.lit(0)
    )
    e_ts = parse_ts_multi(end_c).cast("long") + (
        F.coalesce(clock_seconds(end_uhr), F.lit(0)) if end_uhr else F.lit(0)
    )
    base = df.select(
        safe_num(mean_c).alias("mean_mw"),
        safe_num(work_c).alias("work_mwh"),
        ((e_ts - b_ts) / 3600.0).alias("dur_h"),
    )
    duration_stats = (
        base.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.col("dur_h").isNotNull().cast("long")).alias("with_duration"),
            F.sum((F.col("dur_h") > 0).cast("long")).alias("positive"),
            F.sum((F.col("dur_h") == 0).cast("long")).alias("zero"),
            F.sum((F.col("dur_h") < 0).cast("long")).alias("negative"),
            F.expr("percentile_approx(dur_h, array(0.5, 0.9, 0.99))").alias(
                "p50_90_99_h"
            ),
            F.max("dur_h").alias("max_h"),
        )
        .first()
        .asDict()
    )
    duration_stats["basis"] = (
        "date + clock time" if begin_uhr and end_uhr else "date only"
    )
    j = base.where(
        F.col("dur_h").isNotNull()
        & (F.col("dur_h") > 0)
        & F.col("mean_mw").isNotNull()
        & F.col("work_mwh").isNotNull()
    ).withColumn("implied_mwh", F.col("mean_mw") * F.col("dur_h"))
    work_identity = additive_identity_check(
        j, "work_mwh", ["implied_mwh"], rel_tol=0.05, abs_floor=1.0
    )
    print("duration:", duration_stats)
    print("energy vs mean-power x duration:", work_identity)

# COMMAND ----------


# DBTITLE 1,Volume by time -- year, month, weekday and hour of the start


def _sum_work():
    return (
        F.sum(safe_num(work_c)).alias("total_mwh")
        if work_c
        else F.lit(None).alias("total_mwh")
    )


time_volume = {}
if begin_c:
    bts = parse_ts_multi(begin_c)
    hh = clock_seconds(begin_uhr) / 3600 if begin_uhr else None
    for key, k in (
        ("year", F.year(bts)),
        ("month", F.month(bts)),
        ("weekday", F.dayofweek(bts)),
        *((("hour", F.floor(hh).cast("int")),) if hh is not None else ()),
    ):
        rows = (
            df.groupBy(k.alias("k"))
            .agg(F.count(F.lit(1)).alias("measures"), _sum_work())
            .where(F.col("k").isNotNull())
            .orderBy("k")
            .collect()
        )
        time_volume[key] = [
            (
                int(x["k"]),
                x["measures"],
                round(x["total_mwh"], 0) if x["total_mwh"] is not None else None,
            )
            for x in rows
        ]
        print(key, time_volume[key])

# COMMAND ----------


# DBTITLE 1,Volume by reason, direction, fuel type and grid operator


def _by_column(name, top=15):
    rows = (
        df.groupBy(F.col(name).alias("k"))
        .agg(
            F.count(F.lit(1)).alias("measures"),
            _sum_work(),
            F.avg(safe_num(mean_c)).alias("mean_mw")
            if mean_c
            else F.lit(None).alias("mean_mw"),
        )
        .orderBy(F.desc("measures"))
        .limit(top)
        .collect()
    )
    return [
        (
            x["k"],
            x["measures"],
            round(x["total_mwh"], 0) if x["total_mwh"] is not None else None,
            round(x["mean_mw"], 1) if x["mean_mw"] is not None else None,
        )
        for x in rows
    ]


group_cols = [
    c
    for c in COLS
    if any(h in c.lower() for h in ("grund", "richtung", "primaer", "uenb"))
]
volume_by = {c: _by_column(c) for c in group_cols}
reason_c = next((c for c in group_cols if "grund" in c.lower()), None)
dir_c2 = next((c for c in group_cols if "richtung" in c.lower()), None)
reason_direction = (
    [
        (x["r"], x["d"], x["n"])
        for x in df.groupBy(F.col(reason_c).alias("r"), F.col(dir_c2).alias("d"))
        .agg(F.count(F.lit(1)).alias("n"))
        .orderBy(F.desc("n"))
        .limit(20)
        .collect()
    ]
    if reason_c and dir_c2
    else []
)
print({c: len(v) for c, v in volume_by.items()}, reason_direction[:3])

# COMMAND ----------

# DBTITLE 1,Affected-plant name -- structure, spelling variants and repetition
plant_c = next((c for c in COLS if "anlage" in c.lower()), None)
plant_text = None
if plant_c:
    raw = F.trim(F.col(plant_c).cast("string"))
    norm = F.trim(
        F.regexp_replace(F.lower(raw), r"[^a-z0-9\u00e4\u00f6\u00fc\u00df ]", " ")
    )
    norm = F.trim(F.regexp_replace(norm, r"\s+", " "))
    base = df.select(
        raw.alias("raw"),
        norm.alias("norm"),
        *([F.col(f).alias("fuel") for f in COLS if "primaer" in f.lower()][:1]),
    )
    r = (
        base.where(F.col("raw").isNotNull() & (F.col("raw") != ""))
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("raw").alias("distinct_raw"),
            F.countDistinct("norm").alias("distinct_normalised"),
            F.min(F.length("raw")).alias("len_min"),
            F.max(F.length("raw")).alias("len_max"),
            F.avg(F.length("raw")).alias("len_mean"),
            F.sum(F.col("raw").rlike(r"[0-9]").cast("long")).alias("with_digit"),
            F.sum(F.col("raw").rlike(r"[/,;()]").cast("long")).alias("with_separator"),
        )
        .first()
        .asDict()
    )
    top = (
        base.where(F.col("raw").isNotNull() & (F.col("raw") != ""))
        .groupBy("norm")
        .count()
        .orderBy(F.desc("count"))
        .limit(15)
        .collect()
    )
    tokens = (
        base.where(F.col("norm").isNotNull())
        .select(F.explode(F.split("norm", " ")).alias("t"))
        .where(F.length("t") > 2)
        .groupBy("t")
        .count()
        .orderBy(F.desc("count"))
        .limit(20)
        .collect()
    )
    multi_fuel = (
        base.where(F.col("raw").isNotNull() & F.col("fuel").isNotNull())
        .groupBy("norm")
        .agg(F.countDistinct("fuel").alias("fuels"))
        .where(F.col("fuels") > 1)
        .count()
        if "fuel" in base.columns
        else None
    )
    plant_text = {
        **r,
        "top_names": [(x["norm"], x["count"]) for x in top],
        "top_tokens": [(x["t"], x["count"]) for x in tokens],
        "names_with_several_fuel_types": multi_fuel,
    }
    print(plant_text)

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
        .where(F.col("y").between(2010, F.year(F.current_timestamp())))
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
        "- energy vs mean-power x duration: NOT tested -- the mean-power, energy, start or end columns were not all located."
    )
if duration_stats:
    _vcons.append(
        f"- duration built from {duration_stats['basis']}: {duration_stats['with_duration']} of {duration_stats['rows']} rows have a duration; "
        f"{duration_stats['positive']} positive, {duration_stats['zero']} zero, {duration_stats['negative']} negative; "
        f"p50/p90/p99 hours {duration_stats['p50_90_99_h']}, max {duration_stats['max_h']}. The energy check runs on the rows with a positive duration."
    )
if not value_order and not work_identity:
    _vcons = ["- No power/energy columns located by name for a consistency check."]

_patterns = [
    "Number of measures and total energy (MWh) by period of the start (period, measures, MWh):"
]
for key, vs in time_volume.items():
    _patterns.append(f"- by {key}: {vs}")
_patterns.append(
    "Measures, total energy and mean power by category (value, measures, MWh, mean MW):"
)
for c, vs in volume_by.items():
    _patterns.append(f"- `{c}`: {vs}")
if reason_direction:
    _patterns.append(
        f"- reason x direction (reason, direction, measures): {reason_direction}"
    )

_plant = []
if plant_text:
    pt = plant_text
    _plant += [
        para(
            f"`{plant_c}`: {pt['rows']} non-empty rows, {pt['distinct_raw']} distinct spellings, {pt['distinct_normalised']} after lower-casing and removing punctuation",
            f"({pt['distinct_raw'] - pt['distinct_normalised']} spelling variants merged); name length {pt['len_min']}..{pt['len_max']} (mean {pt['len_mean']:.1f});",
            f"{pt['with_digit']} contain a digit, {pt['with_separator']} contain a separator character.",
        ),
        f"- most frequent names (normalised, measures): {pt['top_names']}",
        f"- most frequent words (word, occurrences): {pt['top_tokens']}",
        f"- names appearing with more than one primary energy type: {pt['names_with_several_fuel_types']}",
    ]
else:
    _plant.append("- No affected-plant column located.")

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

_areas = {
    "Domain understanding": [
        "one row per grid measure that changes plant feed-in: reason, direction, start / end, mean and maximum power, total energy, instructing and requesting grid operators, affected plant, primary energy type",
        f"energy vs mean power x duration: {work_identity['violation_pct'] if work_identity else 'not evaluated'}% of {work_identity['comparable_rows'] if work_identity else 0} rows outside 5%",
        f"reasons: {[(k, n) for k, n, _, _ in volume_by.get(reason_c, [])[:4]]}",
    ],
    "Structure and engineering": [
        f"{total} rows, {len(COLS)} columns, all strings in Bronze; German comma decimals; date and clock time in separate columns",
        f"constant columns {constant_cols}",
        f"duration basis: {duration_stats['basis'] if duration_stats else 'n/a'}",
    ],
    "Temporal": [
        f"volume by year: {time_volume.get('year')}",
        f"weekday and hour profile of starts: {time_volume.get('weekday')} / {time_volume.get('hour')}",
        f"measure duration p50/p90/p99 (h): {duration_stats['p50_90_99_h'] if duration_stats else 'n/a'}",
    ],
    "Spatial": [
        "no coordinates; grid operators and plant names are the only location proxies"
    ],
    "Data quality": [
        f"end before start: {inverted}; mean > max power: {[(r_['label'], r_['violations']) for r_ in value_order]}",
        f"affected-plant spelling variants merged by normalisation: {plant_text['distinct_raw'] - plant_text['distinct_normalised'] if plant_text else 'n/a'}",
    ],
    "Statistical patterns": [
        f"energy and power distributions in Distributions; volume by month {time_volume.get('month')}",
    ],
    "Relationships": [
        f"reason x direction: {reason_direction[:4]}",
        f"names with several energy types: {plant_text['names_with_several_fuel_types'] if plant_text else 'n/a'}",
        "no shared key with the plant register or MaStR (name-based only)",
    ],
    "Analytics use": [
        "volumes and energy by reason, operator and plant over 2013-2020; supports operator and cause analysis"
    ],
    "ML use": ["no target in the source; measures are events, plants only by name"],
    "AI / knowledge use": [
        f"free-text plant names: {plant_text['distinct_normalised'] if plant_text else 'n/a'} distinct normalised names, frequent words {[t for t, _ in plant_text['top_tokens'][:8]] if plant_text else 'n/a'}; a candidate for entity resolution against the plant registers",
    ],
}

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
        ("Volume Patterns", "\n".join(_patterns)),
        ("Affected-Plant Names", "\n".join(_plant)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Referential Integrity", "\n".join(_ri)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Observations by Area", area_block(_areas)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- POWER PLANT LIST (BNETZA KRAFTWERKSLISTE)
# MAGIC
# MAGIC **Purpose:** Profile the two power plant list Bronze tables
# MAGIC (power_plant_list, power_plant_capacity_additions) -- schema,
# MAGIC missingness, a structural check that the CSV header was actually applied
# MAGIC (not `Unnamed:_N`), plant-identifier key cardinality, full-row
# MAGIC duplicates, categorical distributions (fuel type, state, status),
# MAGIC capacity-value parse yield and plausibility, commissioning/decommission
# MAGIC dates, and the layered modelling-risk checklist -- as evidence for
# MAGIC Silver design.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "power_plant_list"
NB_KEY = "01_power_plant_list"
SECTION_TITLE = "Power plant list (power_plant_list, power_plant_capacity_additions)"
DATASETS = ["power_plant_list", "power_plant_capacity_additions"]
TABLES = {d: f"{CATALOG}.{BRONZE_SCHEMA}.{d}" for d in DATASETS}
ID_COL_HINTS = ("kraftwerksnummer", "blocknummer", "anlagenkennziffer", "mastrnummer")
CAP_HINTS = ("nettonennleistung", "bruttoleistung", "nennleistung", "leistung", "mw")
DATE_HINTS = ("datum", "inbetriebnahme", "stilllegung")
# A record-type / record-set column (e.g. Datensatztyp) is not a date although the
# name starts with "date".
NOT_DATE_HINTS = ("typ", "satz")


def is_date_col(name):
    n = name.lower()
    if any(h in n for h in NOT_DATE_HINTS):
        return False
    return any(h in n for h in DATE_HINTS) or "date" in _re.split(r"[^a-z]+", n)


STATUS_HINTS = ("status", "kraftwerksstatus")
FUEL_HINTS = ("energietraeger", "brennstoff", "primaerenergie")

# COMMAND ----------

# DBTITLE 1,Structural check -- was the CSV header actually applied?
frames = {d: spark.table(t) for d, t in TABLES.items()}
struct = {}
for name, df in frames.items():
    cols = df.columns
    unnamed = [c for c in cols if c.lower().startswith(("unnamed", "_c", "col"))]
    # a "column name" that is really a sentence / footnote (very long, spaces,
    # date-like, or starts with a symbol) -> the header row was not the header
    junk_named = [
        c
        for c in cols
        if len(c) > 45
        or c.strip().startswith(("*", "(", "["))
        or "beachten" in c.lower()
        or "wetterdienst" in c.lower()
    ]
    struct[name] = {
        "cols": len(cols),
        "unnamed": unnamed,
        "junk_named": junk_named,
        "broken": len(unnamed) > 0.2 * len(cols) or bool(junk_named),
    }
    print(
        f"{name}: {len(cols)} cols, unnamed={len(unnamed)}, junk-named={junk_named}, "
        f"header-applied={not struct[name]['broken']}"
    )

# COMMAND ----------

# DBTITLE 1,Profile each table -- rows, missingness, approx distinct (one agg per table)
prof = {}
for name, df in frames.items():
    prof[name] = profile_frame(df)
    cols, total = prof[name]["cols"], prof[name]["total"]
    acd, miss = prof[name]["acd"], prof[name]["miss"]
    print("=" * 90, f"\n{name}  rows={total}  cols={len(cols)}")
    for c in cols:
        print(
            f"  {c[:44]:<44} missing={miss[c]:>8} rate={miss[c] / total if total else 0:.4f} approx_distinct={acd[c]}"
        )

# COMMAND ----------

# DBTITLE 1,Confirm constant columns exactly + full-row duplicates
for name, df in frames.items():
    prof[name]["constant"] = constant_column_names(df, prof[name])
    prof[name]["dups"] = full_row_dup_count(df, prof[name]["total"])
    print(
        f"{name}: constant={prof[name]['constant']}  full-row duplicates={prof[name]['dups']}"
    )

# COMMAND ----------

# DBTITLE 1,Plant-identifier key candidates (exact distinct)
id_key = {}
for name, df in frames.items():
    cands = [c for c in df.columns if any(h in c.lower() for h in ID_COL_HINTS)]
    id_key[name] = exact_uniqueness(df, cands)
    print(f"{name}: id-like keys = {id_key[name]}")

# COMMAND ----------

# DBTITLE 1,Semantic validation -- capacity parse yield + plausibility, dates, status
sem = {}
for name, df in frames.items():
    cols = df.columns
    entry = {}
    cap = next((c for c in cols if any(h in c.lower() for h in CAP_HINTS)), None)
    if cap:
        npar = numeric_parseability(df, cap)
        entry["capacity"] = {"column": cap, "parse": npar}
        if npar["is_numeric"]:
            entry["capacity"]["plausibility"] = plausibility(
                df, cap, lo=0.0, sentinels=()
            )
        print(
            f"{name}.{cap}: numeric yield={npar['yield']} is_numeric={npar['is_numeric']}"
        )
    dcols = [c for c in cols if is_date_col(c)]
    # BNetzA records commissioning / retirement as a YEAR ("2009"), not a date --
    # column name carries "Jahr". Profile those as integer years, the rest as
    # timestamps.
    entry["dates"] = {}
    entry["years"] = {}
    for c in [c for c in dcols if "jahr" in c.lower()][:2]:
        pl = plausibility(df, c, lo=1850.0, hi=2035.0, sentinels=())
        npar = numeric_parseability(df, c, decimal_comma=False)
        entry["years"][c] = {"parse": npar, "plausibility": pl}
        print(
            f"  {name}.{c} (year): yield {npar['yield']:.1%}, range "
            f"{pl['min']}..{pl['max']}, below 1850={pl.get('below')}, above 2035={pl.get('above')}"
        )
    for c in [c for c in dcols if "jahr" not in c.lower()][:4]:
        ts = timestamp_semantics(df, c, valid_from="1900-01-01", tz="Europe/Berlin")
        entry["dates"][c] = ts
        for ln in ts["lines"]:
            print(f"  {name}.{c}: {ln}")
    sem[name] = entry

# COMMAND ----------


# DBTITLE 1,Null / blank helper


def is_null_or_blank(colname):
    return qcol(colname).isNull() | (F.trim(qcol(colname).cast("string")) == "")


# COMMAND ----------

# DBTITLE 1,Record type -- what each Datensatztyp value is
rec = {}
for name, df in frames.items():
    tc = next((c for c in df.columns if "datensatztyp" in c.lower()), None)
    if not tc:
        continue
    cap_c = sem[name].get("capacity", {}).get("column")
    id_c = next((c for c in df.columns if "mastrnummer" in c.lower()), None)
    yr_c = next((c for c in df.columns if "inbetriebnahme" in c.lower()), None)
    bl_c = next((c for c in df.columns if "bundesland" in c.lower()), None)
    tot_cap = df.agg(F.sum(safe_num(cap_c))).first()[0] if cap_c else None
    aggs = [F.count(F.lit(1)).alias("rows")]
    if id_c:
        aggs += [
            F.sum(is_null_or_blank(id_c).cast("long")).alias("id_missing"),
            F.countDistinct(F.col(id_c)).alias("ids"),
        ]
    if cap_c:
        v = safe_num(cap_c)
        aggs += [
            F.sum(v).alias("cap_sum"),
            F.min(v).alias("cap_min"),
            F.max(v).alias("cap_max"),
            F.avg(v).alias("cap_mean"),
        ]
    if yr_c:
        aggs += [
            F.min(safe_num(yr_c)).alias("yr_min"),
            F.max(safe_num(yr_c)).alias("yr_max"),
        ]
    if bl_c:
        aggs.append(F.sum(is_null_or_blank(bl_c).cast("long")).alias("state_missing"))
    rows = (
        df.groupBy(F.col(tc).alias("type")).agg(*aggs).orderBy(F.desc("rows")).collect()
    )
    rec[name] = {
        "column": tc,
        "capacity_column": cap_c,
        "total_capacity": tot_cap,
        "types": [x.asDict() for x in rows],
    }
    for x in rec[name]["types"]:
        print(name, x)

# COMMAND ----------

# DBTITLE 1,Largest capacity rows and total-like rows
top_rows, total_like = {}, {}
for name, df in frames.items():
    cap_c = sem[name].get("capacity", {}).get("column")
    if not cap_c:
        continue
    name_cols = [
        c
        for c in df.columns
        if any(h in c.lower() for h in ("name", "bezeichnung", "kraftwerk"))
        and "status" not in c.lower()
        and "nummer" not in c.lower()
    ][:2]
    keep = [
        c
        for c in df.columns
        if "datensatztyp" in c.lower() or "mastrnummer" in c.lower()
    ] + name_cols
    keep += [c for c in df.columns if "energietraeger" in c.lower()][:1]
    rows = (
        df.select(*keep, safe_num(cap_c).alias("__cap"))
        .orderBy(F.desc("__cap"))
        .limit(10)
        .collect()
    )
    top_rows[name] = [
        {k: (str(v)[:60] if v is not None else None) for k, v in x.asDict().items()}
        for x in rows
    ]
    pattern = r"(?i)summe|gesamt|insgesamt|total|kleinanlagen|aggregi"
    cond = F.lit(False)
    for c in [c for c in df.columns if c in keep]:
        cond = cond | F.col(c).cast("string").rlike(pattern)
    total_like[name] = df.where(cond).count()
    print(name, total_like[name], top_rows[name][:3])

# COMMAND ----------

# DBTITLE 1,Identifier cardinality -- nulls, repeats and what differs between repeated rows
id_card = {}
for name, df in frames.items():
    id_c = next((c for c in df.columns if "mastrnummer" in c.lower()), None)
    if not id_c:
        continue
    tc = next((c for c in df.columns if "datensatztyp" in c.lower()), None)
    blank = is_null_or_blank(id_c)
    present = df.where(~blank)
    per = present.groupBy(qcol(id_c).alias("id")).agg(
        F.count(F.lit(1)).alias("n"),
        *[F.countDistinct(qcol(c)).alias(f"d{i}") for i, c in enumerate(df.columns)],
    )
    sizes = per.groupBy("n").count().orderBy("n").collect()
    rep = per.where(F.col("n") > 1)
    r = (
        rep.agg(
            F.count(F.lit(1)).alias("groups"),
            *[
                F.sum((F.col(f"d{i}") > 1).cast("long")).alias(f"v{i}")
                for i, c in enumerate(df.columns)
            ],
        )
        .first()
        .asDict()
    )
    vary = {c: r[f"v{i}"] for i, c in enumerate(df.columns) if r[f"v{i}"]}
    show = [c for c in [id_c, tc, *df.columns[:8]] if c]
    examples = []
    for x in rep.orderBy(F.desc("n")).limit(2).collect():
        rows = (
            present.where(qcol(id_c) == x["id"])
            .select(*[qcol(c) for c in show])
            .limit(6)
            .collect()
        )
        examples.append(
            [
                {
                    k: (str(v)[:40] if v is not None else None)
                    for k, v in y.asDict().items()
                }
                for y in rows
            ]
        )
    missing_by_type = (
        [
            (x["t"], x["n"])
            for x in df.where(blank)
            .groupBy(qcol(tc).alias("t"))
            .agg(F.count(F.lit(1)).alias("n"))
            .collect()
        ]
        if tc
        else []
    )
    id_card[name] = {
        "column": id_c,
        "rows": prof[name]["total"],
        "missing_or_blank": df.where(blank).count(),
        "missing_by_type": missing_by_type,
        "distinct": per.count(),
        "repeat_sizes": [(x["n"], x["count"]) for x in sizes],
        "groups_repeated": r["groups"],
        "columns_varying_within_repeats": vary,
        "examples": examples,
    }
    print(name, {k: v for k, v in id_card[name].items() if k != "examples"})

# COMMAND ----------

# DBTITLE 1,Footnote and free-text values inside data columns
FOOT_PATTERN = r"(?i)^\s*[\(\*\[]|beachten|unsicherheit|quelle|anmerkung|hinweis|davon"
foot = {}
for name, df in frames.items():
    str_cols = [c for c, t in df.dtypes if t == "string"]
    exprs = []
    for i, c in enumerate(str_cols):
        v = F.col(c).cast("string")
        exprs.append(
            F.sum((v.rlike(FOOT_PATTERN) | (F.length(v) > 80)).cast("long")).alias(
                f"f{i}"
            )
        )
    r = df.agg(*exprs).first().asDict() if exprs else {}
    foot[name] = {}
    for i, c in enumerate(str_cols):
        if r.get(f"f{i}"):
            ex = (
                df.where(
                    F.col(c).cast("string").rlike(FOOT_PATTERN)
                    | (F.length(F.col(c).cast("string")) > 80)
                )
                .select(F.substring(F.col(c).cast("string"), 1, 110).alias("v"))
                .distinct()
                .limit(5)
                .collect()
            )
            foot[name][c] = {"rows": r[f"f{i}"], "examples": [x["v"] for x in ex]}
    print(name, {c: v["rows"] for c, v in foot[name].items()})

add_rows = [
    [str(v)[:90] if v is not None else None for v in x]
    for x in frames["power_plant_capacity_additions"].limit(40).collect()
]
print(len(add_rows))

# COMMAND ----------

# DBTITLE 1,Categorical / domain validation -- Bundesland + fuel type
GERMAN_STATES = {
    "Baden-Württemberg",
    "Bayern",
    "Berlin",
    "Brandenburg",
    "Bremen",
    "Hamburg",
    "Hessen",
    "Mecklenburg-Vorpommern",
    "Niedersachsen",
    "Nordrhein-Westfalen",
    "Rheinland-Pfalz",
    "Saarland",
    "Sachsen",
    "Sachsen-Anhalt",
    "Schleswig-Holstein",
    "Thüringen",
}


def _norm_state(s):
    # BNetzA concatenates and transliterates ("Baden-Württemberg" ->
    # "BadenWuerttemberg") -- collapse both sides to a comparable form.
    s = str(s).lower()
    for a, b in (("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss")):
        s = s.replace(a, b)
    return _re.sub(r"[^a-z]", "", s)


domain_checks = {}
for name, df in frames.items():
    cols = df.columns
    bl = next((c for c in cols if "bundesland" in c.lower()), None)
    fu = next((c for c in cols if any(h in c.lower() for h in FUEL_HINTS)), None)
    if bl:
        domain_checks[f"{name}.{bl}"] = categorical_domain(
            df, bl, GERMAN_STATES, name=f"{name}.{bl}", normalize=_norm_state
        )
    if fu:
        # no authoritative fuel list -- report the distinct set for manual review
        vals = [
            x[0]
            for x in df.select(F.col(f"`{fu}`").cast("string"))
            .distinct()
            .limit(60)
            .collect()
            if x[0] is not None
        ]
        domain_checks[f"{name}.{fu}"] = {
            "column": f"{name}.{fu}",
            "distinct_values": sorted(vals),
        }
for k, v in domain_checks.items():
    print(k, v)

# COMMAND ----------

# DBTITLE 1,Temporal consistency -- commissioning must not post-date decommissioning
date_order = {}
for name, df in frames.items():
    cols = df.columns
    com = next((c for c in cols if "inbetriebnahme" in c.lower()), None)
    dec = next(
        (c for c in cols if "stilllegung" in c.lower() or "stillleg" in c.lower()), None
    )
    if com and dec:
        # year columns -> integer comparison; real date columns -> parsed compare
        if "jahr" in com.lower() and "jahr" in dec.lower():
            res = numeric_order_check(df, com, dec)
            res["label"] = f"{name}: commissioning year <= decommissioning year"
            res["earlier"], res["later"] = com, dec
        else:
            res = date_order_check(
                df, com, dec, label=f"{name}: commissioning <= decommissioning"
            )
        date_order[name] = res
        print(date_order[name])

# COMMAND ----------

# DBTITLE 1,Relationship cardinality -- power_plant_list <-> power_plant_capacity_additions
ppl, add = frames["power_plant_list"], frames["power_plant_capacity_additions"]
shared_key = None
for c in ppl.columns:
    if any(h in c.lower() for h in ID_COL_HINTS):
        m = next((cc for cc in add.columns if cc.lower() == c.lower()), None)
        if m:
            shared_key = (c, m)
            break
ppl_add_card = None
if shared_key:
    ck, pk = shared_key
    child_keys = collect_key_set(add, ck)
    parent_keys = collect_key_set(ppl, pk)
    ppl_add_card = cardinality_profile(add, ck, child_keys, parent_keys)
    print(f"additions.{ck} -> power_plant_list.{pk}: {ppl_add_card}")
else:
    print("no shared plant-identifier column between the two tables")

# COMMAND ----------

# DBTITLE 1,Regime evidence -- commissioning decade x status count (edition regime is not in one snapshot)
regime = {}
for name, df in frames.items():
    com = next((c for c in df.columns if "inbetriebnahme" in c.lower()), None)
    st = next(
        (c for c in df.columns if any(h in c.lower() for h in STATUS_HINTS)), None
    )
    if not com:
        continue
    year = safe_num(com) if "jahr" in com.lower() else F.year(parse_ts_multi(com))
    decade = (F.floor(year / 10) * 10).cast("int")
    g = (
        df.withColumn("__decade", decade)
        .where(F.col("__decade").isNotNull())
        .groupBy("__decade")
        .agg(
            F.count(F.lit(1)).alias("plants"),
            *([F.countDistinct(F.col(st)).alias("status_variants")] if st else []),
        )
        .orderBy("__decade")
        .collect()
    )
    regime[name] = [x.asDict() for x in g]
    print(f"{name} by commissioning decade:", regime[name])

# COMMAND ----------

# DBTITLE 1,Low-cardinality categorical distributions
categorical_dist = categorical_distributions(frames, prof, max_distinct=60)

# COMMAND ----------

# DBTITLE 1,Figures
figs = []
if facet_bars(
    {
        "rows per table": [(d, prof[d]["total"]) for d in DATASETS],
        "columns per table": [(d, len(prof[d]["cols"])) for d in DATASETS],
        "full-row duplicates": [(d, prof[d]["dups"]) for d in DATASETS],
        "unnamed columns (header not applied)": [
            (d, len(struct[d]["unnamed"])) for d in DATASETS
        ],
    },
    "Power plant list -- overview",
    "power_plant_list_overview.png",
    rot=30,
    ncols=2,
):
    figs.append(("Power plant list -- overview", "power_plant_list_overview.png"))
if facet_bars(
    {
        d: (next(iter(categorical_dist[d].values())) if categorical_dist[d] else [])
        for d in DATASETS
    },
    "Power plant list -- first categorical column per table",
    "power_plant_list_categorical.png",
    rot=45,
    ncols=2,
):
    figs.append(
        (
            "Power plant list -- first categorical column per table",
            "power_plant_list_categorical.png",
        )
    )

# COMMAND ----------


# DBTITLE 1,Number formatting helper


def fmt_val(x):
    if x is None:
        return "-"
    return f"{x:.4g}" if isinstance(x, float) else str(x)


# COMMAND ----------

# DBTITLE 1,Findings
for d in DATASETS:
    print(
        f"{d}: rows={prof[d]['total']}, cols={len(prof[d]['cols'])}, "
        f"header_applied={not struct[d]['broken']}, constant={prof[d]['constant']}, "
        f"dups={prof[d]['dups']}"
    )

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/power_plant_list.md
_broken = [d for d in DATASETS if struct[d]["broken"]]

_profile = [
    "| table | rows | cols | header applied | constant columns |",
    "|---|---|---|---|---|",
]
for d in DATASETS:
    _profile.append(
        f"| {d} | {prof[d]['total']} | {len(prof[d]['cols'])} | "
        f"{not struct[d]['broken']} | {', '.join(prof[d]['constant']) or '-'} |"
    )

_struct = [
    para(
        "The BNetzA Kraftwerksliste CSV carries several title / disclaimer rows above the real",
        "header and a block of footnotes below the data. If staging skips the wrong number of",
        "rows, Bronze columns come out as `Unnamed:_N` and footnote text lands as data.",
    ),
]
for d in DATASETS:
    s = struct[d]
    _struct.append(
        f"- {d}: {len(s['unnamed'])} unnamed columns, "
        f"{len(s['junk_named'])} footnote-named columns "
        f"({s['junk_named'][:2]}) -> header {'NOT applied' if s['broken'] else 'applied'}."
    )
if _broken:
    _struct.append(
        para(
            f"-> BLOCKED for {_broken}: this is an INGESTION defect, not a source finding.",
            "Fix in `scripts/ingestion/_energy_commerce_formats.py` (header auto-detection +",
            "footnote strip are now in place), re-download the BNetzA file, re-stage, re-upload",
            "to the Volume and re-run Bronze before trusting any column.",
        )
    )
else:
    _struct.append("-> Header applied correctly in the current Bronze tables.")
_foot_cols = {
    d: {c: v["rows"] for c, v in foot[d].items()} for d in DATASETS if foot.get(d)
}
if _foot_cols:
    _struct.append(
        para(
            "However, footnote-like or long free-text values sit inside data columns (a header check does not see them):",
            f"{_foot_cols}. The staging step that skips title and footnote rows did not remove every footnote line.",
        )
    )

_dq = ["Full-row exact duplicates per table:"]
for d in DATASETS:
    _dq.append(f"- {d}: {prof[d]['dups']}")

_entities = [
    "Plant-identifier key candidates (column: distinct / ratio-to-rows / unique):"
]
for d in DATASETS:
    if id_key[d]:
        for c, u in id_key[d].items():
            _entities.append(
                f"- {d}.`{c}`: {u['distinct']} / {u['ratio']} / unique={u['unique']}"
            )
    else:
        _entities.append(
            f"- {d}: no plant-identifier column found by name"
            + (" (expected -- the header is broken)" if struct[d]["broken"] else "")
            + "."
        )

_unit = []
for d in DATASETS:
    e = sem[d]
    if "capacity" in e:
        cap = e["capacity"]
        if cap["parse"]["is_numeric"]:
            pl = cap["plausibility"]
            _unit.append(
                f"- {d}.`{cap['column']}` (capacity, MW): parse yield {cap['parse']['yield']:.1%}, "
                f"range {pl['min']}..{pl['max']}, zero {pl['zero']}, negative {pl['negative']} "
                "-- confirm MW vs kW and the German-comma decimal on the Silver cast."
            )
        else:
            _unit.append(
                f"- {d}.`{cap['column']}`: only {cap['parse']['yield']:.1%} numeric -- either a "
                "text column or (if the header is broken) a mislabelled column."
            )
    for c, ts in e.get("dates", {}).items():
        _unit.append(
            f"- {d}.`{c}` (date): parse yield {ts['yield']:.1%}, range {ts['min_ts']}..{ts['max_ts']}, "
            f"pre-1900={ts['before_valid']}, future-dated={ts['future']}, formats={ts['per_format']}."
        )
    for c, yr in e.get("years", {}).items():
        pl = yr["plausibility"]
        _unit.append(
            f"- {d}.`{c}` (year, not a date): numeric yield {yr['parse']['yield']:.1%}, "
            f"range {pl['min']}..{pl['max']}, below 1850={pl.get('below')}, "
            f"above 2035={pl.get('above')} -- cast to an integer year, not a timestamp."
        )
if not _unit:
    _unit.append("- No capacity or date column located by name.")

_temporal = []
for d in DATASETS:
    for c, ts in sem[d].get("dates", {}).items():
        _temporal.append(
            f"- {d}.`{c}`: {ts['min_ts']}..{ts['max_ts']} (Europe/Berlin). "
            "Planned future dates are recorded ahead of time, so the column is only "
            "'known' up to its own value."
        )
    for c in sem[d].get("years", {}):
        _temporal.append(
            f"- {d}.`{c}`: year granularity only (no month/day) -- a within-year ordering "
            "cannot be established; the commissioning<=decommissioning check runs on the year."
        )
if not _temporal:
    _temporal.append("- No date/year column located; temporal semantics not assessed.")

_rec = []
for name, rc in rec.items():
    _rec.append(
        f"{name}.`{rc['column']}` -- rows, ids (missing / distinct), capacity `{rc['capacity_column']}` (sum / min / max / mean, MW), commissioning year range, rows without state, per value:"
    )
    for x in rc["types"]:
        share = (
            (x.get("cap_sum") or 0) / rc["total_capacity"]
            if rc["total_capacity"]
            else None
        )
        _rec.append(
            f"- `{x['type']}`: {x['rows']} rows; ids {x.get('id_missing')} missing / {x.get('ids')} distinct; capacity sum {fmt_val(x.get('cap_sum'))} "
            f"({fmt_val(share)} of the table total), min {fmt_val(x.get('cap_min'))}, max {fmt_val(x.get('cap_max'))}, mean {fmt_val(x.get('cap_mean'))}; "
            f"years {fmt_val(x.get('yr_min'))}..{fmt_val(x.get('yr_max'))}; rows without state {x.get('state_missing')}."
        )
for name, rows in top_rows.items():
    _rec.append(f"Ten largest capacity rows of {name}: {rows}")
    _rec.append(
        f"Rows whose text columns contain a total- or aggregate-like word: {total_like[name]}."
    )
_rec.append(
    para(
        "Rows of an aggregated record type describe groups of small plants, not plants: their capacity must not be",
        "counted as plant capacity or joined to unit-level tables as if they were single units.",
    )
)

_idc = []
for name, ic in id_card.items():
    _idc.append(
        f"{name}.`{ic['column']}`: {ic['rows']} rows, {ic['missing_or_blank']} missing or blank (by record type: {ic['missing_by_type']}), {ic['distinct']} distinct values."
    )
    _idc.append(
        f"- ids by number of rows they appear on (rows per id, ids): {ic['repeat_sizes']}."
    )
    _idc.append(
        f"- columns whose values differ between the rows of a repeated id (column, ids affected): {ic['columns_varying_within_repeats'] or 'none'}."
    )
    for ex in ic["examples"]:
        _idc.append(f"- example rows of one repeated id: {ex}")

_footnotes = []
for name, cols_ in foot.items():
    for c, v in cols_.items():
        _footnotes.append(
            f"- {name}.`{c}`: {v['rows']} rows match; examples {v['examples']}"
        )
if not _footnotes:
    _footnotes.append("- No footnote-like or over-long values found in string columns.")
_footnotes.append(
    f"power_plant_capacity_additions in full ({len(add_rows)} rows, each row is a list of cell values):"
)
_footnotes += [f"  - {r_}" for r_ in add_rows]

_domain = []
for k, v in domain_checks.items():
    if "distinct_values" in v:
        _domain.append(
            f"- `{k}` (fuel type -- no authoritative reference list): {len(v['distinct_values'])} "
            f"distinct values, e.g. {v['distinct_values'][:15]}. Reconcile against the MaStR energy "
            "carrier catalog at Silver."
        )
    else:
        _domain.append(
            f"- `{k}` vs the 16 German states (spelling normalised -- BNetzA writes "
            f"`BadenWuerttemberg` etc): unexpected={v['unexpected'] or 'none'}, "
            f"unused={v['unused_allowed'] or 'none'}. A remaining unexpected value is a real "
            "anomaly (a foreign location, a code, or a parse error)."
        )
if not _domain:
    _domain.append("- No Bundesland or fuel-type column located by name.")

_tcons = ["Commissioning must not post-date decommissioning:"]
if date_order:
    for name, res in date_order.items():
        _tcons.append(
            f"- {res['label']} (`{res['earlier']}` -> `{res['later']}`): "
            f"{res['violations']}/{res['comparable_rows']} out of order ({res['violation_pct']}%)."
        )
else:
    _tcons.append(
        "- No table exposed both a commissioning and a decommissioning date column."
    )

_card = ["power_plant_list <-> power_plant_capacity_additions cardinality:"]
if ppl_add_card:
    ck, pk = shared_key
    c = ppl_add_card
    _card.append(
        f"- join key `additions.{ck}` -> `power_plant_list.{pk}`: {c['child_rows']} addition rows "
        f"across {c['distinct_child_keys']} distinct plant keys; {c['matched_parent_keys']} of "
        f"{c['parent_keys_total']} plants have >=1 planned addition "
        f"({c['parent_keys_referenced_pct']}%); additions per plant p50/p90/p99 "
        f"{c['child_rows_per_parent_p50']}/{c['child_rows_per_parent_p90']}/"
        f"{c['child_rows_per_parent_p99']}, max {c['max_fanout']}; orphan addition keys "
        f"{c['orphan_child_keys']}."
    )
    _card.append(
        "-> a plant with several planned additions makes the join 1:N; aggregate to the plant or "
        "keep additions as a separate planning fact."
    )
else:
    _card.append(
        "- LIMITATION: no shared plant-identifier column between the two Bronze tables -- the "
        "relationship cannot be measured here; it needs a name/id reconciliation at Silver. "
        "The join to MaStR is likewise a name-based subset (large plants only), not a keyed 1:1."
    )

_regime = [
    para(
        "The BNetzA coal-exit editions (KVBG 2020+) are a real regime change, but",
        "this Bronze table is a single quarterly snapshot -- there is no edition",
        "axis in the data to split on (LIMITATION). The available proxy: plant",
        "count and status-code variety by commissioning decade.",
    ),
    "",
]
for name, rows in regime.items():
    _regime.append(f"- {name}: {rows}")
if not regime:
    _regime.append("- no commissioning-date column to bucket on.")

_coverage = [
    para(
        f"power_plant_list is {prof['power_plant_list']['total']} rows -- ONLY plants above the",
        "BNetzA reporting threshold (~10 MW net) plus aggregated small-plant rows; it is not the",
        "full German fleet. MaStR (26k+ generation units) is the complete register.",
    ),
    para(
        "It is a single quarterly snapshot -- a plant decommissioned last quarter may already be",
        "gone or flagged; treat status as current-as-of the export, not a history.",
    ),
    para(
        "power_plant_capacity_additions is a forward-looking planning summary (2026-2029), not an",
        "observed event log -- its rows are projections that get revised each edition.",
    ),
]

_dist = []
for d in DATASETS:
    for c, pairs in categorical_dist[d].items():
        _dist.append(f"- {d}.`{c}`: " + fmt_pairs(pairs, n=15))

_findings_md = "\n".join(
    f"- {d}: rows={prof[d]['total']}, header_applied={not struct[d]['broken']}, "
    f"constant={prof[d]['constant']}, dups={prof[d]['dups']}"
    for d in DATASETS
)

_silver = []
if _broken:
    _silver.append(
        f"- BLOCKED: {_broken} columns are `Unnamed:` / footnote-named -> fix staging and re-run "
        "Bronze before any Silver work."
    )
if any(prof[d]["dups"] for d in DATASETS):
    _silver.append("- Exact duplicate rows exist -> de-duplicate on load.")
_silver += [
    (
        "- Cast capacity to double with an explicit MW unit and German-comma decimal rule; "
        "quarantine values that fail."
    ),
    (
        "- power_plant_capacity_additions is a projection summary -> model as a Silver "
        "planning/forecast fact, never merged into current-state plant attributes."
    ),
    (
        "- Reconcile the plant identifier to a MaStR unit id at Silver -- power_plant_list covers "
        "only large plants, so the join is a subset, not a 1:1."
    ),
]

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                f"One row per reportable plant/block (plus aggregated small-plant rows). "
                f"header_applied={ {d: not struct[d]['broken'] for d in DATASETS} } -- a broken header means "
                "the grain and every column are unreliable."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            (
                "power_plant_list <-> power_plant_capacity_additions cardinality is not confirmed here; a "
                "plant with multiple planned additions makes it 1:N. Join to MaStR is a subset (large "
                "plants only), so a left join from MaStR leaves most units unmatched."
            ),
        ),
        (
            "Target contamination",
            (
                "Candidate targets: plant retirement (from status), planned capacity change. The "
                "decommission date / status AT EXPORT reflects decisions already made -- a retirement "
                "model must use the plant's state as of an earlier cutoff, not the snapshot."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "Commissioning/decommission dates and planned-addition years are the only time signal; a "
                "planned date is only knowable from its own value onward."
            ),
        ),
        (
            "Proxy leakage",
            (
                "Kraftwerksnummer / operator / exact location are near-unique identifiers -- a model given "
                "them memorises the specific plant."
            ),
        ),
        (
            "Split / entity leakage",
            (
                "Split by plant identifier (or operator) -- blocks of the same plant share attributes and "
                "must stay on one side."
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "Single snapshot, no validity windows -- a plant's attributes at an earlier date cannot be "
                "reconstructed from this table; use MaStR's change history for that."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                "Only plants above the reporting threshold; only the current quarter. A fleet-level model "
                "built from this table ignores small plants entirely and cannot see historical retirements."
            ),
        ),
        (
            "Missingness leakage",
            (
                "Whether a field (e.g. decommission date, KWK flag) is populated correlates with plant "
                "status and fuel -- check before an 'is-missing' feature."
            ),
        ),
        (
            "Duplicate-event leakage",
            (
                f"Full-row duplicates: { {d: prof[d]['dups'] for d in DATASETS} } -- de-duplicate before "
                "treating the plant identifier as a key."
            ),
        ),
        (
            "Target / feature temporal misalignment",
            (
                "Commissioning date vs the planned-addition year (in the other table) are on different "
                "clocks -- align to one as-of date before pairing."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            (
                "Capacity unit (MW vs kW) and decimal convention unconfirmed. Net vs gross capacity "
                "columns are near-collinear -- do not use both as independent features."
            ),
        ),
        (
            "Data-generation-process leakage",
            (
                "'Datensatztyp' (single plant vs aggregated small-plant vs decommissioned) is a "
                "record-classification field, not a physical property -- a feature keyed on it encodes how "
                "BNetzA compiled the list."
            ),
        ),
        (
            "Class / label instability",
            (
                "Kraftwerksstatus categories and the KVBG/EnWG legal-basis labels change with each coal-exit "
                "amendment -- a class defined by a raw status is only stable within one edition."
            ),
        ),
        (
            "Label availability lag",
            (
                "A retirement is reflected in the list a quarter or more after the decision -- a real-time "
                "model cannot assume the status is current."
            ),
        ),
        (
            "Source / version / regime change",
            (
                "The coal-exit law (KVBG 2020) and its auction rounds reshaped the status taxonomy and the "
                "planned-shutdown pipeline -- do not pool editions from before and after without an edition "
                "indicator."
            ),
        ),
        (
            "Sample-vs-full divergence",
            (
                "Every statistic here is a full Spark aggregation or `.distinct().count()` -- no sampling. "
                "Numeric parse yield is reported per column; check it before building a regression target."
            ),
        ),
    ]
)

_agg_types = {
    n: [
        x
        for x in rc["types"]
        if any(h in str(x["type"]).lower() for h in ("aggreg", "klein"))
    ]
    for n, rc in rec.items()
}
_areas = {
    "Domain understanding": [
        "the register of German power plants above the reporting threshold plus aggregated small plants, with status, technology, fuel, feed-in type and commissioning / decommissioning years; a second table gives expected capacity changes",
        f"record types: { {n: [(x['type'], x['rows']) for x in rc['types']] for n, rc in rec.items()} }",
        f"aggregated-type rows: { {n: [(x['type'], x['rows'], fmt_val(x.get('cap_sum'))) for x in v] for n, v in _agg_types.items()} }",
    ],
    "Structure and engineering": [
        f"header applied: { {d: not struct[d]['broken'] for d in DATASETS} }; footnote-like values inside data columns: {_foot_cols or 'none'}",
        f"identifier: { {n: (ic['column'], ic['missing_or_blank'], ic['distinct']) for n, ic in id_card.items()} } (column, missing, distinct)",
        "no shared key with the second table or with MaStR",
    ],
    "Temporal": [
        "years (not dates) for commissioning and decommissioning; a snapshot without an edition axis",
        f"order check: { {k: (v['violations'], v.get('comparable_rows')) for k, v in date_order.items()} }",
    ],
    "Spatial": [
        f"state and country columns only; foreign plants present: {[x for x in categorical_dist['power_plant_list'].get('Land', [])[1:]]}",
    ],
    "Data quality": [
        f"footnote text in values: {_foot_cols or 'none'}",
        f"ids repeated on several rows: { {n: ic['repeat_sizes'] for n, ic in id_card.items()} }; columns that differ between repeats: { {n: ic['columns_varying_within_repeats'] for n, ic in id_card.items()} }",
    ],
    "Statistical patterns": [
        f"capacity by record type: { {n: [(x['type'], fmt_val(x.get('cap_mean')), fmt_val(x.get('cap_max'))) for x in rc['types']] for n, rc in rec.items()} } (type, mean, max)"
    ],
    "Relationships": [
        f"additions to plant list: {ppl_add_card if ppl_add_card else 'no shared identifier'}"
    ],
    "Analytics use": [
        "capacity and status by technology, fuel and state; the aggregated small-plant rows must be separated from plant rows"
    ],
    "ML use": ["no target; plant-level identifiers and status are near-unique"],
    "AI / knowledge use": [
        "technology, fuel and status vocabularies (see Categorical); no free text except footnote lines that leaked into the data"
    ],
}

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Structural Integrity", "\n".join(_struct)),
        ("Data Quality", "\n".join(_dq)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Unit & Semantic Validation", "\n".join(_unit)),
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Record Types and Aggregate Rows", "\n".join(_rec)),
        ("Identifier Cardinality", "\n".join(_idc)),
        ("Footnote and Free-text Values", "\n".join(_footnotes)),
        ("Temporal Semantics", "\n".join(_temporal)),
        ("Temporal Consistency", "\n".join(_tcons)),
        ("Relationship Cardinality", "\n".join(_card)),
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- POWER PLANT (COMBINED-CYCLE SENSOR READINGS, SAMPLES)
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

# DBTITLE 1,Discover files
entries, kinds, frames = discover_files(ROOT)

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
prof = profile_frames_without_file(DFS)

# COMMAND ----------

# DBTITLE 1,Rows per source file
per_file = {}
for k, df in DFS.items():
    rows = df.groupBy("__file").count().orderBy("__file").collect()
    per_file[k] = [(r["__file"], r["count"]) for r in rows]
    print(k, per_file[k])

# COMMAND ----------

# DBTITLE 1,Constant columns + full-row duplicates
constant_and_duplicates(DFS, prof)

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- MASTR SOLAR, STORAGE AND GAS TABLES
# MAGIC
# MAGIC **Purpose:** Profile the MaStR solar, storage and gas unit and plant tables (`einheiten_solar`,
# MAGIC `anlagen_eeg_solar`, the storage and gas types): nulls, constant columns,
# MAGIC key, rates per column kind and date-like columns -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "mastr"
NB_KEY = "07_solar_storage_and_gas"
SECTION_TITLE = "Solar, storage and gas unit and plant tables"
TABLES = [
    "einheiten_solar",
    "anlagen_eeg_solar",
    "einheiten_stromspeicher",
    "anlagen_eeg_speicher",
    "anlagen_stromspeicher",
    "einheiten_gaserzeuger",
    "einheiten_gasspeicher",
    "anlagen_gasspeicher",
    "einheiten_gasverbraucher",
]

# COMMAND ----------

# DBTITLE 1,Read tables
FRAMES = {t: bronze_frame(f"{CATALOG}.{BRONZE_SCHEMA}.mastr_{t}") for t in TABLES}

# COMMAND ----------

# DBTITLE 1,Standard EDA -- profile, keys, rates, dates and findings export
standard_group_eda(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    FRAMES,
    TABLES,
    "mastr_07_rows.png",
    coverage=False,
    links=False,
)
