# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- MASTR RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Cross-table checks across all 28 MaStR Bronze tables. The
# MAGIC own-entity key of each table is taken from an exact distinct-count (not
# MAGIC the HLL estimate), and every join is checked against an EXPLICIT
# MAGIC relationship spec -- child key column, parent key column, both resolved
# MAGIC case-insensitively because MaStR's field names drift in case
# MAGIC (EegMaStRNummer vs EegMastrNummer). For each relationship: orphan rate,
# MAGIC unused-parent count, and a full cardinality profile (child rows, distinct
# MAGIC child keys, parents referenced, child-rows-per-parent distribution, max
# MAGIC fan-out). Also: every coded column in 01-04 reconciled against the
# MAGIC reference catalogs (05), cross-table event-date ordering on the shared
# MAGIC EinheitMastrNummer, and cross-carrier coordinate agreement. Key sets are
# MAGIC collected and reconciled with Python set math.

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

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Helpers -- resolve a *MastrNummer column by suffix, case-insensitive
frames = {d: spark.table(t) for d, t in TABLES.items()}
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