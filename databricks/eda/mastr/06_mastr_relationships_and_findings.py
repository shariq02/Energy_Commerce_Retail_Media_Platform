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
# MAGIC unused-parent count, and a row-level fan-out probe. Key sets are
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
    # row-level fan-out on the child side: does one child key repeat across rows?
    fan = (
        frames[child]
        .groupBy(F.col(ccol))
        .count()
        .agg(F.max("count").alias("mx"), F.avg("count").alias("av"))
        .first()
    )
    ri["child_max_rows_per_key"] = int(fan["mx"]) if fan and fan["mx"] else None
    ri["child_avg_rows_per_key"] = round(fan["av"], 3) if fan and fan["av"] else None
    ri["expectation"] = RELATIONSHIP_EXPECTATION.get(child, "subset")
    # A real integrity problem = orphans where the child was expected to be a
    # subset of the parent. "disjoint" / "partial-scope" orphans are expected.
    ri["orphan_problem"] = ri["expectation"] == "subset" and ri["orphans"] > 0
    ri_results.append(ri)
    print(
        f"{child}.{ccol} -> {role} [{ri['expectation']}]: match_rate={ri['match_rate']} "
        f"orphans={ri['orphans']} unused_parent={ri['unused_parent']} "
        f"child_rows_per_key(max/avg)="
        f"{ri['child_max_rows_per_key']}/{ri['child_avg_rows_per_key']}"
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
        f"unused_parent={r['unused_parent']}, "
        f"child rows/key max={r['child_max_rows_per_key']} avg={r['child_avg_rows_per_key']}"
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
                f"Row-level fan-out probe run per relationship: 1:N in "
                f"{[r['child'] for r in _fanout] or 'none'}. lokationen link arrays (03) are M:N and "
                "explode further -- verify exploded row counts against pre-explosion counts."
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
                "This notebook checks key-set membership only -- it confirms WHICH units have a change/"
                "support record, not WHEN. A temporally safe feature still needs the date guards in 02/04."
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
            "Catalog codes referenced across tables are version-dependent (05) -- pin the release.",
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
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("EDA Findings", "\n".join(_verdict)),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
