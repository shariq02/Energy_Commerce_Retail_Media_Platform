# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- POWER PLANT LIST (BNETZA KRAFTWERKSLISTE)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
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

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

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
DATE_HINTS = ("datum", "inbetriebnahme", "stilllegung", "date")
STATUS_HINTS = ("status", "kraftwerksstatus")
FUEL_HINTS = ("energietraeger", "brennstoff", "primaerenergie")

# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

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
        print(
            f"  {c[:44]:<44} missing={miss[c]:>8} rate={miss[c] / total if total else 0:.4f} approx_distinct={acd[c]}"
        )

# COMMAND ----------

# DBTITLE 1,Confirm constant columns exactly + full-row duplicates
for name, df in frames.items():
    cands = [c for c in prof[name]["cols"] if prof[name]["acd"][c] <= 1]
    if cands:
        rr = (
            df.agg(*[F.countDistinct(F.col(c)).alias(c) for c in cands])
            .first()
            .asDict()
        )
        prof[name]["constant"] = sorted(c for c in cands if (rr[c] or 0) <= 1)
    else:
        prof[name]["constant"] = []
    prof[name]["dups"] = prof[name]["total"] - df.distinct().count()
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
    dcols = [c for c in cols if any(h in c.lower() for h in DATE_HINTS)]
    entry["dates"] = {}
    for c in dcols[:4]:
        ts = timestamp_semantics(df, c, valid_from="1900-01-01", tz="Europe/Berlin")
        entry["dates"][c] = ts
        for ln in ts["lines"]:
            print(f"  {name}.{c}: {ln}")
    sem[name] = entry

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
domain_checks = {}
for name, df in frames.items():
    cols = df.columns
    bl = next((c for c in cols if "bundesland" in c.lower()), None)
    fu = next((c for c in cols if any(h in c.lower() for h in FUEL_HINTS)), None)
    if bl:
        domain_checks[f"{name}.{bl}"] = categorical_domain(
            df, bl, GERMAN_STATES, name=f"{name}.{bl}"
        )
    if fu:
        # no authoritative fuel list -- report the distinct set for manual review
        vals = [
            x[0]
            for x in df.select(F.col(fu).cast("string")).distinct().limit(60).collect()
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
        date_order[name] = date_order_check(
            df, com, dec, label=f"{name}: commissioning <= decommissioning"
        )
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
    decade = (F.floor(F.year(parse_ts_multi(com)) / 10) * 10).cast("int")
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
categorical_dist = {}
for name, df in frames.items():
    cat_cols = [c for c in prof[name]["cols"] if 1 < prof[name]["acd"][c] <= 60]
    dists = {}
    for c in cat_cols:
        vc = df.groupBy(c).count().orderBy(F.desc("count")).limit(50).collect()
        dists[c] = [(x[c], x["count"]) for x in vc]
    categorical_dist[name] = dists
    print(f"{name} categorical columns: {cat_cols}")

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
            "Fix in `scripts/ingestion/stage_power_plant_list.py` (header auto-detection +",
            "footnote strip are now in place), re-download the BNetzA file, re-stage, re-upload",
            "to the Volume and re-run Bronze before trusting any column.",
        )
    )
else:
    _struct.append("-> Header applied correctly in the current Bronze tables.")

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
if not _unit:
    _unit.append("- No capacity or date column located by name.")

_temporal = []
for d in DATASETS:
    for c, ts in sem[d].get("dates", {}).items():
        _temporal.append(
            f"- {d}.`{c}`: {ts['min_ts']}..{ts['max_ts']} (Europe/Berlin). "
            "Commissioning/decommission dates -- planned future dates are recorded ahead of time, "
            "so the column is only 'known' up to its own value."
        )
if not _temporal:
    _temporal.append("- No date column located; temporal semantics not assessed.")

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
            f"- `{k}` vs the 16 German states: unexpected={v['unexpected'] or 'none'}, "
            f"unused={v['unused_allowed'] or 'none'}. An unexpected value (or a code instead of a "
            "name) points to a header/parse problem."
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
        ("Temporal Semantics", "\n".join(_temporal)),
        ("Temporal Consistency", "\n".join(_tcons)),
        ("Relationship Cardinality", "\n".join(_card)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
