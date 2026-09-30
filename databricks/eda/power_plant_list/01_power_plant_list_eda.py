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
    return _qc(colname).isNull() | (F.trim(_qc(colname).cast("string")) == "")


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
    per = present.groupBy(_qc(id_c).alias("id")).agg(
        F.count(F.lit(1)).alias("n"),
        *[F.countDistinct(_qc(c)).alias(f"d{i}") for i, c in enumerate(df.columns)],
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
            present.where(_qc(id_c) == x["id"])
            .select(*[_qc(c) for c in show])
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
            .groupBy(_qc(tc).alias("t"))
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
            "Fix in `scripts/ingestion/stage_power_plant_list.py` (header auto-detection +",
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
