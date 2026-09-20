# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- ACCUWEATHER (SAMPLES CATALOG SCHEMA)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile every table of the `samples.accuweather` schema, read
# MAGIC in place (no Bronze table) -- table inventory, column dictionary,
# MAGIC missingness, duplicates, key candidates, numeric plausibility, time
# MAGIC columns and forecast-versus-observation structure, lead time between
# MAGIC timestamp columns, location entities and coordinate validity, cadence and
# MAGIC gaps per location, cross-table key overlap, categorical distributions,
# MAGIC and the layered modelling-risk checklist -- as evidence.

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib
import itertools

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# MAGIC %run ../_samples_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "accuweather"
NB_KEY = "01_accuweather"
SECTION_TITLE = "AccuWeather tables (Samples: samples.accuweather)"
SRC_CATALOG = "samples"
SRC_SCHEMA = "accuweather"
GLOBAL_BBOX = (-90.0, 90.0, -180.0, 180.0)
FULL_DUP_MAX_ROWS = 50_000_000
KEY_HINTS = ("key", "id", "code", "location", "station", "city")
TIME_HINTS = (
    "time",
    "date",
    "epoch",
    "issue",
    "valid",
    "forecast",
    "observ",
    "updated",
    "created",
)
GEO_HINTS = (
    "lat",
    "lon",
    "country",
    "region",
    "state",
    "city",
    "postal",
    "zip",
    "location",
    "timezone",
    "tz",
)
UNIT_HINTS = ("unit", "metric", "imperial", "scale")
VALUE_BOUNDS = {
    "humid": (0.0, 100.0),
    "cloud": (0.0, 100.0),
    "probab": (0.0, 100.0),
    "uv": (0.0, 20.0),
    "temp": (-130.0, 140.0),
    "lat": (-90.0, 90.0),
    "lon": (-180.0, 180.0),
}
NUMERIC_TYPES = ("tinyint", "smallint", "int", "bigint", "float", "double", "decimal")

# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Table inventory
FQ_SCHEMA = f"{SRC_CATALOG}.{SRC_SCHEMA}"
tables = [
    r["tableName"]
    for r in spark.sql(f"SHOW TABLES IN {FQ_SCHEMA}").collect()
    if not r["isTemporary"]
]
tables = sorted(tables)
print(f"{FQ_SCHEMA}: {len(tables)} tables")
print(tables)

# COMMAND ----------

# DBTITLE 1,Table sizes (Delta detail where available)
detail = {}
for t in tables:
    with contextlib.suppress(Exception):
        r = spark.sql(f"DESCRIBE DETAIL {FQ_SCHEMA}.{t}").first().asDict()
        detail[t] = {
            "format": r.get("format"),
            "size_bytes": r.get("sizeInBytes"),
            "files": r.get("numFiles"),
        }
for t in tables:
    print(t, detail.get(t, "no detail"))

# COMMAND ----------

# DBTITLE 1,Column dictionary (types and comments)
cols_info = {}
with contextlib.suppress(Exception):
    rows = spark.sql(
        f"SELECT table_name, column_name, data_type, comment, ordinal_position "
        f"FROM {SRC_CATALOG}.information_schema.columns WHERE table_schema = '{SRC_SCHEMA}' "
        "ORDER BY table_name, ordinal_position"
    ).collect()
    for r in rows:
        cols_info.setdefault(r["table_name"], []).append(r.asDict())
for t in tables:
    print(t, len(cols_info.get(t, [])), "columns in information_schema")

# COMMAND ----------

# DBTITLE 1,Table comments
comments = {}
with contextlib.suppress(Exception):
    for r in spark.sql(
        f"SELECT table_name, comment FROM {SRC_CATALOG}.information_schema.tables WHERE table_schema = '{SRC_SCHEMA}'"
    ).collect():
        comments[r["table_name"]] = r["comment"]
print({k: v for k, v in comments.items() if v})

# COMMAND ----------

# DBTITLE 1,Frames
DFS = {t: spark.table(f"{FQ_SCHEMA}.{t}") for t in tables}
DTYPE = {
    t: {f.name: f.dataType.simpleString() for f in df.schema.fields}
    for t, df in DFS.items()
}
for t in tables:
    print(t, len(DTYPE[t]), "columns")

# COMMAND ----------

# DBTITLE 1,Resolve column roles per table
ROLE = {}
for t, dt in DTYPE.items():
    cols = list(dt)
    ROLE[t] = {
        "numeric": [c for c in cols if dt[c].startswith(NUMERIC_TYPES)],
        "time_typed": [
            c for c in cols if dt[c] in ("timestamp", "date", "timestamp_ntz")
        ],
        "time_named": [c for c in cols if any(h in c.lower() for h in TIME_HINTS)],
        "geo": [c for c in cols if any(h in c.lower() for h in GEO_HINTS)],
        "key": [c for c in cols if any(h in c.lower() for h in KEY_HINTS)],
        "unit": [c for c in cols if any(h in c.lower() for h in UNIT_HINTS)],
        "lat": next((c for c in cols if c.lower() in ("latitude", "lat")), None),
        "lon": next(
            (c for c in cols if c.lower() in ("longitude", "lon", "lng")), None
        ),
        "string": [c for c in cols if dt[c] == "string"],
    }
    print(t, {k: (v if not isinstance(v, list) else v[:8]) for k, v in ROLE[t].items()})

# COMMAND ----------

# DBTITLE 1,Profile each table -- rows, missingness, approx distinct
prof = {}
for t, df in DFS.items():
    prof[t] = profile_frame(df)
    p = prof[t]
    print(f"{t}: rows={p['total']} cols={len(p['cols'])}")

# COMMAND ----------

# DBTITLE 1,Per-column profile (printed)
for t in tables:
    p = prof[t]
    print("=" * 90, f"\n{t}  rows={p['total']}  cols={len(p['cols'])}")
    for c in p["cols"]:
        print(
            f"  {c[:40]:<40} {DTYPE[t][c][:18]:<18} missing={p['miss'][c]:>9} "
            f"rate={p['miss'][c] / p['total'] if p['total'] else 0:.4f} approx_distinct={p['acd'][c]}"
        )

# COMMAND ----------

# DBTITLE 1,Constant columns + full-row duplicates
for t, df in DFS.items():
    prof[t]["constant"] = constant_cols(df, prof[t])
    if prof[t]["total"] <= FULL_DUP_MAX_ROWS:
        prof[t]["dups"] = full_row_dup_count(df, prof[t]["total"])
    else:
        prof[t]["dups"] = None
    print(f"{t}: constant={prof[t]['constant']}  full-row duplicates={prof[t]['dups']}")

# COMMAND ----------

# DBTITLE 1,Key candidates -- exact uniqueness
uniq = {}
for t, df in DFS.items():
    cands = [c for c in ROLE[t]["key"] if c in df.columns][:6]
    uniq[t] = exact_uniqueness(df, cands)
    for c, u in uniq[t].items():
        print(f"{t}.{c}: {u}")

# COMMAND ----------

# DBTITLE 1,Numeric parse yield + moments
num = {}
for t, df in DFS.items():
    cols = ROLE[t]["numeric"][:40]
    num[t] = numeric_scan(df, cols)
    for c, s in num[t].items():
        print(
            f"{t}.{c:<24} min={s['min']} max={s['max']} mean={fmt_num(s['mean'])} sd={fmt_num(s['sd'])} zero={s['zero']} negative={s['negative']}"
        )

# COMMAND ----------

# DBTITLE 1,Plausibility against broad physical bounds
plaus = {}
for t, df in DFS.items():
    plaus[t] = {}
    for c in num[t]:
        key = next((k for k in VALUE_BOUNDS if k in c.lower()), None)
        if not key:
            continue
        lo, hi = VALUE_BOUNDS[key]
        plaus[t][c] = plausibility(df, c, lo=lo, hi=hi, sentinels=())
        print(t, c, {a: plaus[t][c].get(a) for a in ("min", "max", "below", "above")})

# COMMAND ----------

# DBTITLE 1,Time columns -- range, granularity, kind
tinfo = {}
for t, df in DFS.items():
    tinfo[t] = {}
    for c in ROLE[t]["time_typed"]:
        r = (
            df.agg(
                F.min(qcol(c)).alias("lo"),
                F.max(qcol(c)).alias("hi"),
                F.countDistinct(qcol(c)).alias("distinct"),
                F.sum((F.hour(qcol(c).cast("timestamp")) != 0).cast("long")).alias(
                    "has_time"
                ),
            )
            .first()
            .asDict()
        )
        tinfo[t][c] = {
            "kind": "typed",
            **{k: str(v) if k in ("lo", "hi") else v for k, v in r.items()},
        }
    for c in ROLE[t]["time_named"]:
        if c in tinfo[t] or c not in num[t]:
            continue
        if any(h in c.lower() for h in ("epoch", "unix", "timestamp", "_ts")):
            e = epoch_scan(df, c)
            if e.get("n"):
                tinfo[t][c] = {"kind": "epoch", **e}
    for c, v in tinfo[t].items():
        print(t, c, v)

# COMMAND ----------

# DBTITLE 1,Lead time between timestamp columns
lead = {}
for t, df in DFS.items():
    cols = [c for c in ROLE[t]["time_typed"]][:3]
    lead[t] = {}
    for a, b in itertools.combinations(cols, 2):
        d = (
            F.unix_timestamp(qcol(b).cast("timestamp"))
            - F.unix_timestamp(qcol(a).cast("timestamp"))
        ) / 3600.0
        q = (
            df.select(d.alias("h"))
            .where(F.col("h").isNotNull())
            .approxQuantile("h", [0.0, 0.01, 0.5, 0.99, 1.0], 0.001)
        )
        lead[t][(a, b)] = q
        print(f"{t}: hours from {a} to {b} (min/p1/p50/p99/max): {q}")

# COMMAND ----------

# DBTITLE 1,Location entities and rows per location
loc_key = {}
loc_stats = {}
for t, df in DFS.items():
    cands = [
        c
        for c in ROLE[t]["key"]
        if any(h in c.lower() for h in ("location", "city", "station", "key"))
    ]
    loc_key[t] = cands[0] if cands else None
    if loc_key[t]:
        g = df.groupBy(qcol(loc_key[t])).count()
        loc_stats[t] = (
            g.agg(
                F.count(F.lit(1)).alias("locations"),
                F.min("count").alias("min"),
                F.expr("percentile_approx(`count`, 0.5)").alias("p50"),
                F.max("count").alias("max"),
            )
            .first()
            .asDict()
        )
        print(t, loc_key[t], loc_stats[t])

# COMMAND ----------

# DBTITLE 1,Cadence and gaps per location
cadence = {}
for t, df in DFS.items():
    lk, tcs = loc_key.get(t), ROLE[t]["time_typed"]
    if not lk or not tcs:
        continue
    tc = tcs[0]
    ts = F.unix_timestamp(qcol(tc).cast("timestamp"))
    gaps = (
        df.select(qcol(lk).alias("k"), ts.alias("t"))
        .withColumn(
            "gap", F.col("t") - F.lag("t").over(Window.partitionBy("k").orderBy("t"))
        )
        .where(F.col("gap").isNotNull())
    )
    cadence[t] = (
        gaps.agg(
            F.expr("percentile_approx(gap, 0.5)").alias("gap_p50"),
            F.expr("percentile_approx(gap, 0.99)").alias("gap_p99"),
            F.max("gap").alias("gap_max"),
            F.sum((F.col("gap") == 0).cast("long")).alias("zero_gaps"),
        )
        .first()
        .asDict()
    )
    cadence[t]["time_column"] = tc
    cadence[t]["location_column"] = lk
    print(t, cadence[t])

# COMMAND ----------

# DBTITLE 1,Duplicate (location, time) keys
dupk = {}
for t, df in DFS.items():
    lk, tcs = loc_key.get(t), ROLE[t]["time_typed"]
    if lk and tcs:
        dupk[t] = dup_key_composition(df, [lk, tcs[0]])
        print(t, (lk, tcs[0]), dupk[t])

# COMMAND ----------

# DBTITLE 1,Geography -- coordinate validity
geo = {}
for t, df in DFS.items():
    if ROLE[t]["lat"] and ROLE[t]["lon"]:
        geo[t] = spatial_validity(
            df, ROLE[t]["lat"], ROLE[t]["lon"], bbox=GLOBAL_BBOX, name=t
        )
        print(t, geo[t])

# COMMAND ----------

# DBTITLE 1,Geography -- distinct places and coordinate stability per location
geo_ent = {}
for t, df in DFS.items():
    lk = loc_key.get(t)
    if lk and ROLE[t]["lat"] and ROLE[t]["lon"]:
        geo_ent[t] = (
            df.groupBy(qcol(lk))
            .agg(
                F.approx_count_distinct(as_str(ROLE[t]["lat"])).alias("lat_variants"),
                F.approx_count_distinct(as_str(ROLE[t]["lon"])).alias("lon_variants"),
            )
            .agg(
                F.count(F.lit(1)).alias("locations"),
                F.sum(
                    ((F.col("lat_variants") > 1) | (F.col("lon_variants") > 1)).cast(
                        "long"
                    )
                ).alias("moving"),
            )
            .first()
            .asDict()
        )
        print(t, geo_ent[t])

# COMMAND ----------

# DBTITLE 1,Cross-table key overlap
shared = {}
by_col = {}
for t in tables:
    for c in ROLE[t]["key"]:
        by_col.setdefault(c.lower(), []).append((t, c))
for name, occ in by_col.items():
    if len(occ) >= 2:
        shared[name] = occ
print({k: [t for t, _ in v] for k, v in shared.items()})

# COMMAND ----------

# DBTITLE 1,Referential integrity between tables sharing a key column
ri = []
MAX_KEYS = 500_000
for name, occ in shared.items():
    sets = []
    for t, c in occ:
        if prof[t]["acd"][c] > MAX_KEYS:
            continue
        sets.append((t, c, collect_key_set(DFS[t], c)))
    if len(sets) < 2:
        continue
    sets.sort(key=lambda x: len(x[2]))
    parent = sets[-1]
    for t, c, keys in sets[:-1]:
        res = referential_integrity(
            keys, parent[2], child=f"{t}.{c}", parent=f"{parent[0]}.{parent[1]}"
        )
        ri.append(res)
        print(res)

# COMMAND ----------

# DBTITLE 1,Low-cardinality categorical distributions
cat_dist = {}
for t, df in DFS.items():
    cat_dist[t] = {}
    cats = [c for c in ROLE[t]["string"] if 1 < prof[t]["acd"][c] <= 40][:10]
    for c in cats:
        vc = df.groupBy(qcol(c)).count().orderBy(F.desc("count")).limit(25).collect()
        cat_dist[t][c] = [(r[0], r["count"]) for r in vc]
    print(t, list(cat_dist[t]))

# COMMAND ----------

# DBTITLE 1,Unit columns
unit_vals = {}
for t, df in DFS.items():
    unit_vals[t] = {
        c: cat_dist[t].get(c, []) for c in ROLE[t]["unit"] if c in cat_dist[t]
    }
    print(t, unit_vals[t])

# COMMAND ----------

# DBTITLE 1,Figures
figs = []
if facet_bars(
    {
        "rows per table": [(t, prof[t]["total"]) for t in tables],
        "columns per table": [(t, len(prof[t]["cols"])) for t in tables],
    },
    "AccuWeather -- overview",
    "accuweather_overview.png",
    rot=60,
    ncols=1,
    logy=True,
):
    figs.append(("AccuWeather -- overview", "accuweather_overview.png"))
_first_cat = {t: next(iter(v.values())) for t, v in cat_dist.items() if v}
if _first_cat and facet_bars(
    dict(list(_first_cat.items())[:6]),
    "AccuWeather -- first categorical column per table",
    "accuweather_categorical.png",
    rot=45,
    ncols=2,
):
    figs.append(
        (
            "AccuWeather -- first categorical column per table",
            "accuweather_categorical.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Findings
for t in tables:
    print(
        f"{t}: rows={prof[t]['total']}, cols={len(prof[t]['cols'])}, constant={prof[t]['constant']}, "
        f"dups={prof[t]['dups']}, time={list(tinfo[t])}, location key={loc_key.get(t)}"
    )

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/accuweather.md
_profile = [
    "| table | rows | cols | size (bytes) | format | constant columns |",
    "|---|---|---|---|---|---|",
]
for t in tables:
    d = detail.get(t, {})
    _profile.append(
        f"| {t} | {prof[t]['total']} | {len(prof[t]['cols'])} | {d.get('size_bytes', '-')} | "
        f"{d.get('format', '-')} | {', '.join(prof[t]['constant']) or '-'} |"
    )

_prov = [
    para(
        "The tables are read in place from the Databricks Samples catalog",
        f"(`{FQ_SCHEMA}`); there is no Bronze table and no ECRMAP acquisition step.",
    ),
    f"Tables found: {len(tables)} -- {tables}.",
]
_commented = {t: c for t, c in comments.items() if c}
if _commented:
    _prov.append("Table comments in the catalog:")
    _prov += [f"- `{t}`: {c}" for t, c in _commented.items()]
else:
    _prov.append(
        "- No table comments in the catalog: provenance, licence and real/synthetic status are not documented here (LIMITATION)."
    )
_col_comments = [
    (t, ci["column_name"], ci["comment"])
    for t, lst in cols_info.items()
    for ci in lst
    if ci.get("comment")
]
if _col_comments:
    _prov.append("Column comments in the catalog (first 40):")
    _prov += [f"- `{t}.{c}`: {cm}" for t, c, cm in _col_comments[:40]]

_struct = []
for t in tables:
    _struct.append(
        f"- `{t}`: {len(DTYPE[t])} columns -- numeric {len(ROLE[t]['numeric'])}, string {len(ROLE[t]['string'])}, "
        f"timestamp/date {ROLE[t]['time_typed'] or 'none'}; key-like {ROLE[t]['key'][:6] or 'none'}; "
        f"geography-like {ROLE[t]['geo'][:6] or 'none'}."
    )

_dq = ["Full-row exact duplicates, constant columns, and (location, time) duplicates:"]
for t in tables:
    dk = dupk.get(t)
    _dq.append(
        f"- `{t}`: full-row duplicates {prof[t]['dups']}; constant columns {prof[t]['constant'] or 'none'}"
        + (
            f"; ({loc_key.get(t)}, {ROLE[t]['time_typed'][0]}) duplicated keys {dk['dup_groups']} of {dk['distinct_keys']} "
            f"({dk['identical']} identical, {dk['conflicting']} conflicting)"
            if dk
            else ""
        )
        + "."
    )
_dq.append("Columns with more than 5% missing values:")
for t in tables:
    p = prof[t]
    high = [
        (c, round(p["miss"][c] / p["total"], 3))
        for c in p["cols"]
        if p["total"] and p["miss"][c] / p["total"] > 0.05
    ]
    _dq.append(f"- `{t}`: {high or 'none'}")

_entities = [
    "Key candidates (column: distinct / ratio-to-rows / unique) and locations:"
]
for t in tables:
    for c, u in uniq[t].items():
        _entities.append(
            f"- `{t}.{c}`: {u['distinct']} / {u['ratio']} / unique={u['unique']}"
        )
    if t in loc_stats:
        s = loc_stats[t]
        _entities.append(
            f"- `{t}` by `{loc_key[t]}`: {s['locations']} distinct; rows per location min {s['min']}, p50 {s['p50']}, max {s['max']}."
        )

_unit = []
for t in tables:
    for c, s in num[t].items():
        line = (
            f"- `{t}.{c}`: range {s['min']}..{s['max']}, mean {fmt_num(s['mean'])}, sd {fmt_num(s['sd'])}, "
            f"zero {s['zero']}, negative {s['negative']}"
        )
        if c in plaus[t]:
            line += f"; outside broad bounds: below {plaus[t][c]['below']}, above {plaus[t][c]['above']}"
        _unit.append(line + ".")
    if unit_vals[t]:
        _unit.append(f"- `{t}` unit-like columns: {unit_vals[t]}.")
if not _unit:
    _unit.append("- No numeric column found.")

_domain = []
for t in tables:
    for c, pairs_ in cat_dist[t].items():
        _domain.append(f"- `{t}.{c}`: " + fmt_pairs(pairs_, n=8).replace("\n", " "))
if not _domain:
    _domain.append("- No low-cardinality string column located.")

_geo = []
for t, g in geo.items():
    _geo.append(
        f"- `{t}`: coordinates present {g['present']}, missing {g['missing']}, at 0/0 {g['null_island']}, outside the global box "
        f"{g['outside_bbox']}, looks lat/lon-swapped {g['looks_swapped']}."
    )
for t, g in geo_ent.items():
    _geo.append(
        f"- `{t}`: {g['locations']} locations; {g['moving']} report more than one coordinate."
    )
if not _geo:
    _geo.append("- No latitude / longitude column pair located.")

_temporal = []
for t in tables:
    for c, v in tinfo[t].items():
        if v["kind"] == "typed":
            _temporal.append(
                f"- `{t}.{c}` ({v['kind']}): {v['lo']} .. {v['hi']}; {v['distinct']} distinct values; sub-daily component rows: {v['has_time']}."
            )
        else:
            _temporal.append(
                f"- `{t}.{c}` (epoch, {v['unit']}): {v['min_ts']} .. {v['max_ts']} (UTC); {v['distinct_ts']} distinct instants."
            )
    named_only = [c for c in ROLE[t]["time_named"] if c not in tinfo[t]]
    if named_only:
        _temporal.append(
            f"- `{t}` time-named columns not typed as time: {named_only[:8]} (kept as text / numeric -- meaning to be read from the column dictionary)."
        )
if not _temporal:
    _temporal.append("- No time column located.")

_tcons = []
for t, cd in cadence.items():
    _tcons.append(
        f"- `{t}` per `{cd['location_column']}` over `{cd['time_column']}`: gap p50 {cd['gap_p50']} s, p99 {cd['gap_p99']} s, "
        f"max {cd['gap_max']} s; zero-second gaps (repeated timestamps) {cd['zero_gaps']}."
    )
for t, d in lead.items():
    for (a, b), q in d.items():
        _tcons.append(f"- `{t}`: hours from `{a}` to `{b}` (min/p1/p50/p99/max): {q}.")
if not _tcons:
    _tcons.append(
        "- No table had both a location key and a timestamp column; cadence not assessed."
    )

_card = ["Keys shared across tables:"]
for name, occ in shared.items():
    _card.append(f"- `{name}`: {[t for t, _ in occ]}")
for r in ri:
    _card.append(f"- {r['child']} -> {r['parent']}: " + " ".join(ri_interpretation(r)))
if not shared:
    _card.append("- No key-like column name is shared between tables.")

_regime = [
    para(
        "Forecast versus observation: a table with two timestamp columns whose difference is",
        "positive and non-constant is a forecast (issue time vs valid time); a table with one",
        "timestamp and repeated locations is a series of observations. Evidence per table:",
    )
]
for t in tables:
    _regime.append(
        f"- `{t}`: timestamp columns {list(tinfo[t]) or 'none'}; lead-time pairs {list(lead[t]) or 'none'}."
    )

_coverage = [
    para(
        "Coverage is whatever the tables contain; no statement of the set of locations,",
        "the period, or the selection rule is available in the data.",
    )
]
for t in tables:
    if t in loc_stats:
        _coverage.append(f"- `{t}`: {loc_stats[t]['locations']} `{loc_key[t]}` values.")

_dist = []
for t in tables:
    for c, pairs_ in cat_dist[t].items():
        _dist.append(f"- `{t}.{c}`: " + fmt_pairs(pairs_, n=10).replace("\n", " "))

_findings_md = "\n".join(
    f"- `{t}`: rows={prof[t]['total']}, cols={len(prof[t]['cols'])}, dups={prof[t]['dups']}, "
    f"time columns={list(tinfo[t])}, location key={loc_key.get(t)}"
    for t in tables
)

_silver = [
    "- Read from the Samples catalog; no Bronze table exists for these tables.",
    "- The set of tables, their keys and their time semantics above decide how many Silver structures are legitimately needed; nothing is assumed here.",
    "- Forecast-versus-observation evidence (Regime / Version Evidence) determines whether any table needs a separate issue-time key.",
]

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            f"Per-table grain must be read from the key candidates and (location, time) duplicate counts: {dupk}. Tables={len(tables)}.",
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            f"Tables sharing key columns: { {k: [t for t, _ in v] for k, v in shared.items()} }; rows per location: {loc_stats}. Confirm row-level cardinality before joining.",
        ),
        (
            "Target contamination",
            "No target is defined by the source; forecast columns are predictions, so using them as features for their own observed counterpart is circular.",
        ),
        (
            "Temporal / post-event leakage",
            f"Timestamp evidence per table: { {t: list(tinfo[t]) for t in tables} }; lead times: {lead}. A forecast is only knowable from its issue time onward.",
        ),
        (
            "Proxy leakage",
            "Location keys and names are near-identifiers; a model given them memorises the place.",
        ),
        (
            "Split / entity leakage",
            "Split by location and by contiguous time; rows of one location and neighbouring times are strongly dependent.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            "A forecast table without an issue time cannot be used point-in-time; the lead-time evidence above shows which tables carry one.",
        ),
        (
            "Survivorship / coverage bias",
            f"Locations per table: { {t: loc_stats[t]['locations'] for t in loc_stats} }; the selection of locations is not documented.",
        ),
        (
            "Missingness leakage",
            "Columns above 5% missing are listed in Data Quality; missingness that tracks weather condition is informative, not random.",
        ),
        (
            "Duplicate-event leakage",
            f"Full-row duplicates: { {t: prof[t]['dups'] for t in tables} }; repeated (location, time) rows: {dupk}.",
        ),
        (
            "Target / feature temporal misalignment",
            "Compare each table's time columns and cadence (Temporal Consistency) before pairing with any other source; local versus UTC time is not assumed here.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            f"Unit-like columns: {unit_vals}. Metric and imperial values of the same measure in one table are mirror columns.",
        ),
        (
            "Data-generation-process leakage",
            "Provider-generated forecasts embed the provider's model; whether the data is real or synthetic is not stated in the catalog metadata found.",
        ),
        (
            "Class / label instability",
            "Condition text / icon codes are provider taxonomies and can change between provider versions.",
        ),
        (
            "Label availability lag",
            "Observations arrive after the fact; forecasts are available before -- which side each table is on is in the Regime / Version Evidence section.",
        ),
        (
            "Source / version / regime change",
            "No provider version field is assumed; look for constant version-like columns in the constant-column list.",
        ),
        (
            "Sample-vs-full divergence",
            "Every statistic is a full Spark aggregation over the table read, except the approximate distinct counts and percentiles, which are labelled approximate; whether the Samples tables are a subset of the provider's data is not stated.",
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
        ("Geography", "\n".join(_geo)),
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