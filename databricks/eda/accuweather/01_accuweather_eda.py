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
# MAGIC in place (no Bronze table) -- table inventory and naming structure, column
# MAGIC dictionary, missingness, duplicates, composite keys, numeric plausibility
# MAGIC (unit-aware), cap / sentinel values, time columns and horizon, cadence and
# MAGIC completeness against a full grid, location entities and coordinate
# MAGIC consistency, imperial-versus-metric agreement, forecast-versus-historical
# MAGIC overlap, categorical distributions, and the layered modelling-risk
# MAGIC checklist -- as evidence.

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib
from functools import reduce

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
FQ_SCHEMA = f"{SRC_CATALOG}.{SRC_SCHEMA}"
GLOBAL_BBOX = (-90.0, 90.0, -180.0, 180.0)
FULL_DUP_MAX_ROWS = 50_000_000
NUMERIC_TYPES = ("tinyint", "smallint", "int", "bigint", "float", "double", "decimal")
MAX_NUMERIC_COLS = 120
# Exact column names that identify a location; no substring matching.
LOCATION_NAMES = ("city_name", "location_key", "location_id", "city_id", "station_id")
TIME_PREFERENCE = ("datetime_valid_local", "date")
ISSUE_HINTS = (
    "issue",
    "generated",
    "fetch",
    "created",
    "updated",
    "extract",
    "snapshot",
    "retriev",
)
TZ_HINTS = ("utc", "timezone", "tz", "offset")
CODE_SUFFIXES = ("_code", "_flag", "_icon")
CAP_SHARE = 0.05
# (match kind, text, imperial bounds, metric bounds) -- matched on column names.
BOUNDS = [
    ("startswith", "humidity_relative", (0.0, 100.0), (0.0, 100.0)),
    ("startswith", "cloud_cover", (0.0, 100.0), (0.0, 100.0)),
    ("endswith", "probability", (0.0, 100.0), (0.0, 100.0)),
    ("startswith", "latitude", (-90.0, 90.0), (-90.0, 90.0)),
    ("startswith", "longitude", (-180.0, 180.0), (-180.0, 180.0)),
    ("startswith", "temperature", (-100.0, 140.0), (-75.0, 60.0)),
    ("startswith", "dew_point", (-100.0, 120.0), (-75.0, 50.0)),
    ("startswith", "index_uv", (0.0, 20.0), (0.0, 20.0)),
    ("startswith", "uv_index", (0.0, 20.0), (0.0, 20.0)),
]

# COMMAND ----------


# DBTITLE 1,Helpers
def bounds_for(col, unit):
    c = col.lower()
    for kind, text, imp, met in BOUNDS:
        if (kind == "startswith" and c.startswith(text)) or (
            kind == "endswith" and c.endswith(text)
        ):
            return imp if unit == "imperial" else met
    return None


def short(items, n=12):
    items = list(items)
    if len(items) <= n:
        return items or "none"
    return f"{items[:n]} (+{len(items) - n} more)"


# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Table inventory
tables = sorted(
    r["tableName"]
    for r in spark.sql(f"SHOW TABLES IN {FQ_SCHEMA}").collect()
    if not r["isTemporary"]
)
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

# DBTITLE 1,Table comments (deduplicated)
comment_tables = {}
with contextlib.suppress(Exception):
    for r in spark.sql(
        f"SELECT table_name, comment FROM {SRC_CATALOG}.information_schema.tables WHERE table_schema = '{SRC_SCHEMA}'"
    ).collect():
        if r["comment"]:
            comment_tables.setdefault(r["comment"], []).append(r["table_name"])
for text, names in comment_tables.items():
    print(f"{len(names)} tables share this comment: {names}\n{text[:600]}")

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

# DBTITLE 1,Table naming structure (period / grain / unit system)
STRUCT = {}
for t in tables:
    parts = t.split("_")
    STRUCT[t] = {
        "period": parts[0],
        "unit": parts[-1] if parts[-1] in ("imperial", "metric") else None,
        "grain": "_".join(parts[1:-1])
        if parts[-1] in ("imperial", "metric")
        else "_".join(parts[1:]),
    }
print({t: tuple(s.values()) for t, s in STRUCT.items()})

# COMMAND ----------

# DBTITLE 1,Resolve column roles per table
ROLE = {}
for t, dt in DTYPE.items():
    cols = list(dt)
    typed = [c for c in cols if dt[c] in ("timestamp", "date", "timestamp_ntz")]
    ROLE[t] = {
        "numeric": [c for c in cols if dt[c].startswith(NUMERIC_TYPES)][
            :MAX_NUMERIC_COLS
        ],
        "string": [c for c in cols if dt[c] == "string"],
        "time_typed": typed,
        "time": next(
            (c for c in TIME_PREFERENCE if c in typed), typed[0] if typed else None
        ),
        "loc": next((c for c in LOCATION_NAMES if c in cols), None),
        "sub": [c for c in ("day_flag",) if c in cols],
        "lat": next((c for c in cols if c.lower() == "latitude"), None),
        "lon": next((c for c in cols if c.lower() == "longitude"), None),
        "country": next((c for c in cols if c.lower() == "country_code"), None),
        "issue": [c for c in cols if any(h in c.lower() for h in ISSUE_HINTS)],
        "tz": [c for c in cols if any(h in c.lower() for h in TZ_HINTS)],
    }
    ROLE[t]["key"] = (
        [ROLE[t]["loc"], ROLE[t]["time"], *ROLE[t]["sub"]]
        if ROLE[t]["loc"] and ROLE[t]["time"]
        else []
    )
    print(t, {k: v for k, v in ROLE[t].items() if k not in ("numeric", "string")})

# COMMAND ----------

# DBTITLE 1,Profile each table -- rows, missingness, approx distinct
prof = {}
for t, df in DFS.items():
    prof[t] = profile_frame(df)
    print(f"{t}: rows={prof[t]['total']} cols={len(prof[t]['cols'])}")

# COMMAND ----------

# DBTITLE 1,Constant columns, all-missing columns and full-row duplicates
for t, df in DFS.items():
    p = prof[t]
    p["constant"] = constant_cols(df, p)
    p["all_missing"] = sorted(
        c for c in p["cols"] if p["total"] and p["miss"][c] == p["total"]
    )
    p["dups"] = (
        full_row_dup_count(df, p["total"]) if p["total"] <= FULL_DUP_MAX_ROWS else None
    )
    print(
        f"{t}: constant={len(p['constant'])} all-missing={len(p['all_missing'])} full-row duplicates={p['dups']}"
    )

# COMMAND ----------

# DBTITLE 1,Composite key -- location, time and day/night flag
dupk, uniq = {}, {}
for t, df in DFS.items():
    key = ROLE[t]["key"]
    if not key:
        continue
    dupk[t] = dup_key_composition(df, key)
    uniq[t] = exact_uniqueness(df, [ROLE[t]["loc"]])
    print(t, key, dupk[t])

# COMMAND ----------

# DBTITLE 1,Numeric parse yield + moments
num = {}
for t, df in DFS.items():
    num[t] = numeric_scan(df, ROLE[t]["numeric"])
    print(
        t, sum(1 for s in num[t].values() if s["is_numeric"]), "numeric columns parsed"
    )

# COMMAND ----------

# DBTITLE 1,Plausibility against unit-aware bounds
plaus = {}
for t, df in DFS.items():
    plaus[t] = {}
    for c, s in num[t].items():
        b = bounds_for(c, STRUCT[t]["unit"])
        if not b or not s["is_numeric"]:
            continue
        plaus[t][c] = plausibility(df, c, lo=b[0], hi=b[1], sentinels=())
        plaus[t][c]["bounds"] = b
        if plaus[t][c].get("below") or plaus[t][c].get("above"):
            print(
                t,
                c,
                b,
                {a: plaus[t][c].get(a) for a in ("min", "max", "below", "above")},
            )

# COMMAND ----------

# DBTITLE 1,Cap / sentinel-like values (share of rows at the column maximum)
caps = {}
for t, df in DFS.items():
    cols = [
        c
        for c, s in num[t].items()
        if s["is_numeric"]
        and s["max"] is not None
        and s["max"] > 0
        and prof[t]["acd"][c] > 1
    ]
    caps[t] = []
    if not cols:
        continue
    r = (
        df.agg(
            *[
                F.sum(
                    (to_double(c) == F.lit(float(num[t][c]["max"]))).cast("long")
                ).alias(f"m{i}")
                for i, c in enumerate(cols)
            ]
        )
        .first()
        .asDict()
    )
    for i, c in enumerate(cols):
        nn = num[t][c]["non_null"]
        share = (r[f"m{i}"] or 0) / nn if nn else 0.0
        if share >= CAP_SHARE:
            caps[t].append((c, num[t][c]["max"], r[f"m{i}"], round(share, 4)))
    print(t, caps[t])

# COMMAND ----------

# DBTITLE 1,Time columns -- range, distinct values, sub-daily component
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
        tinfo[t][c] = {k: str(v) if k in ("lo", "hi") else v for k, v in r.items()}
        print(t, c, tinfo[t][c])

# COMMAND ----------

# DBTITLE 1,Horizon per table (days between first and last time value)
horizon = {}
for t in tables:
    c = ROLE[t]["time"]
    if c and c in tinfo[t]:
        r = (
            DFS[t]
            .agg(
                F.datediff(
                    F.max(qcol(c).cast("timestamp")), F.min(qcol(c).cast("timestamp"))
                ).alias("days")
            )
            .first()
        )
        horizon[t] = r["days"]
print(horizon)

# COMMAND ----------

# DBTITLE 1,Location entities and rows per location
loc_stats = {}
for t, df in DFS.items():
    lk = ROLE[t]["loc"]
    if not lk:
        continue
    loc_stats[t] = (
        df.groupBy(qcol(lk))
        .count()
        .agg(
            F.count(F.lit(1)).alias("locations"),
            F.min("count").alias("min"),
            F.expr("percentile_approx(`count`, 0.5)").alias("p50"),
            F.max("count").alias("max"),
        )
        .first()
        .asDict()
    )
    print(t, lk, loc_stats[t])

# COMMAND ----------

# DBTITLE 1,Cadence per location (and day/night flag)
cadence = {}
for t, df in DFS.items():
    key = ROLE[t]["key"]
    if not key:
        continue
    part = [
        qcol(c).alias(f"p{i}") for i, c in enumerate([ROLE[t]["loc"], *ROLE[t]["sub"]])
    ]
    pnames = [f"p{i}" for i in range(len(part))]
    ts = F.unix_timestamp(qcol(ROLE[t]["time"]).cast("timestamp")).alias("t")
    gaps = (
        df.select(*part, ts)
        .withColumn(
            "gap",
            F.col("t") - F.lag("t").over(Window.partitionBy(*pnames).orderBy("t")),
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
    cadence[t]["partition"] = [ROLE[t]["loc"], *ROLE[t]["sub"]]
    cadence[t]["time_column"] = ROLE[t]["time"]
    print(t, cadence[t])

# COMMAND ----------

# DBTITLE 1,Completeness against the full location x time (x flag) grid
complete = {}
for t, df in DFS.items():
    key = ROLE[t]["key"]
    if not key:
        continue
    names = ["c", "t", *[f"s{i}" for i in range(len(ROLE[t]["sub"]))]]
    keys = df.select(
        qcol(key[0]).alias("c"),
        qcol(key[1]).cast("timestamp").alias("t"),
        *[qcol(s).alias(f"s{i}") for i, s in enumerate(ROLE[t]["sub"])],
    ).distinct()
    grid = keys.select("c").distinct().crossJoin(keys.select("t").distinct())
    for n in names[2:]:
        grid = grid.crossJoin(keys.select(n).distinct())
    missing = grid.join(keys, on=names, how="left_anti")
    worst = missing.groupBy("c").count().orderBy(F.desc("count")).limit(5).collect()
    complete[t] = {
        "expected": grid.count(),
        "actual": keys.count(),
        "missing": missing.count(),
        "worst": [(r["c"], r["count"]) for r in worst],
    }
    print(t, complete[t])

# COMMAND ----------

# DBTITLE 1,Geography -- coordinate validity
geo = {}
for t, df in DFS.items():
    if ROLE[t]["lat"] and ROLE[t]["lon"]:
        geo[t] = spatial_validity(
            df, ROLE[t]["lat"], ROLE[t]["lon"], bbox=GLOBAL_BBOX, name=t
        )
        print(
            t,
            {
                k: geo[t][k]
                for k in ("present", "missing", "null_island", "outside_bbox")
            },
        )

# COMMAND ----------

# DBTITLE 1,Location attributes across all tables (coordinates, country)
_parts = [
    DFS[t].select(
        qcol(ROLE[t]["loc"]).alias("c"),
        to_double(ROLE[t]["lat"]).alias("la"),
        to_double(ROLE[t]["lon"]).alias("lo"),
        (
            as_str(ROLE[t]["country"])
            if ROLE[t]["country"]
            else F.lit(None).cast("string")
        ).alias("cc"),
    )
    for t in tables
    if ROLE[t]["loc"] and ROLE[t]["lat"] and ROLE[t]["lon"]
]
loc_attr = None
if _parts:
    allc = reduce(lambda a, b: a.unionByName(b), _parts)
    loc_attr = (
        allc.groupBy("c")
        .agg(
            F.countDistinct("la").alias("lat_variants"),
            F.countDistinct("lo").alias("lon_variants"),
            F.countDistinct("cc").alias("country_variants"),
        )
        .agg(
            F.count(F.lit(1)).alias("locations"),
            F.sum(
                ((F.col("lat_variants") > 1) | (F.col("lon_variants") > 1)).cast("long")
            ).alias("coord_conflicts"),
            F.sum((F.col("country_variants") > 1).cast("long")).alias(
                "country_conflicts"
            ),
        )
        .first()
        .asDict()
    )
print(loc_attr)

# COMMAND ----------

# DBTITLE 1,Location sets across tables
city_sets = {
    t: collect_key_set(DFS[t], ROLE[t]["loc"]) for t in tables if ROLE[t]["loc"]
}
ri = []
same_cities = None
if city_sets:
    ref = max(city_sets, key=lambda t: len(city_sets[t]))
    same_cities = all(s == city_sets[ref] for s in city_sets.values())
    for t, s in city_sets.items():
        if s != city_sets[ref]:
            ri.append(
                referential_integrity(s, city_sets[ref], child=f"{t}", parent=f"{ref}")
            )
print("identical location sets in every table:", same_cities, ri)

# COMMAND ----------

# DBTITLE 1,Variant pairs (imperial/metric and forecast/historical)
variants = {}
for t, s in STRUCT.items():
    variants.setdefault((s["period"], s["grain"]), {})[s["unit"]] = t
UNIT_PAIRS = [
    (v["imperial"], v["metric"])
    for v in variants.values()
    if "imperial" in v and "metric" in v
]
periods = {}
for t, s in STRUCT.items():
    periods.setdefault((s["grain"], s["unit"]), {})[s["period"]] = t
PERIOD_PAIRS = [
    (v["forecast"], v["historical"])
    for v in periods.values()
    if "forecast" in v and "historical" in v
]
print("imperial/metric:", UNIT_PAIRS)
print("forecast/historical:", PERIOD_PAIRS)

# COMMAND ----------

# DBTITLE 1,Imperial vs metric -- schema differences
schema_diff = {}
for a, b in UNIT_PAIRS:
    ca, cb = set(DTYPE[a]), set(DTYPE[b])
    schema_diff[(a, b)] = {
        "only_imperial": sorted(ca - cb),
        "only_metric": sorted(cb - ca),
        "dtype_differs": sorted(c for c in ca & cb if DTYPE[a][c] != DTYPE[b][c]),
    }
    print(a, b, schema_diff[(a, b)])

# COMMAND ----------


# DBTITLE 1,Key frames and pair alignment
def key_frame(t, subs):
    r = ROLE[t]
    return (
        DFS[t]
        .select(
            qcol(r["loc"]).alias("k0"),
            qcol(r["time"]).cast("timestamp").alias("k1"),
            *[qcol(s).alias(f"k{2 + i}") for i, s in enumerate(subs)],
        )
        .distinct()
    )


def align_pair(a, b):
    subs = ROLE[a]["sub"] if ROLE[a]["sub"] == ROLE[b]["sub"] else []
    ka, kb = key_frame(a, subs), key_frame(b, subs)
    names = ["k0", "k1", *[f"k{2 + i}" for i in range(len(subs))]]
    only_a, only_b = ka.join(kb, names, "left_anti"), kb.join(ka, names, "left_anti")
    return {
        "subs": subs,
        "names": names,
        "keys_a": ka.count(),
        "keys_b": kb.count(),
        "both": ka.join(kb, names, "inner").count(),
        "only_a": only_a.count(),
        "only_b": only_b.count(),
        "only_a_sample": [
            str(r["k1"]) for r in only_a.orderBy("k1").limit(4).collect()
        ],
        "only_b_sample": [
            str(r["k1"]) for r in only_b.orderBy("k1").limit(4).collect()
        ],
    }


# COMMAND ----------

# DBTITLE 1,Imperial vs metric -- row alignment
unit_align = {}
for a, b in UNIT_PAIRS:
    if ROLE[a]["key"] and ROLE[b]["key"]:
        unit_align[(a, b)] = align_pair(a, b)
        print(a, b, {k: v for k, v in unit_align[(a, b)].items() if k != "names"})

# COMMAND ----------


# DBTITLE 1,Compare shared numeric columns on a joined key set
def compare_pair(a, b, max_cols=60):
    info = align_pair_cache[(a, b)]
    names = info["names"]
    shared = [
        c
        for c in ROLE[a]["numeric"]
        if c in ROLE[b]["numeric"]
        and num[a].get(c, {}).get("is_numeric")
        and num[b].get(c, {}).get("is_numeric")
        and c not in (ROLE[a]["lat"], ROLE[a]["lon"])
    ][:max_cols]
    subs = info["subs"]

    def side(t, tag):
        r = ROLE[t]
        return DFS[t].select(
            qcol(r["loc"]).alias("k0"),
            qcol(r["time"]).cast("timestamp").alias("k1"),
            *[qcol(s).alias(f"k{2 + i}") for i, s in enumerate(subs)],
            *[to_double(c).alias(f"{tag}_{c}") for c in shared],
        )

    j = side(a, "a").join(side(b, "b"), names, "inner")
    if not shared:
        return []
    aggs = []
    for i, c in enumerate(shared):
        both = F.col(f"a_{c}").isNotNull() & F.col(f"b_{c}").isNotNull()
        aggs += [
            F.sum(both.cast("long")).alias(f"n{i}"),
            F.avg(F.col(f"a_{c}")).alias(f"ma{i}"),
            F.avg(F.col(f"b_{c}")).alias(f"mb{i}"),
            F.corr(F.col(f"a_{c}"), F.col(f"b_{c}")).alias(f"r{i}"),
            F.sum((both & (F.col(f"a_{c}") == F.col(f"b_{c}"))).cast("long")).alias(
                f"e{i}"
            ),
            F.avg(F.when(both, F.abs(F.col(f"a_{c}") - F.col(f"b_{c}")))).alias(
                f"mae{i}"
            ),
        ]
    r = j.agg(*aggs).first().asDict()
    return [
        {
            "col": c,
            "n": r[f"n{i}"],
            "mean_a": r[f"ma{i}"],
            "mean_b": r[f"mb{i}"],
            "corr": r[f"r{i}"],
            "equal": r[f"e{i}"],
            "mae": r[f"mae{i}"],
        }
        for i, c in enumerate(shared)
    ]


align_pair_cache = dict(unit_align)
unit_cols = {}
for pair in UNIT_PAIRS:
    if pair in unit_align:
        unit_cols[pair] = compare_pair(*pair)
        rows = unit_cols[pair]
        same = [x["col"] for x in rows if x["n"] and x["equal"] == x["n"]]
        print(
            pair, f"{len(rows)} shared numeric columns; identical in both: {len(same)}"
        )

# COMMAND ----------

# DBTITLE 1,Forecast vs historical -- key overlap
period_align = {}
for a, b in PERIOD_PAIRS:
    if ROLE[a]["key"] and ROLE[b]["key"]:
        period_align[(a, b)] = align_pair(a, b)
        print(a, b, {k: v for k, v in period_align[(a, b)].items() if k != "names"})

# COMMAND ----------

# DBTITLE 1,Forecast vs historical -- values on the overlap window
align_pair_cache.update(period_align)
period_cols = {}
for pair, info in period_align.items():
    if info["both"] > 0:
        period_cols[pair] = compare_pair(*pair, max_cols=30)
        print(
            pair,
            f"{info['both']} overlapping keys; {len(period_cols[pair])} shared numeric columns",
        )

# COMMAND ----------

# DBTITLE 1,Categorical and coded columns
cat_dist = {}
for t, df in DFS.items():
    p = prof[t]
    cats = [
        c
        for c in ROLE[t]["string"]
        if 1 < p["acd"][c] <= 40 and c not in ROLE[t]["key"]
    ]
    cats += [
        c
        for c in p["cols"]
        if c.lower().endswith(CODE_SUFFIXES)
        and 1 < p["acd"][c] <= 60
        and c not in cats
        and c != ROLE[t]["country"]
    ]
    cat_dist[t] = {}
    for c in cats[:10]:
        vc = df.groupBy(qcol(c)).count().orderBy(F.desc("count")).limit(25).collect()
        cat_dist[t][c] = [(r[0], r["count"]) for r in vc]
    print(t, list(cat_dist[t]))

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
_missing_by_table = [(t, complete[t]["missing"]) for t in tables if t in complete]
if _missing_by_table and facet_bars(
    {"missing (location, time) cells vs full grid": _missing_by_table},
    "AccuWeather -- completeness",
    "accuweather_completeness.png",
    rot=60,
    ncols=1,
):
    figs.append(("AccuWeather -- completeness", "accuweather_completeness.png"))
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
        f"{t}: rows={prof[t]['total']}, cols={len(prof[t]['cols'])}, constant={len(prof[t]['constant'])}, "
        f"all-missing={len(prof[t]['all_missing'])}, dups={prof[t]['dups']}, key={ROLE[t]['key']}, "
        f"missing grid cells={complete.get(t, {}).get('missing')}"
    )

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/accuweather.md
_profile = [
    "| table | period | grain | unit | rows | cols | size (bytes) | constant cols | all-missing cols |",
    "|---|---|---|---|---|---|---|---|---|",
]
for t in tables:
    d, s = detail.get(t, {}), STRUCT[t]
    _profile.append(
        f"| {t} | {s['period']} | {s['grain']} | {s['unit']} | {prof[t]['total']} | {len(prof[t]['cols'])} | "
        f"{d.get('size_bytes', '-')} | {len(prof[t]['constant'])} | {len(prof[t]['all_missing'])} |"
    )

_prov = [
    para(
        "The tables are read in place from the Databricks Samples catalog",
        f"(`{FQ_SCHEMA}`); there is no Bronze table and no ECRMAP acquisition step.",
    ),
    f"Tables found: {len(tables)} -- {tables}.",
]
if comment_tables:
    _prov.append("Table comments in the catalog (each distinct comment shown once):")
    for text, names in comment_tables.items():
        _prov.append(f"- shared by {len(names)} tables ({short(names, 3)}):")
        _prov += [f"  > {ln.strip()[:400]}" for ln in text.splitlines() if ln.strip()][
            :12
        ]
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
else:
    _prov.append(
        "- No column comments in the catalog: column meanings and units come only from the names."
    )
_prov.append(
    "- Issue / extract-time columns (names containing "
    + ", ".join(ISSUE_HINTS)
    + f"): {short(sorted({c for t in tables for c in ROLE[t]['issue']}))}. "
    + f"Time-zone-like columns: {short(sorted({c for t in tables for c in ROLE[t]['tz']}))}."
)

_struct = [
    para(
        "Table names encode three things: period (`forecast` / `historical`), grain",
        "(`daily_calendar`, `daynight`, `hourly`) and unit system (`imperial` / `metric`).",
    )
]
for t in tables:
    r = ROLE[t]
    _struct.append(
        f"- `{t}`: {len(DTYPE[t])} columns -- numeric {len(r['numeric'])}, string {len(r['string'])}, "
        f"time {r['time_typed'] or 'none'}; location `{r['loc']}`; time key `{r['time']}`; sub-key {r['sub'] or 'none'}."
    )

_dq = [
    "Full-row exact duplicates, constant columns, all-missing columns and composite-key duplicates:"
]
for t in tables:
    dk = dupk.get(t)
    _dq.append(
        f"- `{t}`: full-row duplicates {prof[t]['dups']}; constant columns {len(prof[t]['constant'])} {short(prof[t]['constant'], 8)}; "
        f"all-missing columns {len(prof[t]['all_missing'])} {short(prof[t]['all_missing'], 8)}"
        + (
            f"; key {ROLE[t]['key']} duplicated {dk['dup_groups']} of {dk['distinct_keys']} "
            f"({dk['identical']} identical, {dk['conflicting']} conflicting)"
            if dk
            else ""
        )
        + "."
    )
_dq.append("Partly missing columns (rate above 5%, excluding all-missing):")
for t in tables:
    p = prof[t]
    part = [
        (c, round(p["miss"][c] / p["total"], 3))
        for c in p["cols"]
        if p["total"] and 0.05 < p["miss"][c] / p["total"] < 1.0
    ]
    _dq.append(f"- `{t}`: {short(part, 10)}")
_dq.append(
    "Cap / sentinel-like values (columns with at least 5% of rows at the column maximum):"
)
for t in tables:
    _dq.append(
        f"- `{t}`: (column, max, rows at max, share) {short(caps.get(t, []), 8)}"
    )
_dq.append("Completeness against the full location x time (x day/night flag) grid:")
for t, c in complete.items():
    _dq.append(
        f"- `{t}`: expected {c['expected']}, present {c['actual']}, missing {c['missing']}; "
        f"locations with most missing (location, count): {c['worst'] or 'none'}."
    )

_entities = [
    "Location key `city_name` and its uniqueness per table (distinct / ratio / unique):"
]
for t in tables:
    if t in uniq:
        for c, u in uniq[t].items():
            _entities.append(
                f"- `{t}.{c}`: {u['distinct']} / {u['ratio']} / unique={u['unique']}"
            )
    if t in loc_stats:
        s = loc_stats[t]
        _entities.append(
            f"  rows per location: min {s['min']}, p50 {s['p50']}, max {s['max']} ({s['locations']} locations)."
        )
if loc_attr:
    _entities.append(
        f"- across all tables: {loc_attr['locations']} locations; {loc_attr['coord_conflicts']} with more than one "
        f"coordinate pair and {loc_attr['country_conflicts']} with more than one country code."
    )
if same_cities is not None:
    _entities.append(
        f"- the location set is {'identical' if same_cities else 'NOT identical'} in every table"
        + (f": {ri}" if ri else ".")
    )

_unit = []
for t in tables:
    for c, s in num[t].items():
        if not s["is_numeric"] or prof[t]["acd"][c] <= 1:
            continue
        line = (
            f"- `{t}.{c}`: range {s['min']}..{s['max']}, mean {fmt_num(s['mean'])}, sd {fmt_num(s['sd'])}, "
            f"zero {s['zero']}, negative {s['negative']}"
        )
        if c in plaus[t]:
            p = plaus[t][c]
            line += f"; bounds {p['bounds']} ({STRUCT[t]['unit']}): below {p['below']}, above {p['above']}"
        _unit.append(line + ".")
if not _unit:
    _unit.append("- No numeric column found.")
_unit.append(
    "Bounds are applied only to columns matched by name prefix / suffix and depend on the table's unit system."
)

_domain = []
for t in tables:
    for c, pairs_ in cat_dist[t].items():
        _domain.append(f"- `{t}.{c}`: " + fmt_pairs(pairs_, n=8).replace("\n", " "))
if not _domain:
    _domain.append("- No low-cardinality string column located.")

_geo = []
for t, g in geo.items():
    _geo.append(
        f"- `{t}`: coordinates present {g['present']}, missing {g['missing']}, at 0/0 {g['null_island']}, "
        f"outside the global box {g['outside_bbox']}."
    )
if loc_attr:
    _geo.append(
        f"- coordinates and country code per location across all tables: {loc_attr['coord_conflicts']} locations with conflicting "
        f"coordinates, {loc_attr['country_conflicts']} with conflicting country codes."
    )
_geo.append(
    "A latitude/longitude-swap test is not meaningful against a global box and is not reported."
)

_temporal = []
for t in tables:
    for c, v in tinfo[t].items():
        _temporal.append(
            f"- `{t}.{c}`: {v['lo']} .. {v['hi']}; {v['distinct']} distinct values; rows with a non-midnight time {v['has_time']}."
        )
    if t in horizon:
        _temporal.append(
            f"  span of the time key `{ROLE[t]['time']}`: {horizon[t]} days."
        )
if not _temporal:
    _temporal.append("- No time column located.")

_tcons = []
for t, cd in cadence.items():
    _tcons.append(
        f"- `{t}` per {cd['partition']} over `{cd['time_column']}`: gap p50 {cd['gap_p50']} s, p99 {cd['gap_p99']} s, "
        f"max {cd['gap_max']} s; zero-second gaps {cd['zero_gaps']}."
    )
if not _tcons:
    _tcons.append(
        "- No table had both a location key and a time key; cadence not assessed."
    )

_card = [
    "Row alignment between the two unit systems (same period and grain), on the key (location, time[, day/night flag]):"
]
for (a, b), r in unit_align.items():
    _card.append(
        f"- `{a}` vs `{b}`: keys {r['keys_a']} / {r['keys_b']}; in both {r['both']}; only imperial {r['only_a']} {r['only_a_sample']}; "
        f"only metric {r['only_b']} {r['only_b_sample']}."
    )
_card.append(
    "Rows are related only through the key above; there is no surrogate row identifier."
)

_variants = ["Schema differences between the two unit systems:"]
for (a, b), d in schema_diff.items():
    _variants.append(
        f"- `{a}` vs `{b}`: only in imperial {d['only_imperial']}; only in metric {d['only_metric']}; different type {d['dtype_differs']}."
    )
_variants.append(
    "Agreement of shared numeric columns on the joined keys (column: n, mean imperial, mean metric, corr, identical rows, mean abs diff):"
)
for pair, rows in unit_cols.items():
    same = [x["col"] for x in rows if x["n"] and x["equal"] == x["n"]]
    _variants.append(
        f"- `{pair[0]}` vs `{pair[1]}`: {len(rows)} shared numeric columns; identical in every joined row: {short(same, 20)}."
    )
    differing = [x for x in rows if x["n"] and x["equal"] != x["n"]]
    for x in differing[:15]:
        _variants.append(
            f"  - `{x['col']}`: n {x['n']}, {fmt_num(x['mean_a'])} vs {fmt_num(x['mean_b'])}, corr {fmt_num(x['corr'])}, "
            f"identical {x['equal']}, mean abs diff {fmt_num(x['mae'])}"
        )
    if len(differing) > 15:
        _variants.append(f"  - (+{len(differing) - 15} more differing columns)")

_fh = [
    "Forecast versus historical tables (same grain and unit): key overlap and value comparison on the overlap:"
]
for (a, b), r in period_align.items():
    _fh.append(
        f"- `{a}` vs `{b}`: keys {r['keys_a']} / {r['keys_b']}; overlapping {r['both']}; only forecast {r['only_a']}; only historical {r['only_b']}."
    )
for pair, rows in period_cols.items():
    _fh.append(
        f"- `{pair[0]}` (forecast) minus `{pair[1]}` (historical) on {period_align[pair]['both']} overlapping keys:"
    )
    for x in sorted(rows, key=lambda x: -(x["mae"] or 0))[:12]:
        _fh.append(
            f"  - `{x['col']}`: n {x['n']}, forecast mean {fmt_num(x['mean_a'])}, historical mean {fmt_num(x['mean_b'])}, "
            f"corr {fmt_num(x['corr'])}, identical {x['equal']}, mean abs diff {fmt_num(x['mae'])}"
        )
if not period_align:
    _fh.append("- No forecast/historical pair with a usable key.")

_regime = [
    para(
        "Forecast versus observation: no table carries an issue / extract time unless one is listed",
        "under Provenance, so a forecast row is identified only by its valid time. Evidence per table:",
    )
]
for t in tables:
    _regime.append(
        f"- `{t}`: time columns {list(tinfo[t]) or 'none'}; issue-like columns {ROLE[t]['issue'] or 'none'}; span {horizon.get(t)} days."
    )

_coverage = [
    "The catalog comment (see Provenance) states the scope of the sample; the data itself carries no statement of it."
]
for t in tables:
    if t in loc_stats:
        _coverage.append(
            f"- `{t}`: {loc_stats[t]['locations']} `{ROLE[t]['loc']}` values."
        )

_dist = []
for t in tables:
    for c, pairs_ in cat_dist[t].items():
        _dist.append(f"- `{t}.{c}`: " + fmt_pairs(pairs_, n=10).replace("\n", " "))

_findings_md = "\n".join(
    f"- `{t}`: rows={prof[t]['total']}, cols={len(prof[t]['cols'])}, dups={prof[t]['dups']}, key={ROLE[t]['key']}, "
    f"key duplicates={dupk.get(t, {}).get('dup_groups')}, missing grid cells={complete.get(t, {}).get('missing')}, "
    f"cap-like columns={len(caps.get(t, []))}"
    for t in tables
)

_silver = [
    "- Read from the Samples catalog; no Bronze table exists for these tables.",
    "- The set of tables, keys and time semantics above decide how many Silver structures are legitimately needed; nothing is assumed here.",
    "- The imperial/metric alignment and schema-difference results decide whether the two unit systems are separate records or one record in two units.",
    "- The forecast/historical overlap results decide how a forecast row and an observed row for the same key can be related.",
    "- Cap-like columns and all-missing columns need an explicit keep / null / drop rule.",
]

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            f"Composite keys and their duplicate counts: {dupk}. Table grains follow the table names (daily_calendar, daynight, hourly).",
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            f"Rows per location: { {t: (s['min'], s['p50'], s['max']) for t, s in loc_stats.items()} }. Joining daynight to daily on (location, date) doubles rows unless the day/night flag is used.",
        ),
        (
            "Target contamination",
            "No target is defined by the source; forecast columns are predictions, so using them as features for their own observed counterpart is circular.",
        ),
        (
            "Temporal / post-event leakage",
            f"Issue-time columns: { {t: ROLE[t]['issue'] for t in tables if ROLE[t]['issue']} or 'none' }. Without one, a forecast row cannot be tied to the moment it was issued.",
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
            f"Forecast/historical overlap: { {f'{a} vs {b}': r['both'] for (a, b), r in period_align.items()} }. Where the windows overlap, a historical row can leak into a forecast evaluation.",
        ),
        (
            "Survivorship / coverage bias",
            f"Locations per table: { {t: s['locations'] for t, s in loc_stats.items()} }; the sample is stated to be a reduced selection.",
        ),
        (
            "Missingness leakage",
            f"All-missing columns per table: { {t: len(prof[t]['all_missing']) for t in tables} }; partly missing columns are listed in Data Quality and can track weather condition.",
        ),
        (
            "Duplicate-event leakage",
            f"Full-row duplicates: { {t: prof[t]['dups'] for t in tables} }; composite-key duplicates: { {t: d['dup_groups'] for t, d in dupk.items()} }.",
        ),
        (
            "Target / feature temporal misalignment",
            f"Time-zone-like columns: { {t: ROLE[t]['tz'] for t in tables if ROLE[t]['tz']} or 'none' }; the local-time key is not assumed to be UTC.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            f"Imperial and metric tables hold the same measures in two units; identical-column counts: { {f'{a}|{b}': sum(1 for x in rows if x['n'] and x['equal'] == x['n']) for (a, b), rows in unit_cols.items()} }. Using both as features double-counts.",
        ),
        (
            "Data-generation-process leakage",
            "Provider-generated forecasts embed the provider's model; the catalog comment describes the data as an AccuWeather sample.",
        ),
        (
            "Class / label instability",
            "Condition text / icon and weather codes are provider taxonomies and can change between provider versions.",
        ),
        (
            "Label availability lag",
            "Observations arrive after the fact; forecasts are available before -- which side each table is on is in the table name.",
        ),
        (
            "Source / version / regime change",
            "No provider version field is assumed; look for constant version-like columns in the constant-column list.",
        ),
        (
            "Sample-vs-full divergence",
            "Every statistic is a full Spark aggregation over the table read, except approximate distinct counts and percentiles, which are labelled; the catalog comment states these tables are a reduced portion of the provider's data.",
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
        ("Unit-System Variants", "\n".join(_variants)),
        ("Forecast vs Historical", "\n".join(_fh)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
