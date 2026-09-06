# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- DWD RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Cross-table checks across the 12 DWD Bronze tables --
# MAGIC station-id joinability, referential integrity between measurements and
# MAGIC metadata, station overlap across measurements, city consistency, full
# MAGIC relationship-cardinality profiles, cross-measurement timestamp
# MAGIC alignment, point-in-time consistency of measurements vs station
# MAGIC validity windows, and an evidence-based verdict on whether the 7
# MAGIC measurements can be combined. Station-id sets are small and collected;
# MAGIC the (station, timestamp) overlap is derived from one tagged union + one
# MAGIC presence matrix.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "dwd"
NB_KEY = "04_relationships_and_findings"
SECTION_TITLE = "Cross-table relationships & verdict"
MEASUREMENTS = [
    "air_temperature",
    "cloudiness",
    "moisture",
    "precipitation",
    "pressure",
    "sun",
    "wind",
]
MEASUREMENT_TABLES = {m: f"{CATALOG}.{BRONZE_SCHEMA}.dwd_{m}" for m in MEASUREMENTS}
META = {
    "station_geography": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_station_geography",
    "station_name_history": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_station_name_history",
    "parameter_unit": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_parameter_unit",
    "device_instrument": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_device_instrument",
}


def as_ts(col):
    # try_to_timestamp -> NULL on bad input (a plain to_timestamp with a format
    # THROWS CANNOT_PARSE_TIMESTAMP under ANSI).
    s = F.regexp_replace(F.col(col).cast("string"), r"\.0$", "")
    return F.coalesce(
        F.try_to_timestamp(s, F.lit("yyyyMMddHH")),
        F.try_to_timestamp(F.substring(s, 1, 10), F.lit("yyyyMMddHH")),
    )


# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Distinct station-id sets per table (collected -- small) + measurement row counts
station_set = {}
table_rows = {}
for name, t in {**MEASUREMENT_TABLES, **META}.items():
    df = spark.table(t)
    sid = find_col(df, "STATIONS_ID", "Stations_id", "stations_id")
    station_set[name] = {
        str(x[0]) for x in df.select(F.col(sid).cast("string")).distinct().collect()
    }
    print(
        f"{name:<24} distinct stations = {len(station_set[name])}: {sorted(station_set[name])}"
    )
for m, t in MEASUREMENT_TABLES.items():
    table_rows[m] = spark.table(t).count()

# COMMAND ----------

# DBTITLE 1,Station overlap + referential integrity (Python set math)
union_stations = set().union(*(station_set[m] for m in MEASUREMENTS))
measure_missing = {m: len(union_stations - station_set[m]) for m in MEASUREMENTS}
print(f"union of stations across measurements = {len(union_stations)}")
print("missing per measurement (vs union):", measure_missing)
ref_integrity = {}
for meta_name in ("station_geography", "station_name_history", "parameter_unit"):
    orphans = len(union_stations - station_set[meta_name])
    unused = len(station_set[meta_name] - union_stations)
    ref_integrity[meta_name] = (orphans, unused)
    print(
        f"{meta_name}: measurement-stations-not-in-metadata={orphans}  metadata-stations-unused={unused}"
    )

# COMMAND ----------

# DBTITLE 1,City <-> station consistency + city coordinates (one union + distinct, collected)
city_map = None
for t in MEASUREMENT_TABLES.values():
    df = spark.table(t)
    sid = find_col(df, "STATIONS_ID")
    part = df.select(F.col(sid).cast("string").alias("station_id"), F.col("city"))
    city_map = part if city_map is None else city_map.union(part)
cm = {(x["station_id"], x["city"]) for x in city_map.distinct().collect()}
station_cities = {}
for s, c in cm:
    station_cities.setdefault(s, set()).add(c)
multi_city = {s: sorted(cs) for s, cs in station_cities.items() if len(cs) > 1}
city_counts = {}
for s, cs in station_cities.items():
    for c in cs:
        city_counts[c] = city_counts.get(c, 0) + 1
print("stations mapped to >1 city:", multi_city)
print("stations per city:", city_counts)

# COMMAND ----------

# DBTITLE 1,Relationship cardinality -- measurement station -> each metadata table
geo = spark.table(META["station_geography"])
gsid = find_col(geo, "STATIONS_ID", "Stations_id", "stations_id")
meta_card = {}
for meta_name, meta_t in META.items():
    md = spark.table(meta_t)
    s = find_col(md, "STATIONS_ID", "Stations_id", "stations_id")
    child_keys = {
        str(x[0]) for x in md.select(F.col(s).cast("string")).distinct().collect()
    }
    cp = cardinality_profile(md, s, child_keys, union_stations)
    meta_card[meta_name] = cp
    kind = "1:1" if cp["max_fanout"] <= 1 else "1:N (fan-out on station_id alone)"
    print(f"measurement -> {meta_name}: {kind}  {cp}")

# COMMAND ----------

# DBTITLE 1,Cross-measurement (station, MESS_DATUM) presence matrix -- one tagged union
u = None
for m, t in MEASUREMENT_TABLES.items():
    df = spark.table(t)
    s, d = find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")
    part = df.select(
        F.col(s).cast("string").alias("station"),
        F.col(d).cast("string").alias("ts"),
        F.lit(m).alias("src"),
    )
    u = part if u is None else u.union(part)
p = u.groupBy("station", "ts").agg(
    *[F.max((F.col("src") == m).cast("int")).alias(m) for m in MEASUREMENTS]
)

# COMMAND ----------

# DBTITLE 1,Overlap / cardinality stats -- one agg over the presence matrix
pairs = [(a, b) for i, a in enumerate(MEASUREMENTS) for b in MEASUREMENTS[i + 1 :]]
S = (
    p.agg(
        F.count(F.lit(1)).alias("union_keys"),
        *[F.sum(m).alias("present__" + m) for m in MEASUREMENTS],
        *[F.sum(F.col(a) * F.col(b)).alias(f"pair__{a}__{b}") for a, b in pairs],
    )
    .first()
    .asDict()
)
union_keys = S["union_keys"]
present = {m: S["present__" + m] for m in MEASUREMENTS}
pair_overlap = []
for a, b in pairs:
    shared = S[f"pair__{a}__{b}"]
    pair_overlap.append((a, b, shared, present[a] - shared, present[b] - shared))
    print(
        f"{a:<16} x {b:<16}  shared={shared:>12}  only_{a}={present[a] - shared:>12}  only_{b}={present[b] - shared:>12}"
    )
key_unique = {m: table_rows[m] == present[m] for m in MEASUREMENTS}
for m in MEASUREMENTS:
    print(
        f"{m:<16} rows={table_rows[m]:>12}  distinct (station, ts)={present[m]:>12}  key_unique={key_unique[m]}"
    )

# COMMAND ----------

# DBTITLE 1,Point-in-time consistency -- measurement hours vs the station geography validity window
geo_von = find_col(geo, "von_datum", "Von_Datum", "von")
geo_bis = find_col(geo, "bis_datum", "Bis_Datum", "bis")
pit = {}
if gsid and geo_von:
    win = geo.select(
        F.col(gsid).cast("string").alias("station"),
        F.try_to_timestamp(
            F.regexp_replace(F.col(geo_von).cast("string"), r"\.0$", ""),
            F.lit("yyyyMMdd"),
        ).alias("gv"),
        F.try_to_timestamp(
            F.regexp_replace(F.col(geo_bis).cast("string"), r"\.0$", ""),
            F.lit("yyyyMMdd"),
        ).alias("gb")
        if geo_bis
        else F.lit(None).cast("timestamp").alias("gb"),
    )
    # widest window per station (min von .. max bis, open bis -> now)
    wspan = win.groupBy("station").agg(
        F.min("gv").alias("gv"),
        F.max(F.coalesce("gb", F.current_timestamp())).alias("gb"),
    )
    for m, t in MEASUREMENT_TABLES.items():
        df = spark.table(t)
        s, dd = find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")
        j = (
            df.select(F.col(s).cast("string").alias("station"), as_ts(dd).alias("ts"))
            .where(F.col("ts").isNotNull())
            .join(wspan, on="station", how="inner")
        )
        r = (
            j.agg(
                F.count(F.lit(1)).alias("comparable"),
                F.sum(
                    ((F.col("ts") < F.col("gv")) | (F.col("ts") > F.col("gb"))).cast(
                        "long"
                    )
                ).alias("outside"),
            )
            .first()
            .asDict()
        )
        comp = r["comparable"] or 0
        pit[m] = {
            "comparable_rows": comp,
            "outside_window": r["outside"] or 0,
            "outside_pct": round((r["outside"] or 0) / comp * 100, 4) if comp else None,
        }
        print(
            f"{m}: measurement hours outside the station geography window -> {pit[m]}"
        )

# COMMAND ----------

# DBTITLE 1,Verdict -- can the 7 measurements be combined downstream?
max_pair_only = max((max(o[3], o[4]) for o in pair_overlap), default=0)
schemas = {
    m: tuple(sorted(spark.table(MEASUREMENT_TABLES[m]).columns)) for m in MEASUREMENTS
}
schema_disjoint = len(set(schemas.values())) == len(schemas)
print(f"(station, MESS_DATUM) unique in every measurement : {all(key_unique.values())}")
print(f"largest non-shared timestamp count in any pair    : {max_pair_only}")
print(f"every measurement has a distinct value-column set  : {schema_disjoint}")

# COMMAND ----------

# DBTITLE 1,Figures
figs = []
labels = [f"{a[:4]}x{b[:4]}" for a, b, *_ in pair_overlap]
shared_v = [o[2] for o in pair_overlap]
nonshared_v = [o[3] + o[4] for o in pair_overlap]
xx = np.arange(len(labels))
fig, ax = plt.subplots(figsize=(13, 4))
ax.bar(xx, shared_v, label="shared (station, ts)")
ax.bar(xx, nonshared_v, bottom=shared_v, label="only one side")
ax.set_xticks(xx)
ax.set_xticklabels(labels, rotation=90)
ax.legend()
ax.set_title("DWD -- cross-measurement timestamp overlap per pair")
ax.set_ylabel("(station, ts) keys")
fig.tight_layout()
_save_and_show(fig, "dwd_cross_measurement_overlap.png")
figs.append(
    (
        "DWD cross-measurement (station, timestamp) overlap per pair",
        "dwd_cross_measurement_overlap.png",
    )
)

if barplot(
    [(n, len(station_set[n])) for n in {**MEASUREMENT_TABLES, **META}],
    "DWD -- distinct stations per Bronze table",
    "table",
    "stations",
    rot=40,
    filename="dwd_stations_per_bronze_table.png",
):
    figs.append(
        ("DWD distinct stations per Bronze table", "dwd_stations_per_bronze_table.png")
    )

if barplot(
    list(measure_missing.items()),
    "DWD -- stations absent from each measurement (vs union of all 7)",
    "measurement",
    "missing",
    rot=30,
    filename="dwd_stations_absent_per_measurement.png",
):
    figs.append(
        (
            "DWD stations absent from each measurement",
            "dwd_stations_absent_per_measurement.png",
        )
    )

metas = list(ref_integrity)
x = np.arange(len(metas))
fig, ax = plt.subplots(figsize=(10, 4))
ax.bar(
    x - 0.2,
    [ref_integrity[k][0] for k in metas],
    width=0.4,
    label="orphan measurement stations",
)
ax.bar(
    x + 0.2,
    [ref_integrity[k][1] for k in metas],
    width=0.4,
    label="unused metadata stations",
)
ax.set_xticks(x)
ax.set_xticklabels(metas, rotation=20, ha="right")
ax.legend()
ax.set_title("DWD -- referential integrity: measurements <-> metadata")
ax.set_ylabel("stations")
fig.tight_layout()
_save_and_show(fig, "dwd_referential_integrity.png")
figs.append(
    (
        "DWD referential integrity: measurements <-> metadata",
        "dwd_referential_integrity.png",
    )
)

if barplot(
    sorted(city_counts.items()),
    "DWD -- distinct stations per city",
    "city",
    "stations",
    filename="dwd_stations_per_city.png",
):
    figs.append(("DWD distinct stations per city", "dwd_stations_per_city.png"))

# COMMAND ----------

# DBTITLE 1,Findings
print(
    "distinct stations per table :",
    {n: len(station_set[n]) for n in {**MEASUREMENT_TABLES, **META}},
)
print("stations missing per measurement (vs union):", measure_missing)
print("referential integrity (orphans, unused)   :", ref_integrity)
print("stations mapped to >1 city                 :", multi_city)
print(
    "measurement->metadata max fan-out          :",
    {k: v["max_fanout"] for k, v in meta_card.items()},
)
print("(station, MESS_DATUM) unique in every measurement:", all(key_unique.values()))
print(
    "measurement hours outside station window   :",
    {m: v["outside_pct"] for m, v in pit.items()},
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/dwd.md
_ent = [
    f"Distinct station ids per Bronze table: { {n: len(station_set[n]) for n in {**MEASUREMENT_TABLES, **META}} }",
    f"Union of stations across the 7 measurements: {len(union_stations)} -> {sorted(union_stations)}",
    f"Stations absent from each measurement (vs the union): {measure_missing}",
    f"Stations mapped to >1 city: {multi_city}   |   distinct stations per city: {city_counts}",
]

_rel = [
    "Referential integrity -- measurement stations vs metadata (orphans, unused):",
    *[
        f"- {k}: orphan measurement stations={v[0]}, unused metadata stations={v[1]}"
        for k, v in ref_integrity.items()
    ],
    "",
    "Relationship cardinality -- measurement station -> metadata table:",
]
for k, cp in meta_card.items():
    _rel.append(
        f"- {k}: {cp['child_rows']} rows over {cp['distinct_child_keys']} station keys; "
        f"{cp['matched_parent_keys']} of {cp['parent_keys_total']} measurement stations have a row "
        f"({cp['parent_keys_referenced_pct']}%); rows-per-station p50/p90/p99 "
        f"{cp['child_rows_per_parent_p50']}/{cp['child_rows_per_parent_p90']}/"
        f"{cp['child_rows_per_parent_p99']}, max fan-out {cp['max_fanout']}, orphan keys "
        f"{cp['orphan_child_keys']}."
    )
_rel += [
    "",
    f"(STATIONS_ID, MESS_DATUM) unique within each measurement: {key_unique}",
    "",
    "Cross-measurement (station, MESS_DATUM) overlap per pair (shared, only_a, only_b):",
    *[
        f"- {a} x {b}: shared={sh}, only_{a}={oa}, only_{b}={ob}"
        for a, b, sh, oa, ob in pair_overlap
    ],
    "",
    f"union of (station, ts) across all 7 measurements = {union_keys}",
]

_tcons = [
    para(
        "Measurement hours vs the station's geography validity window (widest",
        "von..bis, open bis -> now). A row outside the window means the station was",
        "producing data for a period its geography record does not cover.",
    ),
    "",
]
if pit:
    for m, v in pit.items():
        _tcons.append(
            f"- {m}: {v['outside_window']}/{v['comparable_rows']} hours outside the window "
            f"({v['outside_pct']}%)."
        )
else:
    _tcons.append("- station_geography has no von/bis columns -- not assessable.")

_spatial = [
    f"`city` <-> station_id: {len(multi_city)} station(s) mapped to >1 city {multi_city or ''}.",
    para(
        "Coordinate-level spatial validity is in section 02. There is no second",
        "coordinate source to cross-check DWD's own against (LIMITATION); the",
        "checkable cross-table consistency is that a station resolves to one city.",
    ),
]

_verdict = [
    f"- (station, MESS_DATUM) is unique in every measurement: {all(key_unique.values())}  -> pairwise measurement<->measurement joins are 1:1 on the overlap.",
    f"- largest non-shared timestamp count in any measurement pair: {max_pair_only}  -> an inner join to a wide table drops that tail.",
    f"- all 7 measurements have disjoint value-column sets: {schema_disjoint}.",
    "- Verdict: combine into a wide table only where timestamps overlap; keep one model per measurement in Silver, align at Gold.",
]

_silver = [
    "- Shared join key is (STATIONS_ID, MESS_DATUM); unique per measurement -> safe fan-out-free measurement<->measurement joins.",
]
if any(cp["max_fanout"] > 1 for cp in meta_card.values()):
    _silver.append(
        "- station_geography / station_name_history are 1:N on station_id -> join with the von/bis validity window, never station_id alone."
    )
if any(v[0] > 0 for v in ref_integrity.values()):
    _silver.append(
        "- Referential-integrity orphans exist -> left-join + a data-quality flag; do not drop the fact row."
    )
if pit and any(v["outside_window"] for v in pit.values()):
    _silver.append(
        "- Some measurement hours fall outside the station's geography validity window -> extend the window or flag; do not silently drop."
    )
if not multi_city:
    _silver.append(
        "- `city` is consistent 1:1 with station_id here -> safe to carry as a station attribute."
    )
_silver.append(
    "- A cross-measurement wide 'all weather at station S, hour H' table drops rows (overlap numbers above) -> that is a Gold consolidation, not Silver."
)

_no_target = para(
    "No candidate ML target lives across these 7 measurement + metadata tables --",
    "this notebook is a joinability audit, not a labelled-outcome source.",
)
_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            para(
                "(STATIONS_ID, MESS_DATUM) is unique within every measurement",
                f"({all(key_unique.values())}) -- a station-level (not row-level) split is",
                "required for any downstream model combining these tables.",
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            para(
                "measurement -> metadata is 1:1 only for",
                f"{[k for k, v in meta_card.items() if v['max_fanout'] <= 1]};",
                f"{[k for k, v in meta_card.items() if v['max_fanout'] > 1]} fan out and MUST be",
                "joined on the von/bis window, not station_id alone (full profile above).",
            ),
        ),
        ("Target contamination", _no_target),
        (
            "Temporal / post-event leakage",
            para(
                "Point-in-time consistency above quantifies measurement hours outside",
                "the station's geography window -- a metadata attribute joined without",
                "the window can attach a future location to a historical row.",
            ),
        ),
        (
            "Proxy leakage",
            "Station id / city are near-unique site identifiers -- a model given them memorises the station.",
        ),
        (
            "Split / entity leakage",
            "Split by station id across ALL tables at once so a station's rows stay on one side of every join.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            "The validity-window join is the point-in-time mechanism -- an un-windowed metadata join is point-in-time leakage.",
        ),
        (
            "Survivorship / coverage bias",
            para(
                f"Referential-integrity orphans { {k: v[0] for k, v in ref_integrity.items()} }",
                f"and cross-measurement non-overlap (max {max_pair_only}) mean a joined",
                "training set silently drops the under-instrumented stations/hours.",
            ),
        ),
        (
            "Missingness leakage",
            "Whether a station appears in a metadata table correlates with its tenure in the network -- an 'is-known' flag can leak that.",
        ),
        (
            "Duplicate-event leakage",
            "Per-measurement (station, ts) duplicate composition is in section 01 -- de-duplicate before joining or splitting.",
        ),
        (
            "Target / feature temporal misalignment",
            "MESS_DATUM (hour) vs metadata von/bis (day) are different resolutions -- align before pairing.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            f"All 7 measurements have disjoint value-column sets ({schema_disjoint}) -- no direct column-overlap leakage between them.",
        ),
        (
            "Data-generation-process leakage",
            "A parser trailer row in metadata (02) would inject a non-station into every station-keyed join here -- filter it first.",
        ),
        (
            "Class / label instability",
            "parameter_unit / device_instrument codes are DWD enumerations that change between archive versions -- pin the version.",
        ),
        ("Label availability lag", "Not applicable -- joinability audit, no label."),
        (
            "Source / version / regime change",
            "Station coverage and the metadata schema shifted over the archive's multi-decade span (per-decade evidence in 01/03).",
        ),
        (
            "Sample-vs-full divergence",
            "Every statistic here (station sets, overlap counts, cardinality, point-in-time) is a full Spark aggregation or a fully collected small set -- no sampling.",
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Entities / Keys", "\n".join(_ent)),
        ("Relationships", "\n".join(_rel)),
        ("Temporal Consistency", "\n".join(_tcons)),
        ("Spatial Consistency", "\n".join(_spatial)),
        ("EDA Findings", "\n".join(_verdict)),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
