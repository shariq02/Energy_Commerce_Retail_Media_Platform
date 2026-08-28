# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- DWD RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Cross-table checks across the 12 DWD Bronze tables --
# MAGIC station-id joinability, referential integrity between measurements and
# MAGIC metadata, station overlap across measurements, city consistency,
# MAGIC cross-measurement timestamp alignment, join cardinality, and an
# MAGIC evidence-based verdict on whether the 7 measurements can be combined.
# MAGIC Station-id sets are small and collected; the (station, timestamp)
# MAGIC overlap is derived from one tagged union + one presence matrix instead
# MAGIC of repeated pairwise joins.

# COMMAND ----------

# DBTITLE 1,Imports
import matplotlib.pyplot as plt
import numpy as np

from pyspark.sql import DataFrame, functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
MEASUREMENTS = ["air_temperature", "cloudiness", "moisture", "precipitation", "pressure", "sun", "wind"]
MEASUREMENT_TABLES = {m: f"{CATALOG}.{BRONZE_SCHEMA}.dwd_{m}" for m in MEASUREMENTS}
META = {"station_geography": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_station_geography",
        "station_name_history": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_station_name_history",
        "parameter_unit": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_parameter_unit",
        "device_instrument": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_device_instrument"}

# COMMAND ----------

# DBTITLE 1,Helpers
def find_col(df: DataFrame, *cands: str) -> str | None:
    low = {c.lower(): c for c in df.columns}
    for x in cands:
        if x.lower() in low:
            return low[x.lower()]
    return None


def barplot(pairs, title, xlabel, ylabel="count", rot=0, figsize=(10, 4)):
    plt.figure(figsize=figsize)
    plt.bar([str(p[0]) for p in pairs], [p[1] for p in pairs])
    plt.title(title); plt.xlabel(xlabel); plt.ylabel(ylabel)
    plt.xticks(rotation=rot, ha="right" if rot else "center")
    plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Distinct station-id sets per table (collected -- small) + measurement row counts
station_set = {}
table_rows = {}
for name, t in {**MEASUREMENT_TABLES, **META}.items():
    df = spark.table(t)
    sid = find_col(df, "STATIONS_ID", "Stations_id", "stations_id")
    station_set[name] = {str(x[0]) for x in df.select(F.col(sid).cast("string")).distinct().collect()}
    print(f"{name:<24} distinct stations = {len(station_set[name])}: {sorted(station_set[name])}")
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
    print(f"{meta_name}: measurement-stations-not-in-metadata={orphans}  metadata-stations-unused={unused}")

# COMMAND ----------

# DBTITLE 1,City <-> station consistency (one union + distinct, collected)
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

# DBTITLE 1,Join cardinality -- measurement station -> metadata (rows per station)
meta_card = {}
for meta_name, meta_t in META.items():
    md = spark.table(meta_t)
    s = find_col(md, "STATIONS_ID", "Stations_id", "stations_id")
    stats = md.groupBy(s).count().agg(
        F.min("count").alias("min"), F.max("count").alias("max"), F.avg("count").alias("avg"),
        F.sum((F.col("count") > 1).cast("long")).alias("stations_with_fanout"),
    ).first().asDict()
    meta_card[meta_name] = stats
    kind = "1:1" if stats["max"] == 1 else "1:N (fan-out on station_id alone)"
    print(f"measurement -> {meta_name}: {kind}  {stats}")
pu = spark.table(META["parameter_unit"])
pcode = find_col(pu, "Parameter", "parameter", "Kennung", "Parameter_ohne_Einheit")
if pcode:
    print("parameter_unit codes:", sorted(str(x[0]) for x in pu.select(pcode).distinct().collect()))
for m, t in MEASUREMENT_TABLES.items():
    print(m, "value columns ->", spark.table(t).columns)

# COMMAND ----------

# DBTITLE 1,Cross-measurement (station, MESS_DATUM) presence matrix -- one tagged union
u = None
for m, t in MEASUREMENT_TABLES.items():
    df = spark.table(t)
    s, d = find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")
    part = df.select(F.col(s).cast("string").alias("station"), F.col(d).cast("string").alias("ts"),
                     F.lit(m).alias("src"))
    u = part if u is None else u.union(part)
p = u.groupBy("station", "ts").agg(*[F.max((F.col("src") == m).cast("int")).alias(m) for m in MEASUREMENTS])

# COMMAND ----------

# DBTITLE 1,Overlap / cardinality stats -- one agg over the presence matrix
pairs = [(a, b) for i, a in enumerate(MEASUREMENTS) for b in MEASUREMENTS[i + 1:]]
S = p.agg(
    F.count(F.lit(1)).alias("union_keys"),
    *[F.sum(m).alias("present__" + m) for m in MEASUREMENTS],
    *[F.sum(F.col(a) * F.col(b)).alias(f"pair__{a}__{b}") for a, b in pairs],
).first().asDict()
union_keys = S["union_keys"]
present = {m: S["present__" + m] for m in MEASUREMENTS}
pair_overlap = []
for a, b in pairs:
    shared = S[f"pair__{a}__{b}"]
    pair_overlap.append((a, b, shared, present[a] - shared, present[b] - shared))
    print(f"{a:<16} x {b:<16}  shared={shared:>12}  only_{a}={present[a] - shared:>12}  only_{b}={present[b] - shared:>12}")
key_unique = {m: table_rows[m] == present[m] for m in MEASUREMENTS}
for m in MEASUREMENTS:
    print(f"{m:<16} rows={table_rows[m]:>12}  distinct (station, ts)={present[m]:>12}  key_unique={key_unique[m]}")

# COMMAND ----------

# DBTITLE 1,Verdict -- can the 7 measurements be combined downstream?
max_pair_only = max((max(o[3], o[4]) for o in pair_overlap), default=0)
schemas = {m: tuple(sorted(spark.table(MEASUREMENT_TABLES[m]).columns)) for m in MEASUREMENTS}
schema_disjoint = len(set(schemas.values())) == len(schemas)
print(f"(station, MESS_DATUM) unique in every measurement : {all(key_unique.values())}")
print(f"largest non-shared timestamp count in any pair    : {max_pair_only}")
print(f"every measurement has a distinct value-column set  : {schema_disjoint}")
print("=> combine as a wide table only where timestamps overlap; otherwise keep "
      "one model per measurement and align at Gold, not Silver.")

# COMMAND ----------

# DBTITLE 1,Figures
labels = [f"{a[:4]}x{b[:4]}" for a, b, *_ in pair_overlap]
shared_v = [o[2] for o in pair_overlap]
nonshared_v = [o[3] + o[4] for o in pair_overlap]
xx = np.arange(len(labels))
plt.figure(figsize=(13, 4))
plt.bar(xx, shared_v, label="shared (station, ts)")
plt.bar(xx, nonshared_v, bottom=shared_v, label="only one side")
plt.xticks(xx, labels, rotation=90); plt.legend()
plt.title("DWD -- cross-measurement timestamp overlap per pair"); plt.ylabel("(station, ts) keys")
plt.tight_layout(); plt.show()

barplot([(n, len(station_set[n])) for n in {**MEASUREMENT_TABLES, **META}],
        "DWD -- distinct stations per Bronze table", "table", "stations", rot=40)
barplot(list(measure_missing.items()),
        "DWD -- stations absent from each measurement (vs union of all 7)", "measurement", "missing", rot=30)
metas = list(ref_integrity)
x = np.arange(len(metas))
plt.figure(figsize=(10, 4))
plt.bar(x - 0.2, [ref_integrity[k][0] for k in metas], width=0.4, label="orphan measurement stations")
plt.bar(x + 0.2, [ref_integrity[k][1] for k in metas], width=0.4, label="unused metadata stations")
plt.xticks(x, metas, rotation=20, ha="right"); plt.legend()
plt.title("DWD -- referential integrity: measurements <-> metadata"); plt.ylabel("stations")
plt.tight_layout(); plt.show()
barplot(sorted(city_counts.items()), "DWD -- distinct stations per city", "city", "stations")

# COMMAND ----------

# DBTITLE 1,Findings
print("distinct stations per table :", {n: len(station_set[n]) for n in {**MEASUREMENT_TABLES, **META}})
print("stations missing per measurement (vs union):", measure_missing)
print("referential integrity (orphans, unused)   :", ref_integrity)
print("stations mapped to >1 city                 :", multi_city)
print("measurement->metadata cardinality          :", {k: v["max"] for k, v in meta_card.items()})
print("(station, MESS_DATUM) unique in every measurement:", all(key_unique.values()))
print("largest non-shared timestamp count in any pair  :", max_pair_only)
print("all 7 measurements have disjoint value-column sets:", schema_disjoint)
