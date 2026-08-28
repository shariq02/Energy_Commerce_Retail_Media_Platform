# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- DWD STATION METADATA
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Profile the four DWD metadata Bronze tables
# MAGIC (station_geography, station_name_history, device_instrument,
# MAGIC parameter_unit) -- schema, missingness, constant columns, duplicates,
# MAGIC validity periods, station relocation / name-history analysis, a
# MAGIC geographic station plot, parameter -> measurement -> unit
# MAGIC reconciliation, and metadata coverage gaps vs the measurement tables.
# MAGIC The metadata tables are small, so each is collected once and analysed
# MAGIC in Python; only the measurement station set is scanned in Spark.

# COMMAND ----------

# DBTITLE 1,Imports
from functools import reduce

import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
TABLES = {
    "station_geography": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_station_geography",
    "station_name_history": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_station_name_history",
    "device_instrument": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_device_instrument",
    "parameter_unit": f"{CATALOG}.{BRONZE_SCHEMA}.dwd_parameter_unit",
}
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
META_NON_VALUE = {
    "STATIONS_ID",
    "CITY",
    "MESS_DATUM",
    "QN_9",
    "QN_3",
    "QN_4",
    "QN_8",
    "EOR",
}

# COMMAND ----------


# DBTITLE 1,Helpers
def find_key(cols, *cands):
    low = {c.lower(): c for c in cols}
    for x in cands:
        if x.lower() in low:
            return low[x.lower()]
    return None


def barplot(pairs, title, xlabel, ylabel="rows", rot=0, figsize=(10, 4)):
    plt.figure(figsize=figsize)
    plt.bar([str(p[0]) for p in pairs], [p[1] for p in pairs])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=rot, ha="right" if rot else "center")
    plt.tight_layout()
    plt.show()


# COMMAND ----------

# DBTITLE 1,Collect the 4 metadata tables (small) and profile in Python
meta = {}
for name, t in TABLES.items():
    df = spark.table(t)
    recs = [x.asDict() for x in df.collect()]
    cols = df.columns
    total = len(recs)
    dups = total - len({tuple(sorted(d.items())) for d in recs})
    consts = [c for c in cols if len({d[c] for d in recs}) <= 1]
    meta[name] = {"cols": cols, "recs": recs, "total": total}
    print(
        "=" * 90,
        f"\n{name}  rows={total}  cols={cols}  full_row_duplicates={dups}  constant_columns={consts}",
    )
    for c in cols:
        missing = sum(1 for d in recs if d[c] is None or str(d[c]).strip() == "")
        print(f"  {c:<30} missing={missing:>6}  distinct={len({d[c] for d in recs})}")
    for d in recs[:20]:
        print("  ", d)

# COMMAND ----------

# DBTITLE 1,Per-station row counts + validity-period checks (Python)
station_counts = {}
period_stats = {}
for name, x in meta.items():
    sid = find_key(x["cols"], "Stations_id", "STATIONS_ID", "stations_id")
    if sid:
        acc = {}
        for d in x["recs"]:
            acc[d[sid]] = acc.get(d[sid], 0) + 1
        station_counts[name] = sorted(acc.items())
        print(f"{name} rows per station:", station_counts[name])
    von = find_key(x["cols"], "von_datum", "Von_Datum", "von")
    bis = find_key(x["cols"], "bis_datum", "Bis_Datum", "bis")
    if von and bis:
        open_ended = sum(1 for d in x["recs"] if not str(d[bis] or "").strip())
        inverted = sum(
            1
            for d in x["recs"]
            if str(d[bis] or "").strip()
            and str(d[bis]).strip().isdigit()
            and str(d[von] or "").strip().isdigit()
            and int(d[bis]) < int(d[von])
        )
        period_stats[name] = (open_ended, inverted, x["total"])
        print(
            f"{name}: open_ended={open_ended}  inverted_ranges={inverted}  of {x['total']}"
        )

# COMMAND ----------

# DBTITLE 1,station_geography relocation + station_name_history changes (Python)
geo = meta["station_geography"]
gsid = find_key(geo["cols"], "Stations_id", "STATIONS_ID", "stations_id")
lat = find_key(geo["cols"], "Geogr_Breite", "geo_latitude_deg", "Geographische_Breite")
lon = find_key(geo["cols"], "Geogr_Laenge", "geo_longitude_deg", "Geographische_Laenge")
elev = find_key(geo["cols"], "Stationshoehe", "station_elevation_m", "Stationshoehe_m")
print(f"geography columns -> id={gsid} lat={lat} lon={lon} elev={elev}")
geo_moves = {}
for d in geo["recs"]:
    g = geo_moves.setdefault(
        d[gsid], {"location_rows": 0, "lat": [], "lon": [], "elev": []}
    )
    g["location_rows"] += 1
    for k, col in (("lat", lat), ("lon", lon), ("elev", elev)):
        if col:
            try:
                g[k].append(float(d[col]))
            except (TypeError, ValueError):
                pass
for sid, g in geo_moves.items():
    g["lat_span"] = round(max(g["lat"]) - min(g["lat"]), 5) if g["lat"] else None
    g["lon_span"] = round(max(g["lon"]) - min(g["lon"]), 5) if g["lon"] else None
    g["elev_span_m"] = round(max(g["elev"]) - min(g["elev"]), 2) if g["elev"] else None
    print(
        f"station {sid}: { ({k: v for k, v in g.items() if k not in ('lat', 'lon', 'elev')}) }"
    )

nh = meta["station_name_history"]
nsid = find_key(nh["cols"], "Stations_id", "STATIONS_ID", "stations_id")
nname = find_key(nh["cols"], "Stationsname", "Betreibername", "Name")
name_changes = {}
for d in nh["recs"]:
    n = name_changes.setdefault(d[nsid], {"history_rows": 0, "names": set()})
    n["history_rows"] += 1
    if nname:
        n["names"].add(d[nname])
for sid, n in name_changes.items():
    print(
        f"station {sid}: history_rows={n['history_rows']}  distinct_names={len(n['names'])}"
    )

# COMMAND ----------

# DBTITLE 1,parameter_unit -> measurement -> unit reconciliation
pu = meta["parameter_unit"]
pcode = find_key(
    pu["cols"], "Parameter", "parameter", "Kennung", "Parameter_ohne_Einheit"
)
punit = find_key(pu["cols"], "Einheit", "einheit", "unit")
declared = {str(d[pcode]) for d in pu["recs"]} if pcode else set()
print("parameter_unit declared codes:", sorted(declared))
if punit:
    pairs = {}
    for d in pu["recs"]:
        pairs[(d[pcode], d[punit])] = pairs.get((d[pcode], d[punit]), 0) + 1
    print("code -> unit:", pairs)
observed = {}
for m, t in MEASUREMENT_TABLES.items():
    observed[m] = [c for c in spark.table(t).columns if c.upper() not in META_NON_VALUE]
observed_flat = {c for cs in observed.values() for c in cs}
print("value columns per measurement:", observed)
print(
    "value codes in measurements NOT in parameter_unit:",
    sorted(observed_flat - declared),
)
print(
    "parameter_unit codes never a measurement value column:",
    sorted(declared - observed_flat),
)

# COMMAND ----------

# DBTITLE 1,Metadata coverage gaps vs the measurement station set (one Spark scan)
mstations = reduce(
    lambda a, b: a.union(b),
    (
        spark.table(t).select(
            F.col(find_key(spark.table(t).columns, "STATIONS_ID"))
            .cast("string")
            .alias("s")
        )
        for t in MEASUREMENT_TABLES.values()
    ),
).distinct()
measure_stations = {x["s"] for x in mstations.collect()}
print(
    f"distinct stations across measurements = {len(measure_stations)}: {sorted(measure_stations)}"
)
meta_gaps = {}
for name, x in meta.items():
    sid = find_key(x["cols"], "Stations_id", "STATIONS_ID", "stations_id")
    if sid is None:
        continue
    have = {str(d[sid]) for d in x["recs"]}
    missing = measure_stations - have
    meta_gaps[name] = len(missing)
    print(
        f"{name}: measurement stations with no row = {len(missing)}  {sorted(missing)}"
    )

# COMMAND ----------

# DBTITLE 1,Figure -- geographic station plot (lon x lat)
if gsid and lat and lon:
    plt.figure(figsize=(7, 8))
    for sid, g in geo_moves.items():
        if g["lon"] and g["lat"]:
            plt.scatter(g["lon"], g["lat"], label=str(sid), s=60)
            plt.plot(g["lon"], g["lat"], linewidth=0.6)
    plt.legend(title="station id", fontsize=8)
    plt.title("DWD station_geography -- station locations (lines = relocations)")
    plt.xlabel("longitude")
    plt.ylabel("latitude")
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# DBTITLE 1,Figure -- relocations, name history, coverage gaps, table sizes, validity periods
barplot(
    [(sid, g["location_rows"]) for sid, g in geo_moves.items()],
    "DWD station_geography -- location rows per station",
    "station id",
    "rows",
    rot=45,
)
if elev:
    barplot(
        [
            (sid, g["elev_span_m"])
            for sid, g in geo_moves.items()
            if g["elev_span_m"] is not None
        ],
        "DWD station_geography -- elevation span per station (m)",
        "station id",
        "metres",
        rot=45,
    )
barplot(
    [(sid, n["history_rows"]) for sid, n in name_changes.items()],
    "DWD station_name_history -- history rows per station",
    "station id",
    "rows",
    rot=45,
)
if meta_gaps:
    barplot(
        list(meta_gaps.items()),
        "DWD -- measurement stations missing a metadata row",
        "metadata table",
        "stations",
        rot=20,
    )
barplot(
    [(n, meta[n]["total"]) for n in TABLES],
    "DWD metadata -- rows per table",
    "table",
    rot=20,
)
for name, pairs in station_counts.items():
    barplot(pairs, f"DWD {name} -- rows per station", "station id", "rows", rot=45)
for name, (open_ended, inverted, total) in period_stats.items():
    barplot(
        [
            ("open-ended", open_ended),
            ("closed", total - open_ended),
            ("inverted", inverted),
        ],
        f"DWD {name} -- validity-period rows",
        "period type",
        "rows",
    )

# COMMAND ----------

# DBTITLE 1,Findings
print(
    "relocations (station -> location rows):",
    {sid: g["location_rows"] for sid, g in geo_moves.items() if g["location_rows"] > 1},
)
print(
    "name changes (station -> distinct names):",
    {sid: len(n["names"]) for sid, n in name_changes.items() if len(n["names"]) > 1},
)
print("validity periods:", period_stats)
print("metadata coverage gaps vs measurements:", meta_gaps)
