# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- DWD STATION METADATA
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the four DWD metadata Bronze tables
# MAGIC (station_geography, station_name_history, device_instrument,
# MAGIC parameter_unit) -- schema, missingness, constant columns, duplicates, a
# MAGIC structural check for parser trailer rows, validity periods and their
# MAGIC ordering, station relocation / name-history analysis, spatial validity
# MAGIC of the coordinates, parameter -> measurement -> unit reconciliation,
# MAGIC metadata coverage gaps vs the measurement tables, and the layered
# MAGIC modelling-risk checklist. The metadata tables are small, collected once
# MAGIC and analysed in Python; only the measurement station set is scanned in
# MAGIC Spark.

# COMMAND ----------

# DBTITLE 1,Imports
from functools import reduce

import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "dwd"
NB_KEY = "02_station_metadata"
SECTION_TITLE = "Station metadata (geography, name history, device, parameter_unit)"
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
META_NON_VALUE = {"STATIONS_ID", "CITY", "MESS_DATUM", "EOR", "V_N_I"}
# A generated DWD metadata export ends with a free-text trailer
# ("generiert: ... Deutscher Wetterdienst") that a naive CSV read turns into a
# data row -- a non-numeric STATIONS_ID or a cell carrying these tokens.
TRAILER_TOKENS = ("wetterdienst", "generiert", "erzeugt", "stand:")


def measurement_value_cols(cols):
    return [
        c
        for c in cols
        if c.upper() not in META_NON_VALUE and not c.upper().startswith("QN")
    ]


def find_key(cols, *cands):
    low = {c.lower(): c for c in cols}
    for x in cands:
        if x.lower() in low:
            return low[x.lower()]
    return None


def find_key_like(cols, *substrings):
    for c in cols:
        if any(s in c.lower() for s in substrings):
            return c
    return None


def to_float(x):
    # DWD metadata stores coordinates as strings, sometimes with a German
    # decimal comma; a bare float() silently dropped every lat/lon value.
    if x is None:
        return None
    try:
        return float(str(x).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None


# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

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

# DBTITLE 1,Structural check -- CSV parser trailer rows
trailer = {}
for name, x in meta.items():
    sid = find_key(x["cols"], "Stations_id", "STATIONS_ID", "stations_id")
    bad_id = []
    token_rows = []
    for d in x["recs"]:
        if (
            sid
            and d[sid] is not None
            and not str(d[sid]).strip().removesuffix(".0").isdigit()
        ):
            bad_id.append(d[sid])
        joined = " ".join(str(v) for v in d.values() if v is not None).lower()
        if any(tok in joined for tok in TRAILER_TOKENS):
            token_rows.append({k: d[k] for k in list(d)[:3]})
    trailer[name] = {
        "non_numeric_station_id": bad_id[:10],
        "non_numeric_station_id_count": len(bad_id),
        "trailer_token_rows": token_rows[:5],
        "trailer_token_row_count": len(token_rows),
        "suspect": bool(bad_id or token_rows),
    }
    print(f"{name}: {trailer[name]}")
_trailer_hit = [n for n, v in trailer.items() if v["suspect"]]

# COMMAND ----------

# DBTITLE 1,Per-station row counts + validity-period ordering (Python)
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
            and str(d[bis]).strip().removesuffix(".0").isdigit()
            and str(d[von] or "").strip().removesuffix(".0").isdigit()
            and int(float(d[bis])) < int(float(d[von]))
        )
        period_stats[name] = (open_ended, inverted, x["total"])
        print(
            f"{name}: open_ended={open_ended}  inverted_ranges={inverted}  of {x['total']}"
        )

# COMMAND ----------

# DBTITLE 1,station_geography relocation + station_name_history changes (Python)
geo = meta["station_geography"]
gsid = find_key(geo["cols"], "Stations_id", "STATIONS_ID", "stations_id")
lat = find_key(
    geo["cols"], "Geogr_Breite", "geo_latitude_deg", "Geographische_Breite"
) or find_key_like(geo["cols"], "breit", "latitud")
lon = find_key(
    geo["cols"], "Geogr_Laenge", "geo_longitude_deg", "Geographische_Laenge"
) or find_key_like(geo["cols"], "laeng", "longitud")
elev = find_key(
    geo["cols"], "Stationshoehe", "station_elevation_m", "Stationshoehe_m"
) or find_key_like(geo["cols"], "hoehe", "elevation", "height")
print(f"geography columns -> id={gsid} lat={lat} lon={lon} elev={elev}")
geo_moves = {}
for d in geo["recs"]:
    g = geo_moves.setdefault(
        d[gsid], {"location_rows": 0, "lat": [], "lon": [], "elev": []}
    )
    g["location_rows"] += 1
    for k, col in (("lat", lat), ("lon", lon), ("elev", elev)):
        if col:
            fv = to_float(d[col])
            if fv is not None:
                g[k].append(fv)
for sid, g in geo_moves.items():
    g["lat_span"] = round(max(g["lat"]) - min(g["lat"]), 5) if g["lat"] else None
    g["lon_span"] = round(max(g["lon"]) - min(g["lon"]), 5) if g["lon"] else None
    g["elev_span_m"] = round(max(g["elev"]) - min(g["elev"]), 2) if g["elev"] else None
    # crude relocation distance: 1 deg lat ~ 111 km, 1 deg lon ~ 71 km at 52N
    g["reloc_km"] = (
        round(
            (((g["lat_span"] or 0) * 111) ** 2 + ((g["lon_span"] or 0) * 71) ** 2)
            ** 0.5,
            2,
        )
        if g["location_rows"] > 1
        else 0.0
    )
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

# DBTITLE 1,Spatial validity of the station coordinates (Germany bounding box)
geo_df = spark.table(TABLES["station_geography"])
spatial = None
if lat and lon:
    spatial = spatial_validity(geo_df, lat, lon, name="station_geography")
    print("spatial validity:", spatial)
big_moves = {sid: g["reloc_km"] for sid, g in geo_moves.items() if g["reloc_km"] > 5}
print("relocations > 5 km (station -> km):", big_moves)

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
    upairs = {}
    for d in pu["recs"]:
        upairs[(d[pcode], d[punit])] = upairs.get((d[pcode], d[punit]), 0) + 1
    print("code -> unit:", upairs)
observed = {}
for m, t in MEASUREMENT_TABLES.items():
    observed[m] = measurement_value_cols(spark.table(t).columns)
observed_flat = {c for cs in observed.values() for c in cs}
codes_unknown = sorted(observed_flat - declared)
codes_unused = sorted(declared - observed_flat)
print("value codes in measurements NOT in parameter_unit:", codes_unknown)
print("parameter_unit codes never a measurement value column:", codes_unused)

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
figs = []
if gsid and lat and lon and any(g["lon"] and g["lat"] for g in geo_moves.values()):
    fig, ax = plt.subplots(figsize=(7, 8))
    for sid, g in geo_moves.items():
        if g["lon"] and g["lat"]:
            ax.scatter(g["lon"], g["lat"], label=str(sid), s=60)
            ax.plot(g["lon"], g["lat"], linewidth=0.6)
    ax.legend(title="station id", fontsize=8)
    ax.set_title("DWD station_geography -- station locations (lines = relocations)")
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")
    fig.tight_layout()
    _save_and_show(fig, "dwd_station_geography.png")
    figs.append(
        (
            "DWD station_geography -- station locations & relocations",
            "dwd_station_geography.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Figure -- metadata overview (one faceted figure)
if facet_bars(
    {
        "geography: location rows per station": [
            (sid, g["location_rows"]) for sid, g in geo_moves.items()
        ],
        "geography: elevation span (m) per station": [
            (sid, g["elev_span_m"])
            for sid, g in geo_moves.items()
            if g["elev_span_m"] is not None
        ],
        "name_history: rows per station": [
            (sid, n["history_rows"]) for sid, n in name_changes.items()
        ],
        "measurement stations missing a metadata row": list(meta_gaps.items()),
        "metadata rows per table": [(n, meta[n]["total"]) for n in TABLES],
    },
    "DWD metadata -- overview",
    "dwd_metadata_overview.png",
    rot=45,
    ncols=2,
):
    figs.append(("DWD metadata -- overview", "dwd_metadata_overview.png"))

if facet_bars(
    dict(station_counts),
    "DWD metadata -- rows per station, by table",
    "dwd_metadata_rows_per_station.png",
    rot=45,
    ncols=2,
):
    figs.append(
        (
            "DWD metadata -- rows per station, by table",
            "dwd_metadata_rows_per_station.png",
        )
    )

if facet_bars(
    {
        name: [
            ("open-ended", open_ended),
            ("closed", total - open_ended),
            ("inverted", inverted),
        ]
        for name, (open_ended, inverted, total) in period_stats.items()
    },
    "DWD metadata -- validity-period row composition, by table",
    "dwd_metadata_validity_periods.png",
    rot=20,
    ncols=2,
):
    figs.append(
        (
            "DWD metadata -- validity-period row composition, by table",
            "dwd_metadata_validity_periods.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Findings
_relocations = {
    sid: g["location_rows"] for sid, g in geo_moves.items() if g["location_rows"] > 1
}
_renames = {
    sid: len(n["names"]) for sid, n in name_changes.items() if len(n["names"]) > 1
}
print("relocations (station -> location rows):", _relocations)
print("name changes (station -> distinct names):", _renames)
print("validity periods:", period_stats)
print("metadata coverage gaps vs measurements:", meta_gaps)
print("parser trailer suspected in:", _trailer_hit)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/dwd.md
_profile = [
    "| table | rows | cols | full-row dups | constant columns |",
    "|---|---|---|---|---|",
]
for name, x in meta.items():
    dups = x["total"] - len({tuple(sorted(d.items())) for d in x["recs"]})
    consts = [c for c in x["cols"] if len({d[c] for d in x["recs"]}) <= 1]
    _profile.append(
        f"| {name} | {x['total']} | {len(x['cols'])} | {dups} | {', '.join(consts) or '-'} |"
    )

_struct = [
    para(
        "A generated DWD metadata export ends with a free-text trailer",
        "(`generiert: ... Deutscher Wetterdienst`); a naive CSV read turns it into",
        "a data row with a non-numeric STATIONS_ID.",
    ),
]
for name, v in trailer.items():
    _struct.append(
        f"- {name}: {v['non_numeric_station_id_count']} non-numeric station id(s) "
        f"{v['non_numeric_station_id'][:5]}, {v['trailer_token_row_count']} row(s) carrying a "
        f"trailer token -> {'TRAILER ROW PRESENT' if v['suspect'] else 'clean'}."
    )
if _trailer_hit:
    _struct.append(
        para(
            f"-> INGESTION defect in {_trailer_hit}: fix `scripts/ingestion/stage_dwd.py`",
            "to strip the trailer, re-stage, re-upload to the Volume and re-run the DWD",
            "Bronze loader; until then, filter non-numeric STATIONS_ID before any join.",
        )
    )
else:
    _struct.append("-> No trailer rows in the current Bronze metadata tables.")

_dq = ["Validity-period columns (von_datum / bis_datum):"]
for name, (open_ended, inverted, total) in period_stats.items():
    _dq.append(
        f"- {name}: open-ended={open_ended}, inverted ranges={inverted}, of {total} rows"
    )
_dq.append("")
_dq.append(
    "Per-station row counts: "
    + "; ".join(f"{name} -> {dict(pairs)}" for name, pairs in station_counts.items())
)

_domain = [
    f"parameter_unit declared codes: {sorted(declared)}",
    f"value-column codes present in measurements but NOT in parameter_unit: {codes_unknown}",
    f"parameter_unit codes never used as a measurement value column: {codes_unused}",
    "value columns per measurement: " + str(observed),
    para(
        "An unknown code is a measured parameter with no declared unit -- attach it",
        "at Silver from the DWD parameter description, do not leave the unit null.",
    ),
]

_spatial = ["Spatial validity of station_geography coordinates:"]
if spatial:
    _spatial.append(
        f"- present={spatial['present']}, missing={spatial['missing']}, "
        f"outside the Germany bounding box={spatial['outside_bbox']}, (0,0)={spatial['null_island']}, "
        f"lat/lon possibly swapped={spatial['looks_swapped']}."
    )
else:
    _spatial.append("- no lat/lon column located by name.")
_spatial.append(f"- relocations > 5 km (station -> km): {big_moves or 'none'}.")
_spatial.append(
    para(
        "A coordinate outside Germany / at (0,0) is a quarantine class, not a",
        "silent NULL. A multi-km relocation means the station's location is",
        "time-varying -- join on the von/bis window, not station id alone. There is",
        "no second coordinate source to cross-check against (LIMITATION).",
    )
)

_entities = [
    f"Distinct stations across the 7 measurement tables: {len(measure_stations)} -> {sorted(measure_stations)}.",
    "Metadata rows per station id:",
]
for name, pairs in station_counts.items():
    _entities.append(f"- {name}: {dict(pairs)}")

_relo = [
    "station_geography carries multiple location rows per station where coordinates/elevation changed over time:"
]
for sid, g in geo_moves.items():
    _relo.append(
        f"- station {sid}: {g['location_rows']} location row(s), lat span={g['lat_span']}, "
        f"lon span={g['lon_span']}, elevation span={g['elev_span_m']} m, ~{g['reloc_km']} km moved"
    )
_relo.append("")
_relo.append("station_name_history name/operator changes:")
for sid, n in name_changes.items():
    _relo.append(
        f"- station {sid}: {n['history_rows']} history row(s), {len(n['names'])} distinct name(s)"
    )

_gaps = ["Measurement stations with NO row in each metadata table:"]
for name, x in meta.items():
    sid = find_key(x["cols"], "Stations_id", "STATIONS_ID", "stations_id")
    if sid is None:
        continue
    have = {str(d[sid]) for d in x["recs"]}
    _gaps.append(
        f"- {name}: {meta_gaps.get(name, 0)} missing -> {sorted(measure_stations - have)}"
    )

_silver = []
if _trailer_hit:
    _silver.append(
        f"- BLOCKED for {_trailer_hit}: strip the parser trailer in stage_dwd.py and re-run Bronze."
    )
if _relocations:
    _silver.append(
        "- station_geography has >1 location row for some stations (relocations) -> station location is time-varying; join measurement rows on the von/bis window, not station_id alone."
    )
if _renames:
    _silver.append(
        "- station_name_history has >1 name/operator per station over time -> a second time-varying attribute stream."
    )
if any(v > 0 for v in meta_gaps.values()):
    _silver.append(
        "- Some measurement stations have no metadata row -> a left join must not drop the fact row; flag the unmatched station."
    )
if any(inv > 0 for _, inv, _ in period_stats.values()):
    _silver.append(
        "- Inverted von>bis validity ranges exist -> a fix/exclusion rule is required (rule not yet established)."
    )
_silver.append(
    "- device_instrument / parameter_unit are small static lookups -> reference dimensions; reconcile parameter codes with the measurement value-column names (list above)."
)

_no_target = para(
    "Station relocation and name/operator changes are event indicators, not",
    "attributes of a static station dimension -- if used as a target, treat them",
    "as change events; the metadata tables carry no other labelled outcome.",
)
_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            para(
                "station_geography / station_name_history are one row per (station,",
                "validity period), NOT one row per station -- a split must group by station id.",
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            para(
                f"measurement -> metadata is 1:N for the {len(_relocations)} relocated and",
                f"{len(_renames)} renamed stations unless scoped to the von/bis window -- an",
                "un-windowed join cartesian-explodes those stations' fact rows.",
            ),
        ),
        ("Target contamination", _no_target),
        (
            "Temporal / post-event leakage",
            para(
                "Joining a measurement row to a station's metadata must use the validity",
                "window covering that row's MESS_DATUM -- a later relocation's coordinates",
                "in a historical feature is future information.",
            ),
        ),
        (
            "Proxy leakage",
            "Station id / name / exact coordinates identify one site -- a model given them memorises the station.",
        ),
        (
            "Split / entity leakage",
            "Split by station id, not by row -- a station's multiple validity-period rows must stay on one side.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            "Same as Temporal -- use the metadata value valid during the archive row's period, never the latest.",
        ),
        (
            "Survivorship / coverage bias",
            f"{sum(1 for v in meta_gaps.values() if v)} metadata table(s) miss some measurement stations -- a joined feature set silently drops those stations' rows.",
        ),
        (
            "Missingness leakage",
            "Whether a station has a geography / name-history row correlates with how long it has been in the network.",
        ),
        (
            "Duplicate-event leakage",
            "Full-row duplicates per table shown in Profile -- de-duplicate before treating a validity-period row as one event.",
        ),
        (
            "Target / feature temporal misalignment",
            "von_datum / bis_datum bound the period; align a joined metadata attribute to the fact row's hour, not the period edge.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            "parameter_unit reconciliation above -- attach units before combining measured parameters.",
        ),
        (
            "Data-generation-process leakage",
            "A parser trailer row (Structural Integrity) would inject a non-station into every station-keyed join -- filter it first.",
        ),
        (
            "Class / label instability",
            "device_instrument / parameter_unit codes are DWD enumerations that evolve between archive versions -- pin the version.",
        ),
        ("Label availability lag", "Not applicable -- static reference metadata."),
        (
            "Source / version / regime change",
            para(
                "Validity periods span decades; the network, instrumentation and the",
                "metadata schema itself changed over that span -- a metadata attribute is",
                "only comparable within one era.",
            ),
        ),
        (
            "Sample-vs-full divergence",
            "All four metadata tables are fully collected (small); the measurement-station union is a full unsampled Spark scan.",
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
        ("Categorical / Domain Validation", "\n".join(_domain)),
        ("Spatial Consistency", "\n".join(_spatial)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Coverage", "\n".join(_gaps)),
        ("Domain Findings", "\n".join(_relo)),
        ("ML-Readiness Evidence", _ml),
        (
            "EDA Findings",
            "\n".join(
                [
                    f"- relocations (station -> location rows): {_relocations}",
                    f"- name changes (station -> distinct names): {_renames}",
                    f"- validity periods (open-ended, inverted, total): {period_stats}",
                    f"- metadata coverage gaps vs measurements: {meta_gaps}",
                    f"- parser trailer suspected in: {_trailer_hit or 'none'}",
                ]
            ),
        ),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
