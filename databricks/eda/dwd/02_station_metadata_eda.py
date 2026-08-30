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
import contextlib
import os as _os
import re as _re
from functools import reduce

import matplotlib.pyplot as plt
from pyspark.sql import functions as F

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


def measurement_value_cols(cols):
    return [
        c
        for c in cols
        if c.upper() not in META_NON_VALUE and not c.upper().startswith("QN")
    ]


# COMMAND ----------

# DBTITLE 1,Helpers


def find_key(cols, *cands):
    low = {c.lower(): c for c in cols}
    for x in cands:
        if x.lower() in low:
            return low[x.lower()]
    return None


def find_key_like(cols, *substrings):
    for c in cols:
        cl = c.lower()
        if any(s in cl for s in substrings):
            return c
    return None


def to_float(x):
    # DWD metadata stores coordinates as strings and sometimes with a German
    # decimal comma; a bare float() silently dropped every lat/lon value.
    if x is None:
        return None
    try:
        return float(str(x).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None


def barplot(pairs, title, xlabel, ylabel="rows", rot=0, figsize=(10, 4), filename=None):
    plt.figure(figsize=figsize)
    plt.bar([str(p[0]) for p in pairs], [p[1] for p in pairs])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=rot, ha="right" if rot else "center")
    plt.tight_layout()
    if filename:
        plt.savefig(fig_path(filename), dpi=110, bbox_inches="tight")
    plt.show()


# COMMAND ----------

# DBTITLE 1,Profiling-export helper (writes src/schemas/profiling/<source>.md)


def _repo_root():
    p = _os.path.abspath(_os.getcwd())
    for _ in range(12):
        if _os.path.isdir(_os.path.join(p, "src", "schemas")) and _os.path.isdir(
            _os.path.join(p, "databricks", "eda")
        ):
            return p
        if _os.path.dirname(p) == p:
            break
        p = _os.path.dirname(p)
    with contextlib.suppress(Exception):
        wp = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        i = wp.rfind("/databricks/eda/")
        if i > 0:
            for cand in (wp[:i], "/Workspace" + wp[:i]):
                if _os.path.isdir(_os.path.join(cand, "src", "schemas")):
                    return cand
    raise RuntimeError(
        "repo root not found -- run from <repo>/databricks/eda/<source>/"
    )


def _profiling_dir():
    d = _os.path.join(_repo_root(), "src", "schemas", "profiling")
    _os.makedirs(_os.path.join(d, "figures"), exist_ok=True)
    return d


def fig_path(name):
    return _os.path.join(_profiling_dir(), "figures", name)


def fmt_pairs(pairs, n=25):
    # Render (label, value) pairs as markdown list lines, capped at n with a
    # "... (N more)" tail so the profiling .md never carries a 1000-row dump.
    items = list(pairs)
    out = [f"- {lbl}: {val}" for lbl, val in items[:n]]
    if len(items) > n:
        out.append(f"- ... ({len(items) - n} more)")
    return "\n".join(out)


def _facet_grid(items, suptitle, filename, ncols=3, panel=(4.6, 3.2)):
    items = [(str(k), draw) for k, draw in items if draw is not None]
    if not items:
        print(f"  _facet_grid: no data -> {filename}")
        return False
    ncols = min(ncols, len(items))
    nrows = -(-len(items) // ncols)
    fig, axes = plt.subplots(
        nrows, ncols, figsize=(panel[0] * ncols, panel[1] * nrows), squeeze=False
    )
    flat = list(axes.flatten())
    for ax, (title, draw) in zip(flat, items):
        draw(ax)
        ax.set_title(title, fontsize=9)
        ax.tick_params(labelsize=7)
    for ax in flat[len(items) :]:
        ax.set_visible(False)
    fig.suptitle(suptitle)
    fig.tight_layout()
    fig.savefig(fig_path(filename), dpi=110, bbox_inches="tight")
    plt.show()
    plt.close(fig)
    return True


def facet_bars(groups, suptitle, filename, rot=45, ncols=3, logy=False):
    def _mk(pairs):
        if not pairs:
            return None

        def draw(ax):
            ax.bar([str(p[0]) for p in pairs], [p[1] for p in pairs])
            if logy:
                ax.set_yscale("log")
            ax.tick_params(axis="x", labelrotation=rot)

        return draw

    src = groups.items() if hasattr(groups, "items") else groups
    return _facet_grid([(k, _mk(list(v))) for k, v in src], suptitle, filename, ncols)


def facet_hists(groups, suptitle, filename, bins=40, ncols=3, logy=True):
    def _mk(vals):
        if vals is None or not len(vals):
            return None

        def draw(ax):
            ax.hist(list(vals), bins=bins, log=logy)

        return draw

    src = groups.items() if hasattr(groups, "items") else groups
    return _facet_grid([(k, _mk(v)) for k, v in src], suptitle, filename, ncols)


def write_profiling(source, notebook_key, section_title, blocks, figures=None):
    d = _profiling_dir()
    md = _os.path.join(d, source + ".md")
    lines = [f"<!-- BEGIN {source}:{notebook_key} -->", f"## {section_title}", ""]
    for heading, body in blocks:
        if body is None or str(body).strip() == "":
            continue
        lines += [f"### {heading}", "", str(body).rstrip(), ""]
    for cap, name in figures or []:
        if not _os.path.exists(_os.path.join(d, "figures", name)):
            print(f"  profiling export: skipping absent figure {name}")
            continue
        lines += [f"### Figure -- {cap}", "", f"![{cap}](figures/{name})", ""]
    lines.append(f"<!-- END {source}:{notebook_key} -->")
    block = "\n".join(lines)
    existing = ""
    if _os.path.exists(md):
        with open(md, encoding="utf-8") as fh:
            existing = fh.read()
    pat = _re.compile(
        r"<!-- BEGIN "
        + _re.escape(source)
        + r":([\w.\-]+) -->.*?<!-- END "
        + _re.escape(source)
        + r":\1 -->",
        _re.DOTALL,
    )
    kept = {mm.group(1): mm.group(0) for mm in pat.finditer(existing)}
    kept[notebook_key] = block
    intro = f"_Auto-generated by the Phase 3 EDA notebooks (`databricks/eda/{source}/`). One `## ` section per notebook; re-running a notebook replaces its own section, other sections are preserved._"
    header = f"# {source.upper()} EDA PROFILE\n\n{intro}\n\n"
    body = "\n\n".join(kept[k] for k in sorted(kept))
    out = header + body + "\n"
    tmp = md + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(out)
    _os.replace(tmp, md)
    print(f"profiling export -> {md}  ('{notebook_key}', {len(kept)} section(s))")


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
lat = find_key(
    geo["cols"], "Geogr_Breite", "geo_latitude_deg", "Geographische_Breite"
) or find_key_like(geo["cols"], "breit", "latitud")
lon = find_key(
    geo["cols"], "Geogr_Laenge", "geo_longitude_deg", "Geographische_Laenge"
) or find_key_like(geo["cols"], "laeng", "longitud")
elev = find_key(
    geo["cols"], "Stationshoehe", "station_elevation_m", "Stationshoehe_m"
) or find_key_like(geo["cols"], "hoehe", "elevation", "height")
print(f"geography columns -> {geo['cols']}")
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
    observed[m] = measurement_value_cols(spark.table(t).columns)
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
    plt.savefig(fig_path("dwd_station_geography.png"), dpi=110, bbox_inches="tight")
    plt.show()

# COMMAND ----------

# DBTITLE 1,Figure -- metadata overview (one faceted figure)
facet_bars(
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
)

# COMMAND ----------

# DBTITLE 1,Figure -- per-table rows per station + validity-period composition (faceted)
facet_bars(
    dict(station_counts),
    "DWD metadata -- rows per station, by table",
    "dwd_metadata_rows_per_station.png",
    rot=45,
    ncols=2,
)
facet_bars(
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

_entities = [
    f"Distinct stations across the 7 measurement tables: {len(measure_stations)} -> {sorted(measure_stations)}."
]
_entities.append("Metadata rows per station id:")
for name, pairs in station_counts.items():
    _entities.append(f"- {name}: {dict(pairs)}")

_relo = [
    "station_geography carries multiple location rows per station where coordinates/elevation changed over time:"
]
for sid, g in geo_moves.items():
    _relo.append(
        f"- station {sid}: {g['location_rows']} location row(s), "
        f"lat span={g['lat_span']}, lon span={g['lon_span']}, elevation span={g['elev_span_m']} m"
    )
_relo.append("")
_relo.append("station_name_history name/operator changes:")
for sid, n in name_changes.items():
    _relo.append(
        f"- station {sid}: {n['history_rows']} history row(s), {len(n['names'])} distinct name(s)"
    )

_recon = [
    f"parameter_unit declared codes: {sorted(declared)}",
    f"value-column codes present in measurements but NOT in parameter_unit: {sorted(observed_flat - declared)}",
    f"parameter_unit codes never used as a measurement value column: {sorted(declared - observed_flat)}",
    "value columns per measurement: " + str(observed),
]

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
if _relocations:
    _silver.append(
        "- station_geography has >1 location row for some stations (relocations) -> station location is time-varying; join measurement rows on the von/bis window, not on station_id alone."
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

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Data Quality", "\n".join(_dq)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Coverage", "\n".join(_gaps)),
        ("Domain Findings", "\n".join(_relo) + "\n\n" + "\n".join(_recon)),
        (
            "EDA Findings",
            "\n".join(
                [
                    f"- relocations (station -> location rows): {_relocations}",
                    f"- name changes (station -> distinct names): {_renames}",
                    f"- validity periods (open-ended, inverted, total): {period_stats}",
                    f"- metadata coverage gaps vs measurements: {meta_gaps}",
                ]
            ),
        ),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=[
        (
            "DWD station_geography -- station locations & relocations",
            "dwd_station_geography.png",
        ),
        ("DWD metadata -- overview", "dwd_metadata_overview.png"),
        (
            "DWD metadata -- rows per station, by table",
            "dwd_metadata_rows_per_station.png",
        ),
        (
            "DWD metadata -- validity-period row composition, by table",
            "dwd_metadata_validity_periods.png",
        ),
    ],
)
