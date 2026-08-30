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
import contextlib
import os as _os
import re as _re

import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

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

# COMMAND ----------

# DBTITLE 1,Helpers

def find_col(df: DataFrame, *cands: str) -> str | None:
    low = {c.lower(): c for c in df.columns}
    for x in cands:
        if x.lower() in low:
            return low[x.lower()]
    return None


def barplot(
    pairs, title, xlabel, ylabel="count", rot=0, figsize=(10, 4), filename=None
):
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
    stats = (
        md.groupBy(s)
        .count()
        .agg(
            F.min("count").alias("min"),
            F.max("count").alias("max"),
            F.avg("count").alias("avg"),
            F.sum((F.col("count") > 1).cast("long")).alias("stations_with_fanout"),
        )
        .first()
        .asDict()
    )
    meta_card[meta_name] = stats
    kind = "1:1" if stats["max"] == 1 else "1:N (fan-out on station_id alone)"
    print(f"measurement -> {meta_name}: {kind}  {stats}")
pu = spark.table(META["parameter_unit"])
pcode = find_col(pu, "Parameter", "parameter", "Kennung", "Parameter_ohne_Einheit")
if pcode:
    print(
        "parameter_unit codes:",
        sorted(str(x[0]) for x in pu.select(pcode).distinct().collect()),
    )
for m, t in MEASUREMENT_TABLES.items():
    print(m, "value columns ->", spark.table(t).columns)

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

# DBTITLE 1,Verdict -- can the 7 measurements be combined downstream?
max_pair_only = max((max(o[3], o[4]) for o in pair_overlap), default=0)
schemas = {
    m: tuple(sorted(spark.table(MEASUREMENT_TABLES[m]).columns)) for m in MEASUREMENTS
}
schema_disjoint = len(set(schemas.values())) == len(schemas)
print(f"(station, MESS_DATUM) unique in every measurement : {all(key_unique.values())}")
print(f"largest non-shared timestamp count in any pair    : {max_pair_only}")
print(f"every measurement has a distinct value-column set  : {schema_disjoint}")
print(
    "=> combine as a wide table only where timestamps overlap; otherwise keep "
    "one model per measurement and align at Gold, not Silver."
)

# COMMAND ----------

# DBTITLE 1,Figures
labels = [f"{a[:4]}x{b[:4]}" for a, b, *_ in pair_overlap]
shared_v = [o[2] for o in pair_overlap]
nonshared_v = [o[3] + o[4] for o in pair_overlap]
xx = np.arange(len(labels))
plt.figure(figsize=(13, 4))
plt.bar(xx, shared_v, label="shared (station, ts)")
plt.bar(xx, nonshared_v, bottom=shared_v, label="only one side")
plt.xticks(xx, labels, rotation=90)
plt.legend()
plt.title("DWD -- cross-measurement timestamp overlap per pair")
plt.ylabel("(station, ts) keys")
plt.tight_layout()
plt.savefig(fig_path("dwd_cross_measurement_overlap.png"), dpi=110, bbox_inches="tight")
plt.show()

barplot(
    [(n, len(station_set[n])) for n in {**MEASUREMENT_TABLES, **META}],
    "DWD -- distinct stations per Bronze table",
    "table",
    "stations",
    rot=40,
    filename="dwd_stations_per_bronze_table.png",
)
barplot(
    list(measure_missing.items()),
    "DWD -- stations absent from each measurement (vs union of all 7)",
    "measurement",
    "missing",
    rot=30,
    filename="dwd_stations_absent_per_measurement.png",
)
metas = list(ref_integrity)
x = np.arange(len(metas))
plt.figure(figsize=(10, 4))
plt.bar(
    x - 0.2,
    [ref_integrity[k][0] for k in metas],
    width=0.4,
    label="orphan measurement stations",
)
plt.bar(
    x + 0.2,
    [ref_integrity[k][1] for k in metas],
    width=0.4,
    label="unused metadata stations",
)
plt.xticks(x, metas, rotation=20, ha="right")
plt.legend()
plt.title("DWD -- referential integrity: measurements <-> metadata")
plt.ylabel("stations")
plt.tight_layout()
plt.savefig(fig_path("dwd_referential_integrity.png"), dpi=110, bbox_inches="tight")
plt.show()
barplot(
    sorted(city_counts.items()),
    "DWD -- distinct stations per city",
    "city",
    "stations",
    filename="dwd_stations_per_city.png",
)

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
    "measurement->metadata cardinality          :",
    {k: v["max"] for k, v in meta_card.items()},
)
print("(station, MESS_DATUM) unique in every measurement:", all(key_unique.values()))
print("largest non-shared timestamp count in any pair  :", max_pair_only)
print("all 7 measurements have disjoint value-column sets:", schema_disjoint)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/dwd.md
_ent = [
    f"Distinct station ids per Bronze table: { {n: len(station_set[n]) for n in {**MEASUREMENT_TABLES, **META}} }",
    f"Union of stations across the 7 measurements: {len(union_stations)} -> {sorted(union_stations)}",
    f"Stations absent from each measurement (vs the union): {measure_missing}",
    f"Stations mapped to >1 city: {multi_city}   |   distinct stations per city: {city_counts}",
]
_rel = [
    "Referential integrity — measurement stations vs metadata (orphans, unused):",
    *[
        f"- {k}: orphan measurement stations={v[0]}, unused metadata stations={v[1]}"
        for k, v in ref_integrity.items()
    ],
    "",
    "Join cardinality — measurement.station_id -> metadata table (rows per station id):",
    *[
        f"- {k}: {'1:1' if v['max'] == 1 else '1:N fan-out'}  {v}"
        for k, v in meta_card.items()
    ],
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
_verdict = [
    f"- (station, MESS_DATUM) is unique in every measurement: {all(key_unique.values())}  -> pairwise measurement<->measurement joins are 1:1 on the overlap.",
    f"- largest non-shared timestamp count in any measurement pair: {max_pair_only}  -> an inner join to a wide table drops that tail.",
    f"- all 7 measurements have disjoint value-column sets: {schema_disjoint}.",
    "- Verdict: combine into a wide table only where timestamps overlap; keep one model per measurement in Silver, align at Gold.",
]
_silver = [
    "- Shared join key is (STATIONS_ID, MESS_DATUM); unique per measurement -> safe fan-out-free joins.",
]
if any(v["max"] > 1 for v in meta_card.values()):
    _silver.append(
        "- station_geography / station_name_history are 1:N on station_id -> join with the von/bis validity window, never station_id alone."
    )
if any(v[0] > 0 for v in ref_integrity.values()):
    _silver.append(
        "- Referential-integrity orphans exist -> left-join + a data-quality flag; do not drop the fact row."
    )
if not multi_city:
    _silver.append(
        "- `city` is consistent 1:1 with station_id here -> safe to carry as a station attribute."
    )
_silver.append(
    "- A cross-measurement wide 'all weather at station S, hour H' table drops rows (overlap numbers above) -> that is a Gold consolidation, not Silver."
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Entities / Keys", "\n".join(_ent)),
        ("Relationships", "\n".join(_rel)),
        ("EDA Findings", "\n".join(_verdict)),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=[
        (
            "DWD cross-measurement (station, timestamp) overlap per pair",
            "dwd_cross_measurement_overlap.png",
        ),
        (
            "DWD distinct stations per Bronze table",
            "dwd_stations_per_bronze_table.png",
        ),
        (
            "DWD stations absent from each measurement",
            "dwd_stations_absent_per_measurement.png",
        ),
        (
            "DWD referential integrity: measurements <-> metadata",
            "dwd_referential_integrity.png",
        ),
        ("DWD distinct stations per city", "dwd_stations_per_city.png"),
    ],
)