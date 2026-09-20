# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- IOT DEVICE TELEMETRY (SAMPLES)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Profile the `iot` dataset read in place from the Databricks
# MAGIC Samples Volume (no Bronze table) -- file inventory and README provenance,
# MAGIC schema and nesting, missingness, constant columns, duplicates, device /
# MAGIC record keys and readings per device, timestamp unit and range, geography
# MAGIC validity and per-country coordinate spread, sensor value plausibility and
# MAGIC shape, categorical consistency, indicators of generated data, and the
# MAGIC layered modelling-risk checklist -- as evidence.

# COMMAND ----------

# DBTITLE 1,Imports
import itertools

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# MAGIC %run ../_samples_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "iot"
NB_KEY = "01_iot_devices"
SECTION_TITLE = "IoT device telemetry (Samples: iot)"
ROOT = f"{SAMPLES_VOLUME_ROOT}/iot"
GLOBAL_BBOX = (-90.0, 90.0, -180.0, 180.0)
NAME_HINTS = {
    "device": ("device_id", "deviceid", "device"),
    "device_name": ("device_name", "devicename", "name"),
    "ip": ("ip",),
    "cca2": ("cca2",),
    "cca3": ("cca3",),
    "country": ("cn", "country"),
    "lat": ("latitude", "lat"),
    "lon": ("longitude", "lon", "lng"),
    "scale": ("scale", "unit"),
    "lcd": ("lcd",),
    "ts": ("timestamp", "ts", "event_time", "time"),
}
VALUE_HINTS = ("temp", "humidity", "battery", "c02", "co2")
# Physical plausibility bounds by column-name fragment (the unit is read from
# the `scale` column, so the temperature bound is deliberately wide).
BOUNDS = {"humidity": (0.0, 100.0), "temp": (-60.0, 150.0)}

# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Discover files
entries, kinds, frames = load_volume_frames(ROOT)
print(f"{ROOT}: {len(entries)} files")
for e in entries:
    print(f"  {file_kind(e['name']):<10} {e['size']:>12}  {e['path']}")
print({k: len(v) for k, v in kinds.items()})

# COMMAND ----------

# DBTITLE 1,Support files (README) -- provenance evidence
support = read_support_text(kinds)
for name, lines in support.items():
    print("=" * 90, f"\n{name}")
    for ln in lines[:40]:
        print("  " + ln[:160])
if not support:
    print("no README / licence file in the directory")

# COMMAND ----------

# DBTITLE 1,Select the frame
FRAME = "json" if "json" in frames else next(iter(frames))
df = frames[FRAME]["df"]
DATA_COLS = [c for c in df.columns if c != "__file"]
print(f"frame={FRAME}  columns={DATA_COLS}")
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Resolve column roles
role = {}
for r, hints in NAME_HINTS.items():
    role[r] = next((c for h in hints for c in DATA_COLS if c.lower() == h), None)
value_cols = [c for c in DATA_COLS if any(h in c.lower() for h in VALUE_HINTS)]
print(role)
print("value columns:", value_cols)

# COMMAND ----------

# DBTITLE 1,Structural check -- corrupt records and nesting
nested = [
    f.name
    for f in df.schema.fields
    if f.dataType.simpleString().startswith(("struct", "array", "map"))
]
struct = {
    "corrupt_record": "_corrupt_record" in DATA_COLS,
    "nested": nested,
    "schema": df.schema.simpleString(),
}
print(struct)

# COMMAND ----------

# DBTITLE 1,Profile -- rows, missingness, approx distinct
prof = profile_frame(df.drop("__file"))
print(f"rows={prof['total']}  cols={len(prof['cols'])}")
for c in prof["cols"]:
    print(
        f"  {c[:40]:<40} missing={prof['miss'][c]:>8} "
        f"rate={prof['miss'][c] / prof['total'] if prof['total'] else 0:.4f} "
        f"approx_distinct={prof['acd'][c]}"
    )

# COMMAND ----------

# DBTITLE 1,Constant columns + full-row duplicates
d0 = df.drop("__file")
prof["constant"] = constant_cols(d0, prof)
prof["dups"] = full_row_dup_count(d0, prof["total"])
print(f"constant={prof['constant']}  full-row duplicates={prof['dups']}")

# COMMAND ----------

# DBTITLE 1,Key candidates -- exact uniqueness
key_cands = [role[r] for r in ("device", "device_name", "ip") if role[r]]
uniq = exact_uniqueness(d0, key_cands)
for c, u in uniq.items():
    print(c, u)

# COMMAND ----------

# DBTITLE 1,Device / timestamp key duplicates
dev, tsc = role["device"], role["ts"]
dup_key = None
if dev and tsc:
    dup_key = dup_key_composition(d0, [dev, tsc])
    print(f"({dev}, {tsc}): {dup_key}")

# COMMAND ----------

# DBTITLE 1,Readings per device
per_device = None
if dev:
    pd_ = df.groupBy(qcol(dev)).count()
    per_device = (
        pd_.agg(
            F.count(F.lit(1)).alias("devices"),
            F.min("count").alias("min"),
            F.expr("percentile_approx(`count`, 0.5)").alias("p50"),
            F.expr("percentile_approx(`count`, 0.99)").alias("p99"),
            F.max("count").alias("max"),
        )
        .first()
        .asDict()
    )
    print(per_device)

# COMMAND ----------

# DBTITLE 1,Numeric parse yield + moments
num_cols = [c for c in DATA_COLS if c != "__file"]
num = numeric_scan(df, num_cols)
for c, s in num.items():
    if s["is_numeric"]:
        print(
            f"{c:<16} yield={s['yield']} min={s['min']} max={s['max']} "
            f"mean={fmt_num(s['mean'])} sd={fmt_num(s['sd'])} zero={s['zero']} negative={s['negative']}"
        )

# COMMAND ----------

# DBTITLE 1,Sensor plausibility
plaus = {}
for c in value_cols:
    if not num[c]["is_numeric"]:
        continue
    key = next((k for k in BOUNDS if k in c.lower()), None)
    lo, hi = BOUNDS.get(key, (None, None))
    plaus[c] = plausibility(df, c, lo=lo, hi=hi, sentinels=())
    print(
        c,
        {k: plaus[c].get(k) for k in ("min", "max", "mean", "below", "above", "zero")},
    )

# COMMAND ----------

# DBTITLE 1,Quantiles and distribution shape
quant, shape = {}, {}
for c in value_cols:
    if not num[c]["is_numeric"]:
        continue
    quant[c] = quantiles(df, c)
    v = to_double(c)
    shape[c] = (
        df.agg(F.skewness(v).alias("skew"), F.kurtosis(v).alias("kurt"))
        .first()
        .asDict()
    )
    print(c, quant[c], shape[c])

# COMMAND ----------

# DBTITLE 1,Histograms (binned in Spark) + uniformity
hist, flat = {}, {}
for c in value_cols:
    s = num[c]
    if s["is_numeric"]:
        hist[c] = hist_counts(df, c, s["min"], s["max"])
        counts = [n for _, n in hist[c]]
        mean_n = sum(counts) / len(counts) if counts else 0
        var = sum((n - mean_n) ** 2 for n in counts) / len(counts) if counts else 0
        flat[c] = round((var**0.5) / mean_n, 3) if mean_n else None
        print(c, "bin-count coefficient of variation:", flat[c])

# COMMAND ----------

# DBTITLE 1,Pairwise correlation of sensor columns
ncols = [c for c in value_cols if num[c]["is_numeric"]]
numdf = df.select(*[to_double(c).alias(c) for c in ncols])
corr = {(a, b): numdf.stat.corr(a, b) for a, b in itertools.combinations(ncols, 2)}
for pair, v in corr.items():
    print(pair, round(v, 4) if v is not None else None)
mirrors = mirror_columns({c: num[c] for c in ncols if num[c]["sd"]})
print("mirror pairs:", mirrors)

# COMMAND ----------

# DBTITLE 1,Categorical distributions
cat_cols = [c for c in prof["cols"] if 1 < prof["acd"][c] <= 60]
cat_dist = {}
for c in cat_cols:
    vc = df.groupBy(qcol(c)).count().orderBy(F.desc("count")).limit(30).collect()
    cat_dist[c] = [(r[0], r["count"]) for r in vc]
    print(c, cat_dist[c][:12])

# COMMAND ----------

# DBTITLE 1,Categorical consistency -- country name vs code
spread = []
pairs = [
    (role["cca3"], role["cca2"]),
    (role["cca3"], role["country"]),
    (role["cca2"], role["country"]),
]
for g, v in pairs:
    if g and v:
        spread.append(group_attribute_spread(d0, g, v))
        print(spread[-1])

# COMMAND ----------

# DBTITLE 1,Geography -- coordinate validity
geo = None
if role["lat"] and role["lon"]:
    geo = spatial_validity(
        d0, role["lat"], role["lon"], bbox=GLOBAL_BBOX, name="device"
    )
    print(geo)

# COMMAND ----------

# DBTITLE 1,Geography -- per-country coordinate spread
country_col = role["cca3"] or role["cca2"] or role["country"]
country_spread = []
if country_col and role["lat"] and role["lon"]:
    la, lo = to_double(role["lat"]), to_double(role["lon"])
    rows = (
        df.groupBy(qcol(country_col).alias("c"))
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.min(la).alias("lat_min"),
            F.max(la).alias("lat_max"),
            F.min(lo).alias("lon_min"),
            F.max(lo).alias("lon_max"),
        )
        .collect()
    )
    country_spread = [r.asDict() for r in rows]
    wide = [r for r in country_spread if r["lat_max"] - r["lat_min"] > 60]
    print(f"countries={len(country_spread)}  with latitude span > 60 deg: {len(wide)}")

# COMMAND ----------

# DBTITLE 1,Device coordinate stability
stab = None
if dev and role["lat"] and role["lon"]:
    s = (
        df.groupBy(qcol(dev))
        .agg(
            F.approx_count_distinct(as_str(role["lat"])).alias("lat_variants"),
            F.approx_count_distinct(as_str(role["lon"])).alias("lon_variants"),
            F.count(F.lit(1)).alias("n"),
        )
        .where(F.col("n") > 1)
        .agg(
            F.count(F.lit(1)).alias("multi_reading_devices"),
            F.sum(
                ((F.col("lat_variants") > 1) | (F.col("lon_variants") > 1)).cast("long")
            ).alias("moving"),
        )
        .first()
        .asDict()
    )
    stab = s
    print(stab)

# COMMAND ----------

# DBTITLE 1,IP address format
ip_check = None
if role["ip"]:
    ipc = as_str(role["ip"])
    ip_check = (
        d0.agg(
            F.sum(ipc.rlike(r"^\d{1,3}(\.\d{1,3}){3}$").cast("long")).alias("ipv4"),
            F.sum(
                ipc.rlike(r"^(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.)").cast(
                    "long"
                )
            ).alias("private"),
            F.count(F.lit(1)).alias("rows"),
        )
        .first()
        .asDict()
    )
    print(ip_check)

# COMMAND ----------

# DBTITLE 1,Timestamp unit and range
ts_info = None
if role["ts"]:
    tcol = role["ts"]
    if num[tcol]["is_numeric"]:
        ts_info = epoch_scan(d0, tcol)
        ts_info["kind"] = "epoch"
    else:
        ts_info = timestamp_semantics(
            d0, tcol, valid_from="1990-01-01", tz="unspecified"
        )
        ts_info["kind"] = "text"
    print(ts_info)

# COMMAND ----------

# DBTITLE 1,Within-device time series -- gaps and lag-1 autocorrelation
series = None
multi = bool(per_device and per_device["p50"] and per_device["p50"] > 1)
if multi and role["ts"] and dev and value_cols:
    tcol = role["ts"]
    tt = (
        to_double(tcol)
        if num[tcol]["is_numeric"]
        else F.unix_timestamp(parse_ts_multi(tcol))
    )
    w = Window.partitionBy(qcol(dev)).orderBy(tt)
    base = df.select(
        qcol(dev).alias("d"), tt.alias("t"), *[to_double(c).alias(c) for c in ncols]
    )
    lagged = base.withColumn("gap", F.col("t") - F.lag("t").over(w))
    gaps = (
        lagged.agg(
            F.expr("percentile_approx(gap, 0.5)").alias("gap_p50"),
            F.expr("percentile_approx(gap, 0.99)").alias("gap_p99"),
            F.max("gap").alias("gap_max"),
        )
        .first()
        .asDict()
    )
    ac = {}
    for c in ncols:
        lg = lagged.withColumn("prev", F.lag(c).over(w))
        ac[c] = lg.stat.corr(c, "prev")
    series = {"gaps": gaps, "autocorr": ac}
    print(series)
else:
    print(
        "one reading per device (or no device / time column): within-device series not assessable"
    )

# COMMAND ----------

# DBTITLE 1,Figures
figs = []
_panels = {c: hist[c] for c in hist}
if facet_bars(
    _panels,
    "IoT -- sensor value distributions",
    "iot_distributions.png",
    rot=60,
    ncols=2,
):
    figs.append(("IoT -- sensor value distributions", "iot_distributions.png"))
if cat_dist and facet_bars(
    {c: cat_dist[c] for c in list(cat_dist)[:4]},
    "IoT -- categorical columns",
    "iot_categorical.png",
    rot=45,
    ncols=2,
):
    figs.append(("IoT -- categorical columns", "iot_categorical.png"))

# COMMAND ----------

# DBTITLE 1,Findings
print(
    f"rows={prof['total']}, cols={len(prof['cols'])}, constant={prof['constant']}, "
    f"dups={prof['dups']}, devices={per_device}, geo={geo}, ts={ts_info}"
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/iot.md
_profile = [
    "| frame | source files | rows | cols | constant columns | nested columns |",
    "|---|---|---|---|---|---|",
    para(
        f"| {FRAME} | {len(frames[FRAME]['paths'])} | {prof['total']} | {len(prof['cols'])} |",
        f"{', '.join(prof['constant']) or '-'} | {', '.join(nested) or '-'} |",
    ),
]

_prov = [
    para(
        "The dataset is read in place from the Databricks Samples Volume;",
        "there is no Bronze table and no ECRMAP acquisition step.",
    ),
    f"Files under `{ROOT}`: {len(entries)} ({ {k: len(v) for k, v in kinds.items()} }).",
]
for name, lines in support.items():
    _prov.append(f"README-type file `{name}` (first lines):")
    _prov += [f"  > {ln[:160]}" for ln in lines[:25] if ln.strip()]
if not support:
    _prov.append(
        "- No README / licence file in the directory: provenance, generator and licence are not documented here (LIMITATION)."
    )

_struct = [
    f"- schema: `{struct['schema']}`",
    f"- corrupt-record column present: {struct['corrupt_record']}; nested columns: {nested or 'none'}.",
    f"- column roles resolved by name: { {k: v for k, v in role.items() if v} }.",
]

_dq = [
    f"Full-row exact duplicates: {prof['dups']}.",
    "Missingness (rate per column): "
    + ", ".join(
        f"{c}={prof['miss'][c] / prof['total'] if prof['total'] else 0:.4f}"
        for c in prof["cols"]
    ),
]
if dup_key:
    _dq.append(
        f"Duplicates on (`{dev}`, `{tsc}`): {dup_key['dup_groups']} duplicated keys out of "
        f"{dup_key['distinct_keys']} ({dup_key['identical']} identical, {dup_key['conflicting']} conflicting)."
    )

_entities = ["Key candidates (column: distinct / ratio-to-rows / unique):"]
for c, u in uniq.items():
    _entities.append(f"- `{c}`: {u['distinct']} / {u['ratio']} / unique={u['unique']}")
if not uniq:
    _entities.append("- no device / name / ip column located by name.")
if per_device:
    _entities.append(
        f"- readings per device: {per_device['devices']} devices; min {per_device['min']}, "
        f"p50 {per_device['p50']}, p99 {per_device['p99']}, max {per_device['max']}."
    )

_unit = []
for c in DATA_COLS:
    s = num[c]
    if s["is_numeric"]:
        line = (
            f"- `{c}`: numeric yield {s['yield']:.1%}, range {s['min']}..{s['max']}, "
            f"mean {fmt_num(s['mean'])}, sd {fmt_num(s['sd'])}, zero {s['zero']}, negative {s['negative']}"
        )
        if c in plaus:
            line += f"; below-bound {plaus[c].get('below')}, above-bound {plaus[c].get('above')}"
        _unit.append(line + ".")
if role["scale"] and role["scale"] in cat_dist:
    _unit.append(
        f"- `{role['scale']}` (temperature unit label) values: {cat_dist[role['scale']][:6]} -- the temperature column is only comparable within one scale."
    )
if ip_check:
    _unit.append(
        f"- `{role['ip']}`: IPv4-format {ip_check['ipv4']}/{ip_check['rows']}, private-range {ip_check['private']}."
    )

_domain = []
for c, pairs_ in cat_dist.items():
    _domain.append(
        f"- `{c}`: {len(pairs_)} values shown; "
        + fmt_pairs(pairs_, n=8).replace("\n", " ")
    )
for sp in spread:
    _domain.append(
        f"- `{sp['group_col']}` -> `{sp['value_col']}`: {sp['groups']} groups, "
        f"{sp['inconsistent_groups']} map to more than one value."
    )
if not _domain:
    _domain.append("- No low-cardinality categorical column located.")

_geo = []
if geo:
    _geo.append(
        f"- coordinates present {geo['present']}, missing {geo['missing']}, at 0/0 {geo['null_island']}, "
        f"outside the global box {geo['outside_bbox']}, looks lat/lon-swapped {geo['looks_swapped']}."
    )
if country_spread:
    wide = [r for r in country_spread if r["lat_max"] - r["lat_min"] > 60]
    _geo.append(
        f"- {len(country_spread)} distinct `{country_col}` values; {len(wide)} span more than 60 degrees "
        "of latitude (a real country almost never does -- a sign that coordinates are not tied to the country label)."
    )
if stab:
    _geo.append(
        f"- devices with more than one reading: {stab['multi_reading_devices']}; of those, "
        f"{stab['moving']} report more than one coordinate."
    )
if not _geo:
    _geo.append("- No latitude / longitude columns located.")

_temporal = []
if ts_info and ts_info.get("kind") == "epoch" and ts_info.get("n"):
    _temporal.append(
        f"- `{role['ts']}`: numeric epoch in {ts_info['unit']}; range {ts_info['min_ts']} .. {ts_info['max_ts']} (UTC); "
        f"{ts_info['distinct_days']} distinct days, {ts_info['distinct_ts']} distinct instants."
    )
elif ts_info and ts_info.get("kind") == "text":
    _temporal += [f"- {ln}" for ln in ts_info["lines"]]
else:
    _temporal.append("- No timestamp column located; temporal semantics not assessed.")
if series:
    _temporal.append(
        f"- within-device gaps (seconds): p50 {series['gaps']['gap_p50']}, p99 {series['gaps']['gap_p99']}, max {series['gaps']['gap_max']}."
    )

_tcons = []
if series:
    _tcons.append(
        "Lag-1 autocorrelation within device: "
        + ", ".join(
            f"{c}={v:.3f}" for c, v in series["autocorr"].items() if v is not None
        )
        + " -- values near zero mean successive readings behave as independent draws."
    )
else:
    _tcons.append(
        para(
            "Devices have (at most) one reading each in this file, so there is no within-device",
            "series and temporal consistency (gaps, ordering, autocorrelation) is not applicable.",
        )
    )

_value = ["Pairwise Pearson correlation between sensor columns:"]
_value += [f"- {a} vs {b}: {v:.3f}" for (a, b), v in corr.items() if v is not None]
_value.append(f"- mirror / duplicate column pairs: {mirrors or 'none'}")
_value.append("Distribution shape (skewness / excess kurtosis / bin-count CV):")
for c in shape:
    _value.append(
        f"- `{c}`: skew {fmt_num(shape[c]['skew'])}, kurtosis {fmt_num(shape[c]['kurt'])}, bin CV {flat.get(c)}"
    )

_regime = [
    para(
        "Indicators that values are generated rather than measured (each is evidence,",
        "not proof): a low bin-count coefficient of variation means a near-uniform spread;",
        "correlations near zero between physically related sensors; zero autocorrelation;",
        "coordinates unrelated to the country label; a constant or near-constant timestamp.",
    ),
    f"- bin-count CV per sensor column: {flat}",
    f"- sensor correlations: { {f'{a}~{b}': round(v, 3) for (a, b), v in corr.items() if v is not None} }",
    f"- devices whose coordinates fall outside their country's plausible extent: {len([r for r in country_spread if r['lat_max'] - r['lat_min'] > 60])} of {len(country_spread)} country groups",
]

_coverage = [
    para(
        "Coverage of the file cannot be checked against an external reference:",
        "the README (if any) is the only statement of origin.",
    ),
    f"- devices: {per_device['devices'] if per_device else 'unknown'}; countries: {len(country_spread) or 'unknown'}.",
]

_dist = []
for c, q in quant.items():
    _dist.append(
        f"- `{c}` quantiles (p1/p25/p50/p75/p99): "
        + ", ".join(fmt_num(v) for v in q.values())
    )
for c, pairs_ in cat_dist.items():
    _dist.append(f"- `{c}`: " + fmt_pairs(pairs_, n=10).replace("\n", " "))

_findings_md = (
    f"- rows={prof['total']}, cols={len(prof['cols'])}, constant={prof['constant']}, dups={prof['dups']}, "
    f"devices={per_device['devices'] if per_device else 'n/a'}"
)

_silver = [
    "- Read from the Samples Volume; no Bronze table exists for this dataset.",
    "- Timestamp needs an explicit unit rule (epoch unit inferred above); sensor columns need typed casts with a quarantine for failures.",
    "- Whether a device identifier is a stable entity key depends on the readings-per-device finding above.",
    "- Provenance and real/synthetic status are unresolved in the data itself; label the table accordingly.",
]

_dup_map = {"full_row": prof["dups"], "device_ts": dup_key}
_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            f"One row per device reading; rows={prof['total']}; readings per device={per_device}. If devices have one reading each, the grain is a device snapshot, not a time series.",
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            "The device identifier is the only join candidate; fan-out to another table depends on the readings-per-device finding.",
        ),
        (
            "Target contamination",
            "No target is defined by the source; humidity or temperature as a target leaves the other sensor columns as features whose independence is tested in Value Consistency.",
        ),
        (
            "Temporal / post-event leakage",
            f"Timestamp evidence: {ts_info}. A near-constant timestamp means there is no time ordering to leak from or to respect.",
        ),
        (
            "Proxy leakage",
            "Device name / id / IP / country code are near-identifiers; a model given them memorises the device.",
        ),
        (
            "Split / entity leakage",
            "Split by device; if each device has one reading, a random row split is already entity-disjoint.",
        ),
        (
            "Historical-reference (point-in-time) leakage",
            "Not applicable -- no reference table or validity windows.",
        ),
        (
            "Survivorship / coverage bias",
            f"Coverage is whatever the file contains; devices={per_device}, countries={len(country_spread)}.",
        ),
        (
            "Missingness leakage",
            "Missingness per column is in Data Quality; check that it is not correlated with device attributes before using is-missing features.",
        ),
        (
            "Duplicate-event leakage",
            f"Duplicates: {_dup_map}.",
        ),
        (
            "Target / feature temporal misalignment",
            "Sensor columns share one timestamp per row; no lag structure is represented.",
        ),
        (
            "Unit / sign / circular-feature leakage",
            f"Temperature unit is given by a label column (values: {cat_dist.get(role['scale'], 'n/a')}); mirror pairs: {mirrors or 'none'}.",
        ),
        (
            "Data-generation-process leakage",
            "See Regime / Version Evidence: indicators of generated data (near-uniform values, near-zero correlations, coordinates unrelated to the country label) mean patterns learned here describe the generator, not devices.",
        ),
        (
            "Class / label instability",
            "No target label; categorical columns (colour / scale) are static attributes.",
        ),
        (
            "Label availability lag",
            "Not applicable -- no delayed label.",
        ),
        (
            "Source / version / regime change",
            "Single static file; no version indicator.",
        ),
        (
            "Sample-vs-full divergence",
            "Every statistic is a full Spark aggregation over the file read; whether the file is a sample of a larger set is not stated in the data.",
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
        ("Value Consistency", "\n".join(_value)),
        ("Regime / Version Evidence", "\n".join(_regime)),
        ("Coverage & Sampling Bias", "\n".join(_coverage)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
