# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- DWD WEATHER MEASUREMENTS
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Profile the seven DWD weather-measurement Bronze tables
# MAGIC (air_temperature ... wind) -- schema, missingness, quality flags,
# MAGIC constant columns, temporal coverage & frequency (expected vs actual
# MAGIC hourly grid, gaps, longest gap per station), station x measurement
# MAGIC coverage matrix, per-station duplicates (identical vs conflicting),
# MAGIC value distributions & plausibility, QN-vs-missingness relationship --
# MAGIC as evidence for Silver design.

# COMMAND ----------

# DBTITLE 1,Imports
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
MEASUREMENTS = [
    "air_temperature",
    "cloudiness",
    "moisture",
    "precipitation",
    "pressure",
    "sun",
    "wind",
]
TABLES = {m: f"{CATALOG}.{BRONZE_SCHEMA}.dwd_{m}" for m in MEASUREMENTS}
NON_VALUE = {"STATIONS_ID", "CITY", "MESS_DATUM", "QN_9", "QN_3", "QN_4", "QN_8", "EOR"}

# Physical plausibility windows for the common DWD hourly parameters (values
# outside, and not the -999 sentinel, are "suspicious" not necessarily wrong).
PLAUSIBLE = {
    "TT_TU": (-40.0, 45.0),
    "RF_TU": (0.0, 100.0),
    "TF_STD": (-40.0, 45.0),
    "P": (900.0, 1080.0),
    "P0": (900.0, 1080.0),
    "N": (0.0, 8.0),
    "V_N": (0.0, 8.0),
    "R1": (0.0, 200.0),
    "RS_IND": (0.0, 1.0),
    "SD_SO": (0.0, 60.0),
    "F": (0.0, 60.0),
    "D": (0.0, 360.0),
}

# COMMAND ----------


# DBTITLE 1,Helpers
def find_col(df: DataFrame, *cands: str) -> str | None:
    low = {c.lower(): c for c in df.columns}
    for x in cands:
        if x.lower() in low:
            return low[x.lower()]
    return None


def as_ts(col: str):
    return F.to_timestamp(F.col(col).cast("string"), "yyyyMMddHH")


def value_columns(df: DataFrame) -> list:
    return [c for c in df.columns if c.upper() not in NON_VALUE]


def barplot(pairs, title, xlabel, ylabel="rows", rot=0, figsize=(10, 4)):
    plt.figure(figsize=figsize)
    plt.bar([str(p[0]) for p in pairs], [p[1] for p in pairs])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=rot, ha="right" if rot else "center")
    plt.tight_layout()
    plt.show()


def histplot(values, title, xlabel, bins=50):
    plt.figure(figsize=(10, 4))
    plt.hist(values, bins=bins)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel("count")
    plt.tight_layout()
    plt.show()


# COMMAND ----------

# DBTITLE 1,Profile -- rows, missingness, approx distinct, min/max ts, constant columns (one agg per table)
frames = {m: spark.table(t) for m, t in TABLES.items()}
prof = {}
for m in MEASUREMENTS:
    df = frames[m]
    cols = df.columns
    exprs = [
        F.count(F.lit(1)).alias("__rows"),
        F.min(find_col(df, "MESS_DATUM")).alias("__min_ts"),
        F.max(find_col(df, "MESS_DATUM")).alias("__max_ts"),
    ]
    for c in cols:
        miss = F.col(c).isNull() | (F.trim(F.col(c)) == "")
        exprs += [
            F.sum(miss.cast("long")).alias(c + "__m"),
            F.approx_count_distinct(c).alias(c + "__d"),
        ]
    r = df.agg(*exprs).first().asDict()
    prof[m] = {
        "cols": cols,
        "total": r["__rows"],
        "min_ts": r["__min_ts"],
        "max_ts": r["__max_ts"],
        "miss": {c: r[c + "__m"] for c in cols},
        "acd": {c: r[c + "__d"] for c in cols},
    }
    sid, city = find_col(df, "STATIONS_ID"), find_col(df, "city")
    print(
        "=" * 90,
        f"\n{m}  rows={r['__rows']}  {r['__min_ts']}..{r['__max_ts']}  "
        f"stations~={r[sid + '__d']}  cities~={r[city + '__d']}\n",
        "=" * 90,
    )
    for c in cols:
        print(
            f"  {c:<26} missing={r[c + '__m']:>12} rate={r[c + '__m'] / r['__rows']:.4f} approx_distinct={r[c + '__d']}"
        )
    print("constant columns:", [c for c in cols if r[c + "__d"] <= 1])
totals = {m: prof[m]["total"] for m in MEASUREMENTS}
coverage = {
    m: {
        "stations": prof[m]["acd"][find_col(frames[m], "STATIONS_ID")],
        "cities": prof[m]["acd"][find_col(frames[m], "city")],
        "min_ts": prof[m]["min_ts"],
        "max_ts": prof[m]["max_ts"],
    }
    for m in MEASUREMENTS
}
constant_cols = {
    m: [c for c in prof[m]["cols"] if prof[m]["acd"][c] <= 1] for m in MEASUREMENTS
}

# COMMAND ----------

# DBTITLE 1,Station / city breakdown + per-station row counts (one groupBy per table)
station_counts = {}
for m in MEASUREMENTS:
    df = frames[m]
    city, sid = find_col(df, "city"), find_col(df, "STATIONS_ID")
    g = df.groupBy(city, sid).count().collect()
    per_station = {}
    for x in g:
        per_station[str(x[sid])] = per_station.get(str(x[sid]), 0) + x["count"]
    station_counts[m] = per_station
    print(f"--- {m} ---", sorted((x[city], x[sid], x["count"]) for x in g))

# COMMAND ----------

# DBTITLE 1,QN quality-flag vs missingness / out-of-range (one groupBy per table)
qn_dist = {}
qn_quality = {}
for m in MEASUREMENTS:
    df = frames[m]
    qn = find_col(df, "QN_9", "QN_3", "QN_4", "QN_8", "QN")
    vcols = value_columns(df)
    if qn is None:
        continue
    any_sentinel = F.lit(False)
    any_oor = F.lit(False)
    for c in vcols:
        v = F.col(c).cast("double")
        any_sentinel = any_sentinel | (v == -999)
        b = PLAUSIBLE.get(c.upper())
        if b:
            any_oor = any_oor | ((v != -999) & ((v < b[0]) | (v > b[1])))
    g = (
        df.groupBy(qn)
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(any_sentinel.cast("long")).alias("rows_with_-999"),
            F.sum(any_oor.cast("long")).alias("rows_out_of_range"),
        )
        .orderBy(F.desc("rows"))
        .collect()
    )
    qn_dist[m] = [(x[qn], x["rows"]) for x in g]
    qn_quality[m] = [x.asDict() for x in g]
    print(f"--- {m} ({qn}) ---", qn_quality[m])

# COMMAND ----------

# DBTITLE 1,Duplicate (station, timestamp) -- identical vs conflicting (one groupBy per table)
dup_breakdown = {}
for m in MEASUREMENTS:
    df = frames[m]
    cols = df.columns
    key = [find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")]
    dk = df.groupBy(*key).agg(
        F.count(F.lit(1)).alias("n"),
        F.countDistinct(F.hash(*[F.col(c) for c in cols])).alias("row_variants"),
    )
    b = (
        dk.agg(
            F.sum((F.col("n") > 1).cast("long")).alias("dup_groups"),
            F.sum(((F.col("n") > 1) & (F.col("row_variants") == 1)).cast("long")).alias(
                "identical"
            ),
            F.sum(((F.col("n") > 1) & (F.col("row_variants") > 1)).cast("long")).alias(
                "conflicting"
            ),
        )
        .first()
        .asDict()
    )
    dup_breakdown[m] = b
    print(f"{m:<18} {b}")

# COMMAND ----------

# DBTITLE 1,Value columns -- range, sentinel, percentiles, plausibility (one agg per table)
value_stats = {}
for m in MEASUREMENTS:
    df = frames[m]
    exprs = []
    for c in value_columns(df):
        v = F.col(c).cast("double")
        b = PLAUSIBLE.get(c.upper())
        exprs += [
            F.min(v).alias(c + "_min"),
            F.max(v).alias(c + "_max"),
            F.min(F.when(v != -999, v)).alias(c + "_min_ns"),
            F.max(F.when(v != -999, v)).alias(c + "_max_ns"),
            F.sum((v == -999).cast("long")).alias(c + "_sentinel"),
            F.avg(F.when(v != -999, v)).alias(c + "_mean"),
            F.stddev(F.when(v != -999, v)).alias(c + "_sd"),
            F.expr(
                f"percentile_approx(case when cast(`{c}` as double) != -999 then cast(`{c}` as double) end, array(0.01,0.5,0.99))"
            ).alias(c + "_p"),
            F.sum((v == 0).cast("long")).alias(c + "_zero"),
            *(
                [
                    F.sum(((v != -999) & ((v < b[0]) | (v > b[1]))).cast("long")).alias(
                        c + "_oor"
                    )
                ]
                if b
                else []
            ),
        ]
    value_stats[m] = df.agg(*exprs).first().asDict()
    for c in value_columns(df):
        print(
            f"{m}.{c:<12}",
            {
                k[len(c) + 1 :]: value_stats[m][k]
                for k in value_stats[m]
                if k.startswith(c + "_")
            },
        )

# COMMAND ----------

# DBTITLE 1,Hourly coverage % + longest gap per station (one windowed pass per table)
freq_cov = {}
for m in MEASUREMENTS:
    df = frames[m]
    sid, dts = find_col(df, "STATIONS_ID"), find_col(df, "MESS_DATUM")
    w = Window.partitionBy("station").orderBy("ts")
    per_station = (
        df.select(F.col(sid).alias("station"), as_ts(dts).alias("ts"))
        .where(F.col("ts").isNotNull())
        .distinct()
        .withColumn(
            "gap_h",
            (F.col("ts").cast("long") - F.lag("ts").over(w).cast("long")) / 3600 - 1,
        )
        .groupBy("station")
        .agg(
            F.min("ts").alias("min_ts"),
            F.max("ts").alias("max_ts"),
            F.count(F.lit(1)).alias("observed_hours"),
            F.max(F.when(F.col("gap_h") > 0, F.col("gap_h"))).alias(
                "longest_gap_hours"
            ),
            F.sum(F.when(F.col("gap_h") > 0, F.col("gap_h")).otherwise(0)).alias(
                "total_missing_hours"
            ),
        )
        .withColumn(
            "expected_hours",
            (
                (F.col("max_ts").cast("long") - F.col("min_ts").cast("long")) / 3600 + 1
            ).cast("long"),
        )
        .withColumn(
            "coverage_pct",
            F.round(F.col("observed_hours") / F.col("expected_hours") * 100, 2),
        )
        .orderBy("station")
    )
    freq_cov[m] = [x.asDict() for x in per_station.collect()]
    print(f"--- {m} ---", freq_cov[m])

# COMMAND ----------

# DBTITLE 1,Station x measurement coverage matrix (reuses per-station row counts)
all_stations = sorted({s for m in MEASUREMENTS for s in station_counts[m]})
coverage_matrix = [
    {"station": s, **{m: station_counts[m].get(s, 0) for m in MEASUREMENTS}}
    for s in all_stations
]
for row in coverage_matrix:
    print(row)

# COMMAND ----------

# DBTITLE 1,Value-column sample for histograms / box plots (one sampled pass per table)
value_pdf = {}
for m in MEASUREMENTS:
    df = frames[m]
    vcols = value_columns(df)
    value_pdf[m] = (
        df.select(
            *[
                F.when(F.col(c).cast("double") != -999, F.col(c).cast("double")).alias(
                    c
                )
                for c in vcols
            ]
        )
        .sample(0.05, seed=42)
        .limit(150_000)
        .toPandas()
    )
    print(f"{m} value sample rows: {len(value_pdf[m])}")

# COMMAND ----------

# DBTITLE 1,Figure -- rows / stations / cities / temporal span per measurement
barplot(
    [(m, totals[m]) for m in MEASUREMENTS],
    "DWD -- rows per measurement",
    "measurement",
    rot=30,
)
barplot(
    [(m, coverage[m]["stations"]) for m in MEASUREMENTS],
    "DWD -- distinct stations per measurement",
    "measurement",
    "stations",
    rot=30,
)
barplot(
    [(m, coverage[m]["cities"]) for m in MEASUREMENTS],
    "DWD -- distinct cities per measurement",
    "measurement",
    "cities",
    rot=30,
)
plt.figure(figsize=(10, 4))
for i, m in enumerate(MEASUREMENTS):
    plt.plot(
        [int(str(coverage[m]["min_ts"])[:4]), int(str(coverage[m]["max_ts"])[:4])],
        [i, i],
        marker="|",
        markersize=12,
    )
plt.yticks(range(len(MEASUREMENTS)), MEASUREMENTS)
plt.title("DWD -- observation year span per measurement")
plt.xlabel("year")
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure -- QN distribution, dup composition, coverage %, longest gap
for m, pairs in qn_dist.items():
    barplot(pairs, f"DWD {m} -- QN quality-flag distribution", "QN value", "rows")
x = np.arange(len(MEASUREMENTS))
plt.figure(figsize=(11, 4))
plt.bar(
    x - 0.2,
    [dup_breakdown[m]["identical"] for m in MEASUREMENTS],
    width=0.4,
    label="fully-identical repeat groups",
)
plt.bar(
    x + 0.2,
    [dup_breakdown[m]["conflicting"] for m in MEASUREMENTS],
    width=0.4,
    label="keys with conflicting rows",
)
plt.xticks(x, MEASUREMENTS, rotation=30, ha="right")
plt.legend()
plt.title("DWD -- duplicate key composition")
plt.ylabel("key groups")
plt.tight_layout()
plt.show()
for m in MEASUREMENTS:
    barplot(
        [(r["station"], r["coverage_pct"]) for r in freq_cov[m]],
        f"DWD {m} -- hourly coverage % per station",
        "station id",
        "coverage %",
        rot=45,
    )
    barplot(
        [(r["station"], r["longest_gap_hours"] or 0) for r in freq_cov[m]],
        f"DWD {m} -- longest missing-hours gap per station",
        "station id",
        "hours",
        rot=45,
    )

# COMMAND ----------

# DBTITLE 1,Figure -- station x measurement coverage heatmap
grid = np.array(
    [[row[m] for m in MEASUREMENTS] for row in coverage_matrix], dtype=float
)
plt.figure(figsize=(9, max(3, 0.4 * len(all_stations))))
plt.imshow(
    np.where(grid > 0, np.log10(grid + 1), np.nan), aspect="auto", cmap="viridis"
)
plt.colorbar(label="log10(row count)")
plt.xticks(range(len(MEASUREMENTS)), MEASUREMENTS, rotation=45, ha="right")
plt.yticks(range(len(all_stations)), all_stations)
plt.title("DWD -- station x measurement coverage (row count)")
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Figure -- value distributions and box plots (sampled, sentinel excluded)
for m in MEASUREMENTS:
    pdf = value_pdf[m]
    cols = [c for c in pdf.columns if pdf[c].notna().any()]
    for c in cols:
        histplot(
            pdf[c].dropna().tolist(), f"DWD {m}.{c} -- value distribution (sampled)", c
        )
    if cols:
        plt.figure(figsize=(max(6, 1.6 * len(cols)), 4))
        plt.boxplot(
            [pdf[c].dropna().tolist() for c in cols], labels=cols, showfliers=True
        )
        plt.title(f"DWD {m} -- value column spread (sampled)")
        plt.ylabel("value")
        plt.tight_layout()
        plt.show()

# COMMAND ----------

# DBTITLE 1,Findings
for m in MEASUREMENTS:
    cov = [r["coverage_pct"] for r in freq_cov[m] if r["coverage_pct"] is not None]
    worst_gap = max((r["longest_gap_hours"] or 0 for r in freq_cov[m]), default=0)
    b = dup_breakdown[m]
    print(
        f"{m}: rows={totals[m]}, stations={coverage[m]['stations']}, "
        f"hourly coverage {min(cov) if cov else 'n/a'}-{max(cov) if cov else 'n/a'}%, "
        f"longest gap {worst_gap}h, dup identical={b['identical']}/conflicting={b['conflicting']}, "
        f"constant cols={constant_cols[m]}"
    )
