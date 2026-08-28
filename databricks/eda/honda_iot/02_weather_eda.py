# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- HONDA IOT WEATHER
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Profile honda_iot_weather -- schema (the sanitized
# MAGIC WeatherStation.Weather.* column names), missingness, constant columns,
# MAGIC frequency partitions, time continuity (expected vs actual, interval
# MAGIC consistency, gaps), value ranges, distributions & plausibility
# MAGIC (Ta range, Igm >= 0, stuck runs), duplicates (identical vs
# MAGIC conflicting) -- as evidence for Silver design and for the
# MAGIC energy<->weather join in notebook 03.

# COMMAND ----------

# DBTITLE 1,Imports
import matplotlib.pyplot as plt
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_weather"
FREQ_SECONDS = {"1min": 60, "15min": 900, "1h": 3600}
# Ta = air temperature degC ; Igm = global irradiance W/m2.
PLAUSIBLE = {"Ta": (-40.0, 50.0), "Igm": (0.0, 1500.0)}

# COMMAND ----------


# DBTITLE 1,Helpers
def barplot(pairs, title, xlabel, ylabel="rows", rot=0):
    plt.figure(figsize=(10, 4))
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

# DBTITLE 1,Profile -- rows, missingness, approx distinct, constant columns, value ranges (one agg)
df = spark.table(TABLE)
COLS = df.columns
VCOLS = [c for c in COLS if c not in ("frequency", "datetime_utc")]
exprs = [F.count(F.lit(1)).alias("__rows")]
for c in COLS:
    miss = F.col(c).isNull() | (F.trim(F.col(c)) == "")
    exprs += [
        F.sum(miss.cast("long")).alias(c + "__m"),
        F.approx_count_distinct(c).alias(c + "__d"),
    ]
for c in VCOLS:
    v = F.col(c).cast("double")
    b = PLAUSIBLE.get(c.split("_")[-1])
    exprs += [
        F.min(v).alias(c + "_min"),
        F.max(v).alias(c + "_max"),
        F.avg(v).alias(c + "_avg"),
        F.stddev(v).alias(c + "_sd"),
        F.expr(f"percentile_approx(cast(`{c}` as double), array(0.01,0.5,0.99))").alias(
            c + "_p"
        ),
        F.sum((v == 0).cast("long")).alias(c + "_zero"),
        F.sum(
            (F.col(c).isNotNull() & (F.trim(F.col(c)) != "") & v.isNull()).cast("long")
        ).alias(c + "_non_numeric"),
        *(
            [F.sum(((v < b[0]) | (v > b[1])).cast("long")).alias(c + "_out_of_range")]
            if b
            else []
        ),
    ]
S = df.agg(*exprs).first().asDict()
total = S["__rows"]
constant_cols = [c for c in COLS if S[c + "__d"] <= 1]
print(f"rows={total}  columns={len(COLS)}  ->  {COLS}")
for c in COLS:
    print(
        f"  {c:<32} missing={S[c + '__m']:>10} rate={S[c + '__m'] / total:.4f} approx_distinct={S[c + '__d']}"
    )
print("constant columns:", constant_cols)
for c in VCOLS:
    print(f"  {c:<32}", {k[len(c) + 1 :]: S[k] for k in S if k.startswith(c + "_")})
df.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Rows per frequency + timestamp coverage (one groupBy)
d = (
    df.groupBy("frequency")
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.min("datetime_utc").alias("min_ts"),
        F.max("datetime_utc").alias("max_ts"),
        F.countDistinct("datetime_utc").alias("distinct_ts"),
    )
    .orderBy("frequency")
    .collect()
)
freq_rows = [(x["frequency"], x["rows"]) for x in d]
print([x.asDict() for x in d])

# COMMAND ----------

# DBTITLE 1,Duplicate (frequency, datetime_utc) key -- identical vs conflicting (one groupBy)
dk = df.groupBy("frequency", "datetime_utc").agg(
    F.count(F.lit(1)).alias("n"),
    F.countDistinct(F.hash(*[F.col(c) for c in COLS])).alias("row_variants"),
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
print("duplicate key composition:", b)

# COMMAND ----------

# DBTITLE 1,Time continuity + interval consistency (one windowed pass)
w = Window.partitionBy("frequency").orderBy("ts")
deltas = (
    df.select("frequency", F.to_timestamp("datetime_utc").alias("ts"))
    .where(F.col("ts").isNotNull())
    .distinct()
    .withColumn("delta_s", F.col("ts").cast("long") - F.lag("ts").over(w).cast("long"))
)
g = deltas.groupBy("frequency", "delta_s").count().collect()
continuity = []
for freq, step in FREQ_SECONDS.items():
    fg = [x for x in g if x["frequency"] == freq]
    if not fg:
        continue
    observed = sum(x["count"] for x in fg)
    intervals = sum(x["count"] for x in fg if x["delta_s"] is not None)
    on_step = sum(x["count"] for x in fg if x["delta_s"] == step)
    longest_gap = max(
        ((x["delta_s"] / step - 1) for x in fg if x["delta_s"] and x["delta_s"] > step),
        default=0,
    )
    missing = sum(
        (x["delta_s"] / step - 1) * x["count"]
        for x in fg
        if x["delta_s"] and x["delta_s"] > step
    )
    expected = observed + missing
    continuity.append(
        (
            freq,
            observed,
            round(observed / expected * 100, 2) if expected else None,
            round(on_step / intervals * 100, 2) if intervals else None,
            round(longest_gap, 1),
        )
    )
    print(continuity[-1])
delta_dist = sorted(
    ((x["frequency"], x["delta_s"], x["count"]) for x in g), key=lambda t: -t[2]
)[:20]
print("top interval sizes (frequency, delta_s, count):", delta_dist)

# COMMAND ----------

# DBTITLE 1,Stuck-run detection per value column (one windowed pass)
w2 = Window.partitionBy("frequency").orderBy("datetime_utc")
stuck_exprs = []
for c in VCOLS:
    v = F.col(c).cast("double")
    stuck_exprs.append(
        F.sum(
            (
                v.isNotNull()
                & (v == F.lag(v, 1).over(w2))
                & (v == F.lag(v, 11).over(w2))
            ).cast("long")
        ).alias(c)
    )
stuck = df.where(F.col("frequency") == "1h").agg(*stuck_exprs).first().asDict()
print("stuck>=12-run (1h):", stuck)

# COMMAND ----------

# DBTITLE 1,Value sample + hourly window + diurnal profile
value_pdf = (
    df.select(*[F.col(c).cast("double").alias(c) for c in VCOLS])
    .sample(0.1, seed=42)
    .limit(150_000)
    .toPandas()
)
ts_pdf = (
    df.where(F.col("frequency") == "1h")
    .select("datetime_utc", *[F.col(c).cast("double").alias(c) for c in VCOLS])
    .orderBy("datetime_utc")
    .limit(3000)
    .toPandas()
)
hourly = (
    df.where(F.col("frequency") == "1h")
    .groupBy(F.hour(F.to_timestamp("datetime_utc")).alias("hod"))
    .agg(*[F.avg(F.col(c).cast("double")).alias(c) for c in VCOLS])
    .orderBy("hod")
    .collect()
)
print("value sample rows:", len(value_pdf))

# COMMAND ----------

# DBTITLE 1,Figure -- frequency, coverage %, longest gap
barplot(freq_rows, "Honda weather -- rows per frequency", "frequency", "rows")
if continuity:
    barplot(
        [(r[0], r[2]) for r in continuity],
        "Honda weather -- coverage % by frequency",
        "frequency",
        "%",
    )
    barplot(
        [(r[0], r[4]) for r in continuity],
        "Honda weather -- longest gap (steps) by frequency",
        "frequency",
        "steps",
    )

# COMMAND ----------

# DBTITLE 1,Figure -- value distributions, hourly window, diurnal profile
for c in VCOLS:
    s = value_pdf[c].dropna()
    if len(s):
        histplot(
            s.tolist(), f"Honda weather.{c} -- distribution (n={len(s)} sample)", c
        )
if not ts_pdf.empty:
    fig, axes = plt.subplots(len(VCOLS), 1, figsize=(12, 3 * len(VCOLS)), squeeze=False)
    for i, c in enumerate(VCOLS):
        axes[i][0].plot(range(len(ts_pdf)), ts_pdf[c], linewidth=0.8)
        axes[i][0].set_title(f"Honda weather -- {c} (first 3000 hourly points)")
    plt.tight_layout()
    plt.show()
for c in VCOLS:
    plt.figure(figsize=(9, 3))
    plt.plot([h["hod"] for h in hourly], [h[c] for h in hourly], marker="o")
    plt.title(f"Honda weather -- mean {c} by hour of day")
    plt.xlabel("hour")
    plt.ylabel(c)
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# DBTITLE 1,Findings
print("constant columns:", constant_cols)
print("duplicate key composition:", b)
print("coverage % / on-step % per frequency:", [(r[0], r[2], r[3]) for r in continuity])
print(
    "value plausibility (range / out_of_range / zero):",
    {
        c: {
            k[len(c) + 1 :]: S[k]
            for k in S
            if k.startswith(c + "_")
            and ("out_of_range" in k or "zero" in k or "min" in k or "max" in k)
        }
        for c in VCOLS
    },
)
print("stuck runs:", stuck)
