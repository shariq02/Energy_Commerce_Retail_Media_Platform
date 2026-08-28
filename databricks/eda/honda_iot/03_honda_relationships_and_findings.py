# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- HONDA IOT RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Cross-table checks across the 7 Honda IoT Bronze tables --
# MAGIC timestamp-grid alignment per frequency, join cardinality on
# MAGIC (frequency, datetime_utc), pairwise overlap matrix, full 7-way join
# MAGIC yield, and an evidence-based verdict on combining the datasets. All
# MAGIC key-overlap analysis is derived from one tagged union + one presence
# MAGIC matrix rather than repeated pairwise joins.

# COMMAND ----------

# DBTITLE 1,Imports
import matplotlib.pyplot as plt
import numpy as np

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
DATASETS = ["electricity_p", "electricity_w", "heating_p", "heating_w",
            "cooling_p", "cooling_w", "weather"]
ENERGY = [d for d in DATASETS if d != "weather"]
TABLES = {d: f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_{d}" for d in DATASETS}

# COMMAND ----------

# DBTITLE 1,Helper
def barplot(pairs, title, xlabel, ylabel="rows", rot=0):
    plt.figure(figsize=(10, 4))
    plt.bar([str(p[0]) for p in pairs], [p[1] for p in pairs])
    plt.title(title); plt.xlabel(xlabel); plt.ylabel(ylabel)
    plt.xticks(rotation=rot, ha="right" if rot else "center")
    plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Tagged union of (frequency, datetime_utc, source) across all 7 tables
u = None
for d, t in TABLES.items():
    part = spark.table(t).select("frequency", "datetime_utc", F.lit(d).alias("src"))
    u = part if u is None else u.union(part)

# COMMAND ----------

# DBTITLE 1,Timestamp grid + row counts per (dataset, frequency) -- one groupBy
grid = u.groupBy("src", "frequency").agg(
    F.min("datetime_utc").alias("min_ts"), F.max("datetime_utc").alias("max_ts"),
    F.countDistinct("datetime_utc").alias("distinct_ts"), F.count(F.lit(1)).alias("rows"),
).collect()
table_rows = {}
for x in grid:
    table_rows[x["src"]] = table_rows.get(x["src"], 0) + x["rows"]
    print(x.asDict())
for d, t in TABLES.items():
    print(f"{d:<16} columns -> {spark.table(t).columns}")

# COMMAND ----------

# DBTITLE 1,Key-presence matrix -- one groupBy over the union
p = u.groupBy("frequency", "datetime_utc").agg(
    *[F.max((F.col("src") == d).cast("int")).alias(d) for d in DATASETS]
)

# COMMAND ----------

# DBTITLE 1,Overlap / referential stats -- one agg over the presence matrix
pairs = [(a, b) for i, a in enumerate(DATASETS) for b in DATASETS[i + 1:]]
stats = p.agg(
    F.count(F.lit(1)).alias("union_keys"),
    *[F.sum(d).alias("present__" + d) for d in DATASETS],
    F.sum(F.least(*[F.col(d) for d in DATASETS])).alias("all_seven"),
    *[F.sum(F.col(a) * F.col(b)).alias(f"pair__{a}__{b}") for a, b in pairs],
    *[F.sum(F.col(e) * F.col("weather")).alias(f"ew__{e}") for e in ENERGY],
).first().asDict()

union_ct = stats["union_keys"]
present = {d: stats["present__" + d] for d in DATASETS}
grid_missing = {d: union_ct - present[d] for d in DATASETS}
overlap = {(a, b): stats[f"pair__{a}__{b}"] for a, b in pairs}
join_yield = {e: (present[e], stats[f"ew__{e}"]) for e in ENERGY}
key_unique = {d: table_rows.get(d, 0) == present[d] for d in DATASETS}
seven_ct = stats["all_seven"]

print(f"union keys={union_ct}  keys in all 7={seven_ct}")
for d in DATASETS:
    print(f"  {d:<16} present={present[d]:>10}  missing vs union={grid_missing[d]:>10}  "
          f"rows={table_rows.get(d)}  key_unique={key_unique[d]}")
for e in ENERGY:
    l, m = join_yield[e]
    print(f"  {e:<16} energy keys={l}  matched to weather={m}  ({m / l * 100:.1f}%)" if l else f"  {e}: no keys")

# COMMAND ----------

# DBTITLE 1,Verdict -- can the Honda datasets be combined?
print(f"(frequency, datetime_utc) unique in every table : {all(key_unique.values())}")
print(f"keys shared by all 7                            : {seven_ct} of {union_ct} "
      f"({seven_ct / union_ct * 100:.1f}%)")
print(f"energy<->weather match rate                     : "
      f"{ {e: round(m / l * 100, 1) for e, (l, m) in join_yield.items() if l} }")
print("=> shared 1:1 key exists; a wide 'all Honda metrics at (freq, ts)' table is "
      "feasible on the intersection but loses the non-overlapping tail; one fact per "
      "dataset (or per metric joining P+W), a wide table is Gold.")

# COMMAND ----------

# DBTITLE 1,Figure -- pairwise overlap heatmap
n = len(DATASETS)
m = np.zeros((n, n))
for i, a in enumerate(DATASETS):
    m[i, i] = present[a]
    for j, b in enumerate(DATASETS):
        if (a, b) in overlap:
            m[i, j] = m[j, i] = overlap[(a, b)]
plt.figure(figsize=(8, 7))
plt.imshow(np.log10(m + 1), cmap="viridis")
plt.colorbar(label="log10(shared keys + 1)")
plt.xticks(range(n), DATASETS, rotation=45, ha="right"); plt.yticks(range(n), DATASETS)
plt.title("Honda -- pairwise (frequency, datetime_utc) overlap"); plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Figure -- missing keys vs union, and energy<->weather join yield
barplot(list(grid_missing.items()),
        "Honda -- (frequency, datetime_utc) keys missing vs union", "dataset", "missing keys", rot=30)
x = np.arange(len(ENERGY))
plt.figure(figsize=(11, 4))
plt.bar(x - 0.2, [join_yield[e][0] for e in ENERGY], width=0.4, label="energy keys")
plt.bar(x + 0.2, [join_yield[e][1] for e in ENERGY], width=0.4, label="matched to weather")
plt.xticks(x, ENERGY, rotation=30, ha="right"); plt.legend()
plt.title("Honda -- energy<->weather join yield on (frequency, datetime_utc)"); plt.ylabel("keys")
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Findings
print("key unique per table       :", key_unique)
print("keys missing vs union      :", grid_missing)
print("energy<->weather join yield :", join_yield)
print("keys shared by all 7        :", seven_ct, "of", union_ct)
print("pairwise overlap            :", overlap)
