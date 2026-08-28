# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- IPINYOU REFERENCE
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Profile ipinyou_reference (the merged city / region /
# MAGIC user_profile_tags lookup, discriminated by lookup_type) -- schema,
# MAGIC per-type row counts, lookup_id uniqueness & contiguity within type,
# MAGIC name_en / name_cn completeness -- as evidence for Silver design and the
# MAGIC referential-integrity checks in notebook 03. This table is small, so
# MAGIC it is collected once and analysed in Python.

# COMMAND ----------

# DBTITLE 1,Imports
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.ipinyou_reference"

# COMMAND ----------


# DBTITLE 1,Helper
def barplot(pairs, title, xlabel, ylabel="rows", rot=0):
    plt.figure(figsize=(9, 4))
    plt.bar([str(p[0]) for p in pairs], [p[1] for p in pairs])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=rot, ha="right" if rot else "center")
    plt.tight_layout()
    plt.show()


# COMMAND ----------

# DBTITLE 1,Schema, row count, missingness, approx distinct, constant columns (one pass)
df = spark.table(TABLE)
COLS = df.columns
exprs = [F.count(F.lit(1)).alias("__rows")]
for c in COLS:
    miss = F.col(c).isNull() | (F.trim(F.col(c)) == "")
    exprs += [
        F.sum(miss.cast("long")).alias(c + "__m"),
        F.approx_count_distinct(c).alias(c + "__d"),
    ]
r = df.agg(*exprs).first().asDict()
total = r["__rows"]
constant_cols = [c for c in COLS if r[c + "__d"] <= 1]
print(f"rows={total}  columns={len(COLS)}  ->  {COLS}")
for c in COLS:
    print(
        f"  {c:<14} missing={r[c + '__m']:>8} rate={r[c + '__m'] / total:.4f} approx_distinct={r[c + '__d']}"
    )
print("constant columns:", constant_cols)
df.show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,Collect the full table (small) for row-level analysis
rows = df.select(*COLS).collect()
recs = [x.asDict() for x in rows]
full_row_dups = total - len({tuple(sorted(d.items())) for d in recs})
print("full-row duplicates:", full_row_dups)

by_type = {}
for d in recs:
    by_type.setdefault(d["lookup_type"], []).append(d)
type_rows = sorted(((lt, len(v)) for lt, v in by_type.items()), key=lambda p: -p[1])
print("rows per lookup_type:", type_rows)

# COMMAND ----------

# DBTITLE 1,lookup_id uniqueness, id range and contiguity per lookup_type
uniq = {}
for lt, v in by_type.items():
    ids = sorted(int(d["lookup_id"]) for d in v if d["lookup_id"] not in (None, ""))
    dup_ids = len(ids) - len(set(ids))
    gaps = sorted(set(range(ids[0], ids[-1] + 1)) - set(ids)) if ids else []
    uniq[lt] = {
        "n": len(ids),
        "min": ids[0] if ids else None,
        "max": ids[-1] if ids else None,
        "dup_ids": dup_ids,
        "gaps_in_range": len(gaps),
        "gap_examples": gaps[:20],
    }
    print(f"{lt}: {uniq[lt]}")

# COMMAND ----------

# DBTITLE 1,name_en / name_cn completeness, distinct names, placeholder tokens per type
PLACEHOLDERS = {"unknown", "other", "others", "n/a", "na", "null", "未知", "其他"}
name_missing = []
for lt, v in by_type.items():
    en_missing = sum(1 for d in v if not (d["name_en"] or "").strip())
    cn_missing = sum(1 for d in v if not (d["name_cn"] or "").strip())
    both = sum(
        1 for d in v if (d["name_en"] or "").strip() and (d["name_cn"] or "").strip()
    )
    ph = sum(
        1
        for d in v
        if (d["name_en"] or "").strip().lower() in PLACEHOLDERS
        or (d["name_cn"] or "").strip() in PLACEHOLDERS
    )
    name_missing.append((lt, en_missing, cn_missing, len(v)))
    print(
        f"{lt}: name_en_missing={en_missing} name_cn_missing={cn_missing} both_present={both} "
        f"placeholder_like={ph} distinct_en={len({d['name_en'] for d in v})} "
        f"distinct_cn={len({d['name_cn'] for d in v})}"
    )
both_present = sum(
    1 for d in recs if (d["name_en"] or "").strip() and (d["name_cn"] or "").strip()
)

# COMMAND ----------

# DBTITLE 1,Extra columns beyond the expected 4 (staging superset artefacts)
extra = [c for c in COLS if c not in {"lookup_type", "lookup_id", "name_en", "name_cn"}]
print("extra columns:", extra)
for c in extra:
    acc = {}
    for d in recs:
        acc.setdefault(d["lookup_type"], {})
        acc[d["lookup_type"]][d[c]] = acc[d["lookup_type"]].get(d[c], 0) + 1
    print(f"  {c}:", acc)

# COMMAND ----------

# DBTITLE 1,Figure -- rows per lookup_type + name missingness
barplot(type_rows, "iPinYou reference -- rows per lookup_type", "lookup_type", "rows")
types = [n[0] for n in name_missing]
x = np.arange(len(types))
plt.figure(figsize=(9, 4))
plt.bar(
    x - 0.2,
    [n[1] / n[3] for n in name_missing],
    width=0.4,
    label="name_en missing rate",
)
plt.bar(
    x + 0.2,
    [n[2] / n[3] for n in name_missing],
    width=0.4,
    label="name_cn missing rate",
)
plt.xticks(x, types)
plt.legend()
plt.title("iPinYou reference -- name missingness by lookup_type")
plt.ylabel("rate")
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Findings
print("constant columns:", constant_cols)
print("rows per lookup_type:", type_rows)
print("lookup_id uniqueness / contiguity:", uniq)
print("rows with both names present:", both_present, "/", total)
print("full-row duplicates:", full_row_dups)
