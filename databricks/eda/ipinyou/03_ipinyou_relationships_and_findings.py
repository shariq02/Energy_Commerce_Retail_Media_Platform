# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- IPINYOU RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Cross-table checks across the 3 iPinYou Bronze tables --
# MAGIC training vs leaderboard schema/overlap, region & city referential
# MAGIC integrity against ipinyou_reference, user_tags vs the tag lookup -- and
# MAGIC a findings summary for src/schemas/profiling/ipinyou.md.

# COMMAND ----------

# DBTITLE 1,Imports
import matplotlib.pyplot as plt
import numpy as np

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
training = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.ipinyou_training")
leaderboard = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.ipinyou_leaderboard")
reference = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.ipinyou_reference")
FACTS = {"training": training, "leaderboard": leaderboard}

# COMMAND ----------

# DBTITLE 1,Helper
def barplot(pairs, title, xlabel, ylabel="count", rot=0):
    plt.figure(figsize=(9, 4))
    plt.bar([str(p[0]) for p in pairs], [p[1] for p in pairs])
    plt.title(title); plt.xlabel(xlabel); plt.ylabel(ylabel)
    plt.xticks(rotation=rot, ha="right" if rot else "center")
    plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Schema difference
t_cols, l_cols = set(training.columns), set(leaderboard.columns)
print("training-only columns  :", sorted(t_cols - l_cols))
print("leaderboard-only columns:", sorted(l_cols - t_cols))
print("shared columns          :", sorted(t_cols & l_cols))

# COMMAND ----------

# DBTITLE 1,Reference id sets by lookup_type (collect small table once)
ref_ids = {"region": set(), "city": set(), "tag": set()}
for x in reference.select("lookup_type", F.col("lookup_id").cast("string").alias("id")).collect():
    if x["lookup_type"] in ref_ids and x["id"] is not None:
        ref_ids[x["lookup_type"]].add(x["id"])
print({k: len(v) for k, v in ref_ids.items()})

# COMMAND ----------

# DBTITLE 1,(season, bid_id) key overlap -- exact (key relationship check)
t_keys = training.select("season", "bid_id").distinct()
l_keys = leaderboard.select("season", "bid_id").distinct()
t_ct, l_ct = t_keys.count(), l_keys.count()
overlap = t_keys.join(l_keys, on=["season", "bid_id"], how="inner").count()
seasons = {n: sorted(x["season"] for x in df.select("season").distinct().collect()) for n, df in FACTS.items()}
print(f"training keys={t_ct}  leaderboard keys={l_ct}  overlap={overlap}")
print("seasons:", seasons)

# COMMAND ----------

# DBTITLE 1,Entity value sets per fact (collect_set -- one pass per fact)
entity_sets = {}
for name, df in FACTS.items():
    row = df.agg(
        F.collect_set(F.col("region").cast("string")).alias("region"),
        F.collect_set(F.col("city").cast("string")).alias("city"),
        F.collect_set(F.col("advertiser_id").cast("string")).alias("advertiser_id"),
    ).first()
    tags = df.select(F.explode(F.split(F.col("user_tags"), r"[,;\s]+")).alias("t")) \
             .where(F.trim(F.col("t")) != "") \
             .agg(F.collect_set(F.trim(F.col("t"))).alias("tag")).first()["tag"]
    entity_sets[name] = {"region": set(row["region"]), "city": set(row["city"]),
                         "advertiser_id": set(row["advertiser_id"]), "tag": set(tags or [])}
    print(name, {k: len(v) for k, v in entity_sets[name].items()})

# COMMAND ----------

# DBTITLE 1,Referential integrity: distinct-id coverage vs ipinyou_reference (Python set math)
fk = {}
for name in FACTS:
    fk[name] = {}
    for dim, ref_key in (("region", "region"), ("city", "city"), ("tag", "tag")):
        used = entity_sets[name][dim]
        unmatched = used - ref_ids[ref_key]
        fk[name][f"{dim}_used"] = len(used)
        fk[name][f"{dim}_unmatched"] = len(unmatched)
        print(f"{name}.{dim}: used={len(used)} unmatched={len(unmatched)} e.g. {sorted(unmatched)[:20]}")

# COMMAND ----------

# DBTITLE 1,Row-level FK coverage: share of fact ROWS matching a reference id (one pass per fact)
row_cov = {}
for name, df in FACTS.items():
    region_ok = F.col("region").cast("string").isin(list(ref_ids["region"]))
    city_ok = F.col("city").cast("string").isin(list(ref_ids["city"]))
    a = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(region_ok.cast("long")).alias("region_match"),
        F.sum(city_ok.cast("long")).alias("city_match"),
    ).first().asDict()
    row_cov[name] = {"rows": a["rows"],
                     "region_match_rate": round(a["region_match"] / a["rows"], 4),
                     "city_match_rate": round(a["city_match"] / a["rows"], 4)}
    print(f"{name}: {row_cov[name]}")

# COMMAND ----------

# DBTITLE 1,Join cardinality -- training.region / training.city -> reference (expect N:1)
for dim in ("region", "city"):
    s = (
        training.groupBy(F.col(dim).cast("string").alias(dim)).count()
        .agg(F.min("count").alias("min"), F.max("count").alias("max"), F.avg("count").alias("avg"),
             F.count(F.lit(1)).alias("distinct_values")).first().asDict()
    )
    print(f"training.{dim} -> ipinyou_reference.{dim}: {'N:1' if s['max'] > 1 else '1:1'}  {s}")

# COMMAND ----------

# DBTITLE 1,Advertiser overlap training vs leaderboard (Python set math)
t_adv, l_adv = entity_sets["training"]["advertiser_id"], entity_sets["leaderboard"]["advertiser_id"]
adv_overlap = len(t_adv & l_adv)
print(f"training advertisers={len(t_adv)}  leaderboard advertisers={len(l_adv)}  shared={adv_overlap}")
print("training-only:", sorted(t_adv - l_adv)[:20])
print("leaderboard-only:", sorted(l_adv - t_adv)[:20])

# COMMAND ----------

# DBTITLE 1,Verdict -- union training + leaderboard?
print("training-only columns   :", sorted(t_cols - l_cols))
print("leaderboard-only columns :", sorted(l_cols - t_cols))
print(f"(season, bid_id) overlap : {overlap} of {t_ct} / {l_ct}")
print(f"advertiser overlap       : {adv_overlap} of {len(t_adv)} / {len(l_adv)}")
print("seasons:", seasons)
print("=> shared 20+ columns and the same event grain, different label columns "
      "(event_type vs has_conversion / related_clicks_count), near-zero key overlap "
      "-> two separate facts sharing conformed dimensions.")

# COMMAND ----------

# DBTITLE 1,Figure -- key overlap + FK unmatched rates
barplot([("training keys", t_ct), ("leaderboard keys", l_ct), ("overlap", overlap)],
        "iPinYou -- (season, bid_id) key overlap", "", "keys", rot=20)
dims = ["region", "city", "tag"]
x = np.arange(len(dims))
plt.figure(figsize=(10, 4))
for i, name in enumerate(FACTS):
    plt.bar(x + i * 0.35,
            [fk[name][f"{d}_unmatched"] / max(fk[name][f"{d}_used"], 1) for d in dims],
            width=0.35, label=name)
plt.xticks(x + 0.17, dims); plt.legend()
plt.title("iPinYou -- share of id values not found in ipinyou_reference"); plt.ylabel("unmatched rate")
plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Findings
print("schema delta training-only :", sorted(t_cols - l_cols))
print("schema delta leaderboard-only:", sorted(l_cols - t_cols))
print("(season, bid_id) overlap    :", overlap)
print("advertiser overlap          :", adv_overlap, "of", len(t_adv), "/", len(l_adv))
print("distinct-id FK              :", fk)
print("row-level FK match rate     :", row_cov)
