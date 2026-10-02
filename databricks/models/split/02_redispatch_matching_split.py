# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # REDISPATCH MATCHING SPLIT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** train, validation and test partitions for the redispatch asset
# MAGIC matching tier, grouped by normalised asset identity (the matched unit name where
# MAGIC there is one, else the normalised text) and stratified by match tier. Written to
# MAGIC the evaluation split manifest; the frozen dataset is unchanged.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../_model_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
DATASET_ID = "redispatch_matching"
COMPONENT = "models/split/02_redispatch_matching_split"
SOURCE = "models"
RULE_ID = "group_hash:normalised_asset_identity:60/20/20"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()
ctx = TaskContext("redispatch_matching.tier", rid, False, record=False)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx, apply_smoke=False)

# COMMAND ----------

# DBTITLE 1,Matched unit name per asset text (used to build groups only)
_matched = (
    read_ml("target_redispatch_match", ecosystem=ECO)
    .select("affected_asset_text", "matched_unit_name")
    .dropDuplicates(["affected_asset_text"])
)
_rows = (
    df.select("affected_asset_text", "target_match_tier")
    .join(_matched, "affected_asset_text", "left")
    .collect()
)
print(f"{len(_rows)} asset text(s)")

# COMMAND ----------

# DBTITLE 1,Group key and stratum per asset text
_group_of, _tiers = {}, {}
for r in _rows:
    text, tier, name = (
        r["affected_asset_text"],
        r["target_match_tier"],
        r["matched_unit_name"],
    )
    group = (
        f"unit:{normalise_asset_text(name)}"
        if name
        else f"text:{normalise_asset_text(text)}"
    )
    _group_of[text] = group
    _tiers.setdefault(group, []).append(str(tier))
_stratum = {g: max(set(t), key=t.count) for g, t in _tiers.items()}
print(f"{len(_stratum)} group(s), tiers: {sorted(set(_stratum.values()))}")

# COMMAND ----------

# DBTITLE 1,Assign each group to one partition
_assigned = assign_group_partitions(list(_stratum.items()), MATCHING_SPLIT_PERCENT)
_assignment = spark.createDataFrame(
    [(t, g, _assigned[g], _stratum[g]) for t, g in _group_of.items()],
    "affected_asset_text string, group_key string, partition string, stratum string",
)

# COMMAND ----------

# DBTITLE 1,Manifest rows at the dataset grain
manifest = _assignment.select(
    F.lit(DATASET_ID).alias("dataset_id"),
    F.lit(int(ctx.frozen_version)).cast("bigint").alias("frozen_delta_version"),
    grain_key("affected_asset_text").alias("grain_key"),
    F.col("partition"),
    F.lit(None).cast("int").alias("fold_id"),
    F.col("group_key"),
    F.col("stratum"),
    F.lit(RULE_ID).alias("rule_id"),
    F.lit(MODEL_SPLIT_VERSION).alias("split_version"),
)

# COMMAND ----------

# DBTITLE 1,Write the manifest
replace_model_rows(
    manifest,
    "evaluation_split_manifest",
    ecosystem=ECO,
    predicate=f"dataset_id = '{DATASET_ID}'",
)

# COMMAND ----------

# DBTITLE 1,Every dataset row has a partition
_written = read_model("evaluation_split_manifest", ecosystem=ECO).filter(
    F.col("dataset_id") == DATASET_ID
)
check(
    COMPONENT,
    SOURCE,
    "every_row_has_a_partition",
    _written.count() == df.count(),
    detail="manifest rows equal dataset rows",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,No group in two partitions
_split_groups = (
    _written.groupBy("group_key")
    .agg(F.countDistinct("partition").alias("n"))
    .filter(F.col("n") > 1)
    .count()
)
check(
    COMPONENT,
    SOURCE,
    "groups_in_one_partition",
    _split_groups == 0,
    detail=f"groups in several partitions: {_split_groups}",
    metric_value=float(_split_groups),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Rows per tier and partition; tiers under the minimum are not estimable
_per = _written.groupBy("stratum", "partition").count().orderBy("stratum", "partition")
_per.show(50, truncate=False)
for r in _per.collect():
    if r["partition"] != "train" and r["count"] < MIN_TIER_ROWS:
        print(
            f"NOT ESTIMABLE  tier={r['stratum']} partition={r['partition']} rows={r['count']}"
        )
