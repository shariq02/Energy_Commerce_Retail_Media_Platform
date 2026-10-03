# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # WEAK SUPERVISION SPLIT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** train, validation and test partitions for the weak supervision labels,
# MAGIC grouped by place (series entities) or unit (unit entities) and stratified by
# MAGIC entity type. Written to the evaluation split manifest; the frozen dataset is
# MAGIC unchanged.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
DATASET_ID = "weak_supervision"
COMPONENT = "models/split/01_weak_supervision_split"
SOURCE = "models"
KEYS = ["entity_type", "entity_id", "lf_name"]
RULE_ID = "group_hash:place_or_unit:70/15/15"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()
ctx = TaskContext("weak_supervision.labels", rid, False, record=False)

# COMMAND ----------

# DBTITLE 1,Read the frozen dataset
df = read_frozen(ctx, apply_smoke=False)

# COMMAND ----------

# DBTITLE 1,Group key per entity -- place for series, unit for unit entities
grouped = df.withColumn(
    "group_key",
    F.when(
        F.col("entity_type") == "series",
        F.concat(F.lit("series:"), F.split(F.col("entity_id"), r"\|").getItem(0)),
    ).otherwise(F.concat(F.lit("unit:"), F.col("entity_id"))),
)

# COMMAND ----------

# DBTITLE 1,Distinct groups and their entity type
_groups = [
    (r["group_key"], r["entity_type"])
    for r in grouped.select("group_key", "entity_type").distinct().collect()
]
print(f"{len(_groups)} group(s) across {len({s for _, s in _groups})} entity type(s)")

# COMMAND ----------

# DBTITLE 1,Assign each group to one partition
_assigned = assign_group_partitions(_groups, WEAK_SPLIT_PERCENT)
_assignment = spark.createDataFrame(
    list(_assigned.items()), "group_key string, assigned_partition string"
)

# COMMAND ----------

# DBTITLE 1,Manifest rows at the dataset grain
manifest = grouped.join(_assignment, "group_key", "inner").select(
    F.lit(DATASET_ID).alias("dataset_id"),
    F.lit(int(ctx.frozen_version)).cast("bigint").alias("frozen_delta_version"),
    grain_key(*KEYS).alias("grain_key"),
    F.col("assigned_partition").alias("partition"),
    F.lit(None).cast("int").alias("fold_id"),
    F.col("group_key"),
    F.col("entity_type").alias("stratum"),
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

# DBTITLE 1,Both entity types in every partition with enough series
_counts = {
    (r["stratum"], r["partition"]): r["n"]
    for r in _written.groupBy("stratum", "partition")
    .agg(F.countDistinct("group_key").alias("n"))
    .collect()
}
print(_counts)
_short = [
    (s, p)
    for s in ("series", "unit")
    for p in ("train", "validation", "test")
    if _counts.get((s, p), 0) == 0
]
check(
    COMPONENT,
    SOURCE,
    "entity_types_in_every_partition",
    not _short,
    detail=f"missing: {_short}",
    metric_value=float(len(_short)),
    rid=rid,
)
_series = (
    grouped.filter(F.col("entity_type") == "series")
    .join(_assignment, "group_key")
    .groupBy("assigned_partition")
    .agg(F.countDistinct("entity_id").alias("entities"))
    .collect()
)
_thin = [
    r["assigned_partition"] for r in _series if r["entities"] < MIN_SERIES_ENTITIES
]
check(
    COMPONENT,
    SOURCE,
    "minimum_series_entities",
    not _thin,
    detail=f"partitions under {MIN_SERIES_ENTITIES} series entities: {_thin}",
    metric_value=float(len(_thin)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Label distribution per partition
grouped.join(_assignment, "group_key").groupBy(
    "entity_type", "assigned_partition", "lf_name", "lf_label"
).count().orderBy("entity_type", "assigned_partition", "lf_name", "lf_label").show(
    60, truncate=False
)
