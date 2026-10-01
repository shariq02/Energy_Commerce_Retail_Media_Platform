# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL PREFLIGHT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** before any model runs, record which libraries the environment really
# MAGIC has and check that every task's dataset is frozen, readable at its frozen version,
# MAGIC carries its target and resolves features. Missing libraries are reported, not
# MAGIC failed: candidates that need them are recorded as skipped when they run.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ./_model_common

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/00_model_preflight"
SOURCE = "models"
# Datasets whose notebooks name their columns explicitly or carry no feature table.
EXPLICIT_COLUMNS = {
    "weak_supervision",
    "rl_pumped_storage",
    "weather_imputation",
    "self_supervised_other",
}
# Targets that are built at run time (no column in the dataset).
RUNTIME_TARGETS = {
    "honda_anomaly.anomaly",
    "weather_imputation.reconstruction",
    "self_supervised_other.reconstruction",
}
MASK_DATASETS = ("honda_anomaly", "weather_imputation", "self_supervised_other")

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Library availability in this environment
_libs = [
    (lib, library_version(lib), library_available(lib), now_utc()) for lib in LIBRARIES
]
for eco in ("energy", "commerce"):
    replace_model_rows(
        spark.createDataFrame(_libs, MODEL_DDL["library_availability"]),
        "library_availability",
        ecosystem=eco,
        predicate="library IS NOT NULL",
    )
for lib, version, ok, _ in _libs:
    print(f"{'OK  ' if ok else 'MISS'} {lib:12s} {version or '-'}")

# COMMAND ----------

# DBTITLE 1,Every task's dataset is frozen
_frozen = {}
for t in TASKS:
    key = (t["ecosystem"], t["dataset_id"])
    if key not in _frozen:
        _frozen[key] = frozen_version(t["dataset_id"], t["ecosystem"])
check(
    COMPONENT,
    SOURCE,
    "all_datasets_frozen",
    len(_frozen) == 24,
    detail=f"frozen datasets: {len(_frozen)}",
    metric_value=float(len(_frozen)),
    rid=rid,
)
print(f"OK  {len(_frozen)} datasets frozen")

# COMMAND ----------

# DBTITLE 1,Every dataset reads at its frozen version and carries its columns
_columns = {}
for (eco, ds), v in _frozen.items():
    full = ml_fqn(f"dataset_{ds}", eco)
    _columns[(eco, ds)] = spark.sql(
        f"SELECT * FROM {full} VERSION AS OF {v} LIMIT 1"
    ).columns
_bad = []
for t in TASKS:
    cols = _columns[(t["ecosystem"], t["dataset_id"])]
    need = (
        ["partition"] if t["task_id"] in RUNTIME_TARGETS else ["partition", t["target"]]
    )
    _bad += [(t["task_id"], c) for c in need if c not in cols]
check(
    COMPONENT,
    SOURCE,
    "datasets_carry_partition_and_target",
    not _bad,
    detail=f"missing columns: {_bad}",
    metric_value=float(len(_bad)),
    rid=rid,
)
print("OK  partition and target columns present")

# COMMAND ----------

# DBTITLE 1,Features resolve for every dataset that uses the feature contract
_empty = []
for t in TASKS:
    if t["dataset_id"] in EXPLICIT_COLUMNS:
        continue
    ctx = TaskContext(t["task_id"], rid, False)
    try:
        n = len(resolve_features(ctx, _columns[(t["ecosystem"], t["dataset_id"])]))
    except RuntimeError:
        n = 0
    print(f"  {t['task_id']:45s} {n} feature column(s)")
    if n == 0:
        _empty.append(t["task_id"])
check(
    COMPONENT,
    SOURCE,
    "features_resolve",
    not _empty,
    detail=f"no features: {_empty}",
    metric_value=float(len(_empty)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Mask specifications exist for the reconstruction and anomaly datasets
_have = {
    r["dataset_id"]
    for r in read_ml("mask_specification", ecosystem="energy")
    .filter(F.col("split_version") == SPLIT_VERSION)
    .select("dataset_id")
    .distinct()
    .collect()
}
_missing = [d for d in MASK_DATASETS if d not in _have]
check(
    COMPONENT,
    SOURCE,
    "mask_specifications_present",
    not _missing,
    detail=f"missing: {_missing}",
    metric_value=float(len(_missing)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Model schema headroom
for eco in ("energy", "commerce"):
    n = spark.sql(f"SHOW TABLES IN {CATALOG}.{model_schema_for(eco)}").count()
    print(f"{model_schema_for(eco)}: {n} object(s) of {SCHEMA_OBJECT_QUOTA}")
    check(
        COMPONENT,
        SOURCE,
        f"schema_headroom:{eco}",
        n < SCHEMA_OBJECT_WARN,
        detail=f"{n} objects",
        metric_value=float(n),
        rid=rid,
    )

# COMMAND ----------

# DBTITLE 1,Candidates that would be skipped in this environment
_available = {lib: ok for lib, _, ok, _ in _libs}
_needs = {
    "sklearn": "ridge, logistic, sklearn boosted trees, isolation forest, MLP, matching",
    "lightgbm": "lightgbm boosted trees and quantile models",
    "xgboost": "boosted Cox",
    "lifelines": "Cox proportional hazards",
    "sksurv": "random survival forest",
    "torch": "sequence, transformer, autoencoder, masked sequence, Q-learning, GRU policies",
    "mlflow": "run logging and model artifacts",
    "cloudpickle": "model artifact serialisation",
}
for lib, what in _needs.items():
    if not _available.get(lib):
        print(f"WARN {lib} missing -> skipped candidates: {what}")
print("preflight complete")
