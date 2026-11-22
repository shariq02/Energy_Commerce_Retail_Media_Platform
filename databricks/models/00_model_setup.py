# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # MODEL SETUP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** create the model schema and every model table, record which
# MAGIC libraries the environment has, check that the dataset of every task is frozen and
# MAGIC readable, and record the evaluation, approval and monitoring settings before the
# MAGIC first model runs. Idempotent; the frozen dataset schemas are not touched.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ./lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model evaluation library
# MAGIC %run ./lib/_model_evaluate

# COMMAND ----------

# DBTITLE 1,Model governance library
# MAGIC %run ./lib/_model_govern

# COMMAND ----------

# DBTITLE 1,Model operate library
# MAGIC %run ./lib/_model_operate

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/00_model_setup"
# Datasets whose tasks name their columns explicitly instead of the feature contract.
EXPLICIT_COLUMNS = {"rl_pumped_storage", "weather_imputation"}
# Runners whose target is built at run time (no column in the dataset).
RUNTIME_TARGET_RUNNERS = ("anomaly", "reconstruction")
MASK_DATASETS = ("honda_anomaly", "weather_imputation")
ECOSYSTEM_SCHEMA = model_schema_for(ECOSYSTEMS[0])

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Create the model schema and the tables
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{ECOSYSTEM_SCHEMA}")
for name, ddl in MODEL_DDL.items():
    print(f"OK  model table ready: {create_model_table(name, ddl, ECOSYSTEMS[0])}")

# COMMAND ----------

# DBTITLE 1,Library availability in this environment
_libs = [
    (lib, library_version(lib), library_available(lib), now_utc()) for lib in LIBRARIES
]
replace_model_rows(
    spark.createDataFrame(_libs, MODEL_DDL["library_availability"]),
    "library_availability",
    ecosystem=ECOSYSTEMS[0],
    predicate="library IS NOT NULL",
)
for lib, version, ok, _ in _libs:
    print(f"{'OK  ' if ok else 'MISS'} {lib:12s} {version or '-'}")

# COMMAND ----------

# DBTITLE 1,The dataset of every task is frozen
_datasets = {(t["ecosystem"], t["dataset_id"]) for t in TASKS}
_datasets |= {(c["ecosystem"], c["dataset_id"]) for c in CHECK_ONLY.values()}
_frozen = {key: frozen_version(key[1], key[0]) for key in sorted(_datasets)}
guard(
    COMPONENT,
    rid,
    "all_datasets_frozen",
    [key for key, v in _frozen.items() if v is None],
    "not frozen",
)
print(f"OK  {len(_frozen)} datasets frozen")

# COMMAND ----------

# DBTITLE 1,Every dataset reads at its frozen version and carries its columns
_columns = {}
for t in TASKS:
    ctx = TaskContext(t["task_id"], rid, False, record=False)
    _columns[t["task_id"]] = dataset_at_version(ctx, t["fit"]).columns
_bad = []
for t in TASKS:
    need = (
        ["partition"]
        if t["runner"] in RUNTIME_TARGET_RUNNERS
        else ["partition", t["target"]]
    )
    _bad += [(t["task_id"], c) for c in need if c not in _columns[t["task_id"]]]
guard(COMPONENT, rid, "datasets_carry_partition_and_target", _bad, "missing columns")

# COMMAND ----------

# DBTITLE 1,Features resolve for every dataset that uses the feature contract
_empty = []
for t in TASKS:
    if t["dataset_id"] in EXPLICIT_COLUMNS:
        continue
    ctx = TaskContext(t["task_id"], rid, False, record=False)
    try:
        n = len(resolve_features(ctx, _columns[t["task_id"]]))
    except RuntimeError:
        n = 0
    print(f"  {t['task_id']:45s} {n} feature column(s)")
    if n == 0:
        _empty.append(t["task_id"])
guard(COMPONENT, rid, "features_resolve", _empty, "no features")

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
guard(
    COMPONENT,
    rid,
    "mask_specifications_present",
    [d for d in MASK_DATASETS if d not in _have],
    "missing",
)

# COMMAND ----------

# DBTITLE 1,Model schema headroom
_objects = spark.sql(f"SHOW TABLES IN {CATALOG}.{ECOSYSTEM_SCHEMA}").count()
print(f"{ECOSYSTEM_SCHEMA}: {_objects} object(s) of {SCHEMA_OBJECT_QUOTA}")
guard(
    COMPONENT,
    rid,
    "schema_headroom",
    [_objects] if _objects >= SCHEMA_OBJECT_WARN else [],
    "objects",
)

# COMMAND ----------

# DBTITLE 1,Evaluation settings recorded so far
_eval_recorded = {}
for eco in ECOSYSTEMS:
    rows = (
        read_model("evaluation_spec", ecosystem=eco)
        .filter(F.col("dataset_id") == EVAL_SPEC_DATASET)
        .collect()
    )
    _eval_recorded[eco] = {r["spec_key"]: r["spec_value"] for r in rows}
    print(f"{eco}: {len(rows)} evaluation setting(s) recorded")

# COMMAND ----------

# DBTITLE 1,Evaluation settings cannot change once the held-out partition was read
for eco, have in _eval_recorded.items():
    changed = [k for k, v in EVAL_SPEC_DEFAULTS.items() if have.get(k, v) != v]
    read_already = (
        read_model("evaluation_results", ecosystem=eco)
        .filter(~F.col("smoke") & (F.col("partition") == TEST_PARTITION))
        .limit(1)
        .count()
    )
    guard(
        COMPONENT,
        rid,
        f"evaluation_settings_unchanged_after_read:{eco}",
        changed if read_already else [],
        "settings differ from the defaults after a read",
    )

# COMMAND ----------

# DBTITLE 1,Record the evaluation settings
for eco, have in _eval_recorded.items():
    if have == EVAL_SPEC_DEFAULTS:
        print(f"OK  {eco}: evaluation settings already recorded, kept")
        continue
    rows = [
        (EVAL_SPEC_DATASET, key, value, rid, now_utc())
        for key, value in EVAL_SPEC_DEFAULTS.items()
    ]
    replace_model_rows(
        spark.createDataFrame(rows, MODEL_DDL["evaluation_spec"]),
        "evaluation_spec",
        ecosystem=eco,
        predicate=f"dataset_id = '{EVAL_SPEC_DATASET}'",
    )
    print(f"OK  {len(rows)} evaluation setting(s) recorded for {eco}")

# COMMAND ----------

# DBTITLE 1,Record the settings the model notebooks use
_spec = {
    "energy": {
        "survival": {"horizons_years": str(SURVIVAL_HORIZONS_YEARS)},
        "price_daily": {"quantiles": str(QUANTILES)},
        "price_quarter_hour": {"quantiles": str(QUANTILES)},
        "honda_anomaly": {"injection": "mask_specification", "flag_quantile": "0.99"},
        "weather_imputation": {"masks": "mask_specification"},
    },
    "commerce": {
        "next_item_rees46": {
            "user_sample_percent": str(USER_SAMPLE_PERCENT),
            "sample_seed": USER_SAMPLE_SEED,
            "rank_ks": str(RANK_KS),
        },
        "rl_session_sequences": {
            "user_sample_percent": str(USER_SAMPLE_PERCENT),
            "sample_seed": USER_SAMPLE_SEED,
        },
        "next_item_ga4": {"rank_ks": str(RANK_KS)},
    },
    "genomics": {
        name: {
            "tuning_folds": str(TUNE_HASH_FOLDS),
            "tuning_rule": "hash of " + ", ".join(task["fit"]["tuning_hash"]),
        }
        for task in TASKS
        if task["fit"].get("tuning_hash")
        for name in (task["dataset_id"],)
    }
    | {
        name: {"scored_by": entry["scored_by"], "use": "disclosure only"}
        for name, entry in CHECK_ONLY.items()
    },
}
for eco, datasets in _spec.items():
    rows = [
        (ds, key, value, rid, now_utc())
        for ds, items in datasets.items()
        for key, value in items.items()
    ]
    replace_model_rows(
        spark.createDataFrame(rows, MODEL_DDL["evaluation_spec"]),
        "evaluation_spec",
        ecosystem=eco,
        predicate=f"dataset_id <> '{EVAL_SPEC_DATASET}'",
    )
    print(f"OK  {len(rows)} specification row(s) for {eco}")

# COMMAND ----------

# DBTITLE 1,Approval settings recorded so far
_approval_recorded = {}
for eco in ECOSYSTEMS:
    rows = read_model("approval_spec", ecosystem=eco).collect()
    _approval_recorded[eco] = {
        (r["rule_id"], r["parameter"]): r["parameter_value"] for r in rows
    }
    print(f"{eco}: {len(rows)} approval setting row(s) recorded")

# COMMAND ----------

# DBTITLE 1,Approval settings cannot change once a recommendation exists
_wanted_rules = {(r, p): v for r, _c, _d, p, v in APPROVAL_RULES}
for eco, have in _approval_recorded.items():
    changed = [k for k, v in _wanted_rules.items() if k in have and have[k] != v]
    started = read_model("model_approval", ecosystem=eco).limit(1).count()
    guard(
        COMPONENT,
        rid,
        f"approval_settings_unchanged_after_first_recommendation:{eco}",
        changed if started else [],
        "settings differ from the defaults after a recommendation",
    )

# COMMAND ----------

# DBTITLE 1,Record the approval rule table
for eco, have in _approval_recorded.items():
    if have == _wanted_rules:
        print(f"OK  {eco}: rule table already recorded, kept")
        continue
    rows = [(r, c, d, p, v, rid, now_utc()) for r, c, d, p, v in APPROVAL_RULES]
    replace_model_rows(
        spark.createDataFrame(rows, MODEL_DDL["approval_spec"]),
        "approval_spec",
        ecosystem=eco,
        predicate="rule_id IS NOT NULL",
    )
    print(f"OK  {len(rows)} approval setting row(s) recorded for {eco}")

# COMMAND ----------

# DBTITLE 1,Monitoring settings recorded so far
_monitor_recorded = {}
for eco in ECOSYSTEMS:
    rows = read_model("monitoring_spec", ecosystem=eco).collect()
    _monitor_recorded[eco] = {r["parameter"]: r["parameter_value"] for r in rows}
    print(f"{eco}: {len(rows)} monitoring setting row(s) recorded")

# COMMAND ----------

# DBTITLE 1,Monitoring settings cannot change once a monitoring result exists
_wanted_monitoring = {p: v for p, v, _d in OPERATE_DEFAULTS}
for eco, have in _monitor_recorded.items():
    changed = [k for k, v in _wanted_monitoring.items() if k in have and have[k] != v]
    started = read_model("monitoring_results", ecosystem=eco).limit(1).count()
    guard(
        COMPONENT,
        rid,
        f"monitoring_settings_unchanged_after_first_monitoring:{eco}",
        changed if started else [],
        "settings differ from the defaults after monitoring started",
    )

# COMMAND ----------

# DBTITLE 1,Record the monitoring settings
for eco, have in _monitor_recorded.items():
    if have == _wanted_monitoring:
        print(f"OK  {eco}: monitoring settings already recorded, kept")
        continue
    now = now_utc()
    rows = [
        {
            "parameter": p,
            "parameter_value": v,
            "description": d,
            "run_id": rid,
            "recorded_at": now,
        }
        for p, v, d in OPERATE_DEFAULTS
    ]
    replace_rows(rows, "monitoring_spec", eco, "parameter IS NOT NULL")
    print(f"OK  {len(rows)} monitoring setting row(s) recorded for {eco}")

# COMMAND ----------

# DBTITLE 1,Remove the rows of models that are not registered
for eco in ECOSYSTEMS:
    removed = remove_rows_without_entry(eco)
    print(f"{eco}: rows of {len(removed)} model(s) removed")

# COMMAND ----------

# DBTITLE 1,The recorded settings load
for eco in ECOSYSTEMS:
    print(eco, read_thresholds(eco))
    print(eco, spec_from_rows(read_model("approval_spec", ecosystem=eco).collect()))
    print(
        eco,
        operate_spec_from_rows(
            [r.asDict() for r in read_model("monitoring_spec", ecosystem=eco).collect()]
        ),
    )

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_model_findings(
    ECOSYSTEMS,
    "00_model_setup",
    "Model Setup",
    ["evaluation_spec", "approval_spec", "monitoring_spec"],
    rid,
)
