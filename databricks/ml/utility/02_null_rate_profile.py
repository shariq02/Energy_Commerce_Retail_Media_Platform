# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # NULL RATE PROFILE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** profile the null rate of every candidate column on the training partition
# MAGIC of each assembled dataset and record the keep, indicator or drop decision.
# MAGIC Run after the split manifests exist. Run by the owner.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECOSYSTEMS = ("energy", "commerce")
SOURCE = "ml"
COMPONENT = "ml/utility/null_rate_profile"
NON_FEATURE = {
    "dataset_id",
    "dataset_version",
    "ecosystem",
    "_ml_loaded_at",
    "_ml_run_id",
    "partition",
    "fold_id",
    "group_key",
}

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Profile every assembled dataset
_summary = []
for eco in ECOSYSTEMS:
    datasets = (
        read_ml("dataset_manifest", ecosystem=eco)
        .filter(
            F.col("status").isin("assembled", "fitted", "frozen")
            & F.col("grain_key").isNotNull()
        )
        .collect()
    )
    for d in datasets:
        view = f"assembled_{d['dataset_id']}"
        if not spark.catalog.tableExists(ml_fqn(view, eco)):
            print(f"SKIP {d['dataset_id']}: no assembled view")
            continue
        manifest = read_partition_manifest(d["dataset_id"], eco)
        if manifest.limit(1).count() == 0:
            print(f"SKIP {d['dataset_id']}: no partition manifest yet")
            continue
        df = attach_partition(
            read_ml(view, ecosystem=eco), d["dataset_id"], eco, list(d["grain_key"])
        )
        train = df.filter(F.col("partition") == "train")
        cols = [c for c in train.columns if c not in NON_FEATURE]
        rates = null_rates(train, cols)
        exempt = {
            r["column_name"]
            for r in read_ml("null_class_registry", ecosystem=eco)
            .filter(
                (F.col("dataset_id") == d["dataset_id"])
                & F.col("null_class").isin("D", "F")
            )
            .collect()
        }
        drop = set(columns_to_drop(rates, exempt=exempt))
        rows = []
        for c, r in rates.items():
            if c in exempt:
                decision = "exempt_missingness_is_signal"
            elif c in drop:
                decision = "drop"
            elif r >= 0.5:
                decision = "keep_indicator_impute_if_beats_baseline"
            else:
                decision = "keep"
            rows.append((d["dataset_id"], c, float(r), decision, now_utc()))
        out = spark.createDataFrame(rows, REGISTRY_DDL["null_rate_profile"])
        replace_rows(
            out,
            "null_rate_profile",
            ecosystem=eco,
            predicate=f"dataset_id = '{d['dataset_id']}'",
        )
        _summary.append((d["dataset_id"], len(rows), len(drop)))

# COMMAND ----------

# DBTITLE 1,Summary
for ds, n, dropped in _summary:
    print(f"{ds:32s} columns={n:>4} drop={dropped:>3}")
