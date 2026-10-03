# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EVALUATION SPECIFICATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** record the evaluation settings the model notebooks use (user sample,
# MAGIC split fractions, horizons, quantiles, ranking cut-offs, minimum tier rows) in
# MAGIC `evaluation_spec`, so a result can be traced to the settings it was produced with.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Specification rows per ecosystem
_spec = {
    "energy": {
        "weak_supervision": {
            "split_percent": str(WEAK_SPLIT_PERCENT),
            "group": "place for series entities, unit for unit entities",
            "stratum": "entity_type",
            "min_series_entities": str(MIN_SERIES_ENTITIES),
        },
        "redispatch_matching": {
            "split_percent": str(MATCHING_SPLIT_PERCENT),
            "group": "normalised asset identity",
            "stratum": "match tier",
            "min_tier_rows": str(MIN_TIER_ROWS),
        },
        "survival": {"horizons_years": str(SURVIVAL_HORIZONS_YEARS)},
        "price_daily": {"quantiles": str(QUANTILES)},
        "price_quarter_hour": {"quantiles": str(QUANTILES)},
        "honda_anomaly": {"injection": "mask_specification", "flag_quantile": "0.99"},
        "weather_imputation": {"masks": "mask_specification"},
        "self_supervised_other": {"masks": "mask_specification"},
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
}

# COMMAND ----------

# DBTITLE 1,Write the specification
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
        predicate="dataset_id <> 'evaluation'",
    )
    print(f"OK  {len(rows)} specification row(s) for {eco}")
