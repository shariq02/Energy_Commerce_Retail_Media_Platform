# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SPLIT SPECIFICATION (ENERGY)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** write the split specification row of every energy dataset: family, rule,
# MAGIC calendar, embargo, hash seed and fold scheme.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "ml"
COMPONENT = "ml/energy/split/split_specification"
TABLE = "split_specification"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------


# DBTITLE 1,Helper -- calendar columns of one split calendar
def _cal(key):
    c = SPLIT_CALENDARS[key]
    return (
        c["train"][0],
        c["train"][1],
        c["validation"][0],
        c["validation"][1],
        c["test"][0],
        c["test"][1],
    )


# COMMAND ----------

# DBTITLE 1,Specification rows
rows = [
    (
        "price_daily",
        SPLIT_VERSION,
        "temporal",
        "calendar:energy_daily",
        *_cal("energy_daily"),
        EMBARGO_DAYS["energy_daily"],
        None,
        "rolling_origin",
        "",
    ),
    (
        "bias",
        SPLIT_VERSION,
        "temporal",
        "calendar:energy_daily",
        *_cal("energy_daily"),
        EMBARGO_DAYS["energy_daily"],
        None,
        "rolling_origin",
        "",
    ),
    (
        "load",
        SPLIT_VERSION,
        "temporal",
        "calendar:energy_daily",
        *_cal("energy_daily"),
        EMBARGO_DAYS["energy_daily"],
        None,
        "rolling_origin",
        "",
    ),
    (
        "capacity_additions",
        SPLIT_VERSION,
        "temporal",
        "calendar:energy_daily",
        *_cal("energy_daily"),
        EMBARGO_DAYS["energy_daily"],
        None,
        "rolling_origin",
        "",
    ),
    (
        "zone_generation",
        SPLIT_VERSION,
        "temporal",
        "calendar:energy_daily",
        *_cal("energy_daily"),
        EMBARGO_DAYS["energy_daily"],
        None,
        "rolling_origin",
        "zone held-out run is a diagnostic only",
    ),
    (
        "weather_imputation",
        SPLIT_VERSION,
        "self_supervised_blocks",
        "calendar:energy_daily",
        *_cal("energy_daily"),
        EMBARGO_DAYS["energy_daily"],
        None,
        "rolling_origin",
        "time blocks and stations held out; masks in mask_specification",
    ),
    (
        "price_quarter_hour",
        SPLIT_VERSION,
        "temporal",
        "calendar:quarter_hour",
        *_cal("quarter_hour"),
        EMBARGO_DAYS["quarter_hour"],
        None,
        "rolling_origin",
        "own calendar; never pooled with the daily series",
    ),
    (
        "rl_pumped_storage",
        SPLIT_VERSION,
        "episode",
        "calendar:quarter_hour",
        *_cal("quarter_hour"),
        EMBARGO_DAYS["quarter_hour"],
        None,
        "rolling_origin",
        "episodes are local days on the quarter-hour calendar",
    ),
    (
        "redispatch",
        SPLIT_VERSION,
        "temporal",
        "calendar:redispatch",
        *_cal("redispatch"),
        EMBARGO_DAYS["redispatch"],
        None,
        "rolling_origin",
        "old regime only",
    ),
    (
        "rl_redispatch",
        SPLIT_VERSION,
        "episode",
        "calendar:redispatch",
        *_cal("redispatch"),
        EMBARGO_DAYS["redispatch"],
        None,
        "rolling_origin",
        "old regime only",
    ),
    (
        "honda_forecast",
        SPLIT_VERSION,
        "temporal",
        "calendar:honda",
        *_cal("honda"),
        EMBARGO_DAYS["honda"],
        None,
        "rolling_origin",
        "each resolution split on its own",
    ),
    (
        "honda_anomaly",
        SPLIT_VERSION,
        "temporal",
        "calendar:honda",
        *_cal("honda"),
        EMBARGO_DAYS["honda"],
        None,
        "rolling_origin",
        "injected anomalies in mask_specification",
    ),
    (
        "self_supervised_other",
        SPLIT_VERSION,
        "self_supervised_blocks",
        "calendar:energy_daily",
        *_cal("energy_daily"),
        EMBARGO_DAYS["energy_daily"],
        None,
        "rolling_origin",
        "series families use their own calendar",
    ),
    (
        "survival",
        SPLIT_VERSION,
        "unit_grouped",
        "hash:unit_id stratified by event and offshore",
        None,
        None,
        None,
        None,
        None,
        None,
        0,
        USER_HASH_SEED,
        "grouped_kfold",
        "calendar follow-up check in diagnostic_group",
    ),
    (
        "ccpp",
        SPLIT_VERSION,
        "random",
        "hash:sample_key",
        None,
        None,
        None,
        None,
        None,
        None,
        0,
        USER_HASH_SEED,
        "grouped_kfold",
        "no time column; repeats already removed",
    ),
    (
        "weak_supervision",
        SPLIT_VERSION,
        "grouped_kfold",
        "hash:entity_id",
        None,
        None,
        None,
        None,
        None,
        None,
        0,
        USER_HASH_SEED,
        "grouped_kfold",
        "no separate test",
    ),
    (
        "redispatch_matching",
        SPLIT_VERSION,
        "grouped_kfold",
        "hash:affected_asset_text",
        None,
        None,
        None,
        None,
        None,
        None,
        0,
        USER_HASH_SEED,
        "grouped_kfold",
        "no separate test",
    ),
]

# COMMAND ----------

# DBTITLE 1,Write the specification
write_split_specification(rows, ECO)
