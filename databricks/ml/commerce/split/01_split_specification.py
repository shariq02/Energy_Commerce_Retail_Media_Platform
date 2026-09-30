# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SPLIT SPECIFICATION (COMMERCE)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** write the split specification row of every commerce dataset: user-disjoint
# MAGIC hash split, the temporal ordering rule where it applies, the cutoffs and
# MAGIC the fold scheme.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "commerce"
SOURCE = "ml"
COMPONENT = "ml/commerce/split/split_specification"
TABLE = "split_specification"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Specification rows
rows = [
    (
        "session_purchase_rees46",
        SPLIT_VERSION,
        "user_disjoint_temporal",
        "hash:user_id; train users before 2019-11-01, evaluation users from 2019-11-01",
        None,
        None,
        None,
        None,
        None,
        None,
        0,
        USER_HASH_SEED,
        "grouped_kfold",
        "temporal ordering applied",
    ),
    (
        "session_purchase_ga4",
        SPLIT_VERSION,
        "user_disjoint",
        "hash:user_id; early and late diagnostic only",
        None,
        None,
        None,
        None,
        None,
        None,
        0,
        USER_HASH_SEED,
        "grouped_kfold",
        "positives too few to cut by time",
    ),
    (
        "lapse_rees46",
        SPLIT_VERSION,
        "user_disjoint",
        "hash:user_id; single cutoff 2019-11-01",
        None,
        None,
        None,
        None,
        None,
        None,
        0,
        USER_HASH_SEED,
        "grouped_kfold",
        "no temporal partition",
    ),
    (
        "lapse_ga4",
        SPLIT_VERSION,
        "user_disjoint_cutoff",
        "hash:user_id; train users at 2020-11-20, evaluation users at 2020-12-01",
        None,
        None,
        None,
        None,
        None,
        None,
        0,
        USER_HASH_SEED,
        "grouped_kfold",
        "training labels untruncated; residual temporal overlap declared",
    ),
    (
        "next_item_rees46",
        SPLIT_VERSION,
        "user_disjoint_temporal",
        "hash:user_id; train users before 2019-11-01, evaluation users from 2019-11-01",
        None,
        None,
        None,
        None,
        None,
        None,
        0,
        USER_HASH_SEED,
        "grouped_kfold",
        "session steps inherit the user split",
    ),
    (
        "next_item_ga4",
        SPLIT_VERSION,
        "user_disjoint",
        "hash:user_id",
        None,
        None,
        None,
        None,
        None,
        None,
        0,
        USER_HASH_SEED,
        "grouped_kfold",
        "thin",
    ),
    (
        "rl_session_sequences",
        SPLIT_VERSION,
        "user_disjoint_temporal",
        "hash:user_id; train users before 2019-11-01, evaluation users from 2019-11-01",
        None,
        None,
        None,
        None,
        None,
        None,
        0,
        USER_HASH_SEED,
        "grouped_kfold",
        "split by episode",
    ),
]

# COMMAND ----------

# DBTITLE 1,Write the specification
write_split_specification(rows, ECO)
