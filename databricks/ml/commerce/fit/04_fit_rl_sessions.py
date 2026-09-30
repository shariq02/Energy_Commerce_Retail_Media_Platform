# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FIT RL SESSIONS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** fit the datasets, applying the null rule, fitting imputers on the training
# MAGIC partition only and per fold, transforming every partition and
# MAGIC materialising the final tables.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "commerce"
SOURCE = "rees46"
COMPONENT = "ml/commerce/fit/04_fit_rl_sessions"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,ML fit library
# MAGIC %run ../../_ml_fit

# COMMAND ----------

# DBTITLE 1,Fit and transform rl_session_sequences
out_rl_session_sequences = fit_transform(
    "rl_session_sequences",
    ECO,
    group_cols=[],
    fold_mode="grouped",
    calendar_key=None,
    date_col=None,
    impute=True,
    rid=rid,
    source=SOURCE,
)
out_rl_session_sequences = add_ml_provenance(
    out_rl_session_sequences, "rl_session_sequences", ECO, rid
)

# COMMAND ----------

# DBTITLE 1,Gate rl_session_sequences
_grain_rl_session_sequences = list(
    read_ml("dataset_manifest", ecosystem=ECO)
    .filter(F.col("dataset_id") == "rl_session_sequences")
    .first()["grain_key"]
)
assert_unique_grain(
    out_rl_session_sequences,
    _grain_rl_session_sequences,
    component=COMPONENT + "/rl_session_sequences",
    source=SOURCE,
    rid=rid,
)
assert_no_forbidden_columns(
    out_rl_session_sequences,
    component=COMPONENT + "/rl_session_sequences",
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write rl_session_sequences
write_ml(
    out_rl_session_sequences,
    "dataset_rl_session_sequences",
    ecosystem=ECO,
    source=SOURCE,
    component=COMPONENT + "/rl_session_sequences",
    rid=rid,
)
set_dataset_status("rl_session_sequences", ECO, "fitted")

# COMMAND ----------

# DBTITLE 1,Inspect and export findings for rl_session_sequences
_targets = contract_columns("rl_session_sequences", ECO, "target")
_blocks = inspect_ml_table(
    read_ml("dataset_rl_session_sequences", ecosystem=ECO),
    "dataset_rl_session_sequences",
    ecosystem=ECO,
    key_cols=_grain_rl_session_sequences,
    partition_col="partition",
    target_col=_targets[0] if _targets else None,
)
write_ml_findings(
    ECO, "fit__dataset_rl_session_sequences", "dataset_rl_session_sequences", _blocks
)
