# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- ML DATASETS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** exploratory analysis of the ML datasets, run after the ML gate. It
# MAGIC reads the fitted and frozen datasets of the three ecosystems and writes its
# MAGIC findings to `src/schemas/profiling/ml.md` (one section per ecosystem). It adds
# MAGIC what the gate does not give: the target by partition and fold, the shift of
# MAGIC the features from train, columns with one dominant value, the correlation with
# MAGIC the target, redundant feature pairs, gaps of the time axis and group sizes.

# COMMAND ----------

# MAGIC %run ./_eda_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- ML DATASETS OF ENERGY
# MAGIC
# MAGIC **Purpose:** Profile every fitted or frozen energy dataset. Counts, rates and
# MAGIC column names only.

# COMMAND ----------

# DBTITLE 1,Energy datasets
ENERGY = run_ml_eda("energy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- ML DATASETS OF COMMERCE
# MAGIC
# MAGIC **Purpose:** Profile every fitted or frozen commerce dataset. Counts, rates
# MAGIC and column names only.

# COMMAND ----------

# DBTITLE 1,Commerce datasets
COMMERCE = run_ml_eda("commerce")

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- ML DATASETS OF GENOMICS
# MAGIC
# MAGIC **Purpose:** Profile every fitted or frozen genomics dataset. Counts, rates
# MAGIC and column names only; no minimum, maximum or percentile value of any column
# MAGIC is written.

# COMMAND ----------

# DBTITLE 1,Genomics datasets
GENOMICS = run_ml_eda("genomics")
