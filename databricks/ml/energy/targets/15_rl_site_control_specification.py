# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # RL SPECIFICATION ROWS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** register the site-control and plant-dispatch reinforcement-learning
# MAGIC datasets as specifications. Site control needs a trained site forecast
# MAGIC model as its simulator; plant dispatch waits for the ENTSO-E per-unit
# MAGIC output probe.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "ml"
COMPONENT = "ml/energy/targets/rl_specifications"
TABLE = "dataset_manifest"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Register the specification rows
register_dataset(
    "rl_site_control",
    ECO,
    capability="site control (simulated tier)",
    grain=["episode_id", "step"],
    dependency_group=1,
    target_name="reward",
    provenance_tier="simulated",
    status="waits_for_site_forecast_model",
    notes="the simulator is the trained site forecast model; validity is capped by its error",
)
register_dataset(
    "rl_plant_dispatch",
    ECO,
    capability="plant-level dispatch",
    grain=["episode_id", "step"],
    dependency_group=1,
    target_name="reward",
    provenance_tier="constructed",
    status="blocked_entsoe_probe",
    notes="needs per-unit output for units of 100 MW or more; reward can only be market revenue",
)
