# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # DERIVED PV FORECAST
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** view deriving the PV forecast as wind-and-PV minus onshore minus offshore,
# MAGIC because the published PV series is a sign mirror. Keeps value origin and
# MAGIC derivation rule.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "smard"
COMPONENT = "ml/energy/features/derived_pv_forecast"
TABLE = "derived_pv_forecast"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Preflight -- the forecast scopes exist
_scopes = (
    read_gold("generation_forecast", source=SOURCE)
    .select(F.explode("components").alias("c"))
    .select("c.forecast_scope")
    .distinct()
)
assert_values_present(
    _scopes,
    "forecast_scope",
    ["wind_and_photovoltaic", "onshore_wind", "offshore_wind"],
    component=COMPONENT,
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Create the view
write_ml_view(
    f"""
SELECT market_area_code, observation_timestamp_utc, local_date, interval_seconds,
       interval_reference,
       wind_and_pv - onshore - offshore AS forecast_photovoltaic_mwh,
       'derived' AS value_origin,
       'wind_and_photovoltaic minus onshore_wind minus offshore_wind' AS derivation_rule
FROM (
  SELECT g.market_area_code, g.observation_timestamp_utc, g.local_date,
         g.interval_seconds, g.interval_reference,
         MAX(CASE WHEN c.forecast_scope = 'wind_and_photovoltaic' THEN c.energy_mwh END) AS wind_and_pv,
         MAX(CASE WHEN c.forecast_scope = 'onshore_wind' THEN c.energy_mwh END) AS onshore,
         MAX(CASE WHEN c.forecast_scope = 'offshore_wind' THEN c.energy_mwh END) AS offshore
  FROM {CATALOG}.energy_gold.generation_forecast g
  LATERAL VIEW EXPLODE(g.components) t AS c
  GROUP BY g.market_area_code, g.observation_timestamp_utc, g.local_date,
           g.interval_seconds, g.interval_reference
)
WHERE wind_and_pv IS NOT NULL AND onshore IS NOT NULL AND offshore IS NOT NULL
""",
    TABLE,
    ecosystem=ECO,
)
