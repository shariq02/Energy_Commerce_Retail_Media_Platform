# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- COMMERCE VALIDATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** cross-table checks that the GA4/REES46 rollups reconcile
# MAGIC against their source events -- no rollup row invented, none dropped.
# MAGIC Run last, after every other `commerce_gold` notebook.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "gold/commerce/validation"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Gate 1 -- ga4_session count matches distinct (user_pseudo_id, session_id)
_ga4_event = read_silver("ga4_event")
_distinct_ga4_sessions = (
    _ga4_event.select("user_pseudo_id", "session_id").distinct().count()
)
_ga4_session_rows = read_gold("ga4_session", source="ga4").count()
check(
    COMPONENT,
    "ga4",
    "ga4_session_matches_distinct_sessions",
    _ga4_session_rows == _distinct_ga4_sessions,
    detail=f"ga4_session_rows={_ga4_session_rows} distinct={_distinct_ga4_sessions}",
    metric_value=float(_ga4_session_rows - _distinct_ga4_sessions),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Gate 2 -- ga4_user count matches distinct user_pseudo_id
_distinct_ga4_users = _ga4_event.select("user_pseudo_id").distinct().count()
_ga4_user_rows = read_gold("ga4_user", source="ga4").count()
check(
    COMPONENT,
    "ga4",
    "ga4_user_matches_distinct_users",
    _ga4_user_rows == _distinct_ga4_users,
    detail=f"ga4_user_rows={_ga4_user_rows} distinct={_distinct_ga4_users}",
    metric_value=float(_ga4_user_rows - _distinct_ga4_users),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Gate 3 -- rees46_session and rees46_user match distinct source counts
_rees46_event = read_silver("rees46_event")
_distinct_rees46_sessions = (
    _rees46_event.select("user_id", "user_session").distinct().count()
)
_rees46_session_rows = read_gold("rees46_session", source="rees46").count()
check(
    COMPONENT,
    "rees46",
    "rees46_session_matches_distinct_sessions",
    _rees46_session_rows == _distinct_rees46_sessions,
    detail=f"rees46_session_rows={_rees46_session_rows} distinct={_distinct_rees46_sessions}",
    metric_value=float(_rees46_session_rows - _distinct_rees46_sessions),
    rid=rid,
)

_distinct_rees46_users = _rees46_event.select("user_id").distinct().count()
_rees46_user_rows = read_gold("rees46_user", source="rees46").count()
check(
    COMPONENT,
    "rees46",
    "rees46_user_matches_distinct_users",
    _rees46_user_rows == _distinct_rees46_users,
    detail=f"rees46_user_rows={_rees46_user_rows} distinct={_distinct_rees46_users}",
    metric_value=float(_rees46_user_rows - _distinct_rees46_users),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Gate 4 -- ga4_purchase never invents or drops a transaction row
_ga4_purchase_rows = read_gold("ga4_purchase", source="ga4").count()
_source_purchase_rows = _ga4_event.filter(F.col("transaction_id").isNotNull()).count()
check(
    COMPONENT,
    "ga4",
    "ga4_purchase_matches_source_rows",
    _ga4_purchase_rows == _source_purchase_rows,
    detail=f"ga4_purchase_rows={_ga4_purchase_rows} source_rows={_source_purchase_rows}",
    metric_value=float(_ga4_purchase_rows - _source_purchase_rows),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print("COMMERCE GOLD VALIDATION -- all gates PASSED")
print("=" * 70)
