# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # USER AS-OF AGGREGATES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** per session: what the user did in earlier sessions only (session count,
# MAGIC purchase sessions, days since the last session, items seen), rebuilt from
# MAGIC sessions and never taken from the whole-window user aggregates.

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
COMPONENT = "ml/commerce/features/user_asof_aggregates"
TABLE = "features_user_asof_rees46"
GA4_TABLE = "features_user_asof_ga4"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Session time zone
set_utc_session()

# COMMAND ----------

# DBTITLE 1,REES46 sessions
sessions = (
    read_gold("rees46_session", source=SOURCE)
    .withColumn("session_key", F.concat_ws("|", "user_id", "user_session"))
    .withColumn("had_purchase", F.array_contains("event_types", "purchase").cast("int"))
    .select(
        "session_key",
        "user_id",
        "session_start_utc",
        "session_end_utc",
        "event_count",
        "distinct_product_count",
        "had_purchase",
    )
)

# COMMAND ----------

# DBTITLE 1,Earlier-session aggregates per user
_o = Window.partitionBy("user_id").orderBy("session_start_utc", "session_key")
_prior = _o.rowsBetween(Window.unboundedPreceding, -1)
out = sessions.select(
    "session_key",
    "user_id",
    F.count("*").over(_prior).alias("user_hist_session_count"),
    F.coalesce(F.sum("had_purchase").over(_prior), F.lit(0)).alias(
        "user_hist_purchase_sessions"
    ),
    F.coalesce(F.sum("event_count").over(_prior), F.lit(0)).alias(
        "user_hist_event_count"
    ),
    F.coalesce(F.sum("distinct_product_count").over(_prior), F.lit(0)).alias(
        "user_hist_product_views"
    ),
    (
        (
            F.col("session_start_utc").cast("long")
            - F.lag("session_end_utc").over(_o).cast("long")
        )
        / 86400.0
    ).alias("user_hist_days_since_last_session"),
    (
        (
            F.col("session_start_utc").cast("long")
            - F.first("session_start_utc")
            .over(_o.rowsBetween(Window.unboundedPreceding, Window.currentRow))
            .cast("long")
        )
        / 86400.0
    ).alias("user_hist_days_since_first_session"),
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["session_key"]
assert_unique_grain(out, _GRAIN, component=COMPONENT, source=SOURCE, rid=rid)
assert_no_forbidden_columns(out, component=COMPONENT, source=SOURCE, rid=rid)
check(
    COMPONENT,
    SOURCE,
    "non_empty",
    out.limit(1).count() > 0,
    detail="no rows produced; check the input filters",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write
write_ml(out, TABLE, ecosystem=ECO, source=SOURCE, component=COMPONENT, rid=rid)

# COMMAND ----------

# DBTITLE 1,Inspect and export findings
_blocks = inspect_ml_table(
    read_ml(TABLE, ecosystem=ECO), TABLE, ecosystem=ECO, key_cols=_GRAIN
)
write_ml_findings(ECO, "features__" + TABLE, TABLE, _blocks)

# COMMAND ----------

# DBTITLE 1,GA4 sessions
sessions_ga4 = (
    read_gold("ga4_session", source="ga4")
    .withColumn(
        "session_key",
        F.concat_ws("|", "user_pseudo_id", F.col("session_id").cast("string")),
    )
    .select(
        "session_key",
        "user_pseudo_id",
        "session_start_utc",
        "session_end_utc",
        "event_count",
        "transaction_count",
        "total_revenue",
    )
)

# COMMAND ----------

# DBTITLE 1,GA4 earlier-session aggregates per user
_og = Window.partitionBy("user_pseudo_id").orderBy("session_start_utc", "session_key")
_pg = _og.rowsBetween(Window.unboundedPreceding, -1)
out_ga4 = sessions_ga4.select(
    "session_key",
    "user_pseudo_id",
    F.count("*").over(_pg).alias("user_hist_session_count"),
    F.coalesce(F.sum("transaction_count").over(_pg), F.lit(0)).alias(
        "user_hist_transaction_count"
    ),
    F.coalesce(F.sum("total_revenue").over(_pg), F.lit(0.0)).alias("user_hist_revenue"),
    F.coalesce(F.sum("event_count").over(_pg), F.lit(0)).alias("user_hist_event_count"),
    (
        (
            F.col("session_start_utc").cast("long")
            - F.lag("session_end_utc").over(_og).cast("long")
        )
        / 86400.0
    ).alias("user_hist_days_since_last_session"),
)
out_ga4 = add_ml_provenance(out_ga4, GA4_TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate for GA4
assert_unique_grain(
    out_ga4, ["session_key"], component=COMPONENT + "/ga4", source="ga4", rid=rid
)

# COMMAND ----------

# DBTITLE 1,Write GA4
write_ml(
    out_ga4,
    GA4_TABLE,
    ecosystem=ECO,
    source="ga4",
    component=COMPONENT + "/ga4",
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Inspect and export GA4 findings
_blocks = inspect_ml_table(
    read_ml(GA4_TABLE, ecosystem=ECO),
    GA4_TABLE,
    ecosystem=ECO,
    key_cols=["session_key"],
)
write_ml_findings(ECO, "features__" + GA4_TABLE, GA4_TABLE, _blocks)
