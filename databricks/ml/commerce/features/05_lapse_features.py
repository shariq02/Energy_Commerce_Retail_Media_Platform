# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # LAPSE COHORT FEATURES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** per user and cutoff: activity before the cutoff (sessions, events,
# MAGIC purchase sessions, active days, recency), for REES46 at its single cutoff
# MAGIC and GA4 at both cutoffs.

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
COMPONENT = "ml/commerce/features/lapse_features"
TABLE = "features_lapse_rees46"
GA4_TABLE = "features_lapse_ga4"
REES46_CUTOFF = REES46_EVALUATION_START
GA4_CUTOFFS = [GA4_LAPSE_TRAIN_CUTOFF, GA4_LAPSE_EVALUATION_CUTOFF]

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,REES46 activity before the cutoff
_s = (
    read_gold("rees46_session", source=SOURCE)
    .withColumn("had_purchase", F.array_contains("event_types", "purchase").cast("int"))
    .filter(F.col("session_start_utc") < F.lit(REES46_CUTOFF).cast("timestamp"))
)
out = (
    _s.groupBy("user_id")
    .agg(
        F.count("*").alias("lapse_session_count"),
        F.sum("event_count").alias("lapse_event_count"),
        F.sum("had_purchase").alias("lapse_purchase_sessions"),
        F.sum("distinct_product_count").alias("lapse_product_views"),
        F.countDistinct(F.to_date("session_start_utc")).alias("lapse_active_days"),
        (
            (
                F.lit(REES46_CUTOFF).cast("timestamp").cast("long")
                - F.max("session_end_utc").cast("long")
            )
            / 86400.0
        ).alias("lapse_days_since_last_session"),
        (
            (
                F.lit(REES46_CUTOFF).cast("timestamp").cast("long")
                - F.min("session_start_utc").cast("long")
            )
            / 86400.0
        ).alias("lapse_days_since_first_session"),
    )
    .withColumn("cutoff_date", F.lit(REES46_CUTOFF).cast("date"))
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain and forbidden-column gate
_GRAIN = ["user_id", "cutoff_date"]
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

# DBTITLE 1,GA4 activity before each cutoff
_g = read_gold("ga4_session", source="ga4")
_parts = []
for _c in GA4_CUTOFFS:
    _parts.append(
        _g.filter(F.col("session_start_utc") < F.lit(_c).cast("timestamp"))
        .groupBy("user_pseudo_id")
        .agg(
            F.count("*").alias("lapse_session_count"),
            F.sum("event_count").alias("lapse_event_count"),
            F.sum("transaction_count").alias("lapse_transaction_count"),
            F.sum("total_revenue").alias("lapse_revenue"),
            F.countDistinct(F.to_date("session_start_utc")).alias("lapse_active_days"),
            (
                (
                    F.lit(_c).cast("timestamp").cast("long")
                    - F.max("session_end_utc").cast("long")
                )
                / 86400.0
            ).alias("lapse_days_since_last_session"),
        )
        .withColumn("cutoff_date", F.lit(_c).cast("date"))
    )
out_ga4 = _parts[0]
for _p in _parts[1:]:
    out_ga4 = out_ga4.unionByName(_p)
out_ga4 = add_ml_provenance(out_ga4, GA4_TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Grain gate for GA4
assert_unique_grain(
    out_ga4,
    ["user_pseudo_id", "cutoff_date"],
    component=COMPONENT + "/ga4",
    source="ga4",
    rid=rid,
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
    key_cols=["user_pseudo_id", "cutoff_date"],
)
write_ml_findings(ECO, "features__" + GA4_TABLE, GA4_TABLE, _blocks)
