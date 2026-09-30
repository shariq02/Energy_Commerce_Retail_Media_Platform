# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SESSION PURCHASE TARGET
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** whether a purchase happens after the k-event prefix of a session, for
# MAGIC REES46 (k = 2) and GA4 (k = 10). Sessions with fewer events or a purchase
# MAGIC inside the prefix are excluded and counted; nothing gets an implicit
# MAGIC negative.

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
COMPONENT = "ml/commerce/targets/session_purchase"
TABLE = "target_session_purchase_rees46"
GA4_TABLE = "target_session_purchase_ga4"
K_REES46 = 2
K_GA4 = 10

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Session time zone
set_utc_session()

# COMMAND ----------

# DBTITLE 1,REES46 ranked events
_e = (
    read_gold("rees46_event", source=SOURCE)
    .select("event_key", "event_timestamp_utc", "event_type", "user_id", "user_session")
    .withColumn("session_key", F.concat_ws("|", "user_id", "user_session"))
)
_w = Window.partitionBy("session_key").orderBy("event_timestamp_utc", "event_key")
_r = _e.withColumn("rn", F.row_number().over(_w))

# COMMAND ----------

# DBTITLE 1,REES46 label after the prefix
candidates = _r.groupBy("session_key").agg(
    F.first("user_id").alias("user_id"),
    F.min("event_timestamp_utc").alias("session_start_utc"),
    F.count("*").alias("event_count"),
    F.max(
        ((F.col("rn") <= K_REES46) & (F.col("event_type") == "purchase")).cast("int")
    ).alias("purchase_in_prefix"),
    F.max(
        ((F.col("rn") > K_REES46) & (F.col("event_type") == "purchase")).cast("int")
    ).alias("purchase_after_prefix"),
)
out = (
    candidates.filter(
        (F.col("event_count") >= K_REES46) & (F.col("purchase_in_prefix") == 0)
    )
    .select(
        "session_key",
        "user_id",
        F.to_date(F.from_utc_timestamp("session_start_utc", PROJECT_TIMEZONE)).alias(
            "session_date"
        ),
        (F.col("purchase_after_prefix") == 1).alias("target_purchase_after_prefix"),
    )
    .withColumn("provenance_tier", F.lit("constructed"))
)
out = add_ml_provenance(out, TABLE, ECO, rid)

# COMMAND ----------

# DBTITLE 1,Record the excluded sessions
_below = candidates.filter(F.col("event_count") < K_REES46).count()
_in = candidates.filter(
    (F.col("event_count") >= K_REES46) & (F.col("purchase_in_prefix") == 1)
).count()
drop_block = (
    "excluded_sessions",
    markdown_table(
        ["reason", "sessions"],
        [("fewer_events_than_k", _below), ("purchase_inside_prefix", _in)],
    ),
)
print(f"excluded: below k {_below}, purchase in prefix {_in}")

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
    read_ml(TABLE, ecosystem=ECO),
    TABLE,
    ecosystem=ECO,
    key_cols=_GRAIN,
    target_col="target_purchase_after_prefix",
)
write_ml_findings(ECO, "targets__" + TABLE, TABLE, _blocks)

# COMMAND ----------

# DBTITLE 1,Add the exclusions to the findings
write_ml_findings(
    ECO, "targets__" + TABLE + "__exclusions", TABLE + " exclusions", [drop_block]
)

# COMMAND ----------

# DBTITLE 1,GA4 ranked events
_ge = (
    read_gold("ga4_event", source="ga4")
    .select(
        "event_key", "event_timestamp_utc", "event_name", "user_pseudo_id", "session_id"
    )
    .withColumn(
        "session_key",
        F.concat_ws("|", "user_pseudo_id", F.col("session_id").cast("string")),
    )
)
_wg = Window.partitionBy("session_key").orderBy("event_timestamp_utc", "event_key")
_rg = _ge.withColumn("rn", F.row_number().over(_wg))

# COMMAND ----------

# DBTITLE 1,GA4 label after the prefix
candidates_ga4 = _rg.groupBy("session_key").agg(
    F.first("user_pseudo_id").alias("user_id"),
    F.min("event_timestamp_utc").alias("session_start_utc"),
    F.count("*").alias("event_count"),
    F.max(
        ((F.col("rn") <= K_GA4) & (F.col("event_name") == "purchase")).cast("int")
    ).alias("purchase_in_prefix"),
    F.max(
        ((F.col("rn") > K_GA4) & (F.col("event_name") == "purchase")).cast("int")
    ).alias("purchase_after_prefix"),
)
out_ga4 = (
    candidates_ga4.filter(
        (F.col("event_count") >= K_GA4) & (F.col("purchase_in_prefix") == 0)
    )
    .select(
        "session_key",
        "user_id",
        F.to_date(F.from_utc_timestamp("session_start_utc", PROJECT_TIMEZONE)).alias(
            "session_date"
        ),
        (F.col("purchase_after_prefix") == 1).alias("target_purchase_after_prefix"),
    )
    .withColumn("provenance_tier", F.lit("constructed"))
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
    target_col="target_purchase_after_prefix",
)
write_ml_findings(ECO, "targets__" + GA4_TABLE, GA4_TABLE, _blocks)
