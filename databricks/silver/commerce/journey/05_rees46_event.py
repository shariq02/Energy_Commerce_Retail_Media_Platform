# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- REES46 EVENT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** REES46 shop events, one row per event, with UTC and project
# MAGIC time, the category path split and quality flags. Price currency is not
# MAGIC documented by the source.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "rees46"
COMPONENT = "silver/commerce/journey/05_rees46_event"
RID = run_id()
FINDINGS = "rees46"
BT = "rees46_events"
TABLE = "rees46_event"
KEY = ["user_session", "product_id", "event_type", "event_time"]
CONTENT = ["category_id", "category_code", "brand", "price", "user_id"]
# Same (user, session, second) repeated more often than this -> bot burst.
BOT_BURST_EVENTS = 20

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Bronze -- rees46_events
bronze_df = read_bronze(BT)

# COMMAND ----------

# DBTITLE 1,Dedupe -- collapse identical rows, quarantine same-key value conflicts
_typed = (
    bronze_df.withColumn("event_timestamp_native", F.col("event_time"))
    .withColumn("event_time", F.col("event_time").cast("timestamp"))
    .withColumn("price", F.col("price").cast("double"))
)
_kept, _q = resolve_conflicts(_typed, KEY, CONTENT, bronze_table=BT)
write_quarantine(_q.withColumn("source_system", F.lit(SOURCE)), RID)
RECONCILIATION = reconciliation_stats(bronze_df, _kept, _q)

# COMMAND ----------

# DBTITLE 1,Transform -- time, category path, flags
_path = F.split(F.col("category_code"), r"\.")
_burst = Window.partitionBy("user_id", "user_session", "event_timestamp_utc")
_ids_per_code = _kept.groupBy("category_code").agg(
    F.countDistinct("category_id").alias("_ids_per_code")
)
events = (
    _kept.withColumn("event_timestamp_utc", F.col("event_time"))
    .withColumn(
        "event_timestamp_project",
        F.from_utc_timestamp("event_timestamp_utc", PROJECT_TZ),
    )
    .withColumn("local_date", F.to_date("event_timestamp_project"))
    .withColumn("currency_unknown", F.lit(True))
    .withColumn("category_l1", F.get(_path, 0))
    .withColumn("category_l2", F.get(_path, 1))
    .withColumn("category_l3", F.get(_path, 2))
    .join(F.broadcast(_ids_per_code), "category_code", "left")
    .withColumn(
        "quality_flags",
        flag_array(
            {
                "key_conflict_resolved": F.col("_had_key_conflict"),
                "bot_burst_suspected": F.count(F.lit(1)).over(_burst)
                > BOT_BURST_EVENTS,
                "category_code_ambiguous": F.col("_ids_per_code") > 1,
            }
        ),
    )
    .withColumn("measurement_basis", F.lit("shop_event_log"))
    .withColumn("event_key", sha_key(*KEY))
    .withColumn("source_record_id", F.col("event_key"))
)
events = add_semantic_provenance(events, SOURCE, BT, RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- rees46_event
write_semantic(
    conform(events, SEMANTIC_STRUCTURES[TABLE]),
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- rees46_event
written = spark.table(semantic_table(TABLE))
findings_blocks = inspect_table(
    written,
    TABLE,
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["event_key"],
    df_before=bronze_df,
)

# COMMAND ----------

# DBTITLE 1,Export findings -- rees46_event
write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__{TABLE}",
    TABLE,
    [
        *findings_blocks,
        (
            "Bronze -> exact duplicates collapsed -> conflicts quarantined -> kept",
            dict_to_markdown_row(RECONCILIATION),
        ),
    ],
)
