# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- REES46 CUSTOMER ACTIVITY EVENTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `rees46_events` (~110M rows) into source-scoped Silver at its
# MAGIC event grain. Byte-identical duplicates collapse; residual colliding key
# MAGIC groups are quarantined. `price` currency is undocumented --
# MAGIC `currency_unknown = true`. No synthetic country is built
# MAGIC (`rees46_user_country_synthetic` is deferred -- no defensible rule).

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE = "rees46"
COMPONENT = "silver/commerce/journey/01_rees46_events"
RID = run_id()
BT = "rees46_events"

KEY_COLS = ["user_session", "product_id", "event_type", "event_time"]
CONTENT_COLS = ["category_id", "category_code", "brand", "price", "user_id"]

# COMMAND ----------

# DBTITLE 1,rees46_events -> Silver
bronze_df = read_bronze(BT)
df = bronze_df.withColumn("event_time", F.col("event_time").cast("timestamp"))
df = df.withColumn("price", F.col("price").cast("double"))

df, q = resolve_conflicts(df, KEY_COLS, CONTENT_COLS, bronze_table=BT)
write_quarantine(q.withColumn("source_system", F.lit(SOURCE)), RID)

df = df.withColumn("currency_unknown", F.lit(True)).withColumn(
    "_srid", sha_key(*KEY_COLS)
)

# D3 (design record §4 REES46): category_code is a source-provided dotted
# hierarchy path (e.g. "electronics.smartphone.android") -- split on the
# delimiter, not an invented taxonomy (rees46.md Top category_code list).
_cat_parts = F.split(F.col("category_code"), r"\.")
df = (
    df.withColumn("category_l1", _cat_parts.getItem(0))
    .withColumn("category_l2", _cat_parts.getItem(1))
    .withColumn("category_l3", _cat_parts.getItem(2))
)

# D4: bot-burst + category-ambiguity flags. Session = (user_id, user_session),
# never user_session alone.
_burst_w = Window.partitionBy("user_id", "user_session", "event_time")
df = df.withColumn("_bot_burst_suspected", F.count(F.lit(1)).over(_burst_w) > 20)

_cat_ambiguity = df.groupBy("category_code").agg(
    F.countDistinct("category_id").alias("_n_category_ids")
)
df = (
    df.join(F.broadcast(_cat_ambiguity), "category_code", "left")
    .withColumn("_category_code_ambiguous", F.col("_n_category_ids") > 1)
    .drop("_n_category_ids")
)

df = add_provenance(df, SOURCE, "_srid", RID)
write_silver(df, BT, source=SOURCE, component=COMPONENT, rid=RID)
_findings_blocks = inspect_table(
    df,
    BT,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=KEY_COLS,
    df_before=bronze_df,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__{BT}",
    BT,
    _findings_blocks,
)
