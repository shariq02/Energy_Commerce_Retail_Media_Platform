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

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "rees46"
COMPONENT = "silver/energy/customer/01_rees46_events"
RID = run_id()
BT = "rees46_events"

KEY_COLS = ["user_session", "product_id", "event_type", "event_time"]
CONTENT_COLS = ["category_id", "category_code", "brand", "price", "user_id"]

# COMMAND ----------

# DBTITLE 1,rees46_events -> Silver
df = read_bronze(BT).withColumn("event_time", F.col("event_time").cast("timestamp"))
df = df.withColumn("price", F.col("price").cast("double"))

df, q = resolve_conflicts(df, KEY_COLS, CONTENT_COLS, bronze_table=BT)
write_quarantine(q.withColumn("source_system", F.lit(SOURCE)), RID)

df = df.withColumn("currency_unknown", F.lit(True)).withColumn(
    "_srid", sha_key(*KEY_COLS)
)
df = add_provenance(df, SOURCE, "_srid", RID)
write_silver(df, BT, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print(f"REES46 EVENTS -- COMPLETE  (run_id {RID})")
print("=" * 70)
