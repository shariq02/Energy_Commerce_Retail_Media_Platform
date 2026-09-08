# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- SEARCH VISIBILITY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `search_visibility_events` (~112M rows) and its repository
# MAGIC reference into source-scoped Silver. `date` parses (`M/d/yyyy` or
# MAGIC `yyyy-MM-dd`) to a monthly `period`. `country` is search-console traffic
# MAGIC geography, source-attributed. Colliding key groups are quarantined.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "search_visibility"
COMPONENT = "silver/energy/search_signal/01_search_visibility"
RID = run_id()

EVENTS_BT = "search_visibility_events"
REPO_BT = "search_visibility_repository"
EVENT_KEY = ["repository_id", "url", "date", "country", "device"]
EVENT_CONTENT = [
    "citableContent",
    "clicks",
    "impressions",
    "clickThrough",
    "position",
    "index",
]
DATE_FORMATS = ("M/d/yyyy", "yyyy-MM-dd")

# COMMAND ----------

# DBTITLE 1,search_visibility_events -> Silver
ev = read_bronze(EVENTS_BT)
for c in ("clickThrough", "clicks", "impressions", "position"):
    ev = ev.withColumn(c, F.col(c).cast("double"))

ev, q = resolve_conflicts(ev, EVENT_KEY, EVENT_CONTENT, bronze_table=EVENTS_BT)
write_quarantine(q.withColumn("source_system", F.lit(SOURCE)), RID)

ev = (
    ev.withColumn("_srid", sha_key(*EVENT_KEY))
    .withColumnRenamed("clickThrough", "click_through")
    .withColumn(
        "period", F.date_format(parse_ts("date", DATE_FORMATS, "UTC"), "yyyy-MM")
    )
    .drop("date")
)
ev = add_provenance(ev, SOURCE, "_srid", RID)
write_silver(ev, EVENTS_BT, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,search_visibility_repository -> Silver
repo = read_bronze(REPO_BT).withColumn("_srid", F.col("repository_id").cast("string"))
repo = add_provenance(repo, SOURCE, "_srid", RID)
write_silver(repo, REPO_BT, source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print(f"SEARCH VISIBILITY -- COMPLETE  (run_id {RID})")
print("=" * 70)
