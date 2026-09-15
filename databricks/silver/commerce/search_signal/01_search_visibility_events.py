# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- SEARCH VISIBILITY EVENTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `search_visibility_events` (~112M rows) into source-scoped
# MAGIC Silver. `date` parses (`M/d/yyyy` or `yyyy-MM-dd`) to a monthly `period`.
# MAGIC `country` is search-console traffic geography, source-attributed.
# MAGIC Colliding key groups are quarantined. The repository reference table is
# MAGIC `commerce/_reference/01_search_visibility_reference.py`.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "search_visibility"
# The contract's own source_system (src/schemas/contracts/search_visibility.yml)
# differs from SOURCE above -- it's the governed provenance value and the key
# source_ecosystem_map.yml actually uses; SOURCE stays the short contract/
# mapping filename.
SOURCE_SYSTEM = "search_visibility_ramp_dryad"
COMPONENT = "silver/commerce/search_signal/01_search_visibility_events"
RID = run_id()

EVENTS_BT = "search_visibility_events"
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
write_quarantine(q.withColumn("source_system", F.lit(SOURCE_SYSTEM)), RID)

ev = (
    ev.withColumn("_srid", sha_key(*EVENT_KEY))
    .withColumnRenamed("clickThrough", "click_through")
    .withColumn(
        "period", F.date_format(parse_ts("date", DATE_FORMATS, "UTC"), "yyyy-MM")
    )
    .drop("date")
)
ev = add_provenance(ev, SOURCE_SYSTEM, "_srid", RID)
write_silver(ev, EVENTS_BT, source=SOURCE_SYSTEM, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print(f"SEARCH VISIBILITY EVENTS -- COMPLETE  (run_id {RID})")
print("=" * 70)
