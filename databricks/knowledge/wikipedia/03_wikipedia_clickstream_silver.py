# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # KNOWLEDGE SILVER -- WIKIPEDIA CLICKSTREAM
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** read the `wikipedia-datasets` clickstream (JSON lines, February
# MAGIC 2015) in place and write `knowledge.wikipedia_clickstream`: typed columns,
# MAGIC the edge type and the click count, one row per (previous title, current
# MAGIC title, type). A duplicated key keeps the row with the highest click count.
# MAGIC A self-loop stays and carries `is_self_loop`. The source is never changed.
# MAGIC Findings go to `src/findings/knowledge_findings/wikipedia.md`.

# COMMAND ----------

# DBTITLE 1,Text library
# MAGIC %run ./_wikipedia_text

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ./_wikipedia_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ./_wikipedia_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = FINDINGS_SOURCE
COMPONENT = "knowledge/wikipedia/03_wikipedia_clickstream_silver"
RID = run_id()
TABLE = "wikipedia_clickstream"
RAW_SCHEMA = T.StructType(
    [
        T.StructField(name, T.StringType())
        for name in ("prev_id", "curr_id", "prev_title", "curr_title", "n", "type")
    ]
)

# COMMAND ----------

# DBTITLE 1,Read the clickstream
raw = spark.read.schema(RAW_SCHEMA).json(CLICKSTREAM_ROOT)
raw_rows = raw.count()
print(f"OK  edges read: {raw_rows}")

# COMMAND ----------

# DBTITLE 1,Type the columns
typed = raw.select(
    F.expr("try_cast(prev_id as bigint)").alias("prev_id"),
    F.expr("try_cast(curr_id as bigint)").alias("curr_id"),
    F.col("prev_title"),
    F.col("curr_title"),
    F.expr("try_cast(n as bigint)").alias("click_count"),
    F.col("type").alias("edge_type"),
)

# COMMAND ----------

# DBTITLE 1,Keep one row per previous title, current title and type
key = ["prev_title", "curr_title", "edge_type"]
key_window = Window.partitionBy(*key).orderBy(
    F.col("click_count").desc_nulls_last(), F.col("prev_id"), F.col("curr_id")
)
deduplicated = (
    typed.withColumn("_rank", F.row_number().over(key_window))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)

# COMMAND ----------

# DBTITLE 1,Flag self-loops and add provenance
silver = deduplicated.withColumn(
    "is_self_loop",
    (F.col("prev_id") == F.col("curr_id"))
    | (F.col("prev_title") == F.col("curr_title")),
)
silver = add_knowledge_provenance(silver, "clickstream", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- wikipedia_clickstream
(
    silver.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(CLICKSTREAM_TABLE)
)
print(f"OK  table written: {CLICKSTREAM_TABLE}")

# COMMAND ----------

# DBTITLE 1,Count the edges by type
written = spark.table(CLICKSTREAM_TABLE)
type_counts = written.groupBy("edge_type").agg(F.count("*").alias("n")).collect()
by_type = {str(r["edge_type"]): r["n"] for r in type_counts}
total = sum(by_type.values())
self_loops = written.filter(F.col("is_self_loop")).count()
unparsed = written.filter(F.col("click_count").isNull()).count()
print(by_type)

# COMMAND ----------

# DBTITLE 1,Gate -- one row per key
duplicates = written.groupBy(*key).count().filter(F.col("count") > 1).count()
check(
    COMPONENT,
    SOURCE,
    "edge_key_duplicates",
    duplicates == 0,
    detail=f"duplicate_groups={duplicates}",
    metric_value=duplicates,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Gate -- link edges present and click counts parsed
check(
    COMPONENT,
    SOURCE,
    "link_edges_and_click_counts",
    by_type.get(LINK_TYPE, 0) > 0 and unparsed == 0,
    detail=f"link_edges={by_type.get(LINK_TYPE, 0)} unparsed_click_counts={unparsed}",
    metric_value=by_type.get(LINK_TYPE, 0),
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- wikipedia_clickstream
findings_blocks = inspect_knowledge_table(
    written,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=key,
    extra_checks={
        "edges_read": raw_rows,
        "edges_written": total,
        "duplicate_edges_removed": raw_rows - total,
        "edges_by_type": by_type,
        "self_loops": self_loops,
        "unparsed_click_counts": unparsed,
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- wikipedia_clickstream
write_knowledge_findings(
    SOURCE, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, findings_blocks
)
