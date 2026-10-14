# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # KNOWLEDGE GOLD -- WIKIPEDIA EDGES BETWEEN SELECTED ARTICLES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** keep the `link` edges of `knowledge.wikipedia_clickstream` whose
# MAGIC previous and current page are both in `knowledge.wikipedia_selected`, without
# MAGIC self-loops. One row per (previous article id, current article id); a repeated
# MAGIC pair keeps the highest click count. The click count is the edge weight only.
# MAGIC Writes `knowledge.wikipedia_edges` and its findings. Re-run after notebook 02.

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

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = FINDINGS_SOURCE
COMPONENT = "knowledge/wikipedia/04_wikipedia_clickstream_gold"
RID = run_id()
TABLE = "wikipedia_edges"

# COMMAND ----------

# DBTITLE 1,Read the selected articles
selected = spark.table(GOLD_TABLE)
selection_hashes = [r[0] for r in selected.select("terms_hash").distinct().collect()]
assert len(selection_hashes) == 1, (
    f"selected articles hold {len(selection_hashes)} rule hashes"
)
selected_ids = selected.select("article_id").distinct()
print(f"OK  selected articles: {selected_ids.count()}")

# COMMAND ----------

# DBTITLE 1,Keep link edges between selected articles
links = spark.table(CLICKSTREAM_TABLE).filter(
    (F.col("edge_type") == LINK_TYPE) & (~F.col("is_self_loop"))
)
link_count = links.count()
previous_ids = F.broadcast(selected_ids.withColumnRenamed("article_id", "prev_id"))
current_ids = F.broadcast(selected_ids.withColumnRenamed("article_id", "curr_id"))
between = links.join(previous_ids, "prev_id", "inner").join(
    current_ids, "curr_id", "inner"
)

# COMMAND ----------

# DBTITLE 1,Keep one row per article pair
pair_window = Window.partitionBy("prev_id", "curr_id").orderBy(
    F.col("click_count").desc(), F.col("prev_title"), F.col("curr_title")
)
edges = (
    between.withColumn("_rank", F.row_number().over(pair_window))
    .filter(F.col("_rank") == 1)
    .select(
        F.col("prev_id").alias("prev_article_id"),
        F.col("curr_id").alias("curr_article_id"),
        F.col("prev_title"),
        F.col("curr_title"),
        "click_count",
    )
    .withColumn("selection_terms_hash", F.lit(selection_hashes[0]))
)
edges = add_knowledge_provenance(edges, CLICKSTREAM_TABLE.split(".")[-1], RID)

# COMMAND ----------

# DBTITLE 1,Write Gold -- wikipedia_edges
(
    edges.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(EDGES_TABLE)
)
print(f"OK  table written: {EDGES_TABLE}")

# COMMAND ----------

# DBTITLE 1,Read back Gold
written = spark.table(EDGES_TABLE)
total = written.count()
pair_rows = between.count()
print(f"OK  edges kept: {total}")

# COMMAND ----------

# DBTITLE 1,Gate -- one row per article pair
duplicates = (
    written.groupBy("prev_article_id", "curr_article_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
check(
    COMPONENT,
    SOURCE,
    "edge_pair_duplicates",
    duplicates == 0,
    detail=f"duplicate_groups={duplicates}",
    metric_value=duplicates,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Gate -- both ends selected and at least one edge
outside = (
    written.join(
        selected_ids.withColumnRenamed("article_id", "prev_article_id"),
        "prev_article_id",
        "left_anti",
    ).count()
    + written.join(
        selected_ids.withColumnRenamed("article_id", "curr_article_id"),
        "curr_article_id",
        "left_anti",
    ).count()
)
check(
    COMPONENT,
    SOURCE,
    "edges_between_selected_articles",
    total > 0 and outside == 0,
    detail=f"edges={total} ends_outside_selection={outside}",
    metric_value=total,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- wikipedia_edges
findings_blocks = inspect_knowledge_table(
    written,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["prev_article_id", "curr_article_id"],
    extra_checks={
        "link_edges_without_self_loops": link_count,
        "edges_with_both_ends_selected": pair_rows,
        "duplicate_pairs_removed": pair_rows - total,
        "edges_written": total,
        "selection_terms_hash": selection_hashes[0],
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- wikipedia_edges
write_knowledge_findings(
    SOURCE, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, findings_blocks
)
