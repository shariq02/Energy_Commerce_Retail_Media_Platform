# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # KNOWLEDGE SILVER -- WIKIPEDIA ARTICLES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** read the `wikipedia-datasets` article parquet in place, keep the
# MAGIC latest revision per article id, clean the wikitext and split it into sections.
# MAGIC One row per article in `knowledge.wikipedia_article` with a `status`
# MAGIC (`kept`, `redirect` or `empty`). The source table is never changed.
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
import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = FINDINGS_SOURCE
COMPONENT = "knowledge/wikipedia/01_wikipedia_silver"
RID = run_id()
TABLE = "wikipedia_article"

# COMMAND ----------

# DBTITLE 1,Read the article table
articles = spark.read.parquet(SOURCE_ROOT).select(
    "id", "title", "revisionId", "revisionTimestamp", "text"
)
raw_rows = articles.count()
print(f"OK  rows read: {raw_rows}")

# COMMAND ----------

# DBTITLE 1,Keep the latest revision per article id
latest_window = Window.partitionBy("id").orderBy(
    F.col("revisionTimestamp").desc(), F.col("revisionId").desc()
)
latest = (
    articles.withColumn("_rank", F.row_number().over(latest_window))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)

# COMMAND ----------

# DBTITLE 1,Define the section splitter
SECTIONS_TYPE = T.ArrayType(
    T.StructType(
        [
            T.StructField("index", T.IntegerType()),
            T.StructField("section", T.StringType()),
            T.StructField("text", T.StringType()),
        ]
    )
)


@F.pandas_udf(SECTIONS_TYPE)
def split_text(texts: pd.Series) -> pd.Series:
    return texts.map(lambda t: [] if is_redirect(t or "") else split_sections(t or ""))


# COMMAND ----------

# DBTITLE 1,Clean and split the text
silver = (
    latest.withColumn("sections", split_text(F.col("text")))
    .withColumn(
        "status",
        F.when(F.col("text").rlike(r"(?i)^\s*#redirect"), "redirect")
        .when(F.size("sections") == 0, "empty")
        .otherwise("kept"),
    )
    .withColumn("sections", F.when(F.col("status") == "kept", F.col("sections")))
    .select(
        F.col("id").alias("article_id"),
        "title",
        F.col("revisionId").alias("revision_id"),
        F.col("revisionTimestamp").alias("revision_timestamp"),
        "status",
        F.size("sections").alias("section_count"),
        "sections",
        F.lit(SNAPSHOT).alias("snapshot"),
    )
)
silver = add_knowledge_provenance(silver, "articles-only-parquet", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- wikipedia_article
(
    silver.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)
print(f"OK  table written: {SILVER_TABLE}")

# COMMAND ----------

# DBTITLE 1,Count the rows by status
written = spark.table(SILVER_TABLE)
status_counts = written.groupBy("status").agg(F.count("*").alias("n")).collect()
by_status = {r["status"]: r["n"] for r in status_counts}
distinct_ids = sum(by_status.values())
sections_total = written.agg(F.sum("section_count")).first()[0] or 0
print(by_status)

# COMMAND ----------

# DBTITLE 1,Gate -- one row per article id
duplicates = written.groupBy("article_id").count().filter(F.col("count") > 1).count()
check(
    COMPONENT,
    SOURCE,
    "article_id_duplicates",
    duplicates == 0,
    detail=f"duplicate_groups={duplicates}",
    metric_value=duplicates,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Gate -- at least one kept article
check(
    COMPONENT,
    SOURCE,
    "kept_articles",
    by_status.get("kept", 0) > 0,
    detail=f"by_status={by_status}",
    metric_value=by_status.get("kept", 0),
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- wikipedia_article
findings_blocks = inspect_knowledge_table(
    written,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["article_id"],
    extra_checks={
        "rows_read": raw_rows,
        "distinct_article_ids": distinct_ids,
        "duplicate_rows_removed": raw_rows - distinct_ids,
        "articles_kept": by_status.get("kept", 0),
        "articles_redirect": by_status.get("redirect", 0),
        "articles_empty": by_status.get("empty", 0),
        "sections_total": int(sections_total),
        "snapshot": SNAPSHOT,
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- wikipedia_article
write_knowledge_findings(
    SOURCE, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, findings_blocks
)
