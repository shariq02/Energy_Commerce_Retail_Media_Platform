# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # KNOWLEDGE GOLD -- SELECTED WIKIPEDIA ARTICLES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** select articles from `knowledge.wikipedia_article` with the term
# MAGIC lists of `ai/knowledge_corpus/wikipedia/selection_terms.yml`. A term matches
# MAGIC the title or the lead section (whole word, any case). The first matching term
# MAGIC in file order owns the article; each term keeps at most `cap_per_term`
# MAGIC articles, ordered by a hash of the id. Writes `knowledge.wikipedia_selected`.
# MAGIC Re-run this notebook alone after a change of the term lists. Also writes
# MAGIC `knowledge.wikipedia_build_report` (the Silver and Gold reports in one row).

# COMMAND ----------

# DBTITLE 1,Text library
# MAGIC %run ./_wikipedia_text

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ./_wikipedia_common

# COMMAND ----------

# DBTITLE 1,Imports
from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Load the selection terms
terms = load_selection_terms()
print(
    f"OK  rule version {terms.version}, {len(terms.pairs)} terms, "
    f"cap {terms.cap_per_term} per term"
)

# COMMAND ----------

# DBTITLE 1,Read the kept articles
kept = spark.table(SILVER_TABLE).filter(F.col("status") == "kept")
lead = F.expr("try_element_at(sections, 1)")
kept = kept.withColumn("lead_text", F.when(lead["section"] == LEAD_NAME, lead["text"]))

# COMMAND ----------

# DBTITLE 1,Match the terms in the title and the lead section
hits = [
    F.col("title").rlike(term_pattern(term))
    | F.coalesce(F.col("lead_text").rlike(term_pattern(term)), F.lit(False))
    for _, term in terms.pairs
]
term_index = reduce(
    lambda rest, item: F.when(item[1], F.lit(item[0])).otherwise(rest),
    reversed(list(enumerate(hits))),
    F.lit(None).cast("int"),
)
matched = kept.withColumn("term_index", term_index).filter(
    F.col("term_index").isNotNull()
)

# COMMAND ----------

# DBTITLE 1,Order the matches by a hash of the id and apply the cap
rank_window = Window.partitionBy("term_index").orderBy("id_hash", "article_id")
ranked = matched.withColumn(
    "id_hash", F.xxhash64(F.col("article_id").cast("string"))
).withColumn("rank", F.row_number().over(rank_window))
selected = ranked.filter(F.col("rank") <= terms.cap_per_term)

# COMMAND ----------

# DBTITLE 1,Count the matches and the kept articles per term
counts = (
    ranked.groupBy("term_index")
    .agg(
        F.count(F.lit(1)).alias("matched"),
        F.sum((F.col("rank") <= terms.cap_per_term).cast("long")).alias("kept"),
    )
    .collect()
)
per_term = {
    r["term_index"]: {"matched": r["matched"], "kept": r["kept"]} for r in counts
}
for index, (ecosystem, term) in enumerate(terms.pairs):
    stats = per_term.get(index, {"matched": 0, "kept": 0})
    print(
        f"  {ecosystem:<9} {term:<20} "
        f"matched {stats['matched']:>8}  kept {stats['kept']:>6}"
    )

# COMMAND ----------

# DBTITLE 1,Write Gold -- wikipedia_selected
gold = selected.select(
    "article_id",
    "title",
    "revision_id",
    "revision_timestamp",
    "term_index",
    F.col("rank").alias("term_rank"),
    F.lit(terms.version).alias("rule_version"),
    F.lit(terms.file_hash).alias("terms_hash"),
    "sections",
    "snapshot",
)
(gold.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_TABLE))
print(f"OK  table written: {GOLD_TABLE}")

# COMMAND ----------

# DBTITLE 1,Check -- unique ids and the cap
written = spark.table(GOLD_TABLE)
total = written.count()
duplicates = written.groupBy("article_id").count().filter(F.col("count") > 1).count()
assert duplicates == 0, f"{duplicates} article ids appear more than once"
assert total <= terms.cap_per_term * len(terms.pairs)
assert total > 0, "no article was selected"
print(f"OK  {total} articles selected")

# COMMAND ----------

# DBTITLE 1,Write the Gold report
write_report(
    REPORT_DIR,
    "gold_report.json",
    {
        "rule_version": terms.version,
        "terms_hash": terms.file_hash,
        "cap_per_term": terms.cap_per_term,
        "articles_selected": total,
        "terms": [
            {
                "ecosystem": ecosystem,
                "term": term,
                **per_term.get(index, {"matched": 0, "kept": 0}),
            }
            for index, (ecosystem, term) in enumerate(terms.pairs)
        ],
    },
)
print(f"OK  report: {REPORT_DIR}/gold_report.json")

# COMMAND ----------

# DBTITLE 1,Write the build report table
build_report = {
    "silver": read_report(REPORT_DIR, "silver_report.json"),
    "gold": read_report(REPORT_DIR, "gold_report.json"),
}
report_row = [(json.dumps(build_report, ensure_ascii=False),)]
(
    spark.createDataFrame(report_row, ["report_json"])
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(REPORT_TABLE)
)
print(f"OK  table written: {REPORT_TABLE}")
