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
# MAGIC articles, ordered by a hash of the id. Writes `knowledge.wikipedia_selected`
# MAGIC and its findings. Re-run this notebook alone after a change of the term lists.

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
from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = FINDINGS_SOURCE
COMPONENT = "knowledge/wikipedia/02_wikipedia_gold"
RID = run_id()
TABLE = "wikipedia_selected"

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
gold = add_knowledge_provenance(gold, SILVER_TABLE.split(".")[-1], RID)
(gold.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_TABLE))
print(f"OK  table written: {GOLD_TABLE}")

# COMMAND ----------

# DBTITLE 1,Read back Gold
written = spark.table(GOLD_TABLE)
total = written.count()
print(f"OK  {total} articles selected")

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

# DBTITLE 1,Gate -- cap per term and at least one article
check(
    COMPONENT,
    SOURCE,
    "selected_within_cap",
    0 < total <= terms.cap_per_term * len(terms.pairs),
    detail=f"selected={total} cap_per_term={terms.cap_per_term} terms={len(terms.pairs)}",
    metric_value=total,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- wikipedia_selected
term_checks = {
    f"term:{ecosystem}/{term}": (
        f"matched={per_term.get(index, {'matched': 0})['matched']} "
        f"kept={per_term.get(index, {'kept': 0})['kept']}"
    )
    for index, (ecosystem, term) in enumerate(terms.pairs)
}
findings_blocks = inspect_knowledge_table(
    written,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["article_id"],
    extra_checks={
        "rule_version": terms.version,
        "terms_hash": terms.file_hash,
        "cap_per_term": terms.cap_per_term,
        "articles_selected": total,
        "terms_without_match": sum(
            1
            for index in range(len(terms.pairs))
            if not per_term.get(index, {}).get("matched")
        ),
        "terms_at_cap": sum(
            1
            for index in range(len(terms.pairs))
            if per_term.get(index, {}).get("matched", 0) > terms.cap_per_term
        ),
        **term_checks,
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- wikipedia_selected
write_knowledge_findings(
    SOURCE, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, findings_blocks
)
