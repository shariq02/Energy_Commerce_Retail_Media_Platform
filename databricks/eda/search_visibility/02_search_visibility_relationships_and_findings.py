# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- SEARCH VISIBILITY RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** search_visibility_events <-> search_visibility_repository
# MAGIC joinability and referential integrity on repository_id, plus a findings
# MAGIC summary for src/schemas/profiling/search_visibility.md. The repository
# MAGIC table is small, so it is collected once and joined in Python.

# COMMAND ----------

# DBTITLE 1,Imports
import matplotlib.pyplot as plt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
EVENTS = f"{CATALOG}.{BRONZE_SCHEMA}.search_visibility_events"
REPOSITORY = f"{CATALOG}.{BRONZE_SCHEMA}.search_visibility_repository"

# COMMAND ----------


# DBTITLE 1,Helper
def find_col(df: DataFrame, *candidates: str) -> str | None:
    lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def barplot(pairs, title, xlabel, ylabel="count", rot=0):
    plt.figure(figsize=(9, 4))
    plt.bar([str(p[0]) for p in pairs], [p[1] for p in pairs])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=rot, ha="right" if rot else "center")
    plt.tight_layout()
    plt.show()


# COMMAND ----------

# DBTITLE 1,Repository table -- collect once, profile in Python
repo = spark.table(REPOSITORY)
repo_key = find_col(repo, "repository_id", "id", "repositoryId") or repo.columns[0]
repo_recs = [x.asDict() for x in repo.collect()]
repo_total = len(repo_recs)
repo_ids = {str(d[repo_key]) for d in repo_recs if d[repo_key] is not None}
repo_key_unique = repo_total == len(repo_ids)
print(
    f"repository rows={repo_total}  columns={repo.columns}  key={repo_key}  key_unique={repo_key_unique}"
)
for c in repo.columns:
    miss = sum(1 for d in repo_recs if d[c] is None or str(d[c]).strip() == "")
    print(f"  {c:<24} missing={miss}  distinct={len({d[c] for d in repo_recs})}")
for d in repo_recs[:100]:
    print("  ", d)

# COMMAND ----------

# DBTITLE 1,Events per repository_id + date coverage (one groupBy, collected)
events = spark.table(EVENTS)
evr = (
    events.groupBy(F.col("repository_id").cast("string").alias("repository_id"))
    .agg(
        F.count(F.lit(1)).alias("events"),
        F.countDistinct("date").alias("distinct_dates"),
        F.min("date").alias("first_date"),
        F.max("date").alias("last_date"),
    )
    .orderBy(F.desc("events"))
    .collect()
)
event_repo_ids = {x["repository_id"] for x in evr}
per_repo_pairs = [(x["repository_id"], x["events"]) for x in evr[:30]]
for x in evr[:50]:
    print(x.asDict())

# COMMAND ----------

# DBTITLE 1,Events summary + row-level match against the repository id set (one agg)
repo_id_list = list(repo_ids)
S = (
    events.agg(
        F.count(F.lit(1)).alias("ev_total"),
        F.approx_count_distinct("repository_id").alias("events_distinct_repo"),
        F.countDistinct("date").alias("overall_dates"),
        F.sum(
            F.col("repository_id").cast("string").isin(repo_id_list).cast("long")
        ).alias("matched_rows"),
    )
    .first()
    .asDict()
)
ev_total = S["ev_total"]
matched = S["matched_rows"]
overall_dates = S["overall_dates"]
orphan_events = len(event_repo_ids - repo_ids)
unused_repo = len(repo_ids - event_repo_ids)
kind = "1:N (one repository -> many events)" if repo_key_unique else "N:N"
partial = sum(1 for x in evr if x["distinct_dates"] < overall_dates)
print(
    f"events={ev_total}  distinct repository_id~={S['events_distinct_repo']}  matched rows={matched}  "
    f"unmatched rows={ev_total - matched}"
)
print(
    f"orphan event repository_ids (not in repository table)={orphan_events}  e.g. {sorted(event_repo_ids - repo_ids)[:20]}"
)
print(
    f"unused repository ids (never in events)={unused_repo}  e.g. {sorted(repo_ids - event_repo_ids)[:20]}"
)
print(f"events <-> repository cardinality: {kind}")
print(
    f"overall distinct dates={overall_dates}  repositories NOT present in every date={partial} of {len(event_repo_ids)}"
)

# COMMAND ----------

# DBTITLE 1,Figure -- coverage, integrity, events per repository
barplot(
    [(x["repository_id"], x["distinct_dates"]) for x in evr],
    "Search Visibility -- distinct dates per repository_id",
    "repository_id",
    "dates",
    rot=90,
)
barplot(
    [
        ("repository ids", len(repo_ids)),
        ("event repository_ids", len(event_repo_ids)),
        ("orphan events", orphan_events),
        ("unused repo rows", unused_repo),
    ],
    "Search Visibility -- repository_id coverage & integrity",
    "",
    "count",
    rot=30,
)
barplot(
    per_repo_pairs,
    "Search Visibility -- events per repository_id (top 30)",
    "repository_id",
    "events",
    rot=90,
)

# COMMAND ----------

# DBTITLE 1,Findings
print(
    f"repository rows={repo_total}  distinct ids={len(repo_ids)}  key_unique={repo_key_unique}"
)
print(
    f"events distinct repository_id~={S['events_distinct_repo']}  matched rows={matched}/{ev_total}"
)
print(f"orphan events={orphan_events}  unused repository rows={unused_repo}")
print(f"cardinality: {kind}  repositories missing months: {partial}")
