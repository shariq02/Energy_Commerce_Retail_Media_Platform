# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- SEARCH VISIBILITY RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** search_visibility_events <-> search_visibility_repository
# MAGIC joinability and referential integrity on repository_id, plus a findings
# MAGIC summary for src/schemas/profiling/search_visibility.md. The repository
# MAGIC table is small, so it is collected once and joined in Python.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"
SOURCE = "search_visibility"
NB_KEY = "02_relationships_and_findings"
SECTION_TITLE = "events <-> repository relationship (search_visibility)"
EVENTS = f"{CATALOG}.{BRONZE_SCHEMA}.search_visibility_events"
REPOSITORY = f"{CATALOG}.{BRONZE_SCHEMA}.search_visibility_repository"


# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()

print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

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


def _pct(sorted_vals, q):
    if not sorted_vals:
        return None
    i = min(len(sorted_vals) - 1, int(q * len(sorted_vals)))
    return sorted_vals[i]


_ev_counts = sorted(x["events"] for x in evr)
events_per_repo = {
    "child_rows": sum(_ev_counts),
    "distinct_parents_referenced": len(_ev_counts),
    "p50": _pct(_ev_counts, 0.5),
    "p90": _pct(_ev_counts, 0.9),
    "p99": _pct(_ev_counts, 0.99),
    "max_fanout": _ev_counts[-1] if _ev_counts else 0,
    "min": _ev_counts[0] if _ev_counts else 0,
}
print("events per repository distribution:", events_per_repo)

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

# DBTITLE 1,Figures (each gated so a blank result is never referenced)
figs = []
if barplot(
    [(x["repository_id"], x["distinct_dates"]) for x in evr],
    "Search Visibility -- distinct dates per repository_id",
    "repository_id",
    "dates",
    rot=90,
    filename="sv_distinct_dates_per_repository.png",
):
    figs.append(
        (
            "Search Visibility -- distinct dates per repository",
            "sv_distinct_dates_per_repository.png",
        )
    )
if barplot(
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
    filename="sv_repository_coverage_integrity.png",
):
    figs.append(
        (
            "Search Visibility -- repository_id coverage & integrity",
            "sv_repository_coverage_integrity.png",
        )
    )
if barplot(
    per_repo_pairs,
    "Search Visibility -- events per repository_id (top 30)",
    "repository_id",
    "events",
    rot=90,
    filename="sv_events_per_repository.png",
):
    figs.append(
        (
            "Search Visibility -- events per repository_id (top 30)",
            "sv_events_per_repository.png",
        )
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

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/search_visibility.md
_entities = [
    (
        f"Repository table: rows={repo_total}, columns={repo.columns}, key=`{repo_key}`, "
        f"key_unique={repo_key_unique}, distinct ids={len(repo_ids)}."
    ),
    (
        f"Events table: rows={ev_total}, approx distinct repository_id={S['events_distinct_repo']}, "
        f"overall distinct dates={overall_dates}."
    ),
]

_rel = [
    f"events <-> repository cardinality on repository_id: {kind}.",
    (
        f"Full cardinality profile: {events_per_repo['child_rows']} event rows across "
        f"{events_per_repo['distinct_parents_referenced']} referenced repositories; "
        f"events per repository min={events_per_repo['min']}, p50={events_per_repo['p50']}, "
        f"p90={events_per_repo['p90']}, p99={events_per_repo['p99']}, "
        f"max fan-out={events_per_repo['max_fanout']}."
    ),
    (
        f"Row-level match of events against the repository id set: {matched} / {ev_total} "
        f"({matched / ev_total * 100:.2f}%) matched, {ev_total - matched} unmatched."
    ),
    (
        f"Orphan event repository_ids (not in repository table): {orphan_events} "
        f"(e.g. {sorted(event_repo_ids - repo_ids)[:20]})."
    ),
    (
        f"Unused repository ids (never referenced by events): {unused_repo} "
        f"(e.g. {sorted(repo_ids - event_repo_ids)[:20]})."
    ),
    f"Repositories not present in every date/month: {partial} of {len(event_repo_ids)}.",
]

_findings = []
if not repo_key_unique:
    _findings.append(
        f"- Repository key `{repo_key}` is NOT unique in the repository table."
    )
if orphan_events:
    _findings.append(
        f"- {orphan_events} event repository_ids have no matching repository row "
        f"({ev_total - matched} event rows affected) -- referential integrity is not enforced."
    )
if unused_repo:
    _findings.append(
        f"- {unused_repo} repository rows are never referenced by any event."
    )
if partial:
    _findings.append(f"- {partial} repositories have partial date/month coverage.")
if not _findings:
    _findings.append(
        "- events.repository_id is a clean foreign key into the repository table."
    )
_findings_md = "\n".join(_findings)

_silver = []
_silver.append(
    f"- Join events to repository on repository_id ({kind}); "
    + (
        "dedupe repository on the key first."
        if not repo_key_unique
        else "repository key is unique."
    )
)
if orphan_events:
    _silver.append(
        "- Use a left join and flag/quarantine events with an unresolved repository_id; "
        "do not drop them silently."
    )
if unused_repo:
    _silver.append(
        "- Unused repository rows are acceptable as a dimension; no action required."
    )

_ml = ml_readiness_block(
    [
        (
            "Grain / grain drift",
            (
                f"repository is one row per repository_id (unique={repo_key_unique}); events is one row "
                "per (repository, url, month, country, device). A repository-attribute join holds the "
                "events grain only if the repository key is unique."
            ),
        ),
        (
            "Join multiplication (1:N / M:N expansion)",
            f"events -> repository is {kind}; events per repository p50={events_per_repo['p50']}, "
            f"p99={events_per_repo['p99']}, max={events_per_repo['max_fanout']}. "
            + (
                "repository key is unique -> safe 1:N (repository attributes fan out to that many event "
                "rows, as intended)."
                if repo_key_unique
                else "repository key is NOT unique -> a naive join fans out every event row across the "
                "duplicate repository rows; de-duplicate first."
            ),
        ),
        (
            "Target contamination",
            (
                "No target in the repository dimension -- see 01. A repository-level aggregate feature "
                "(e.g. mean CTR) computed over all events includes the row being scored."
            ),
        ),
        (
            "Temporal / post-event leakage",
            (
                "repository attributes carry no date -- joining them to a dated event assumes they were "
                "constant; if ir_platform / name changed over the archive span that is point-in-time "
                "leakage."
            ),
        ),
        (
            "Proxy leakage",
            (
                "repository_id and the derived index are near-unique identifiers for one corpus -- a model "
                "given them memorises the corpus rather than learning transferable ranking behaviour."
            ),
        ),
        (
            "Split / entity leakage",
            (
                "Split any repository-enriched model by repository_id, not by row -- a repository's events "
                "are correlated and must stay on one side."
            ),
        ),
        (
            "Historical-reference (point-in-time) leakage",
            (
                "Same as Temporal: no as-of column on repository attributes; use the value valid during "
                "the archive month if the dimension ever changes."
            ),
        ),
        (
            "Survivorship / coverage bias",
            (
                f"{unused_repo} repository rows are never referenced by events; {partial} of "
                f"{len(event_repo_ids)} referenced repositories have partial month coverage. A panel model "
                "over-weights the full-history repositories and treats missing months as zeros."
            ),
        ),
        (
            "Missingness leakage",
            (
                f"{orphan_events} event repository_ids ({ev_total - matched} rows) have no repository row "
                "-- whether a repository is 'known' correlates with how well it is instrumented; an "
                "'is-resolved' flag leaks that."
            ),
        ),
        (
            "Duplicate-event leakage",
            (
                "Not assessed at this level -- see 01 for the (repository, url, date, country, device) "
                "duplicate-key composition."
            ),
        ),
        (
            "Target / feature temporal misalignment",
            (
                "Not applicable in this join audit -- the events and repository tables have no competing "
                "timestamps."
            ),
        ),
        (
            "Unit / sign / circular-feature leakage",
            "Not applicable -- no numeric measures joined here.",
        ),
        (
            "Data-generation-process leakage",
            (
                "repository_id is assigned by the aggregator's own crawl configuration -- it encodes which "
                "corpora were in scope, not a property of search behaviour."
            ),
        ),
        (
            "Class / label instability",
            "ir_platform and other repository categoricals are small stable enums; no label here.",
        ),
        ("Label availability lag", "Not applicable -- static dimension."),
        (
            "Source / version / regime change",
            (
                "The repository set is a 2017 snapshot of instrumented corpora -- repositories added or "
                "dropped since are absent; do not extrapolate the coverage to today."
            ),
        ),
        (
            "Sample-vs-full divergence",
            (
                "Every statistic here (match rate, orphan counts, per-repository coverage) is a full Spark "
                "aggregation or a fully collected small table -- no sampling."
            ),
        ),
    ]
)

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    [
        ("Entities / Keys", "\n".join(_entities)),
        ("Relationships", "\n".join(_rel)),
        ("EDA Findings", _findings_md),
        ("ML-Readiness Evidence", _ml),
        ("Silver Implications", "\n".join(_silver)),
    ],
    figures=figs,
)
