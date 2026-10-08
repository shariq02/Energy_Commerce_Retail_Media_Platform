# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- WIKIPEDIA RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** Cross-checks between the article frame and the clickstream
# MAGIC frame of `wikipedia-datasets` -- title and id overlap, the share of edges
# MAGIC and clicks that land on or leave from a known article, the most clicked
# MAGIC destinations and the clicks that fall on domain-term titles. Each overlap
# MAGIC is derived from one tagged union rather than repeated pairwise joins.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# MAGIC %run ../_samples_eda_common

# COMMAND ----------

# MAGIC %run ../_wikipedia_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "wikipedia"
NB_KEY = "03_wikipedia_relationships_and_findings"
SECTION_TITLE = "Cross-table relationships (articles and clickstream)"
ROOT_ARTICLES = f"{SAMPLES_VOLUME_ROOT}/wikipedia-datasets/data-001/en_wikipedia/articles-only-parquet"
ROOT_CLICKS = f"{SAMPLES_VOLUME_ROOT}/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json"
ARTICLE_HINTS = {
    "id": ("id", "page_id", "pageid", "curid", "article_id"),
    "title": ("title", "page_title", "article_title"),
}
CLICK_HINTS = {
    "prev_title": ("prev_title", "prev"),
    "curr_title": ("curr_title", "curr"),
    "curr_id": ("curr_id",),
    "count": ("n", "count", "clicks"),
    "type": ("type",),
}
LINK_TYPE = "link"
TOP_N = 20
LIST_CAP = 5000
DOMAIN_TERMS = {
    "energy": (
        "electricity",
        "power plant",
        "wind power",
        "solar power",
        "renewable energy",
        "electric grid",
    ),
    "weather": ("weather", "climate", "meteorology", "precipitation", "storm"),
    "commerce": ("retail", "e-commerce", "online shopping", "advertising", "consumer"),
    "aviation": ("airline", "airport", "aviation", "aircraft", "flight"),
    "mobility": ("electric vehicle", "charging station", "public transport", "railway"),
}

# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Load the article frame
a_entries, a_kinds, a_frames = load_volume_frames(ROOT_ARTICLES, cap=LIST_CAP)
require_frames(a_frames, ROOT_ARTICLES, a_kinds)
if len(a_entries) >= LIST_CAP:
    print(f"WARNING: the article listing reached the cap of {LIST_CAP}")
a_name = "parquet" if "parquet" in a_frames else next(iter(a_frames))
articles = a_frames[a_name]["df"].drop("__file")
print(
    f"articles: frame={a_name} files={len(a_frames[a_name]['paths'])} columns={articles.columns}"
)

# COMMAND ----------

# DBTITLE 1,Load the clickstream frame
c_entries, c_kinds, c_frames = load_volume_frames(ROOT_CLICKS, cap=LIST_CAP)
require_frames(c_frames, ROOT_CLICKS, c_kinds)
c_name = "json" if "json" in c_frames else next(iter(c_frames))
clicks = c_frames[c_name]["df"].drop("__file")
print(
    f"clickstream: frame={c_name} files={len(c_frames[c_name]['paths'])} columns={clicks.columns}"
)

# COMMAND ----------

# DBTITLE 1,Resolve column roles
a_role = resolve_roles(articles.columns, ARTICLE_HINTS)
c_role = resolve_roles(clicks.columns, CLICK_HINTS)
print(a_role)
print(c_role)

# COMMAND ----------

# DBTITLE 1,Link edges
links = clicks.where(as_str(c_role["type"]) == LINK_TYPE) if c_role["type"] else clicks
print(
    f"link filter: type == {LINK_TYPE!r}"
    if c_role["type"]
    else "no type column; all edges used"
)

# COMMAND ----------

# DBTITLE 1,Title overlap -- articles against current titles
ov_curr = None
if a_role["title"] and c_role["curr_title"]:
    ov_curr = key_overlap(articles, a_role["title"], links, c_role["curr_title"])
    print(ov_curr)

# COMMAND ----------

# DBTITLE 1,Title overlap -- articles against previous titles
ov_prev = None
if a_role["title"] and c_role["prev_title"]:
    ov_prev = key_overlap(articles, a_role["title"], links, c_role["prev_title"])
    print(ov_prev)

# COMMAND ----------

# DBTITLE 1,Id overlap -- article ids against current ids
ov_id = None
if a_role["id"] and c_role["curr_id"]:
    ov_id = key_overlap(
        articles, a_role["id"], links, c_role["curr_id"], normalise=False
    )
    print(ov_id)

# COMMAND ----------

# DBTITLE 1,Clicks landing on a known article
cov_in = None
if a_role["title"] and c_role["curr_title"] and c_role["count"]:
    cov_in = weighted_coverage(
        links, c_role["curr_title"], c_role["count"], articles, a_role["title"]
    )
    print(cov_in)

# COMMAND ----------

# DBTITLE 1,Clicks leaving from a known article
cov_out = None
if a_role["title"] and c_role["prev_title"] and c_role["count"]:
    cov_out = weighted_coverage(
        links, c_role["prev_title"], c_role["count"], articles, a_role["title"]
    )
    print(cov_out)

# COMMAND ----------

# DBTITLE 1,Most clicked destinations and whether an article exists
top_dest = []
if a_role["title"] and c_role["curr_title"] and c_role["count"]:
    top = (
        degree_table(links, c_role["curr_title"], c_role["count"])
        .orderBy(F.desc("clicks"))
        .limit(TOP_N)
        .collect()
    )
    norm = [r["__k"].replace("_", " ").strip().lower() for r in top]
    present = {
        r["k"]
        for r in articles.select(title_norm(a_role["title"]).alias("k"))
        .where(F.col("k").isin(norm))
        .collect()
    }
    top_dest = [
        (r["__k"], r["clicks"], n in present) for r, n in zip(top, norm, strict=True)
    ]
    for row in top_dest:
        print(row)

# COMMAND ----------

# DBTITLE 1,Link clicks on domain-term titles
mass = None
if c_role["curr_title"] and c_role["count"]:
    mass = title_term_mass(links, c_role["curr_title"], c_role["count"], DOMAIN_TERMS)
    print(mass)

# COMMAND ----------

# DBTITLE 1,Findings
print(f"title overlap (current): {ov_curr}")
print(f"clicks landing on a known article: {cov_in}")
print(f"clicks leaving from a known article: {cov_out}")

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/wikipedia.md
_overlap = []
for label, ov in (
    ("article titles against current titles of link edges", ov_curr),
    ("article titles against previous titles of link edges", ov_prev),
    ("article ids against current ids of link edges (exact match)", ov_id),
):
    if ov:
        _overlap.append(
            para(
                f"- {label}: {ov['a_distinct']} article keys, {ov['b_distinct']} edge keys, {ov['both']} in both;",
                f"{ov['a_share_in_b']} of the article keys appear in the edges, {ov['b_share_in_a']} of the edge keys have an article.",
            )
        )
if not _overlap:
    _overlap.append("- No usable key pair located in both frames.")

_coverage = []
for label, cov in (
    ("clicks landing on a known article", cov_in),
    ("clicks leaving from a known article", cov_out),
):
    if cov:
        _coverage.append(
            para(
                f"- {label} (link edges): {cov['edges_matched']} of {cov['edges']} edges ({cov['edge_share']}),",
                f"{fmt_num(cov['weight_matched'])} of {fmt_num(cov['weight'])} clicks ({cov['weight_share']}).",
            )
        )
if top_dest:
    _coverage.append(
        f"- most clicked destinations (title, clicks, article exists): {top_dest}"
    )

_domain = []
if mass:
    _domain.append(
        f"- link clicks on titles matching domain terms (of {fmt_num(mass['weight'])} clicks): { {k: fmt_num(v) for k, v in mass.items() if k != 'weight'} }."
    )
    _domain.append(f"- term lists used: {DOMAIN_TERMS}. A proxy, not a selection rule.")

_findings_md = (
    f"- article titles in the link edges: {ov_curr['a_share_in_b'] if ov_curr else 'n/a'}; "
    f"link clicks landing on a known article: {cov_in['weight_share'] if cov_in else 'n/a'}"
)

_areas = {
    "Domain understanding": [
        "the article frame holds page text; the clickstream holds weighted page-to-page edges between the same pages",
    ],
    "Structure and engineering": [
        f"keys: article {a_role}; clickstream {c_role}",
    ],
    "Temporal": [
        "the two frames carry no common time column; the snapshots may differ in date",
    ],
    "Spatial": [],
    "Data quality": [
        f"edge titles without an article: {ov_curr['b_distinct'] - ov_curr['both'] if ov_curr else 'n/a'} of {ov_curr['b_distinct'] if ov_curr else 'n/a'} distinct current titles",
    ],
    "Statistical patterns": [
        f"click weight on known articles: {cov_in['weight_share'] if cov_in else 'n/a'} (landing), {cov_out['weight_share'] if cov_out else 'n/a'} (leaving)",
    ],
    "Relationships": [
        f"title overlap shares: article keys in edges {ov_curr['a_share_in_b'] if ov_curr else 'n/a'}, edge keys with an article {ov_curr['b_share_in_a'] if ov_curr else 'n/a'}",
        f"id overlap: {ov_id if ov_id else 'no id pair located'}",
    ],
    "Analytics use": [
        f"link clicks on domain-term titles: { {k: fmt_num(v) for k, v in (mass or {}).items() if k != 'weight'} }",
    ],
    "ML use": [],
    "AI / knowledge use": [
        "articles and link edges join on the normalised title; the overlap shares above decide how much of the graph attaches to article text",
    ],
}

_corpus = [
    "- Join key: the overlap block shows whether the title or the id joins the two frames and how much falls out.",
    "- Graph without text: edge titles that have no article cannot carry text; their share is in the coverage block.",
    "- Text without graph: article keys that appear in no link edge have no neighbours.",
    "- Selection: the domain-term clicks are the only relevance signal in the clickstream; they match on the title text only.",
]

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Key Overlap", "\n".join(_overlap)),
        ("Click Coverage", "\n".join(_coverage)),
        ("Domain-Term Clicks", "\n".join(_domain)),
        ("EDA Findings", _findings_md),
        ("Observations by Area", area_block(_areas)),
        ("Corpus Implications", "\n".join(_corpus)),
    ],
)