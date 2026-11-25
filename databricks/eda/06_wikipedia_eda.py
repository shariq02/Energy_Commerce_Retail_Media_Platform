# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- WIKIPEDIA
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** exploratory analysis of the Wikipedia data in one notebook. Each section profiles one group of
# MAGIC tables and writes its findings to `src/schemas/profiling/<source>.md` (one section key per group).

# COMMAND ----------

# MAGIC %run ./_eda_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- WIKIPEDIA ARTICLE TEXT (SAMPLES)
# MAGIC
# MAGIC **Purpose:** Profile the `wikipedia-datasets` article parquet read in place
# MAGIC from the Databricks Samples Volume (no Bronze table) -- file inventory and
# MAGIC provenance, schema and nesting, missingness, title and id keys, title
# MAGIC forms, text length and section length (word and character proxies),
# MAGIC length after stripping markup, markup, character issues, duplicate texts
# MAGIC and duplicate keys, revision timestamps and domain-term coverage -- as
# MAGIC evidence.

# COMMAND ----------

# DBTITLE 1,Configuration and frame intake
from pyspark.sql import functions as F

SOURCE = "wikipedia"
NB_KEY = "01_wikipedia_articles"
SECTION_TITLE = "Wikipedia article text (Samples: wikipedia-datasets)"
ROOT = f"{SAMPLES_VOLUME_ROOT}/wikipedia-datasets/data-001/en_wikipedia/articles-only-parquet"
NAME_HINTS = {
    "id": ("id", "page_id", "pageid", "curid", "article_id"),
    "title": ("title", "page_title", "article_title"),
    "text": ("text", "body", "content", "article", "wikitext"),
    "revision": ("revisionid", "revision_id"),
    "timestamp": (
        "timestamp",
        "revisiontimestamp",
        "revision_timestamp",
        "last_modified",
        "date",
    ),
}
LIST_CAP = 5000
TEXT_SAMPLE_FRACTION = 0.02
SAMPLE_SEED = 7
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
print(f"OK  repo root: {_repo_root()}")
print(f"OK  profiling directory: {_profiling_dir()}")
INFO = frame_intake(ROOT, NAME_HINTS, "parquet", cap=LIST_CAP, text_fallback=True)
role, d0, prof, nested = INFO["role"], INFO["d0"], INFO["prof"], INFO["nested"]
entries, kinds, FRAME, DATA_COLS = (
    INFO["entries"],
    INFO["kinds"],
    INFO["frame"],
    INFO["data_cols"],
)

# COMMAND ----------

# DBTITLE 1,Keys, title forms and text measures
key_cands = [role[r] for r in ("id", "title") if role[r]]
uniq = exact_uniqueness(d0, key_cands)
for c, u in uniq.items():
    print(c, u)
forms = tstats = sstats = markup = chars = text_dups = None
if role["title"]:
    forms = title_forms(d0, role["title"])
    print(forms)
else:
    print("no title column located")
if role["text"]:
    tstats = text_stats(d0, role["text"])
    print(tstats)
    sstats = section_stats(
        d0, role["text"], fraction=TEXT_SAMPLE_FRACTION, seed=SAMPLE_SEED
    )
    print(sstats)
    markup = markup_indicators(d0, role["text"])
    print(markup)
    chars = char_issues(d0, role["text"])
    print(chars)
    text_dups = text_duplicates(d0, role["text"])
    print(text_dups)
else:
    print("no text column located")

# COMMAND ----------

# DBTITLE 1,Duplicate keys and date columns


def vary_exprs(exclude):
    cand = {"id": role["id"], "revision": role["revision"], "title": role["title"]}
    out = {k: as_str(c) for k, c in cand.items() if c and k != exclude}
    if role["text"]:
        out["text"] = F.xxhash64(as_str(role["text"]))
    return out


id_dups = title_dups = None
if role["id"]:
    id_dups = key_duplicate_profile(d0, role["id"], vary_exprs("id"))
    print(id_dups)
if role["title"]:
    title_dups = key_duplicate_profile(d0, role["title"], vary_exprs("title"))
    print(title_dups)
ts_info, tprof = None, None
if role["timestamp"]:
    tcol = role["timestamp"]
    ttype = dict(d0.dtypes)[tcol]
    if ttype in ("timestamp", "timestamp_ntz", "date"):
        tprof = timestamp_profile(d0, tcol)
        print(tprof)
    else:
        ts_info = timestamp_semantics(
            d0, tcol, valid_from="2001-01-01", tz="unspecified"
        )
        print(ts_info)
else:
    print("no date or timestamp column located")

# COMMAND ----------

# DBTITLE 1,Cleaned length, domain terms, length histograms and figure
cstats = domain_text = domain_title = None
len_hist = {}
if role["text"]:
    cstats = cleaned_length_stats(
        d0, role["text"], fraction=TEXT_SAMPLE_FRACTION, seed=SAMPLE_SEED
    )
    print(cstats)
    domain_text = domain_term_hits(
        d0,
        role["text"],
        DOMAIN_TERMS,
        fraction=TEXT_SAMPLE_FRACTION,
        seed=SAMPLE_SEED,
    )
    print(domain_text)
    txt = as_str(role["text"])
    len_hist["words per article"] = log2_histogram(d0, word_count(txt))
    len_hist["characters per article"] = log2_histogram(d0, F.length(txt))
    for k, v in len_hist.items():
        print(k, v)
if role["title"]:
    domain_title = domain_term_hits(d0, role["title"], DOMAIN_TERMS)
    print(domain_title)
figs = []
if len_hist and facet_bars(
    len_hist,
    "Wikipedia articles -- text length (power-of-two buckets)",
    "wikipedia_article_length.png",
    rot=45,
    ncols=2,
    logy=True,
):
    figs.append(
        (
            "Wikipedia articles -- text length (power-of-two buckets)",
            "wikipedia_article_length.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/wikipedia.md
head_blocks, missing = intake_blocks(INFO, ROOT, "rows")
_dq = [missing]
if text_dups:
    _dq.append(
        f"Duplicate texts: {text_dups['dup_groups']} groups holding {text_dups['rows_in_dup_groups']} of {text_dups['rows']} rows."
    )
_dq += [
    f"Duplicate {label} keys: {kd['dup_keys']} of {kd['keys']} keys repeat, holding {kd['rows_in_dup_keys']} rows (most rows per key {kd['max_rows_per_key']}); keys whose rows differ in: { {k[7:]: v for k, v in kd.items() if k.startswith('varies_')} }."
    for label, kd in (("id", id_dups), ("title", title_dups))
    if kd
]
if chars:
    _dq.append(
        f"Character issues: replacement character in {chars['replacement_char_rows']} rows, control characters in {chars['control_char_rows']} rows, non-ASCII in {chars['non_ascii_rows']} rows (share of characters {chars['non_ascii_char_share']})."
    )
if markup:
    _dq.append(
        f"Markup (rows containing the pattern): { {k: v for k, v in markup.items() if k != 'rows'} }."
    )

_entities = ["Key candidates (column: distinct / ratio-to-rows / unique):"]
_entities += [
    f"- `{c}`: {u['distinct']} / {u['ratio']} / unique={u['unique']}"
    for c, u in uniq.items()
]
if not uniq:
    _entities.append("- no id or title column located by name.")
if forms:
    _entities.append(
        para(
            f"- title forms: {forms['empty']} empty, {forms['with_underscore']} with underscore, {forms['edge_space']} with edge space,",
            f"{forms['with_colon']} with colon, {forms['disambiguation']} disambiguation, {forms['list_titles']} list titles,",
            f"{forms['digits_only']} digits only, {forms['starts_lowercase']} starting lowercase;",
            f"after trimming, lower-casing and replacing underscores: {forms['collision_groups']} colliding groups over {forms['collision_rows']} rows.",
        )
    )

_text = []
if tstats:
    _text += [
        f"- articles {tstats['rows']}; empty {tstats['empty']}; under 200 characters {tstats['under_200_chars']}; total words {tstats['total_words']}; longest article {tstats['max_chars']} characters.",
        f"- words per article, quantiles p1/p25/p50/p75/p95/p99: {[tstats['words_q'][p] for p in QUANTILE_PROBS]}.",
        f"- characters per article, same quantiles: {[tstats['chars_q'][p] for p in QUANTILE_PROBS]}.",
        f"- articles above {WORDS_PER_CHUNK_PROXY} words (proxy for one chunk): {tstats['over_proxy_words']}.",
    ]
if sstats:
    _text += [
        f"- sections (pieces between inline heading markers) in a {sstats['sample_fraction']:.0%} row sample of {sstats['sampled_articles']} articles: {sstats['sections']}; articles with at least one marker {sstats['with_marker']}; sections per article p50/p95 {sstats['per_article_p50_p95']}, max {sstats['per_article_max']}.",
        f"- words per section quantiles p1/p25/p50/p75/p95/p99: {[sstats['words_q'][p] for p in QUANTILE_PROBS]}; longest {sstats['max_words']}.",
        f"- sections above {WORDS_PER_CHUNK_PROXY} words (proxy for a 256-token limit): {sstats['over_proxy']} of {sstats['sections']}. Words are a proxy; the token count belongs to the build that chunks the text.",
    ]
if cstats:
    _text += [
        f"- after stripping references, templates, links, tags, URLs and quote marks by regular expressions ({cstats['sample_fraction']:.0%} row sample of {cstats['rows']} articles): {cstats['clean_words']} of {cstats['raw_words']} words kept ({cstats['kept_share']}); cleaned words per article quantiles p1/p25/p50/p75/p95/p99 {[cstats['words_q'][p] for p in QUANTILE_PROBS]}; under 50 cleaned words {cstats['under_50']}; above {WORDS_PER_CHUNK_PROXY} words {cstats['over_proxy']}.",
    ]
if not _text:
    _text.append("- No text column located.")

_dist = [
    f"- {k} (bucket, articles): " + fmt_pairs(pairs_, n=30).replace("\n", " ")
    for k, pairs_ in len_hist.items()
]

_temporal = []
if ts_info and ts_info.get("lines"):
    _temporal += [f"- {ln}" for ln in ts_info["lines"]]
if tprof:
    _temporal += [
        f"- `{role['timestamp']}` (timestamp): {tprof['lo']} .. {tprof['hi']}; nulls {tprof['nulls']}.",
        f"- rows per year (year, rows): {tprof['years']}.",
        f"- busiest months (month, rows): {tprof['top_months']}.",
    ]
if not _temporal:
    _temporal.append("- No date column located.")

_domain = []
if domain_text:
    _domain.append(
        f"- text, rows matching domain terms ({domain_text['sample_fraction']:.0%} row sample of {domain_text['rows']} articles): { {k: v for k, v in domain_text.items() if k not in ('rows', 'sample_fraction')} }."
    )
if domain_title:
    _domain.append(
        f"- titles, rows matching domain terms (all {domain_title['rows']} articles): { {k: v for k, v in domain_title.items() if k not in ('rows', 'sample_fraction')} }."
    )
_domain.append(
    f"- term lists used: {DOMAIN_TERMS}. A proxy for relevance, not a selection rule."
)

_findings_md = (
    f"- rows={prof['total']}, cols={len(prof['cols'])}, constant={prof['constant']}, "
    f"unique keys={ {c: u['unique'] for c, u in uniq.items()} }"
)

_areas = {
    "Domain understanding": [
        f"one row per article with columns {DATA_COLS}",
        f"text column `{role['text']}`, title column `{role['title']}`, id column `{role['id']}`",
        f"domain terms in titles: { {k: v for k, v in (domain_title or {}).items() if k not in ('rows', 'sample_fraction')} }",
    ],
    "Structure and engineering": [
        f"{len(entries)} file(s), {sum(e['size'] for e in entries)} bytes, read as {FRAME}",
        f"nested columns: {nested or 'none'}; constant columns: {prof['constant'] or 'none'}",
        f"markup in rows: { {k: v for k, v in (markup or {}).items() if k != 'rows'} }",
    ],
    "Temporal": [
        f"`{role['timestamp']}` range {tprof['lo']} .. {tprof['hi']}"
        if tprof
        else f"date column: {role['timestamp'] or 'none'}",
    ],
    "Spatial": [],
    "Data quality": [
        f"duplicate text groups {text_dups['dup_groups'] if text_dups else 'n/a'}; empty articles {tstats['empty'] if tstats else 'n/a'}; under 200 characters {tstats['under_200_chars'] if tstats else 'n/a'}",
        f"replacement-character rows {chars['replacement_char_rows'] if chars else 'n/a'}; control-character rows {chars['control_char_rows'] if chars else 'n/a'}",
        f"title collisions after normalising: {forms['collision_groups'] if forms else 'n/a'} groups",
    ],
    "Statistical patterns": [
        f"words per article p50 {tstats['words_q'][0.5] if tstats else 'n/a'}, p99 {tstats['words_q'][0.99] if tstats else 'n/a'}",
        f"share of sections above the chunk proxy: {sstats['over_proxy'] / sstats['sections'] if sstats and sstats['sections'] else 'n/a'}",
    ],
    "Relationships": [
        f"id unique: {uniq[role['id']]['unique'] if role['id'] in uniq else 'n/a'}; title unique: {uniq[role['title']]['unique'] if role['title'] in uniq else 'n/a'}",
        f"repeated ids {id_dups['dup_keys'] if id_dups else 'n/a'}, repeated titles {title_dups['dup_keys'] if title_dups else 'n/a'}",
    ],
    "Analytics use": [
        f"measures: article length; dimension: title forms ({forms['with_colon'] if forms else 'n/a'} with colon)",
    ],
    "ML use": [],
    "AI / knowledge use": [
        f"free text in `{role['text']}` with {tstats['total_words'] if tstats else 'n/a'} words; sections per article p50/p95 {sstats['per_article_p50_p95'] if sstats else 'n/a'}",
        "no category or link column in the article frame"
        if not any("categor" in c.lower() or "link" in c.lower() for c in DATA_COLS)
        else "a category or link column is present",
    ],
}

_corpus = [
    "- Chunk fit: the sections and the cleaned-length block (see Text Structure) indicate how often a 256-token limit would split or fail a piece of text; the exact count needs the model tokenizer.",
    "- Cleaning: the markup counts, the kept-word share after stripping and the character issues show how much text needs cleaning before it can become a knowledge unit.",
    "- Exclusion candidates: empty articles, very short articles, redirects, list titles, disambiguation titles, duplicate texts and repeated keys, with the counts above.",
    "- Selection: the domain-term counts are a proxy only; the articles carry no category column unless listed in the structure block.",
    "- Freshness: the revision timestamps above are the only date evidence in the copy; the upstream source is not read here.",
]

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        *head_blocks,
        ("Data Quality", "\n".join(_dq)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Text Structure", "\n".join(_text)),
        ("Temporal Semantics", "\n".join(_temporal)),
        ("Domain-Term Coverage", "\n".join(_domain)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("Observations by Area", area_block(_areas)),
        ("Corpus Implications", "\n".join(_corpus)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- WIKIPEDIA CLICKSTREAM (SAMPLES)
# MAGIC
# MAGIC **Purpose:** Profile the `wikipedia-datasets` clickstream JSON read in place
# MAGIC from the Databricks Samples Volume (no Bronze table) -- file inventory and
# MAGIC raw lines, schema, missingness, edge duplicates, edge types, click counts,
# MAGIC self-loops, id-to-title consistency, title forms, in-degree and out-degree
# MAGIC and click concentration -- as evidence.

# COMMAND ----------

# DBTITLE 1,Configuration and frame intake
SOURCE = "wikipedia"
NB_KEY = "02_wikipedia_clickstream"
SECTION_TITLE = "Wikipedia clickstream (Samples: wikipedia-datasets)"
ROOT = f"{SAMPLES_VOLUME_ROOT}/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json"
NAME_HINTS = {
    "prev_id": ("prev_id",),
    "curr_id": ("curr_id",),
    "prev_title": ("prev_title", "prev"),
    "curr_title": ("curr_title", "curr"),
    "count": ("n", "count", "clicks"),
    "type": ("type",),
}
LINK_TYPE = "link"
INFO = frame_intake(ROOT, NAME_HINTS, "json", raw_lines=3)
role, d0, prof, nested = INFO["role"], INFO["d0"], INFO["prof"], INFO["nested"]
entries, kinds, FRAME, DATA_COLS = (
    INFO["entries"],
    INFO["kinds"],
    INFO["frame"],
    INFO["data_cols"],
)

# COMMAND ----------

# DBTITLE 1,Edge duplicates, edge types, click count, self-loops and id checks
edge_key = [role[r] for r in ("prev_title", "curr_title", "type") if role[r]]
dup_edges = None
if len(edge_key) == 3:
    dup_edges = dup_key_composition(d0, edge_key)
    print(dup_edges)
else:
    print("edge key columns not all located:", edge_key)
types = []
if role["type"]:
    agg = [F.count(F.lit(1)).alias("edges")]
    if role["count"]:
        agg.append(F.sum(to_double(role["count"])).alias("clicks"))
    rows = d0.groupBy(qcol(role["type"]).alias("t")).agg(*agg).orderBy(F.desc("edges"))
    types = [r.asDict() for r in rows.limit(40).collect()]
    for t in types:
        print(t)
num = {}
quant = {}
if role["count"]:
    num = numeric_scan(d0, [role["count"]])
    print(num)
    if num[role["count"]]["is_numeric"]:
        quant = quantiles(d0, role["count"])
        print(quant)
self_loops = None
pairs = [("prev_id", "curr_id"), ("prev_title", "curr_title")]
for a, b in pairs:
    if role[a] and role[b]:
        same = (as_str(role[a]) == as_str(role[b])) & ~is_missing(role[a])
        n = d0.agg(F.sum(same.cast("long")).alias("n")).first()["n"]
        self_loops = {**(self_loops or {}), f"{a}=={b}": n}
print(self_loops)
id_missing = []
if role["type"] and (role["prev_id"] or role["curr_id"]):
    agg = [F.count(F.lit(1)).alias("edges")]
    agg += [
        F.sum(is_missing(role[r]).cast("long")).alias(f"{r}_missing")
        for r in ("prev_id", "curr_id")
        if role[r]
    ]
    rows = d0.groupBy(qcol(role["type"]).alias("t")).agg(*agg).orderBy(F.desc("edges"))
    id_missing = [r.asDict() for r in rows.limit(40).collect()]
    for r in id_missing:
        print(r)
spread = []
for i, t in (("curr_id", "curr_title"), ("prev_id", "prev_title")):
    if role[i] and role[t]:
        spread.append(group_attribute_spread(d0, role[i], role[t]))
        print(spread[-1])
forms = None
if role["curr_title"]:
    nodes = d0.select(qcol(role["curr_title"])).distinct()
    forms = title_forms(nodes, role["curr_title"])
    print(forms)

# COMMAND ----------

# DBTITLE 1,Link edges, degrees, click-count histogram and figure
links = d0.where(as_str(role["type"]) == LINK_TYPE) if role["type"] else d0
print(
    f"link filter: type == {LINK_TYPE!r}"
    if role["type"]
    else "no type column; all edges used"
)
in_deg = out_deg = None
if role["curr_title"] and role["count"]:
    in_deg = degree_summary(degree_table(links, role["curr_title"], role["count"]))
    print(in_deg)
if role["prev_title"] and role["count"]:
    out_deg = degree_summary(degree_table(links, role["prev_title"], role["count"]))
    print(out_deg)
click_hist = []
if role["count"] and num.get(role["count"], {}).get("is_numeric"):
    click_hist = log2_histogram(d0, to_double(role["count"]))
    print(click_hist)
figs = []
_panels = {}
if types:
    _panels["edges per type"] = [(str(t["t"]), t["edges"]) for t in types[:12]]
if click_hist:
    _panels["edges per click-count bucket"] = click_hist
if _panels and facet_bars(
    _panels,
    "Wikipedia clickstream -- edge types and click counts",
    "wikipedia_clickstream.png",
    rot=45,
    ncols=2,
    logy=True,
):
    figs.append(
        (
            "Wikipedia clickstream -- edge types and click counts",
            "wikipedia_clickstream.png",
        )
    )

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/wikipedia.md
head_blocks, missing = intake_blocks(INFO, ROOT, "edges")
_dq = [missing]
if dup_edges:
    _dq.append(
        f"Duplicates on {edge_key}: {dup_edges['dup_groups']} duplicated keys out of {dup_edges['distinct_keys']} ({dup_edges['identical']} identical, {dup_edges['conflicting']} conflicting)."
    )
if self_loops:
    _dq.append(f"Self-loops (rows where both ends are equal): {self_loops}.")
_dq += [f"- type `{r['t']}`: {r}" for r in id_missing]

_entities = [
    f"- `{sp['group_col']}` -> `{sp['value_col']}`: {sp['groups']} ids, {sp['inconsistent_groups']} map to more than one title."
    for sp in spread
]
if forms:
    _entities.append(
        para(
            f"- distinct current titles: {forms['rows']}; {forms['empty']} empty, {forms['with_underscore']} with underscore, {forms['with_colon']} with colon,",
            f"{forms['disambiguation']} disambiguation, {forms['list_titles']} list titles; {forms['collision_groups']} groups collide after trimming, lower-casing and replacing underscores.",
        )
    )
if not _entities:
    _entities.append("- No id or title column pair located.")

_edges = [
    f"- type `{t['t']}`: {t['edges']} edges"
    + (f", {fmt_num(t.get('clicks'))} clicks." if "clicks" in t else ".")
    for t in types
]
if role["count"] and num.get(role["count"]):
    s = num[role["count"]]
    _edges.append(
        f"- `{role['count']}`: numeric yield {s['yield']:.1%}, range {s['min']}..{s['max']}, mean {fmt_num(s['mean'])}, zero {s['zero']}, negative {s['negative']}."
    )
if quant:
    _edges.append(
        f"- `{role['count']}` quantiles p1/p25/p50/p75/p99: {[fmt_num(v) for v in quant.values()]}."
    )
for label, d in (
    ("in-degree by current title", in_deg),
    ("out-degree by previous title", out_deg),
):
    if d:
        _edges.append(
            para(
                f"- {label} (link edges): {d['nodes']} titles; edges per title p50/p90/p99 {d['edges_q']}, max {d['edges_max']};",
                f"clicks per title p50/p90/p99 {d['clicks_q']}, max {d['clicks_max']}; the top 1% of titles hold {d['top1pct_click_share']} of the clicks.",
            )
        )
        _edges.append(f"  top titles (title, clicks, edges): {d['top'][:10]}")

_dist = []
if click_hist:
    _dist.append(
        "- edges per click-count bucket (bucket, edges): "
        + fmt_pairs(click_hist, n=30).replace("\n", " ")
    )

_findings_md = (
    f"- edges={prof['total']}, cols={len(prof['cols'])}, constant={prof['constant']}, "
    f"types={[t['t'] for t in types[:8]]}"
)

_areas = {
    "Domain understanding": [
        f"one row per directed edge between a previous and a current page with a click count and a type; columns {DATA_COLS}",
        f"edge types: {[(t['t'], t['edges']) for t in types[:8]]}",
    ],
    "Structure and engineering": [
        f"{len(entries)} file(s), {sum(e['size'] for e in entries)} bytes, read as {FRAME}",
        f"nested columns: {nested or 'none'}; constant columns: {prof['constant'] or 'none'}",
    ],
    "Temporal": [
        "no time column in the edge frame"
        if not any("time" in c.lower() or "date" in c.lower() for c in DATA_COLS)
        else "a time column is present",
    ],
    "Spatial": [],
    "Data quality": [
        f"duplicate edge keys {dup_edges['dup_groups'] if dup_edges else 'n/a'}; self-loops {self_loops}",
        f"id-to-title inconsistencies: {[(s['group_col'], s['inconsistent_groups']) for s in spread]}",
    ],
    "Statistical patterns": [
        f"click-count quantiles {[fmt_num(v) for v in quant.values()] if quant else 'n/a'}",
        f"click concentration in the top 1% of current titles: {in_deg['top1pct_click_share'] if in_deg else 'n/a'}",
    ],
    "Relationships": [
        f"graph: {in_deg['nodes'] if in_deg else 'n/a'} titles with inbound link edges, {out_deg['nodes'] if out_deg else 'n/a'} with outbound",
    ],
    "Analytics use": [
        f"measure: click count; dimensions: edge type, previous and current title (type values {len(types)})",
    ],
    "ML use": [],
    "AI / knowledge use": [
        f"a weighted link graph of {prof['total']} edges; link edges only: filter `type == {LINK_TYPE}`",
    ],
}

_corpus = [
    "- Graph: the link edges give the page-to-page relation a Knowledge Graph would use; the title form and the id-to-title consistency above decide how nodes are keyed.",
    "- Non-link edges: the other types are external or empty referrers and are not page-to-page relations; their counts are above.",
    "- Weight: click counts are a measured weight; the concentration figures show how skewed they are.",
    "- Date: the copy carries no time column; the file name is the only date evidence (read in the provenance block).",
]

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        *head_blocks,
        ("Data Quality", "\n".join(_dq)),
        ("Entities / Keys", "\n".join(_entities)),
        ("Edge Structure", "\n".join(_edges)),
        ("Distributions", "\n".join(_dist)),
        ("EDA Findings", _findings_md),
        ("Observations by Area", area_block(_areas)),
        ("Corpus Implications", "\n".join(_corpus)),
    ],
    figures=figs,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA -- WIKIPEDIA RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **Purpose:** Cross-checks between the article frame and the clickstream
# MAGIC frame of `wikipedia-datasets` -- title and id overlap, the share of edges
# MAGIC and clicks that land on or leave from a known article, the most clicked
# MAGIC destinations and the clicks that fall on domain-term titles. Each overlap
# MAGIC is derived from one tagged union rather than repeated pairwise joins.

# COMMAND ----------

# DBTITLE 1,Configuration and both frames
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
a_info = frame_intake(
    ROOT_ARTICLES, ARTICLE_HINTS, "parquet", cap=LIST_CAP, profile=False
)
c_info = frame_intake(ROOT_CLICKS, CLICK_HINTS, "json", cap=LIST_CAP, profile=False)
articles, a_role = a_info["d0"], a_info["role"]
clicks, c_role = c_info["d0"], c_info["role"]
links = clicks.where(as_str(c_role["type"]) == LINK_TYPE) if c_role["type"] else clicks
print(
    f"link filter: type == {LINK_TYPE!r}"
    if c_role["type"]
    else "no type column; all edges used"
)

# COMMAND ----------

# DBTITLE 1,Overlaps, click coverage, most clicked destinations and domain-term clicks
ov_curr = ov_prev = ov_id = cov_in = cov_out = mass = None
if a_role["title"] and c_role["curr_title"]:
    ov_curr = key_overlap(articles, a_role["title"], links, c_role["curr_title"])
    print(ov_curr)
if a_role["title"] and c_role["prev_title"]:
    ov_prev = key_overlap(articles, a_role["title"], links, c_role["prev_title"])
    print(ov_prev)
if a_role["id"] and c_role["curr_id"]:
    ov_id = key_overlap(
        articles, a_role["id"], links, c_role["curr_id"], normalise=False
    )
    print(ov_id)
if a_role["title"] and c_role["curr_title"] and c_role["count"]:
    cov_in = weighted_coverage(
        links, c_role["curr_title"], c_role["count"], articles, a_role["title"]
    )
    print(cov_in)
if a_role["title"] and c_role["prev_title"] and c_role["count"]:
    cov_out = weighted_coverage(
        links, c_role["prev_title"], c_role["count"], articles, a_role["title"]
    )
    print(cov_out)
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
if c_role["curr_title"] and c_role["count"]:
    mass = title_term_mass(links, c_role["curr_title"], c_role["count"], DOMAIN_TERMS)
    print(mass)

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
