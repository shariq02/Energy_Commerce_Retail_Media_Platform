# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- WIKIPEDIA ARTICLE TEXT (SAMPLES)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** Profile the `wikipedia-datasets` article parquet read in place
# MAGIC from the Databricks Samples Volume (no Bronze table) -- file inventory and
# MAGIC provenance, schema and nesting, missingness, title and id keys, title
# MAGIC forms, text length and paragraph length (word and character proxies),
# MAGIC markup, character issues, duplicate texts, date hints and domain-term
# MAGIC coverage -- as evidence.

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
NB_KEY = "01_wikipedia_articles"
SECTION_TITLE = "Wikipedia article text (Samples: wikipedia-datasets)"
ROOT = f"{SAMPLES_VOLUME_ROOT}/wikipedia-datasets/data-001/en_wikipedia/articles-only-parquet"
NAME_HINTS = {
    "id": ("id", "page_id", "pageid", "curid", "article_id"),
    "title": ("title", "page_title", "article_title"),
    "text": ("text", "body", "content", "article", "wikitext"),
    "timestamp": ("timestamp", "revision_timestamp", "last_modified", "date"),
}
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

# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Discover files
entries, kinds, frames = load_volume_frames(ROOT)
require_frames(frames, ROOT, kinds)
print(f"{ROOT}: {len(entries)} files, {sum(e['size'] for e in entries)} bytes")
for e in entries[:12]:
    print(f"  {file_kind(e['name']):<10} {e['size']:>12}  {e['path']}")
print({k: len(v) for k, v in kinds.items()})

# COMMAND ----------

# DBTITLE 1,Support files (README) -- provenance evidence
support = read_support_text(kinds)
for name, lines in support.items():
    print("=" * 90, f"\n{name}")
    for ln in lines[:40]:
        print("  " + ln[:160])
if not support:
    print("no README / licence file in the directory")

# COMMAND ----------

# DBTITLE 1,Select the frame
FRAME = "parquet" if "parquet" in frames else next(iter(frames))
df = frames[FRAME]["df"]
DATA_COLS = [c for c in df.columns if c != "__file"]
print(f"frame={FRAME}  files={len(frames[FRAME]['paths'])}  columns={DATA_COLS}")
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Resolve column roles
role = resolve_roles(DATA_COLS, NAME_HINTS)
if role["text"] is None:
    role["text"] = longest_text_col(df, DATA_COLS)
    print("text column chosen by average length:", role["text"])
print(role)

# COMMAND ----------

# DBTITLE 1,Structural check -- nesting and schema
nested = [
    f.name
    for f in df.schema.fields
    if f.dataType.simpleString().startswith(("struct", "array", "map"))
]
struct = {
    "corrupt_record": "_corrupt_record" in DATA_COLS,
    "nested": nested,
    "schema": df.schema.simpleString(),
}
print(struct)

# COMMAND ----------

# DBTITLE 1,Profile -- rows, missingness, approx distinct
d0 = df.drop("__file")
prof = profile_frame(d0)
print(f"rows={prof['total']}  cols={len(prof['cols'])}")
for c in prof["cols"]:
    print(
        f"  {c[:40]:<40} missing={prof['miss'][c]:>9} "
        f"rate={prof['miss'][c] / prof['total'] if prof['total'] else 0:.4f} "
        f"approx_distinct={prof['acd'][c]}"
    )

# COMMAND ----------

# DBTITLE 1,Constant columns
prof["constant"] = constant_cols(d0, prof)
print(f"constant={prof['constant']}")

# COMMAND ----------

# DBTITLE 1,Key candidates -- exact uniqueness
key_cands = [role[r] for r in ("id", "title") if role[r]]
uniq = exact_uniqueness(d0, key_cands)
for c, u in uniq.items():
    print(c, u)

# COMMAND ----------

# DBTITLE 1,Title forms and case collisions
forms = None
if role["title"]:
    forms = title_forms(d0, role["title"])
    print(forms)
else:
    print("no title column located")

# COMMAND ----------

# DBTITLE 1,Text length -- characters and words
tstats = None
if role["text"]:
    tstats = text_stats(d0, role["text"])
    print(tstats)
else:
    print("no text column located")

# COMMAND ----------

# DBTITLE 1,Paragraph length against the chunk proxy (row sample)
pstats = None
if role["text"]:
    pstats = paragraph_stats(
        d0, role["text"], fraction=TEXT_SAMPLE_FRACTION, seed=SAMPLE_SEED
    )
    print(pstats)

# COMMAND ----------

# DBTITLE 1,Markup indicators
markup = None
if role["text"]:
    markup = markup_indicators(d0, role["text"])
    print(markup)

# COMMAND ----------

# DBTITLE 1,Character issues
chars = None
if role["text"]:
    chars = char_issues(d0, role["text"])
    print(chars)

# COMMAND ----------

# DBTITLE 1,Duplicate texts (64-bit hash)
text_dups = None
if role["text"]:
    text_dups = text_duplicates(d0, role["text"])
    print(text_dups)

# COMMAND ----------

# DBTITLE 1,Date columns
ts_info = None
if role["timestamp"]:
    tcol = role["timestamp"]
    ttype = dict(d0.dtypes)[tcol]
    if ttype in ("timestamp", "timestamp_ntz", "date"):
        g = d0.agg(F.min(qcol(tcol)).alias("lo"), F.max(qcol(tcol)).alias("hi")).first()
        ts_info = {"lines": [f"`{tcol}` ({ttype}): {g['lo']} .. {g['hi']}."]}
    else:
        ts_info = timestamp_semantics(
            d0, tcol, valid_from="2001-01-01", tz="unspecified"
        )
    print(ts_info)
else:
    print("no date or timestamp column located")

# COMMAND ----------

# DBTITLE 1,Latest year mentioned per article (row sample)
year_hint = None
if role["text"]:
    year_hint = latest_year_hint(
        d0, role["text"], fraction=TEXT_SAMPLE_FRACTION, seed=SAMPLE_SEED
    )
    print(year_hint)

# COMMAND ----------

# DBTITLE 1,Domain-term coverage in text (row sample)
domain_text = None
if role["text"]:
    domain_text = domain_term_hits(
        d0,
        role["text"],
        DOMAIN_TERMS,
        fraction=TEXT_SAMPLE_FRACTION,
        seed=SAMPLE_SEED,
    )
    print(domain_text)

# COMMAND ----------

# DBTITLE 1,Domain-term coverage in titles
domain_title = None
if role["title"]:
    domain_title = domain_term_hits(d0, role["title"], DOMAIN_TERMS)
    print(domain_title)

# COMMAND ----------

# DBTITLE 1,Length histograms
len_hist = {}
if role["text"]:
    txt = as_str(role["text"])
    len_hist["words per article"] = log2_histogram(d0, word_count(txt))
    len_hist["characters per article"] = log2_histogram(d0, F.length(txt))
    for k, v in len_hist.items():
        print(k, v)

# COMMAND ----------

# DBTITLE 1,Figures
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

# DBTITLE 1,Findings
print(
    f"rows={prof['total']}, cols={len(prof['cols'])}, constant={prof['constant']}, "
    f"keys={ {c: u['unique'] for c, u in uniq.items()} }, text={tstats}, duplicates={text_dups}"
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/wikipedia.md
_profile = [
    "| frame | source files | rows | cols | constant columns | nested columns |",
    "|---|---|---|---|---|---|",
    para(
        f"| {FRAME} | {len(frames[FRAME]['paths'])} | {prof['total']} | {len(prof['cols'])} |",
        f"{', '.join(prof['constant']) or '-'} | {', '.join(nested) or '-'} |",
    ),
]

_prov = [
    para(
        "The dataset is read in place from the Databricks Samples Volume;",
        "there is no Bronze table and no ECRMAP acquisition step.",
    ),
    f"Files under `{ROOT}`: {len(entries)}, {sum(e['size'] for e in entries)} bytes ({ {k: len(v) for k, v in kinds.items()} }).",
]
for name, lines in support.items():
    _prov.append(f"README-type file `{name}` (first lines):")
    _prov += [f"  > {ln[:160]}" for ln in lines[:25] if ln.strip()]
if not support:
    _prov.append(
        "- No README / licence file in the directory: provenance and licence are not documented here (LIMITATION)."
    )

_struct = [
    f"- schema: `{struct['schema']}`",
    f"- corrupt-record column present: {struct['corrupt_record']}; nested columns: {nested or 'none'}.",
    f"- column roles resolved: { {k: v for k, v in role.items() if v} }.",
]

_dq = [
    "Missingness (rate per column): "
    + ", ".join(
        f"{c}={prof['miss'][c] / prof['total'] if prof['total'] else 0:.4f}"
        for c in prof["cols"]
    ),
]
if text_dups:
    _dq.append(
        f"Duplicate texts: {text_dups['dup_groups']} groups holding {text_dups['rows_in_dup_groups']} of {text_dups['rows']} rows."
    )
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
if pstats:
    _text += [
        f"- paragraphs (non-blank lines) in a {pstats['sample_fraction']:.0%} row sample of {pstats['sampled_articles']} articles: {pstats['paragraphs']}; per article p50/p95 {pstats['per_article_p50_p95']}, max {pstats['per_article_max']}.",
        f"- words per paragraph quantiles p1/p25/p50/p75/p95/p99: {[pstats['words_q'][p] for p in QUANTILE_PROBS]}; longest {pstats['max_words']}.",
        f"- paragraphs above {WORDS_PER_CHUNK_PROXY} words (proxy for a 256-token limit): {pstats['over_proxy']} of {pstats['paragraphs']}. Words are a proxy; the token count belongs to the build that chunks the text.",
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
if year_hint:
    _temporal.append(
        f"- latest year mentioned per article, most recent first (year, articles; {TEXT_SAMPLE_FRACTION:.0%} row sample): {year_hint}. A hint of the snapshot date, not a date field."
    )
if not _temporal:
    _temporal.append("- No date column and no year mentions found.")

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
        f"latest years mentioned: {year_hint[:4] if year_hint else 'none found'}",
        f"date column: {role['timestamp'] or 'none'}",
    ],
    "Spatial": [],
    "Data quality": [
        f"duplicate text groups {text_dups['dup_groups'] if text_dups else 'n/a'}; empty articles {tstats['empty'] if tstats else 'n/a'}; under 200 characters {tstats['under_200_chars'] if tstats else 'n/a'}",
        f"replacement-character rows {chars['replacement_char_rows'] if chars else 'n/a'}; control-character rows {chars['control_char_rows'] if chars else 'n/a'}",
        f"title collisions after normalising: {forms['collision_groups'] if forms else 'n/a'} groups",
    ],
    "Statistical patterns": [
        f"words per article p50 {tstats['words_q'][0.5] if tstats else 'n/a'}, p99 {tstats['words_q'][0.99] if tstats else 'n/a'}",
        f"share of paragraphs above the chunk proxy: {pstats['over_proxy'] / pstats['paragraphs'] if pstats and pstats['paragraphs'] else 'n/a'}",
    ],
    "Relationships": [
        f"id unique: {uniq[role['id']]['unique'] if role['id'] in uniq else 'n/a'}; title unique: {uniq[role['title']]['unique'] if role['title'] in uniq else 'n/a'}",
    ],
    "Analytics use": [
        f"measures: article length; dimension: title forms ({forms['with_colon'] if forms else 'n/a'} with colon)",
    ],
    "ML use": [],
    "AI / knowledge use": [
        f"free text in `{role['text']}` with {tstats['total_words'] if tstats else 'n/a'} words; paragraphs per article p50/p95 {pstats['per_article_p50_p95'] if pstats else 'n/a'}",
        "no category or link column in the article frame"
        if not any("categor" in c.lower() or "link" in c.lower() for c in DATA_COLS)
        else "a category or link column is present",
    ],
}

_corpus = [
    "- Chunk fit: the share of paragraphs above the word proxy (see Text Structure) indicates how often a 256-token limit would split or fail a paragraph; the exact count needs the model tokenizer.",
    "- Cleaning: the markup counts and character issues above show how much text needs cleaning before it can become a knowledge unit.",
    "- Exclusion candidates: empty articles, very short articles, redirects, list titles, disambiguation titles and duplicate texts, with the counts above.",
    "- Selection: the domain-term counts are a proxy only; the articles carry no category column unless listed in the structure block.",
    "- Freshness: the date evidence above is all the copy offers; the upstream source is not read here.",
]

write_profiling(
    SOURCE,
    NB_KEY,
    SECTION_TITLE,
    blocks=[
        ("Profile", "\n".join(_profile)),
        ("Provenance / Source Evidence", "\n".join(_prov)),
        ("Structural Integrity", "\n".join(_struct)),
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