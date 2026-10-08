# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EDA -- WIKIPEDIA CLICKSTREAM (SAMPLES)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** Profile the `wikipedia-datasets` clickstream JSON read in place
# MAGIC from the Databricks Samples Volume (no Bronze table) -- file inventory and
# MAGIC raw lines, schema, missingness, edge duplicates, edge types, click counts,
# MAGIC self-loops, id-to-title consistency, title forms, in-degree and out-degree
# MAGIC and click concentration -- as evidence.

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
for e in entries:
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

# DBTITLE 1,First raw lines of the data file
raw_lines = []
if kinds.get("json"):
    raw_lines = head_lines(kinds["json"][0]["path"], 3)
for ln in raw_lines:
    print(ln[:300])

# COMMAND ----------

# DBTITLE 1,Select the frame
FRAME = "json" if "json" in frames else next(iter(frames))
df = frames[FRAME]["df"]
DATA_COLS = [c for c in df.columns if c != "__file"]
print(f"frame={FRAME}  files={len(frames[FRAME]['paths'])}  columns={DATA_COLS}")
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Resolve column roles
role = resolve_roles(DATA_COLS, NAME_HINTS)
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

# DBTITLE 1,Edge duplicates on (previous title, current title, type)
edge_key = [role[r] for r in ("prev_title", "curr_title", "type") if role[r]]
dup_edges = None
if len(edge_key) == 3:
    dup_edges = dup_key_composition(d0, edge_key)
    print(dup_edges)
else:
    print("edge key columns not all located:", edge_key)

# COMMAND ----------

# DBTITLE 1,Edge types -- rows and clicks
types = []
if role["type"]:
    agg = [F.count(F.lit(1)).alias("edges")]
    if role["count"]:
        agg.append(F.sum(to_double(role["count"])).alias("clicks"))
    rows = d0.groupBy(qcol(role["type"]).alias("t")).agg(*agg).orderBy(F.desc("edges"))
    types = [r.asDict() for r in rows.limit(40).collect()]
    for t in types:
        print(t)

# COMMAND ----------

# DBTITLE 1,Click count -- parse yield, moments and quantiles
num = {}
quant = {}
if role["count"]:
    num = numeric_scan(d0, [role["count"]])
    print(num)
    if num[role["count"]]["is_numeric"]:
        quant = quantiles(d0, role["count"])
        print(quant)

# COMMAND ----------

# DBTITLE 1,Self-loops
self_loops = None
pairs = [("prev_id", "curr_id"), ("prev_title", "curr_title")]
for a, b in pairs:
    if role[a] and role[b]:
        same = (as_str(role[a]) == as_str(role[b])) & ~is_missing(role[a])
        n = d0.agg(F.sum(same.cast("long")).alias("n")).first()["n"]
        self_loops = {**(self_loops or {}), f"{a}=={b}": n}
print(self_loops)

# COMMAND ----------

# DBTITLE 1,Missing ids by edge type
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

# COMMAND ----------

# DBTITLE 1,Id-to-title consistency
spread = []
for i, t in (("curr_id", "curr_title"), ("prev_id", "prev_title")):
    if role[i] and role[t]:
        spread.append(group_attribute_spread(d0, role[i], role[t]))
        print(spread[-1])

# COMMAND ----------

# DBTITLE 1,Title forms over distinct current titles
forms = None
if role["curr_title"]:
    nodes = d0.select(qcol(role["curr_title"])).distinct()
    forms = title_forms(nodes, role["curr_title"])
    print(forms)

# COMMAND ----------

# DBTITLE 1,Link edges
links = d0.where(as_str(role["type"]) == LINK_TYPE) if role["type"] else d0
print(
    f"link filter: type == {LINK_TYPE!r}"
    if role["type"]
    else "no type column; all edges used"
)

# COMMAND ----------

# DBTITLE 1,In-degree and click concentration (link edges, by current title)
in_deg = None
if role["curr_title"] and role["count"]:
    in_deg = degree_summary(degree_table(links, role["curr_title"], role["count"]))
    print(in_deg)

# COMMAND ----------

# DBTITLE 1,Out-degree and click concentration (link edges, by previous title)
out_deg = None
if role["prev_title"] and role["count"]:
    out_deg = degree_summary(degree_table(links, role["prev_title"], role["count"]))
    print(out_deg)

# COMMAND ----------

# DBTITLE 1,Click-count histogram
click_hist = []
if role["count"] and num.get(role["count"], {}).get("is_numeric"):
    click_hist = log2_histogram(d0, to_double(role["count"]))
    print(click_hist)

# COMMAND ----------

# DBTITLE 1,Figures
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

# DBTITLE 1,Findings
print(
    f"edges={prof['total']}, cols={len(prof['cols'])}, constant={prof['constant']}, "
    f"types={[t['t'] for t in types[:8]]}, duplicates={dup_edges}, self_loops={self_loops}"
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings -> src/schemas/profiling/wikipedia.md
_profile = [
    "| frame | source files | edges | cols | constant columns | nested columns |",
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
    "First raw lines:",
    *[f"  > {ln[:200]}" for ln in raw_lines],
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
        ("Profile", "\n".join(_profile)),
        ("Provenance / Source Evidence", "\n".join(_prov)),
        ("Structural Integrity", "\n".join(_struct)),
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
