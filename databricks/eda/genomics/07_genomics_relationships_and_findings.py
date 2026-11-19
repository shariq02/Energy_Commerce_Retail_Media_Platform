# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EDA -- GENOMICS RELATIONSHIPS AND FINDINGS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** Cross-table checks over the genomics Gold tables. It checks that
# MAGIC the schema holds exactly the tables the EDA notebooks cover, matches every key
# MAGIC column to its parent table (orphans, repeated parent keys), profiles the two
# MAGIC multi-value tables and measures how many cells hold more than one value --
# MAGIC as evidence.

# COMMAND ----------

# MAGIC %run ../_eda_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
SCHEMA = "genomics_gold"
SOURCE = "genomics"
NB_KEY = "07_relationships_and_findings"
SECTION_TITLE = "Cross-table relationships and multi-value tables"
ALL_TABLES = [
    "benchmark_variant",
    "case_assay_record",
    "cell_line_profile",
    "clinical_trial_profile",
    "clinvar_classification_change",
    "disease_profile",
    "dms_assay",
    "dms_assay_mutant",
    "drug_profile",
    "embedding_split",
    "entity_identity_record",
    "epitope_profile",
    "gene_disease_profile",
    "gene_measurement",
    "gene_profile",
    "genetic_test_profile",
    "multi_value_attribute",
    "multi_value_score",
    "protein_profile",
    "protein_variant_record",
    "somatic_mutation_profile",
    "somatic_site_profile",
    "structural_variant_profile",
    "transcript_profile",
    "tumor_sample_profile",
    "variant_profile",
]
MULTI_ATTRIBUTE = "multi_value_attribute"
MULTI_SCORE = "multi_value_score"
TABLES = [MULTI_ATTRIBUTE, MULTI_SCORE]
# Key columns whose parent is not named after the column.
EXTRA_LINKS = [
    ("structural_variant_profile", "overlapping_gene_key", "gene_profile", "gene_key"),
    ("dms_assay_mutant", "assay_id", "dms_assay", "assay_id"),
]

# COMMAND ----------

# DBTITLE 1,Validate profiling export path
REPO_ROOT = _repo_root()
PROFILING_DIR = _profiling_dir()
print(f"OK  repo root: {REPO_ROOT}")
print(f"OK  profiling directory: {PROFILING_DIR}")

# COMMAND ----------

# DBTITLE 1,Tables in the schema against the tables of the EDA notebooks
IN_SCHEMA = sorted(
    r["tableName"] for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
)
NOT_COVERED = sorted(set(IN_SCHEMA) - set(ALL_TABLES))
NOT_FOUND = sorted(set(ALL_TABLES) - set(IN_SCHEMA))
print(f"tables in the schema: {len(IN_SCHEMA)}; listed here: {len(ALL_TABLES)}")
print("in the schema, not covered:", NOT_COVERED)
print("covered, not in the schema:", NOT_FOUND)
if NOT_COVERED or NOT_FOUND:
    raise ValueError(f"table list differs: {NOT_COVERED} {NOT_FOUND}")

# COMMAND ----------

# DBTITLE 1,Read tables and count rows
FRAMES_ALL = read_tables(CATALOG, SCHEMA, ALL_TABLES)
ROWS = {name: frame.count() for name, frame in FRAMES_ALL.items()}
FRAMES = {name: FRAMES_ALL[name] for name in TABLES}
KINDS = {name: column_kinds(frame) for name, frame in FRAMES.items()}
for name, rows in ROWS.items():
    print(f"  {name:<32} rows={rows:>12,} columns={len(FRAMES_ALL[name].columns)}")

# COMMAND ----------

# DBTITLE 1,Profile -- rows, nulls, constant and empty columns of the multi-value tables
PROFILES = {}
for name, frame in FRAMES.items():
    PROFILES[name] = table_profile(frame)
    p = PROFILES[name]
    print(
        f"{name}: rows={p['rows']:,} columns={p['columns']} "
        f"empty={len(p['empty'])} constant={len(p['constant'])}"
    )

# COMMAND ----------

# DBTITLE 1,Key and duplicates of the multi-value tables
KEYS = {name: table_key_report(frame) for name, frame in FRAMES.items()}
for name, key in KEYS.items():
    print(name, key)

# COMMAND ----------

# DBTITLE 1,Boolean and numeric rates of the multi-value tables
BOOLEANS = {
    name: boolean_rates(frame, KINDS[name]["boolean"]) for name, frame in FRAMES.items()
}
NUMERICS = {
    name: numeric_scan(frame, KINDS[name]["numeric"]) for name, frame in FRAMES.items()
}
for name in TABLES:
    print(
        name, "boolean columns:", len(BOOLEANS[name]), "numeric:", len(NUMERICS[name])
    )

# COMMAND ----------

# DBTITLE 1,Coverage by source of the multi-value tables
COVERAGE = {
    name: coverage_by_source(frame, PROFILES[name]) for name, frame in FRAMES.items()
}
for name, groups in COVERAGE.items():
    print(
        name, "source groups:", len(groups), {g: v["rows"] for g, v in groups.items()}
    )

# COMMAND ----------

# DBTITLE 1,Key columns and their parent tables
LINKS = []
for name, frame in FRAMES_ALL.items():
    for column in frame.columns:
        parent = column[: -len("_key")] + "_profile"
        if (
            column.endswith("_key")
            and parent in ALL_TABLES
            and parent != name
            and column in FRAMES_ALL[parent].columns
        ):
            LINKS.append((name, column, parent, column))
LINKS += EXTRA_LINKS
print(len(LINKS), "links")
for link in LINKS:
    print(" ", link)

# COMMAND ----------

# DBTITLE 1,Match every key column to its parent
MATCHES = {}
PARENT_KEYS = {}
for child, child_col, parent, parent_col in LINKS:
    if (parent, parent_col) not in PARENT_KEYS:
        stats = exact_uniqueness(FRAMES_ALL[parent], [parent_col])[parent_col]
        PARENT_KEYS[(parent, parent_col)] = {
            **stats,
            "repeated_rows": stats["rows"] - stats["nulls"] - stats["distinct"],
        }
    MATCHES[(child, child_col, parent, parent_col)] = key_overlap(
        FRAMES_ALL[child],
        child_col,
        FRAMES_ALL[parent],
        parent_col,
        normalise=False,
    )
    print(
        child, child_col, "->", parent, MATCHES[(child, child_col, parent, parent_col)]
    )

# COMMAND ----------

# DBTITLE 1,Entity keys of multi_value_attribute against their parent tables
ENTITY_ROWS = value_counts(FRAMES[MULTI_ATTRIBUTE], "entity_type")
ENTITY_MATCHES = {}
for entity, rows, _share in ENTITY_ROWS:
    parent = f"{entity}_profile"
    parent_col = f"{entity}_key"
    if parent in ALL_TABLES and parent_col in FRAMES_ALL[parent].columns:
        child = FRAMES[MULTI_ATTRIBUTE].filter(qcol("entity_type") == entity)
        ENTITY_MATCHES[entity] = key_overlap(
            child, "entity_key", FRAMES_ALL[parent], parent_col, normalise=False
        )
    else:
        ENTITY_MATCHES[entity] = None
    print(entity, rows, ENTITY_MATCHES[entity])

# COMMAND ----------

# DBTITLE 1,Cells with more than one value
VALUES_PER_ATTRIBUTE = group_size_shares(
    FRAMES[MULTI_ATTRIBUTE], ["entity_type", "entity_key", "attribute"]
)
VALUES_PER_SCORE = group_size_shares(FRAMES[MULTI_SCORE], ["variant_key", "score_name"])
SCORE_LINK = MATCHES[(MULTI_SCORE, "variant_key", "variant_profile", "variant_key")]
VARIANT_ROWS = ROWS["variant_profile"]
print("values per attribute cell:", VALUES_PER_ATTRIBUTE)
print("values per score cell:", VALUES_PER_SCORE)
print("variants with a multi-value score:", SCORE_LINK["a_distinct"], VARIANT_ROWS)

# COMMAND ----------

# DBTITLE 1,Figure -- orphan keys per link
ranked_links = sorted(MATCHES.items(), key=lambda t: -t[1]["a_not_in_b"])
drawn = barplot(
    [(f"{k[0]}.{k[1]}", m["a_not_in_b"]) for k, m in ranked_links],
    "Key values without a parent row, per link",
    "child table and column",
    ylabel="distinct key values",
    rot=60,
    figsize=(12, 5),
    filename="genomics_07_orphans.png",
)
FIGURES = (
    [("Key values without a parent row, per link", "genomics_07_orphans.png")]
    if drawn
    else []
)

# COMMAND ----------

# DBTITLE 1,Export profiling findings
inventory = md_table(
    ("table", "rows", "columns"),
    [(n, f"{ROWS[n]:,}", len(FRAMES_ALL[n].columns)) for n in ALL_TABLES],
)
link_rows = [
    (
        f"{k[0]}.{k[1]}",
        f"{k[2]}.{k[3]}",
        f"{m['a_distinct']:,}",
        f"{m['a_not_in_b']:,}",
        m["a_share_in_b"],
        PARENT_KEYS[(k[2], k[3])]["repeated_rows"],
    )
    for k, m in MATCHES.items()
]
link_table = md_table(
    ("child", "parent", "distinct keys", "orphans", "match rate", "parent repeats"),
    link_rows,
)
entity_lines = [
    f"- {entity}: {rows:,} rows"
    + (
        f"; {m['a_not_in_b']:,} of {m['a_distinct']:,} entity keys without a parent"
        if m
        else "; no parent table with a matching key column"
    )
    for (entity, rows, _share), m in zip(
        ENTITY_ROWS, ENTITY_MATCHES.values(), strict=True
    )
]
multi_lines = [
    "- values per attribute cell, share of cells: "
    + ", ".join(f"{k} {v}" for k, v in VALUES_PER_ATTRIBUTE.items() if k != "groups"),
    f"- attribute cells with several values: {VALUES_PER_ATTRIBUTE['groups']:,}",
    "- values per score cell, share of cells: "
    + ", ".join(f"{k} {v}" for k, v in VALUES_PER_SCORE.items() if k != "groups"),
    f"- score cells with several values: {VALUES_PER_SCORE['groups']:,}",
    para(
        f"- variants with a multi-value score: {SCORE_LINK['a_distinct']:,}",
        f"({pct(SCORE_LINK['a_distinct'] / VARIANT_ROWS if VARIANT_ROWS else None)}",
        "of `variant_profile`).",
    ),
]
with_orphans = [(k, m) for k, m in MATCHES.items() if m["a_not_in_b"]]
repeated_parents = {
    f"{p}.{c}": s["repeated_rows"]
    for (p, c), s in PARENT_KEYS.items()
    if s["repeated_rows"]
}
findings = [
    f"- {len(ALL_TABLES)} tables in the schema, all covered; {sum(ROWS.values()):,} rows.",
    f"- key columns matched to a parent: {len(MATCHES)}; with orphan keys: {len(with_orphans)}.",
    "- parent key columns with repeated values: "
    + (", ".join(f"{k} ({v:,})" for k, v in repeated_parents.items()) or "none")
    + ".",
    table_findings_lines(PROFILES, KEYS),
]
READINESS = [
    (
        "Grain / grain drift",
        "; ".join(
            f"`{n}` key `{k['key']}` unique: {k.get('unique')}" for n, k in KEYS.items()
        ),
    ),
    (
        "Join multiplication (1:N / M:N expansion)",
        "parent key columns with repeated values: "
        + (", ".join(f"{k} ({v:,})" for k, v in repeated_parents.items()) or "none"),
    ),
    (
        "Duplicate-event leakage",
        "; ".join(
            para(
                f"{n} dup groups {(k.get('duplicates') or {}).get('dup_groups', 0)},",
                f"conflicting {(k.get('duplicates') or {}).get('conflicting', 0)}",
            )
            for n, k in KEYS.items()
        ),
    ),
    (
        "Survivorship / coverage bias",
        para(
            f"{len(with_orphans)} of {len(MATCHES)} links have key values without a",
            "parent row; the list is in the relationship table.",
        ),
    ),
    (
        "Sample-vs-full divergence",
        "every count and rate is read from the whole table; no sample is used.",
    ),
]
EVIDENCE = {
    "Structure and engineering": [
        f"{len(ALL_TABLES)} Gold tables, {len(LINKS)} key links checked"
    ],
    "Relationships": [
        f"{len(with_orphans)} links with orphan keys",
        f"{len(repeated_parents)} parent key columns with repeated values",
    ],
    "Data quality": [
        para(
            f"{VALUES_PER_ATTRIBUTE['groups']:,} attribute cells and",
            f"{VALUES_PER_SCORE['groups']:,} score cells hold several values",
        )
    ],
}
blocks = [
    ("Table Inventory", inventory),
    ("Profile of the Multi-Value Tables", profile_block(PROFILES)),
    ("Data Quality", quality_block(PROFILES)),
    ("Entities / Keys", key_block(KEYS)),
    ("Coverage & Sampling Bias", coverage_block(COVERAGE)),
    ("Referential Integrity", link_table),
    ("Entity Keys of multi_value_attribute", "\n".join(entity_lines)),
    ("Multi-Value Cells", "\n".join(multi_lines)),
    ("Distributions", distribution_block(PROFILES, KINDS, BOOLEANS, NUMERICS)),
    ("EDA Findings", "\n".join(findings)),
    ("ML-Readiness Evidence", ml_readiness_block(READINESS)),
    ("Observations by Area", area_block(EVIDENCE)),
]
write_profiling(SOURCE, NB_KEY, SECTION_TITLE, blocks, FIGURES)
