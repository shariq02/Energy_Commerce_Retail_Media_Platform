# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GOLD GENOMICS -- GENES AND VARIANTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the 10 gene and variant tables of `genomics_gold` and
# MAGIC `multi_value_attribute`. The Silver attributes are pivoted to columns and
# MAGIC split by rule (`GENE_PROFILE_RULES`, `VARIANT_PROFILE_RULES`); a rule not in
# MAGIC a split goes to `gene_profile` or `variant_profile`. A cell with more than
# MAGIC one value (any entity type) goes to `multi_value_attribute`. Variant scores
# MAGIC and population frequencies become columns.

# COMMAND ----------

# DBTITLE 1,Gold library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "genomics"
COMPONENT = "gold/genomics/01_genes_and_variants"
RID = gold_run_id()
GATE = {"source": SOURCE, "component": COMPONENT, "rid": RID}
COMMON = {**GATE, "schema": GENOMICS_GOLD_SCHEMA}
POSITION_ASSEMBLY = "GRCh38"  # phyloP and phastCons 100-way are GRCh38 tracks
LABELS = [
    "variant_key",
    "assembly",
    "contig",
    "position",
    "reference_allele",
    "alternate_allele",
    "gene_key",
]
# variant tables built here by the rule split alone (with the rsid as a label)
VARIANT_SPLITS = ("variant_clinical_profile", "variant_impact_profile")
FREQUENCY_VALUES = (
    "allele_frequency",
    "allele_count",
    "allele_number",
    "homozygote_count",
    "filter_status",
)
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Read Silver
attributes = read_silver("entity_derived_attribute")
genes = master_rows(read_silver("entity_master"), "gene", "gene_key")
variant = from_silver(read_silver("variant"))
locus_score = read_silver("locus_score")
population_frequency = read_silver("variant_population_frequency")
gene_columns = attribute_columns(attributes, "gene")
variant_columns = attribute_columns(attributes, "variant")
PROFILE = {"attributes": attributes, **GATE}

# COMMAND ----------

# DBTITLE 1,Function -- column name of a dataset, population and measure


def combo_name(pair_datasets, dataset, population, measure):
    """`<population>_<measure>`; the dataset leads when two datasets share the
    population and measure."""
    parts = [population, measure]
    if len(pair_datasets[(population, measure)]) > 1:
        parts.insert(0, dataset)
    return _re.sub(r"[^a-z0-9]+", "_", "_".join(str(p) for p in parts).lower())


# COMMAND ----------

# DBTITLE 1,Transform -- multi_value_attribute, gene_profile and the gene split tables
TABLES["multi_value_attribute"] = (
    add_provenance(
        multi_value_attributes(attributes), SOURCE, RID, ecosystem="genomics"
    ),
    ["source_record_id"],
)
TABLES["gene_profile"] = (
    full_profile(
        genes,
        entity_type="gene",
        key="gene_key",
        exclude=mapped_rules(GENE_PROFILE_RULES),
        columns=gene_columns,
        **PROFILE,
    ),
    ["gene_key"],
)
for table, rules in GENE_PROFILE_RULES.items():
    TABLES[table] = (
        split_profile(
            entity_type="gene",
            labels=genes.select("gene_key", "symbol"),
            key="gene_key",
            rules=rules,
            columns=gene_columns,
            **PROFILE,
        ),
        ["gene_key"],
    )

# COMMAND ----------

# DBTITLE 1,Transform -- variant_profile and the variant tables of the rule split
TABLES["variant_profile"] = (
    full_profile(
        variant,
        entity_type="variant",
        key="variant_key",
        exclude=mapped_rules(VARIANT_PROFILE_RULES),
        columns=variant_columns,
        **PROFILE,
    ),
    ["variant_key"],
)
for table in VARIANT_SPLITS:
    TABLES[table] = (
        split_profile(
            entity_type="variant",
            labels=variant.select(*LABELS, "rsid"),
            key="variant_key",
            rules=VARIANT_PROFILE_RULES[table],
            columns=variant_columns,
            **PROFILE,
        ),
        ["variant_key"],
    )

# COMMAND ----------

# DBTITLE 1,Transform -- variant_score_profile (variant scores and position scores)
variant_scores = locus_score.filter(F.col("variant_key").isNotNull())
position_scores = locus_score.filter(F.col("variant_key").isNull())
assert_unique_grain(variant_scores, ["variant_key", "score_name"], **GATE)
assert_unique_grain(position_scores, ["contig", "position", "score_name"], **GATE)
variant_names, position_names = [
    sorted(r[0] for r in frame.select("score_name").distinct().collect())
    for frame in (variant_scores, position_scores)
]
by_variant = (
    variant_scores.groupBy("variant_key")
    .pivot("score_name", variant_names)
    .agg(
        F.first("score_value").alias("value"),
        F.first("score_label").alias("label"),
        F.first("context").alias("context"),
    )
)
by_position = (
    position_scores.groupBy("contig", "position")
    .pivot("score_name", position_names)
    .agg(F.first("score_value").alias("value"))
)
by_position = (
    variant.filter(F.col("assembly") == POSITION_ASSEMBLY)
    .select("variant_key", "contig", "position")
    .join(by_position, ["contig", "position"])
    .drop("contig", "position")
)
TABLES["variant_score_profile"] = (
    add_provenance(
        variant.select(*LABELS).join(
            by_variant.join(by_position, "variant_key", "full"), "variant_key", "right"
        ),
        SOURCE,
        RID,
        ecosystem="genomics",
    ),
    ["variant_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- variant_frequency_profile (population x measure columns)
combos = population_frequency.select("source_dataset", "population", "measure")
combos = [tuple(r) for r in combos.distinct().collect()]
pair_datasets = {}
for dataset, population, measure in combos:
    pair_datasets.setdefault((population, measure), set()).add(dataset)
combo_names = {combo: combo_name(pair_datasets, *combo) for combo in combos}
combo_map = F.create_map(
    *[
        F.lit(part)
        for combo, name in combo_names.items()
        for part in ("|".join(str(p) for p in combo), name)
    ]
)
combo_text = F.concat_ws(
    "|",
    *[
        F.coalesce(F.col(c).cast("string"), F.lit("None"))
        for c in ("source_dataset", "population", "measure")
    ],
)
frequency_rows = population_frequency.withColumn("_combo", combo_map[combo_text])
assert_unique_grain(frequency_rows, ["variant_key", "_combo"], **GATE)
by_population = (
    frequency_rows.groupBy("variant_key")
    .pivot("_combo", sorted(set(combo_names.values())))
    .agg(*[F.first(v).alias(v) for v in FREQUENCY_VALUES])
)
frequency_attributes = pivot_attributes(
    attributes,
    "variant",
    variant_columns,
    key="variant_key",
    rules=VARIANT_PROFILE_RULES["variant_frequency_profile"],
    **GATE,
)
TABLES["variant_frequency_profile"] = (
    add_provenance(
        variant.select(*LABELS).join(
            by_population.join(frequency_attributes, "variant_key", "full"),
            "variant_key",
            "right",
        ),
        SOURCE,
        RID,
        ecosystem="genomics",
    ),
    ["variant_key"],
)

# COMMAND ----------

# DBTITLE 1,Grain gate -- every table
gate_tables(TABLES, **GATE)

# COMMAND ----------

# DBTITLE 1,Write -- every table
write_tables(TABLES, **COMMON)

# COMMAND ----------

# DBTITLE 1,Inspect and export -- every table
inspect_tables(TABLES, findings="genomics", **COMMON)

# COMMAND ----------

# DBTITLE 1,Validation -- every table exists and has rows
validate_tables(TABLES, **COMMON)
