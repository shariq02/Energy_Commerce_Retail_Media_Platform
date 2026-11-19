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
# MAGIC **Purpose:** `gene_profile`, `variant_profile`, `multi_value_attribute` and
# MAGIC `multi_value_score` of `genomics_gold`. The Silver attributes are pivoted to
# MAGIC columns; `variant_profile` also holds the variant scores and the population
# MAGIC frequencies as columns. A cell with more than one value stays empty; its rows
# MAGIC go to `multi_value_attribute` (attributes) or `multi_value_score` (scores).

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

# DBTITLE 1,Transform -- multi_value_attribute and gene_profile (every gene rule)
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
        columns=gene_columns,
        **PROFILE,
    ),
    ["gene_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- variant scores (columns) and multi_value_score
# A variant with more than one row of a score (AlphaMissense: one per transcript)
# keeps the profile cell empty; its rows go to multi_value_score.
variant_scores = locus_score.filter(F.col("variant_key").isNotNull()).withColumn(
    "_values",
    F.count(F.lit(1)).over(Window.partitionBy("variant_key", "score_name")),
)
TABLES["multi_value_score"] = (
    add_provenance(
        from_silver(variant_scores.filter(F.col("_values") > 1).drop("_values")),
        SOURCE,
        RID,
        ecosystem="genomics",
    ),
    ["source_record_id"],
)
variant_scores = variant_scores.filter(F.col("_values") == 1).drop("_values")
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
by_score = by_variant.join(by_position, "variant_key", "full")

# COMMAND ----------

# DBTITLE 1,Transform -- population frequencies (population x measure columns)
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

# COMMAND ----------

# DBTITLE 1,Transform -- variant_profile (attributes, scores and frequencies)
# The structural rules stay in structural_variant_profile (gold/genomics/02).
variant_profile = full_profile(
    variant,
    entity_type="variant",
    key="variant_key",
    exclude=STRUCTURAL_RULES,
    columns=variant_columns,
    **PROFILE,
)
for part, suffix in ((by_score, "__score"), (by_population, "__frequency")):
    variant_profile = join_profile(variant_profile, part, "variant_key", "left", suffix)
TABLES["variant_profile"] = (variant_profile, ["variant_key"])

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
