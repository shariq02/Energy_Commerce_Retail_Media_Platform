# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ML GENOMICS -- VARIANT, GENE AND BENCHMARK DATASETS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** build the genomics datasets of the families `variant`, `gene` and
# MAGIC `benchmark` from the Gold labels. The features are the Gold columns; no feature
# MAGIC or target table is stored. The label-derived columns of each target are declared
# MAGIC in the spec file; they stay in the dataset with the role `label_derived` and are
# MAGIC never features. Class flags of Gold are true or null, so a target is the flag
# MAGIC with null read as false on the rows where its rule was evaluated.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../_ml_inspect

# COMMAND ----------

# DBTITLE 1,ML build library
# MAGIC %run ../_ml_fit

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "genomics"
SOURCE = "genomics"
COMPONENT = "ml/genomics"
# columns that carry provenance or identify a row, never a feature
TECHNICAL = {
    "source_system",
    "source_record_id",
    "ecosystem",
    "_gold_loaded_at",
    "_gold_run_id",
}
IDENTIFIERS = {
    "name",
    "rsid",
    "symbol",
    "full_name",
    "description",
    "ensembl_id",
    "hgnc_id",
    "record_id",
    "pubmed_id",
    "omim",
    "benchmark_set",
    "source_dataset",
}
POSITIVE_LABELS = ("1", "True", "Pathogenic")
NEGATIVE_LABELS = ("0", "False", "Benign", "Common")
FRAMES, BLOCKS = {}, {}


def table(name):
    return spark.table(f"{CATALOG}.{GENOMICS_GOLD_SCHEMA}.{name}")


def features_only(df, keep=()):
    """The frame without provenance, method columns and identifier columns."""
    drop = [
        c
        for c in df.columns
        if c not in keep
        and (c in TECHNICAL or c in IDENTIFIERS or c.endswith(("_source", "_method")))
    ]
    return df.drop(*drop)


def flag(column):
    """A Gold class flag (true or null) as a boolean that reads null as false."""
    return F.coalesce(F.col(column), F.lit(False))


# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Read Gold -- the profiles and the benchmark table
variants = table("variant_profile").filter(
    F.col("clinical_significance_label").isNotNull()
)
genes = table("gene_profile")
structural = table("structural_variant_profile").filter(
    F.col("record_kind") == "variant"
)
benchmarks = table("benchmark_variant")
variant_genes = table("variant_profile").select("variant_key", "gene_key")

# COMMAND ----------

# DBTITLE 1,Benchmark datasets -- one dataset per benchmark set, the source split kept
# the datasets are built in the order of FRAMES: the quick ones first
_label = F.col("label")
_target = F.coalesce(
    F.col("label_integer"),
    F.when(_label.isin(*POSITIVE_LABELS), 1).when(_label.isin(*NEGATIVE_LABELS), 0),
).cast("int")
for _name, _spec in ML_SPEC["datasets"].items():
    if _spec["ecosystem"] != ECO or _spec.get("benchmark_set") is None:
        continue
    _rows = benchmarks.filter(F.col("benchmark_set") == _spec["benchmark_set"])
    _framed = _rows
    if "split_group" in _spec["split"].get("group_cols", []):
        # split by the gene of the variant; a variant without a gene is its own group
        _framed = (
            _rows.join(variant_genes, "variant_key", "left")
            .withColumn(
                "split_group", F.coalesce(F.col("gene_key"), F.col("variant_key"))
            )
            .drop("gene_key")
        )
    FRAMES[_name] = features_only(
        _framed, keep=("source_record_id", "gene", "protein_id", "split")
    ).withColumn("target_label_integer", _target)
    BLOCKS[_name] = [
        (
            "label_balance_per_split",
            markdown_table(
                ["split", "label", "rows"],
                [
                    (r["split"], r["label"], r["n"])
                    for r in _rows.groupBy("split", "label")
                    .agg(F.count("*").alias("n"))
                    .orderBy("split", "label")
                    .collect()
                ],
            ),
        )
    ]

# COMMAND ----------

# DBTITLE 1,Gene datasets -- pharmacogene and cancer role
FRAMES["gene_pharmacogene"] = features_only(genes).withColumn(
    "target_is_pharmacogene", flag("is_pharmacogene")
)
FRAMES["gene_cancer_role"] = features_only(
    genes.filter(F.col("gene_cancer_role").isNotNull())
).withColumn("target_gene_cancer_role", F.col("gene_cancer_role"))

# COMMAND ----------

# DBTITLE 1,Variant datasets -- pathogenic against benign, impact, carrier, actionable
_pathogenic = flag("is_pathogenic")
_labelled = _pathogenic | flag("is_benign")
FRAMES["structural_variant_disruption"] = features_only(
    structural.drop(
        "overlapping_gene_key",
        "gene_disruption_level",
        "gene_coverage_fraction",
        "overlap_length",
        "structural_variant_coverage_fraction",
        "record_kind",
    )
).withColumn(
    "target_has_critical_gene_disruption", flag("has_critical_gene_disruption")
)
FRAMES["variant_pathogenicity"] = features_only(
    variants.filter(_labelled), keep=("gene_key",)
).withColumn("target_is_pathogenic", _pathogenic)
FRAMES["variant_high_impact"] = features_only(
    variants.filter(F.col("pathogenicity_score").isNotNull()), keep=("gene_key",)
).withColumn("target_is_high_impact", flag("is_high_impact"))
_with_frequency = variants.filter(F.col("max_population_frequency").isNotNull())
FRAMES["variant_carrier_screening"] = features_only(
    _with_frequency, keep=("gene_key",)
).withColumn(
    "target_is_carrier_screening_candidate", flag("is_carrier_screening_candidate")
)
FRAMES["variant_actionable_rare"] = features_only(
    _with_frequency, keep=("gene_key",)
).withColumn(
    "target_is_clinically_actionable_rare_variant",
    flag("is_clinically_actionable_rare_variant"),
)

# COMMAND ----------

# DBTITLE 1,Build the datasets -- contract, split, fit and the family rows
build_datasets(FRAMES, rid=rid, source=SOURCE, blocks=BLOCKS)

# COMMAND ----------

# DBTITLE 1,Summary
for _dataset in FRAMES:
    print(
        _dataset,
        read_ml("dataset_manifest", ecosystem=ECO)
        .filter(F.col("dataset_id") == _dataset)
        .first()["status"],
    )
