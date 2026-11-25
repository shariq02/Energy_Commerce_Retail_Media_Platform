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
# MAGIC in the spec file, by name or by a part of the name; they stay in the dataset with
# MAGIC the role `label_derived` and are never features. The identifier and coordinate
# MAGIC columns of a benchmark keep the role `key`. Class flags of Gold are true or null, so a target is the flag
# MAGIC with null read as false on the rows where its rule was evaluated. REBUILD names
# MAGIC the datasets to build again after a change; their frozen families are reopened.

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
# Empty: build every genomics dataset (a frozen family stops the build). Dataset ids:
# build only these and reopen their families first; the gate freezes them again.
REBUILD = ()
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
    if REBUILD and _name not in REBUILD:
        continue
    _rows = benchmarks.filter(F.col("benchmark_set") == _spec["benchmark_set"])
    _framed = _rows
    _groups = [
        *_spec["split"].get("group_cols", []),
        *_spec["split"].get("validation_group_cols", []),
    ]
    if "split_group" in _groups:
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

# DBTITLE 1,Variant datasets, second wave -- reclassification, allele frequency, haploinsufficiency
# ClinVar snapshots in order; the file of 2026-11-11 is the newest snapshot ("current")
SNAPSHOTS = [
    "2019-01",
    "2020-01",
    "2021-01",
    "2022-01",
    "2023-01",
    "2024-01",
    "2025-01",
    "2026-01",
    "2026-10-26",
    "current",
]
SNAPSHOT_DATES = [f"{s}-01" if len(s) == 7 else s for s in SNAPSHOTS]
SNAPSHOT_DATES[-1] = "2026-11-16"
UNCERTAIN_TERMS = (
    "Uncertain significance",
    "Conflicting interpretations of pathogenicity",
)
_pairs = [*zip(SNAPSHOTS, range(len(SNAPSHOTS)), strict=True)]
_pairs.append(("2026-11-11", len(SNAPSHOTS) - 1))
_ordinal_map = F.create_map(*[F.lit(v) for pair in _pairs for v in pair])


def snapshot_ordinal(column):
    """The position of a snapshot name in the order; null for a name that is not known."""
    return F.try_element_at(_ordinal_map, F.col(column))


_changes = table("clinvar_classification_change").filter(
    F.col("classification_type") == "germline"
)
# a run is a stretch of snapshots with one term; a run that ends in a change has a next run
_previous_runs = _changes.select(
    "variant_key",
    F.col("previous_term").alias("term"),
    snapshot_ordinal("previous_first_source_snapshot").alias("first_ordinal"),
    snapshot_ordinal("previous_last_source_snapshot").alias("last_ordinal"),
    F.lit(True).alias("has_next"),
    (F.col("clinical_significance_simple") == "1").alias("next_pathogenic"),
)
_final_runs = _changes.select(
    "variant_key",
    "term",
    snapshot_ordinal("first_source_snapshot").alias("first_ordinal"),
    snapshot_ordinal("last_source_snapshot").alias("last_ordinal"),
    F.lit(False).alias("has_next"),
    F.lit(False).alias("next_pathogenic"),
).join(
    _previous_runs.select("variant_key", "first_ordinal"),
    ["variant_key", "first_ordinal"],
    "left_anti",
)
_unchanged_runs = (
    table("variant_profile")
    .filter(F.col("clinical_significance_label").isin("VUS", "Conflicting"))
    .join(_changes.select("variant_key").distinct(), "variant_key", "left_anti")
    .select(
        "variant_key",
        F.when(F.col("clinical_significance_label") == "VUS", UNCERTAIN_TERMS[0])
        .otherwise(UNCERTAIN_TERMS[1])
        .alias("term"),
        snapshot_ordinal("clinvar_first_snapshot").alias("first_ordinal"),
        snapshot_ordinal("clinvar_last_snapshot").alias("last_ordinal"),
        F.lit(False).alias("has_next"),
        F.lit(False).alias("next_pathogenic"),
    )
)
# an event is a snapshot inside an uncertain run that has a next snapshot; the label is
# the class of the next snapshot (the rest of the run stays uncertain)
_runs = (
    _previous_runs.unionByName(_final_runs)
    .unionByName(_unchanged_runs)
    .filter(F.col("term").isin(*UNCERTAIN_TERMS))
    .withColumn(
        "last_event",
        F.when(F.col("has_next"), F.col("last_ordinal")).otherwise(
            F.col("last_ordinal") - 1
        ),
    )
    .filter(
        F.col("first_ordinal").isNotNull()
        & (F.col("last_event") >= F.col("first_ordinal"))
    )
)
_events = _runs.select(
    "variant_key",
    F.col("term").alias("term_at_snapshot"),
    "first_ordinal",
    "last_ordinal",
    "has_next",
    "next_pathogenic",
    F.explode(F.sequence("first_ordinal", "last_event")).alias("snapshot_ordinal"),
).select(
    "variant_key",
    F.element_at(
        F.array(*[F.lit(d) for d in SNAPSHOT_DATES]), F.col("snapshot_ordinal") + 1
    )
    .cast("date")
    .alias("snapshot_date"),
    "snapshot_ordinal",
    "term_at_snapshot",
    (F.col("snapshot_ordinal") - F.col("first_ordinal") + 1).alias(
        "snapshots_uncertain_so_far"
    ),
    F.when(
        (F.col("snapshot_ordinal") == F.col("last_ordinal")) & F.col("has_next"),
        F.col("next_pathogenic"),
    )
    .otherwise(F.lit(False))
    .alias("target_pathogenic_at_next_snapshot"),
)
# the 39 repeated runs of the change table give a repeated snapshot: one row is kept
FRAMES["variant_reclassification"] = _events.dropDuplicates(
    ["variant_key", "snapshot_date"]
).join(
    features_only(table("variant_profile"), keep=("gene_key",)), "variant_key", "left"
)
_frequency = "gnomad_variants_all_allele_frequency_allele_frequency"
FRAMES["allele_frequency"] = (
    features_only(
        table("variant_profile").filter(F.col(_frequency).isNotNull()),
        keep=("gene_key",),
    )
    .withColumn("split_group", F.coalesce(F.col("gene_key"), F.col("variant_key")))
    .withColumn("target_log_allele_frequency", F.log10(F.col(_frequency) + F.lit(1e-6)))
)
FRAMES["haploinsufficiency"] = features_only(
    genes.filter(F.col("gnomad_loss_of_function_probability").isNotNull())
).withColumn(
    "target_is_haploinsufficient", F.col("gnomad_loss_of_function_probability") >= 0.9
)

# COMMAND ----------

# DBTITLE 1,Gene measurement datasets -- essentiality, expression, gene and disease progression, ClinGen
measurements = table("gene_measurement")
gene_features = features_only(genes)
_gene_subset = genes.select(
    "gene_key",
    "constraint_lof_observed_over_expected",
    "constraint_mis_observed_over_expected",
    "gnomad_loss_of_function_probability",
    "gnomad_missense_z_score",
)
_cell_lines = (
    table("cell_line_profile")
    .groupBy("cell_line_key")
    .agg(
        F.first("oncotree_lineage", ignorenulls=True).alias("cell_line_lineage"),
        F.first("oncotree_primary_disease", ignorenulls=True).alias(
            "cell_line_primary_disease"
        ),
        F.first("model_type", ignorenulls=True).alias("cell_line_model_type"),
    )
)
FRAMES["depmap_essentiality"] = (
    measurements.filter(
        (F.col("record_kind") == "gene_dependency")
        & F.col("gene_key").isNotNull()
        & F.col("gene_effect").isNotNull()
    )
    .select("cell_line_key", "gene_key", "gene_effect")
    .dropDuplicates(["cell_line_key", "gene_key"])
    .join(_cell_lines, "cell_line_key", "left")
    .join(gene_features, "gene_key", "left")
    .withColumn("target_is_essential", F.col("gene_effect") < -0.5)
    .drop("gene_effect")
)
FRAMES["gtex_expression"] = (
    measurements.filter(
        (F.col("record_kind") == "gene_expression_median")
        & F.col("gene_key").isNotNull()
        & F.col("median_transcripts_per_million").isNotNull()
    )
    .select(
        "gene_key",
        "tissue_key",
        F.col("tissue_key").alias("tissue"),
        F.log1p("median_transcripts_per_million").alias("target_log_tpm"),
    )
    .dropDuplicates(["gene_key", "tissue_key"])
    .join(gene_features, "gene_key", "left")
)
# Open Targets scores by year: the gene key is not set on these rows, the Ensembl id is
OPEN_TARGETS_SOURCES = [
    "impc",
    "europepmc",
    "gwas_credible_sets",
    "expression_atlas",
    "clinical_precedence",
    "cancer_gene_census",
    "eva",
    "genomics_england",
    "uniprot_literature",
    "uniprot_variants",
    "orphanet",
    "crispr_screen",
    "gene2phenotype",
    "gene_burden",
    "reactome",
    "intogen",
    "eva_somatic",
    "clingen",
    "cancer_biomarkers",
    "crispr",
]
_scores = measurements.filter(F.col("record_kind") == "gene_disease_score_year")
_pair = ["source_gene_id", "source_disease_id"]


def overall_score(year):
    return _scores.filter(
        (F.col("association_level") == "overall") & (F.col("year") == year)
    ).select(*_pair, F.col("association_score").alias(f"score_{year}"))


_by_source = (
    _scores.filter(
        (F.col("association_level") == "datasource") & (F.col("year") == 2021)
    )
    .groupBy(*_pair)
    .pivot("datasource_id", OPEN_TARGETS_SOURCES)
    .agg(F.max("association_score"))
)
for _source in OPEN_TARGETS_SOURCES:
    _by_source = _by_source.withColumnRenamed(_source, f"score_{_source}_2021")
_novelty = _scores.filter(
    (F.col("association_level") == "overall") & (F.col("year") == 2021)
).select(*_pair, F.col("novelty").alias("novelty_2021"))
_gene_by_ensembl = (
    genes.filter(F.col("ensembl_id").isNotNull())
    .select(
        F.col("ensembl_id").alias("source_gene_id"),
        "constraint_lof_observed_over_expected",
        "constraint_mis_observed_over_expected",
        "gnomad_loss_of_function_probability",
        "gnomad_missense_z_score",
    )
    .dropDuplicates(["source_gene_id"])
)
FRAMES["gene_disease_progression"] = (
    overall_score(2021)
    .filter(F.col("score_2021") < 0.5)
    .join(overall_score(2025), _pair, "left")
    .join(overall_score(2020), _pair, "left")
    .join(overall_score(2019), _pair, "left")
    .join(_by_source, _pair, "left")
    .join(_novelty, _pair, "left")
    .join(_gene_by_ensembl, "source_gene_id", "left")
    .withColumn(
        "disease_ontology", F.regexp_extract("source_disease_id", r"^([A-Za-z]+)_", 1)
    )
    .withColumn(
        "target_is_strengthened", F.coalesce(F.col("score_2025"), F.lit(0.0)) >= 0.5
    )
    .drop("score_2025")
)
_relations = table("relation_record")
_other_sources = (
    _relations.filter(
        (F.col("relation_kind") == "gene_disease")
        & ~F.col("source_dataset").startswith("clingen")
    )
    .groupBy("gene_key")
    .agg(
        F.countDistinct("disease_key").alias("other_source_disease_count"),
        F.countDistinct("source_dataset").alias("other_source_count"),
    )
)
FRAMES["clingen_validity"] = (
    _relations.filter(F.col("source_dataset") == "clingen.gene_validity")
    .select("source_record_id", "gene_key", "disease_key", "classification")
    .filter(F.col("gene_key").isNotNull() & F.col("classification").isNotNull())
    .join(_other_sources, "gene_key", "left")
    .join(_gene_subset, "gene_key", "left")
    .withColumn(
        "target_is_definitive_or_strong",
        F.col("classification").isin("Definitive", "Strong"),
    )
    .drop("classification")
)

# COMMAND ----------

# DBTITLE 1,Assay, tumor, trial and FAERS datasets
_peptide = F.col("epitope_name")


def residue_share(letters):
    """The share of a peptide that is made of the amino acids in `letters`."""
    return F.try_divide(
        F.length(F.regexp_replace(_peptide, f"[^{letters}]", "")), F.length(_peptide)
    )


FRAMES["dms_effect"] = (
    features_only(
        table("dms_assay_mutant")
        .filter(F.col("scan_score_binarized").isNotNull())
        .drop("mutant", "assay_set"),
        keep=("source_record_id",),
    )
    .withColumn("target_is_damaging", F.col("scan_score_binarized").cast("int") == 1)
    .drop("scan_score", "scan_score_binarized")
)
cases = table("case_assay_record")
FRAMES["mhc_binding"] = cases.filter(
    (F.col("record_kind") == "epitope_mhc_assay")
    & (F.col("epitope_object_type") == "Linear peptide")
    & F.col("assay_qualitative_measurement").isNotNull()
).select(
    "source_record_id",
    "epitope_id",
    "mhc_restriction_name",
    "mhc_restriction_class",
    "host_name",
    "assay_method",
    "assay_response_measured",
    F.length(_peptide).alias("peptide_length"),
    residue_share("AILMFVWC").alias("hydrophobic_share"),
    residue_share("DEKRH").alias("charged_share"),
    residue_share("STNQ").alias("polar_share"),
    residue_share("FWY").alias("aromatic_share"),
    residue_share("P").alias("proline_share"),
    F.col("assay_qualitative_measurement")
    .startswith("Positive")
    .alias("target_is_binder"),
)
# the barcode parts and the tissue source site name the cohort: they are left out
FRAMES["cancer_type"] = (
    features_only(
        table("tumor_sample_profile")
        .filter(F.col("cohort").isNotNull())
        .drop(
            "tissue_source_site",
            "project",
            "plate",
            "center",
            "vial",
            "portion",
            "analyte",
        ),
        keep=("participant_barcode",),
    )
    .withColumn("target_cohort", F.col("cohort"))
    .drop("cohort")
)
# fields that are free text, or that a trial writes when it ends
TRIAL_TEXT = [
    "acronym",
    "brief_title",
    "official_title",
    "sponsor_source",
    "baseline_population",
    "limitations_and_caveats",
    "biospecimen_description",
    "biospecimen_retention",
    "individual_participant_data_time_frame",
    "individual_participant_data_access_criteria",
    "individual_participant_data_url",
    "plan_to_share_individual_participant_data_description",
    "eligibility_criteria",
    "gender_description",
    "expanded_access_nct_id",
    "expanded_access_status_for_nct_id",
    "last_known_status",
    "why_stopped",
]
FRAMES["trial_termination"] = (
    features_only(
        table("clinical_trial_profile")
        .filter(
            F.col("overall_status").isin("COMPLETED", "TERMINATED")
            & F.year("start_date").between(2000, 2021)
        )
        .drop(*TRIAL_TEXT),
        keep=("nct_id",),
    )
    .withColumn("target_is_terminated", F.col("overall_status") == "TERMINATED")
    .drop("overall_status")
)
_case_items = cases.filter(F.col("record_kind") == "faers_case_item")
_case = ["quarter", "primary_id"]
_outcome = (
    _case_items.filter(F.col("item_kind") == "outcome")
    .groupBy(*_case)
    .agg(
        F.max(F.col("item_value").isin("DE", "HO")).alias(
            "target_is_death_or_hospitalisation"
        )
    )
)
_indications = (
    _case_items.filter(F.col("item_kind") == "indication")
    .groupBy(*_case)
    .agg(F.count("*").alias("indication_count"))
)
_drugs = (
    cases.filter(F.col("record_kind") == "faers_case_drug")
    .groupBy(*_case)
    .agg(
        F.count("*").alias("drug_count"),
        F.sum((F.col("role_code") == "PS").cast("int")).alias("primary_suspect_count"),
        F.sum((F.col("role_code") == "SS").cast("int")).alias(
            "secondary_suspect_count"
        ),
        F.sum((F.col("role_code") == "C").cast("int")).alias("concomitant_count"),
        F.sum((F.col("role_code") == "I").cast("int")).alias("interacting_count"),
        F.countDistinct("route").alias("route_count"),
    )
)
_age, _age_unit = F.expr("try_cast(age as double)"), F.upper(F.col("age_unit"))
_age_years = (
    F.when(_age_unit == "YR", _age)
    .when(_age_unit == "DEC", _age * 10)
    .when(_age_unit == "MON", _age / 12)
    .when(_age_unit == "WK", _age / 52)
    .when(_age_unit == "DY", _age / 365)
    .when(_age_unit == "HR", _age / 8760)
)
_weight = F.expr("try_cast(weight as double)")
_weight_unit = F.upper(F.col("weight_unit"))
_weight_kg = F.when(_weight_unit == "KG", _weight).when(
    _weight_unit == "LBS", _weight * 0.4536
)
_quarter_start = F.make_date(
    F.substring("quarter", 1, 4).cast("int"),
    (F.substring("quarter", 6, 1).cast("int") - 1) * 3 + 1,
    F.lit(1),
)
FRAMES["faers_outcome"] = (
    cases.filter(F.col("record_kind") == "faers_case")
    .select(
        *_case,
        _quarter_start.alias("quarter_start"),
        _age_years.alias("age_years"),
        "sex",
        _weight_kg.alias("weight_kg"),
        "reporter_country",
        "occupation_code",
        "report_code",
        "initial_or_followup",
    )
    .join(_outcome, _case)
    .join(_indications, _case, "left")
    .join(_drugs, _case, "left")
)

# COMMAND ----------

# DBTITLE 1,Build the datasets -- contract, split, fit and the family rows
_unknown = [d for d in REBUILD if d not in FRAMES]
if _unknown:
    raise RuntimeError(
        f"REBUILD names datasets this notebook does not build: {_unknown}"
    )
for _dataset in REBUILD:
    reopen_family(_dataset)
build_datasets(
    {k: v for k, v in FRAMES.items() if not REBUILD or k in REBUILD},
    rid=rid,
    source=SOURCE,
    blocks=BLOCKS,
)

# COMMAND ----------

# DBTITLE 1,Summary
for _dataset in FRAMES:
    print(
        _dataset,
        read_ml("dataset_manifest", ecosystem=ECO)
        .filter(F.col("dataset_id") == _dataset)
        .first()["status"],
    )
