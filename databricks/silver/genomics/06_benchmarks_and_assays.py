# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- BENCHMARKS AND ASSAYS (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** benchmark variant sets, MAPS embedding splits and ProteinGym assays
# MAGIC with their mutants.
# MAGIC
# MAGIC **Run:** `TABLES` lists the tables to build (default `all`); the others are skipped.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Genomics library
# MAGIC %run ../_genomics_common

# COMMAND ----------

# DBTITLE 1,Entity helpers
# MAGIC %run ../_genomics_entities

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F

SOURCE = "genomics"
COMPONENT = "silver/genomics/06_benchmarks_and_assays"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()
AVAILABLE = ["benchmark_variant", "embedding_split", "dms_assay", "dms_assay_mutant"]
dbutils.widgets.text("TABLES", "all")
SELECTED = {
    name.strip() for name in dbutils.widgets.get("TABLES").split(",") if name.strip()
}
UNKNOWN = SELECTED - {"all", *AVAILABLE}
if UNKNOWN:
    message = f"unknown table(s) in TABLES: {sorted(UNKNOWN)}"
    raise ValueError(message)


def selected(table):
    return "all" in SELECTED or table in SELECTED


# COMMAND ----------

# DBTITLE 1,Build -- benchmark_variant
if selected("benchmark_variant"):
    # -- Helpers -- typed columns and the common row shape
    COLUMNS = GENOMICS_STRUCTURES["benchmark_variant"]

    text = source_text

    number = source_number

    rows = rows_factory(COLUMNS, label_column="benchmark_set")

    def coordinates(dataset, chrom, pos, ref, alt):
        """Variant columns of a set with VCF alleles (GRCh38 assumed)."""
        return {
            "variant_key": variant_key_expr(
                "GRCh38",
                text(chrom, dataset),
                text(pos, dataset),
                text(ref, dataset),
                text(alt, dataset),
            ),
            "assembly_basis": F.lit("assumed_grch38"),
            "contig": chromosome_expr(text(chrom, dataset)),
            "position": number(pos, "bigint", dataset),
            "reference_allele": F.upper(text(ref, dataset)),
            "alternate_allele": F.upper(text(alt, dataset)),
        }

    # -- Transform -- long-range pathogenic sets and eQTL
    parts = []
    for table, dataset in (
        ("long_range_benchmark_pathogenic_coding", LR_CODING),
        ("long_range_benchmark_pathogenic_non_coding", LR_NON_CODING),
    ):
        parts.append(
            rows(
                read_genomics_bronze(table),
                dataset.split(".")[1],
                dataset,
                **coordinates(dataset, "CHROM", "POS", "REF", "ALT"),
                label=text("LABEL", dataset),
                label_integer=number("INT_LABEL", "int", dataset),
                split=text("split", dataset),
                source_label=text("SOURCE", dataset),
                consequence=text("CONSEQUENCE", dataset),
                review_status=text("REVIEW_STATUS", dataset),
                record_id=text("ID", dataset),
                genomic_mutation_id=text("GENOMIC_MUTATION_ID", dataset),
                gene=text("GENE", dataset),
                omim=text("OMIM", dataset),
                pubmed_id=text("PMID", dataset),
                sample_count=number("N_SAMPLES", "bigint", dataset),
                total_sample_count=number("TOTAL_SAMPLES", "bigint", dataset),
                frequency=number("FREQ", "double", dataset),
                allele_count=number("AC", "double", dataset),
                allele_number=number("AN", "double", dataset),
                allele_frequency=number("AF", "double", dataset),
                minor_allele_frequency=number("MAF", "double", dataset),
                minor_allele_count=number("MAC", "double", dataset),
            )
        )
    parts.append(
        rows(
            read_genomics_bronze("long_range_benchmark_causal_eqtl"),
            "causal_eqtl",
            LR_EQTL,
            **coordinates(LR_EQTL, "CHROM", "POS", "REF", "ALT"),
            label=text("label", LR_EQTL),
            split=text("split", LR_EQTL),
            record_id=text("SNP", LR_EQTL),
            tissue=text("tissue", LR_EQTL),
            distance_to_nearest_transcription_start_site=number(
                "distance_to_nearest_TSS", "double", LR_EQTL
            ),
        )
    )

    # -- Transform -- gnomAD balanced and ClinVar versus benign
    parts.append(
        rows(
            read_genomics_bronze("gnomad_balanced_variants"),
            "gnomad_balanced",
            GNOMAD_BALANCED,
            **coordinates(GNOMAD_BALANCED, "chrom", "pos", "ref", "alt"),
            label=text("label", GNOMAD_BALANCED),
            consequence=text("consequence", GNOMAD_BALANCED),
        )
    )
    parts.append(
        rows(
            read_genomics_bronze("clinvar_vs_benign_variants"),
            "clinvar_vs_benign",
            CLINVAR_BENIGN,
            **coordinates(CLINVAR_BENIGN, "chrom", "pos", "ref", "alt"),
            label=text("label", CLINVAR_BENIGN),
            record_id=text("id", CLINVAR_BENIGN),
            review_status=text("review_status", CLINVAR_BENIGN),
            consequence=text("consequence", CLINVAR_BENIGN),
            gpn_star_v100=number("GPN_Star_V100", "double", CLINVAR_BENIGN),
            gpn_star_p243=number("GPN_Star_P243", "double", CLINVAR_BENIGN),
            gpn_star_m447=number("GPN_Star_M447", "double", CLINVAR_BENIGN),
        )
    )

    # -- Transform -- SpectralBio, ProteinGym clinical and MAPS
    SPECTRALBIO = "spectralbio.variants"
    parts.append(
        rows(
            read_genomics_bronze("spectralbio_variants"),
            "spectralbio",
            SPECTRALBIO,
            gene=text("gene", SPECTRALBIO),
            wild_type_aa=text("wt_aa", SPECTRALBIO),
            protein_position=number("position", "int", SPECTRALBIO),
            mutant_aa=text("mut_aa", SPECTRALBIO),
            label=text("label", SPECTRALBIO),
            split=text("subset", SPECTRALBIO),
            record_id=text("name", SPECTRALBIO),
        )
    )
    CLINICAL = "proteingym.clinical_substitutions"
    SINGLE = r"^([A-Z])(\d+)([A-Z])$"
    mutant = text("mutant", CLINICAL)
    parts.append(
        rows(
            read_genomics_bronze("proteingym_clinical_substitutions"),
            "proteingym_clinical",
            CLINICAL,
            protein_id=text("protein", CLINICAL),
            mutant=mutant,
            label_integer=number("DMS_bin_score", "int", CLINICAL),
            wild_type_aa=F.nullif(F.regexp_extract(mutant, SINGLE, 1), F.lit("")),
            protein_position=F.nullif(
                F.regexp_extract(mutant, SINGLE, 2), F.lit("")
            ).cast("int"),
            mutant_aa=F.nullif(F.regexp_extract(mutant, SINGLE, 3), F.lit("")),
        )
    )
    MAPS = "maps_embeddings.metadata"
    parts.append(
        rows(
            read_genomics_bronze("maps_embeddings_metadata"),
            "maps",
            MAPS,
            record_id=text("variant_id", MAPS),
            protein_id=text("protein_id", MAPS),
            protein_position=number("pos1", "int", MAPS),
            wild_type_aa=text("wt_aa", MAPS),
            mutant_aa=text("mut_aa", MAPS),
            sequence_length=number("seq_len", "int", MAPS),
            label=text("label", MAPS),
            split=text("split", MAPS),
            source_label=text("source", MAPS),
            stars=number("stars", "int", MAPS),
            cluster=text("cluster", MAPS),
            shuffle_rank=text("shuffle_rank", MAPS),
            split_file=text("split_file", MAPS),
        )
    )

    # -- Transform -- union and provenance
    benchmark = (
        _union(parts)
        .distinct()
        .withColumn(
            "_srid",
            sha_key(
                "benchmark_set",
                "record_id",
                "genomic_mutation_id",
                "variant_key",
                "contig",
                "position",
                "reference_allele",
                "alternate_allele",
                "protein_id",
                "mutant",
                "protein_position",
                "wild_type_aa",
                "mutant_aa",
                "split_file",
                "split",
                "label",
                "gene",
                "tissue",
            ),
        )
    )
    benchmark = conform_genomics(
        add_genomics_provenance(benchmark, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- benchmark_variant
if selected("benchmark_variant"):
    write_silver(
        benchmark, "benchmark_variant", source=SOURCE, component=COMPONENT, rid=RID
    )

    written = spark.table(genomics_table("benchmark_variant"))
    blocks = inspect_table(
        written,
        "benchmark_variant",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        extra_checks={
            "sets": str(
                {
                    r["benchmark_set"]: r["count"]
                    for r in written.groupBy("benchmark_set").count().collect()
                }
            ),
            "records_without_a_variant_key": written.filter(
                F.col("variant_key").isNull() & F.col("contig").isNotNull()
            ).count(),
            **{
                f"{column}_counts": str(
                    {
                        r[column]: r["count"]
                        for r in written.groupBy(column).count().collect()
                    }
                )
                for column in written.columns
                if column.endswith("_link_method")
            },
        },
    )

    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__benchmark_variant",
        "benchmark_variant",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Build -- embedding_split
if selected("embedding_split"):
    # -- Transform -- one row per split file
    DATASET = "maps_embeddings.metadata"

    text = text_for(DATASET)

    meta = read_genomics_bronze("maps_embeddings_metadata")
    split = (
        meta.filter(text("split_file").isNotNull())
        .groupBy(text("split_file").alias("split_file"))
        .agg(
            F.min(typed_number("X_rows", "int", RULES, f"{DATASET}.X_rows")).alias(
                "matrix_rows"
            ),
            F.min(typed_number("X_cols", "int", RULES, f"{DATASET}.X_cols")).alias(
                "matrix_columns"
            ),
            F.min(text("idx")).alias("index_values"),
            F.min(text("Xmean")).alias("mean_embedding"),
            F.count(F.lit(1)).alias("variant_rows"),
            F.countDistinct(text("idx")).alias("distinct_index_values"),
            F.countDistinct(text("Xmean")).alias("distinct_mean_embedding_values"),
        )
        .withColumn("_srid", F.col("split_file"))
    )
    split = conform_genomics(
        add_genomics_provenance(split, "_srid", RID), "embedding_split"
    )

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- embedding_split
if selected("embedding_split"):
    write_silver(split, "embedding_split", source=SOURCE, component=COMPONENT, rid=RID)

    written = spark.table(genomics_table("embedding_split"))
    blocks = inspect_table(
        written,
        "embedding_split",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["split_file"],
        extra_checks={
            "split_files": written.count(),
            "files_with_several_index_values": written.filter(
                F.col("distinct_index_values") > 1
            ).count(),
            "files_with_several_mean_embedding_values": written.filter(
                F.col("distinct_mean_embedding_values") > 1
            ).count(),
            **{
                f"{column}_counts": str(
                    {
                        r[column]: r["count"]
                        for r in written.groupBy(column).count().collect()
                    }
                )
                for column in written.columns
                if column.endswith("_link_method")
            },
        },
    )

    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__embedding_split",
        "embedding_split",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Build -- dms_assay
if selected("dms_assay"):
    # -- Read Silver -- protein ids
    xwalk = read_silver("entity_id_crosswalk")
    protein_map = entity_id_map(xwalk, "protein")
    COLUMNS = GENOMICS_STRUCTURES["dms_assay"]

    # -- Helpers -- source columns by target column
    DMS = {
        "assay_id": "DMS_id",
        "assay_index": "DMS_index",
        "assay_filename": "DMS_filename",
        "uniprot_id": "UniProt_ID",
        "taxon": "taxon",
        "source_organism": "source_organism",
        "target_sequence": "target_seq",
        "sequence_length": "seq_len",
        "includes_multiple_mutants": "includes_multiple_mutants",
        "total_mutants": "DMS_total_number_mutants",
        "single_mutants": "DMS_number_single_mutants",
        "multiple_mutants": "DMS_number_multiple_mutants",
        "binarization_cutoff": "DMS_binarization_cutoff",
        "binarization_method": "DMS_binarization_method",
        "first_author": "first_author",
        "title": "title",
        "publication_year": "year",
        "journal": "jo",
        "region_mutated": "region_mutated",
        "molecule_name": "molecule_name",
        "selection_assay": "selection_assay",
        "selection_type": "selection_type",
        "coarse_selection_type": "coarse_selection_type",
        "alignment_filename": "MSA_filename",
        "alignment_start": "MSA_start",
        "alignment_end": "MSA_end",
        "alignment_length": "MSA_len",
        "alignment_bit_score": "MSA_bitscore",
        "alignment_theta": "MSA_theta",
        "alignment_sequence_count": "MSA_num_seqs",
        "alignment_percent_coverage": "MSA_perc_cov",
        "alignment_coverage_count": "MSA_num_cov",
        "alignment_effective_sequences": "MSA_N_eff",
        "alignment_effective_sequences_per_length": "MSA_Neff_L",
        "alignment_effective_sequences_per_length_category": "MSA_Neff_L_category",
        "alignment_significant_count": "MSA_num_significant",
        "alignment_significant_per_length": "MSA_num_significant_L",
        "raw_scan_filename": "raw_DMS_filename",
        "raw_scan_phenotype_name": "raw_DMS_phenotype_name",
        "raw_scan_directionality": "raw_DMS_directionality",
        "raw_scan_mutant_column": "raw_DMS_mutant_column",
        "weight_file_name": "weight_file_name",
        "protein_structure_file": "pdb_file",
        "protein_structure_range": "pdb_range",
        "proteingym_version": "ProteinGym_version",
        "raw_mutation_offset": "raw_mut_offset",
    }
    CLINICAL = {
        "assay_id": "DMS_id",
        "target_sequence": "target_seq",
        "file_length": "file_length",
        "assay_filename": "DMS_filename",
        "eve_model_path": "EVE_model_path",
        "alignment_filename": "MSA_filename",
        "alignment_source": "alignment_source",
        "weight_file_name": "weight_file_name",
        "alignment_start": "MSA_start",
        "alignment_end": "MSA_end",
        "alignment_length": "MSA_len",
    }

    # -- Transform -- typed rows of both reference tables

    rows = mapped_rows_factory(COLUMNS, "assay_set")

    DMS_SET = "proteingym.dms_reference"
    CLINICAL_SET = "proteingym.clinical_reference"
    dms = link_entities(
        read_genomics_bronze("proteingym_dms_reference"),
        [
            (
                clean_placeholders("UniProt_ID", RULES, f"{DMS_SET}.UniProt_ID"),
                "uniprot_entry_name",
            ),
            (
                clean_placeholders("UniProt_ID", RULES, f"{DMS_SET}.UniProt_ID"),
                "uniprot",
            ),
        ],
        protein_map,
        "protein_key",
        "protein_link_method",
    )
    assay = rows(dms, "dms", DMS_SET, DMS).unionByName(
        rows(
            read_genomics_bronze("proteingym_clinical_reference"),
            "clinical",
            CLINICAL_SET,
            CLINICAL,
        )
    )
    assay = assay.distinct().withColumn("_srid", sha_key("assay_set", "assay_id"))
    assay = conform_genomics(add_genomics_provenance(assay, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- dms_assay
if selected("dms_assay"):
    write_silver(assay, "dms_assay", source=SOURCE, component=COMPONENT, rid=RID)

    written = spark.table(genomics_table("dms_assay"))
    blocks = inspect_table(
        written,
        "dms_assay",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["assay_set", "assay_id"],
        extra_checks={
            "assays": str(
                {
                    r["assay_set"]: r["count"]
                    for r in written.groupBy("assay_set").count().collect()
                }
            ),
            "dms_assays_without_a_protein_key": written.filter(
                (F.col("assay_set") == "dms") & F.col("protein_key").isNull()
            ).count(),
            **{
                f"{column}_counts": str(
                    {
                        r[column]: r["count"]
                        for r in written.groupBy(column).count().collect()
                    }
                )
                for column in written.columns
                if column.endswith("_link_method")
            },
        },
    )

    write_silver_findings(
        FINDINGS, f"{COMPONENT.split('/')[-1]}__dms_assay", "dms_assay", blocks
    )

# COMMAND ----------

# DBTITLE 1,Build -- dms_assay_mutant
if selected("dms_assay_mutant"):
    # -- Transform -- substitutions of both sets
    SINGLE = r"^([A-Z*])(\d+)([A-Z*])$"
    DMS = "proteingym.dms_substitutions"
    CLINICAL = "proteingym.clinical_substitutions"

    def mutant_columns(mutant):
        one = F.size(F.split(mutant, ":")) == 1
        return {
            "mutation_count": F.size(F.split(mutant, ":")),
            "wild_type_aa": F.when(
                one, F.nullif(F.regexp_extract(mutant, SINGLE, 1), F.lit(""))
            ),
            "position": F.when(
                one, F.nullif(F.regexp_extract(mutant, SINGLE, 2), F.lit(""))
            ).cast("int"),
            "mutant_aa": F.when(
                one, F.nullif(F.regexp_extract(mutant, SINGLE, 3), F.lit(""))
            ),
        }

    text = source_text

    dms_mutant = text("mutant", DMS)
    dms = read_genomics_bronze("proteingym_dms_substitutions").select(
        F.lit("dms").alias("assay_set"),
        text("DMS_id", DMS).alias("assay_id"),
        dms_mutant.alias("mutant"),
        *[value.alias(name) for name, value in mutant_columns(dms_mutant).items()],
        typed_number("DMS_score", "double", RULES, f"{DMS}.DMS_score").alias(
            "scan_score"
        ),
        typed_number("DMS_score_bin", "int", RULES, f"{DMS}.DMS_score_bin").alias(
            "scan_score_binarized"
        ),
        F.lit(DMS).alias("source_dataset"),
    )
    clinical_mutant = text("mutant", CLINICAL)
    clinical = read_genomics_bronze("proteingym_clinical_substitutions").select(
        F.lit("clinical").alias("assay_set"),
        text("protein", CLINICAL).alias("assay_id"),
        clinical_mutant.alias("mutant"),
        *[value.alias(name) for name, value in mutant_columns(clinical_mutant).items()],
        F.lit(None).cast("double").alias("scan_score"),
        typed_number("DMS_bin_score", "int", RULES, f"{CLINICAL}.DMS_bin_score").alias(
            "scan_score_binarized"
        ),
        F.lit(CLINICAL).alias("source_dataset"),
    )
    mutant = dms.unionByName(clinical).filter(F.col("mutant").isNotNull())

    # -- Transform -- keys and provenance
    mutant = mutant.distinct().withColumn(
        "_srid",
        sha_key(
            "assay_set", "assay_id", "mutant", "scan_score", "scan_score_binarized"
        ),
    )
    mutant = conform_genomics(add_genomics_provenance(mutant, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- dms_assay_mutant
if selected("dms_assay_mutant"):
    write_silver(
        mutant, "dms_assay_mutant", source=SOURCE, component=COMPONENT, rid=RID
    )

    written = spark.table(genomics_table("dms_assay_mutant"))
    blocks = inspect_table(
        written,
        "dms_assay_mutant",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        extra_checks={
            "assay_sets": str(
                {
                    r["assay_set"]: r["count"]
                    for r in written.groupBy("assay_set").count().collect()
                }
            ),
            "multiple_mutants": written.filter(F.col("mutation_count") > 1).count(),
            "single_mutants_not_parsed": written.filter(
                (F.col("mutation_count") == 1) & F.col("position").isNull()
            ).count(),
            **{
                f"{column}_counts": str(
                    {
                        r[column]: r["count"]
                        for r in written.groupBy(column).count().collect()
                    }
                )
                for column in written.columns
                if column.endswith("_link_method")
            },
        },
    )

    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__dms_assay_mutant",
        "dms_assay_mutant",
        blocks,
    )
