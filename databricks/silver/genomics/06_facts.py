# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- FACTS (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** expression, tumor sample, somatic mutation, perturbation and FAERS facts.
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

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "genomics"
COMPONENT = "silver/genomics/06_facts"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()
AVAILABLE = [
    "gene_expression_median",
    "tumor_sample",
    "somatic_mutation",
    "perturbation_sample",
    "perturbation_expression",
    "faers_case",
    "faers_case_drug",
]
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

# DBTITLE 1,Read Silver -- gene ids
if selected("gene_expression_median"):
    xwalk = read_silver("entity_id_crosswalk")
    gene_map = entity_id_map(xwalk, "gene")

# COMMAND ----------

# DBTITLE 1,Transform -- wide tissue columns to rows
if selected("gene_expression_median"):
    DATASET = "gtex.gene_median_tpm"
    gtex = read_genomics_bronze("gtex_gene_median_tpm")
    tissues = gtex.columns[2:]
    stack = "stack({}, {})".format(
        len(tissues), ", ".join(f"'{t}', `{t}`" for t in tissues)
    )
    long_rows = gtex.select(
        F.col("Name").alias("gene_ensembl_id"),
        F.col("Description").alias("gene_symbol"),
        F.expr(stack).alias("tissue_key", "median_text"),
    ).withColumn(
        "median_transcripts_per_million",
        typed_number("median_text", "double", RULES, None),
    )
    long_rows = link_entities(
        long_rows,
        [(ensembl_unversioned_expr(F.col("gene_ensembl_id")), "ensembl")],
        gene_map,
        "gene_key",
        "gene_link_method",
    )

# COMMAND ----------

# DBTITLE 1,Transform -- columns and provenance
if selected("gene_expression_median"):
    expression = (
        long_rows.filter(F.col("median_transcripts_per_million").isNotNull())
        .withColumn("source_dataset", F.lit(DATASET))
        .withColumn("_srid", sha_key("gene_ensembl_id", "tissue_key"))
    )
    expression = conform_genomics(
        add_genomics_provenance(expression, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write Silver -- gene_expression_median
if selected("gene_expression_median"):
    write_silver(
        expression,
        "gene_expression_median",
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- gene_expression_median
if selected("gene_expression_median"):
    written = spark.table(genomics_table("gene_expression_median"))
    blocks = inspect_table(
        written,
        "gene_expression_median",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["gene_ensembl_id", "tissue_key"],
        extra_checks={
            "tissues": written.select("tissue_key").distinct().count(),
            "genes_without_a_gene_key": written.filter(F.col("gene_key").isNull())
            .select("gene_ensembl_id")
            .distinct()
            .count(),
            "zero_values": written.filter(
                F.col("median_transcripts_per_million") == 0
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

# COMMAND ----------

# DBTITLE 1,Export findings -- gene_expression_median
if selected("gene_expression_median"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__gene_expression_median",
        "gene_expression_median",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Transform -- barcodes and their parts
if selected("tumor_sample"):
    DATASET = "tcga.mc3_somatic_mutations"
    barcode = clean_placeholders(
        "Tumor_Sample_Barcode", RULES, f"{DATASET}.Tumor_Sample_Barcode"
    )
    counts = (
        read_genomics_bronze("tcga_mc3_somatic_mutations")
        .select(barcode.alias("sample_barcode"))
        .filter(F.col("sample_barcode").isNotNull())
        .groupBy("sample_barcode")
        .agg(F.count(F.lit(1)).alias("mutation_records"))
    )
    parsed = counts.withColumn("parsed", tcga_barcode_udf()(F.col("sample_barcode")))
    sample = parsed.select(
        "sample_barcode",
        F.col("parsed.participant_barcode").alias("participant_barcode"),
        *[
            F.col(f"parsed.{n}").alias(n)
            for n, _ in TCGA_FIELDS
            if n not in ("participant_barcode", "sample_barcode")
        ],
        "mutation_records",
        F.lit(DATASET).alias("source_dataset"),
    ).withColumn("_srid", F.col("sample_barcode"))
    sample = conform_genomics(add_genomics_provenance(sample, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- tumor_sample
if selected("tumor_sample"):
    write_silver(sample, "tumor_sample", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- tumor_sample
if selected("tumor_sample"):
    written = spark.table(genomics_table("tumor_sample"))
    blocks = inspect_table(
        written,
        "tumor_sample",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["sample_barcode"],
        extra_checks={
            "unparsed_barcodes": written.filter(
                F.col("parse_status") == "unparsed"
            ).count(),
            "projects": written.select("project").distinct().count(),
            "participants": written.select("participant_barcode").distinct().count(),
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

# COMMAND ----------

# DBTITLE 1,Export findings -- tumor_sample
if selected("tumor_sample"):
    write_silver_findings(
        FINDINGS, f"{COMPONENT.split('/')[-1]}__tumor_sample", "tumor_sample", blocks
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- id maps and protein changes
if selected("somatic_mutation"):
    xwalk = read_silver("entity_id_crosswalk")
    names = read_silver("entity_name")
    gene_map = entity_id_map(xwalk, "gene").unionByName(
        term_map(names, "gene", ("symbol", "official_symbol"), "gene_symbol")
    )
    transcript_map = entity_id_map(xwalk, "transcript")
    variant_map = entity_id_map(xwalk, "variant")
    protein_changes = (
        read_silver("variant_hgvs")
        .filter(
            F.col("transcript_key").isNotNull()
            & F.col("protein_reference_amino_acid").isNotNull()
            & F.col("protein_position").isNotNull()
            & F.col("protein_alternate_amino_acid").isNotNull()
        )
        .groupBy(
            "transcript_key",
            "protein_reference_amino_acid",
            "protein_position",
            "protein_alternate_amino_acid",
        )
        .agg(
            F.countDistinct("variant_key").alias("variants"),
            F.min("variant_key").alias("change_variant_key"),
        )
        .filter(F.col("variants") == 1)
        .select(
            "transcript_key",
            "protein_reference_amino_acid",
            "protein_position",
            "protein_alternate_amino_acid",
            "change_variant_key",
        )
    )

# COMMAND ----------

# DBTITLE 1,Transform -- columns, parsed protein change and keys
if selected("somatic_mutation"):
    DATASET = "tcga.mc3_somatic_mutations"

    def text(column):
        return clean_placeholders(column, RULES, f"{DATASET}.{column}")

    def number(column, kind):
        return typed_number(column, kind, RULES, f"{DATASET}.{column}")

    EXON = r"^(\d+)/(\d+)$"
    mutation = read_genomics_bronze("tcga_mc3_somatic_mutations")
    mutation = link_entities(
        mutation,
        [
            (text("Gene"), "ensembl"),
            (text("Entrez_Gene_Id"), "ncbi_gene"),
            (F.upper(text("Hugo_Symbol")), "gene_symbol"),
        ],
        gene_map,
        "gene_key",
        "gene_link_method",
    )
    mutation = mutation.withColumn(
        "transcript_accession", ensembl_unversioned_expr(text("Transcript_ID"))
    )
    mutation = link_entities(
        mutation,
        [(F.col("transcript_accession"), "ensembl_transcript")],
        transcript_map,
        "transcript_key",
        "transcript_method",
    )
    mutation = link_entities(
        mutation,
        [(text("dbSNP_RS"), "rsid")],
        variant_map,
        "rsid_variant_key",
        "rsid_method",
    )
    mutation = mutation.withColumn("protein", hgvs_protein_udf()(text("HGVSp_Short")))
    mutation = (
        mutation.withColumn(
            "protein_reference_amino_acid",
            F.col("protein.protein_reference_amino_acid"),
        )
        .withColumn("protein_position", F.col("protein.protein_position"))
        .withColumn(
            "protein_alternate_amino_acid",
            F.col("protein.protein_alternate_amino_acid"),
        )
        .withColumn("protein_edit_type", F.col("protein.protein_edit_type"))
    )
    mutation = mutation.join(
        protein_changes,
        [
            "transcript_key",
            "protein_reference_amino_acid",
            "protein_position",
            "protein_alternate_amino_acid",
        ],
        "left",
    )

# COMMAND ----------

# DBTITLE 1,Transform -- record and provenance
if selected("somatic_mutation"):
    somatic = (
        mutation.select(
            text("Tumor_Sample_Barcode").alias("sample_barcode"),
            text("Hugo_Symbol").alias("gene_symbol"),
            "gene_key",
            "gene_link_method",
            text("Gene").alias("ensembl_gene_id"),
            text("NCBI_Build").alias("assembly"),
            chromosome_expr(text("Chromosome")).alias("contig"),
            number("Start_Position", "bigint").alias("start_position"),
            number("End_Position", "bigint").alias("end_position"),
            text("Strand").alias("strand"),
            text("Variant_Classification").alias("variant_classification"),
            text("Variant_Type").alias("variant_type"),
            text("Reference_Allele").alias("reference_allele"),
            text("Tumor_Seq_Allele1").alias("tumor_allele1"),
            text("Tumor_Seq_Allele2").alias("tumor_allele2"),
            text("dbSNP_RS").alias("rsid"),
            text("HGVSc").alias("coding_change_notation"),
            text("HGVSp_Short").alias("protein_change_short_notation"),
            "protein_reference_amino_acid",
            "protein_position",
            "protein_alternate_amino_acid",
            "protein_edit_type",
            "transcript_key",
            text("Transcript_ID").alias("transcript_source_id"),
            text("Exon_Number").alias("exon_number"),
            F.nullif(F.regexp_extract(text("Exon_Number"), EXON, 1), F.lit(""))
            .cast("int")
            .alias("exon_index"),
            F.nullif(F.regexp_extract(text("Exon_Number"), EXON, 2), F.lit(""))
            .cast("int")
            .alias("exon_count"),
            number("t_depth", "int").alias("tumor_depth"),
            number("t_ref_count", "int").alias("tumor_ref_count"),
            number("t_alt_count", "int").alias("tumor_alt_count"),
            text("IMPACT").alias("impact"),
            text("FILTER").alias("filter_status"),
            F.coalesce("rsid_variant_key", "change_variant_key").alias("variant_key"),
            F.when(F.col("rsid_variant_key").isNotNull(), F.lit("rsid"))
            .when(
                F.col("change_variant_key").isNotNull(),
                F.lit("transcript_protein_change"),
            )
            .otherwise(F.lit("unlinked"))
            .alias("variant_link_method"),
            F.lit(DATASET).alias("source_dataset"),
        )
        .distinct()
        .withColumn(
            "_srid",
            sha_key(
                "sample_barcode",
                "contig",
                "start_position",
                "end_position",
                "reference_allele",
                "tumor_allele1",
                "tumor_allele2",
                "transcript_source_id",
                "coding_change_notation",
                "protein_change_short_notation",
            ),
        )
    )
    somatic = conform_genomics(add_genomics_provenance(somatic, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- somatic_mutation
if selected("somatic_mutation"):
    write_silver(
        somatic, "somatic_mutation", source=SOURCE, component=COMPONENT, rid=RID
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- somatic_mutation
if selected("somatic_mutation"):
    written = spark.table(genomics_table("somatic_mutation"))
    blocks = inspect_table(
        written,
        "somatic_mutation",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        extra_checks={
            "records_in_bronze": read_genomics_bronze(
                "tcga_mc3_somatic_mutations"
            ).count(),
            "records_without_a_gene_key": written.filter(
                F.col("gene_key").isNull()
            ).count(),
            "variant_links": str(
                {
                    r["variant_link_method"]: r["count"]
                    for r in written.groupBy("variant_link_method").count().collect()
                }
            ),
            "unparsed_protein_changes": written.filter(
                F.col("protein_change_short_notation").isNotNull()
                & F.col("protein_position").isNull()
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

# COMMAND ----------

# DBTITLE 1,Export findings -- somatic_mutation
if selected("somatic_mutation"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__somatic_mutation",
        "somatic_mutation",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- drug ids
if selected("perturbation_sample"):
    xwalk = read_silver("entity_id_crosswalk")
    drug_map = entity_id_map(xwalk, "drug")

# COMMAND ----------

# DBTITLE 1,Transform -- sample record and provenance
if selected("perturbation_sample"):
    DATASET = "tahoe.sample_metadata"

    def text(column):
        return clean_placeholders(column, RULES, f"{DATASET}.{column}")

    def number(column):
        return typed_number(column, "double", RULES, f"{DATASET}.{column}")

    samples = read_genomics_bronze("tahoe_sample_metadata").withColumn(
        "tahoe_key", F.concat(F.lit("tahoe:"), F.lower(F.trim(F.col("drug"))))
    )
    samples = link_entities(
        samples,
        [(F.col("tahoe_key"), "tahoe_drug")],
        drug_map,
        "drug_key",
        "drug_link_method",
    )
    sample = (
        samples.select(
            text("sample").alias("sample_id"),
            text("plate").alias("plate"),
            number("mean_gene_count").alias("mean_gene_count"),
            number("mean_tscp_count").alias("mean_tscp_count"),
            number("mean_mread_count").alias("mean_mread_count"),
            number("mean_pcnt_mito").alias("mean_pcnt_mito"),
            text("drug").alias("drug_name"),
            "drug_key",
            "drug_link_method",
            text("drugname_drugconc").alias("drugname_drugconc"),
        )
        .filter(F.col("sample_id").isNotNull())
        .distinct()
        .withColumn("_srid", sha_key("sample_id", "plate", "drugname_drugconc"))
    )
    sample = conform_genomics(add_genomics_provenance(sample, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write Silver -- perturbation_sample
if selected("perturbation_sample"):
    write_silver(
        sample, "perturbation_sample", source=SOURCE, component=COMPONENT, rid=RID
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- perturbation_sample
if selected("perturbation_sample"):
    written = spark.table(genomics_table("perturbation_sample"))
    blocks = inspect_table(
        written,
        "perturbation_sample",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        extra_checks={
            "samples_without_a_drug_key": written.filter(
                F.col("drug_key").isNull()
            ).count(),
            "plates": written.select("plate").distinct().count(),
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

# COMMAND ----------

# DBTITLE 1,Export findings -- perturbation_sample
if selected("perturbation_sample"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__perturbation_sample",
        "perturbation_sample",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- id maps
if selected("perturbation_expression"):
    xwalk = read_silver("entity_id_crosswalk")
    names = read_silver("entity_name")
    gene_map = term_map(
        names, "gene", ("symbol", "official_symbol"), "gene_symbol"
    ).unionByName(
        term_map(
            names,
            "gene",
            ("synonym", "alternate_symbol", "symbol_synonym"),
            "gene_alias",
        )
    )
    drug_map = entity_id_map(xwalk, "drug")

# COMMAND ----------

# DBTITLE 1,Transform -- typed statistics, gene and drug keys
if selected("perturbation_expression"):
    DATASET = "tahoe.pseudobulk_differential_expression"

    def text(column):
        return clean_placeholders(column, RULES, f"{DATASET}.{column}")

    def number(column, kind="double"):
        return typed_number(column, kind, RULES, f"{DATASET}.{column}")

    expression = read_genomics_bronze(
        "tahoe_pseudobulk_differential_expression"
    ).withColumn("tahoe_key", F.concat(F.lit("tahoe:"), F.lower(F.trim(F.col("drug")))))
    symbol = F.upper(F.trim(F.col("gene_name")))
    expression = link_entities(
        expression,
        [(symbol, "gene_symbol"), (symbol, "gene_alias")],
        gene_map,
        "gene_key",
        "gene_link_method",
    )
    expression = link_entities(
        expression,
        [(F.col("tahoe_key"), "tahoe_drug")],
        drug_map,
        "drug_key",
        "drug_link_method",
    )

# COMMAND ----------

# DBTITLE 1,Transform -- columns and provenance
if selected("perturbation_expression"):
    perturbation = (
        expression.select(
            text("gene_name").alias("gene_symbol"),
            "gene_key",
            "gene_link_method",
            number("baseMean").alias("base_mean"),
            number("log2FoldChange").alias("log2_fold_change"),
            number("lfcSE").alias("log2_fold_change_standard_error"),
            number("stat").alias("statistic"),
            number("pvalue").alias("p_value"),
            number("padj").alias("adjusted_p_value"),
            text("plate").alias("plate"),
            number("n_cells_trt", "int").alias("n_cells_treated"),
            number("n_cells_ctrl", "int").alias("n_cells_control"),
            text("Cell_ID_DepMap").alias("cell_line_key"),
            text("Cell_ID_Cellosaur").alias("cellosaur_id"),
            text("Cell_Name_Vevo").alias("cell_name_vevo"),
            text("drug").alias("drug_name"),
            "drug_key",
            "drug_link_method",
            number("concentration").alias("concentration"),
            text("concentration_unit").alias("concentration_unit"),
        )
        .distinct()
        .withColumn(
            "_srid",
            sha_key(
                "gene_symbol",
                "cell_line_key",
                "cellosaur_id",
                "drug_name",
                "concentration",
                "concentration_unit",
                "plate",
            ),
        )
    )
    perturbation = conform_genomics(
        add_genomics_provenance(perturbation, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write Silver -- perturbation_expression
if selected("perturbation_expression"):
    write_silver(
        perturbation,
        "perturbation_expression",
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- perturbation_expression
if selected("perturbation_expression"):
    written = spark.table(genomics_table("perturbation_expression"))
    blocks = inspect_table(
        written,
        "perturbation_expression",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        extra_checks={
            "rows_without_a_gene_key": written.filter(
                F.col("gene_key").isNull()
            ).count(),
            "rows_without_a_drug_key": written.filter(
                F.col("drug_key").isNull()
            ).count(),
            "rows_without_a_p_value": written.filter(F.col("p_value").isNull()).count(),
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

# COMMAND ----------

# DBTITLE 1,Export findings -- perturbation_expression
if selected("perturbation_expression"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__perturbation_expression",
        "perturbation_expression",
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Transform -- arrays per case version
if selected("faers_case"):
    DEMO = "faers.demo"

    def text(column, dataset=DEMO):
        return clean_placeholders(column, RULES, f"{dataset}.{column}")

    reactions = (
        read_genomics_bronze("faers_reac")
        .groupBy("quarter", "primaryid")
        .agg(
            F.sort_array(
                F.collect_set(
                    F.struct(
                        text("pt", "faers.reac").alias("reaction"),
                        text("drug_rec_act", "faers.reac").alias(
                            "drug_recurrence_action"
                        ),
                    )
                )
            ).alias("reactions")
        )
    )
    outcomes = (
        read_genomics_bronze("faers_outc")
        .groupBy("quarter", "primaryid")
        .agg(
            F.sort_array(F.collect_set(text("outc_cod", "faers.outc"))).alias(
                "outcomes"
            )
        )
    )
    report_sources = (
        read_genomics_bronze("faers_rpsr")
        .groupBy("quarter", "primaryid")
        .agg(
            F.sort_array(F.collect_set(text("rpsr_cod", "faers.rpsr"))).alias(
                "report_sources"
            )
        )
    )

# COMMAND ----------

# DBTITLE 1,Transform -- demographics, dates and provenance
if selected("faers_case"):
    demo = read_genomics_bronze("faers_demo")
    case = (
        demo.join(reactions, ["quarter", "primaryid"], "left")
        .join(outcomes, ["quarter", "primaryid"], "left")
        .join(report_sources, ["quarter", "primaryid"], "left")
        .select(
            "quarter",
            F.col("primaryid").alias("primary_id"),
            text("caseid").alias("case_id"),
            text("caseversion").alias("case_version"),
            text("i_f_code").alias("initial_or_followup"),
            text("event_dt").alias("event_date_raw"),
            text("mfr_dt").alias("manufacturer_date_raw"),
            text("init_fda_dt").alias("initial_fda_date_raw"),
            text("fda_dt").alias("fda_date_raw"),
            text("rept_dt").alias("report_date_raw"),
            faers_date("event_dt").alias("event_date"),
            faers_date("mfr_dt").alias("manufacturer_date"),
            faers_date("init_fda_dt").alias("initial_fda_date"),
            faers_date("fda_dt").alias("fda_date"),
            faers_date("rept_dt").alias("report_date"),
            text("rept_cod").alias("report_code"),
            text("mfr_num").alias("manufacturer_number"),
            text("mfr_sndr").alias("manufacturer_sender"),
            typed_number("age", "double", RULES, f"{DEMO}.age").alias("age"),
            text("age_cod").alias("age_unit"),
            text("age_grp").alias("age_group"),
            text("sex").alias("sex"),
            text("e_sub").alias("electronic_submission"),
            typed_number("wt", "double", RULES, f"{DEMO}.wt").alias("weight"),
            text("wt_cod").alias("weight_unit"),
            text("occp_cod").alias("occupation_code"),
            text("reporter_country").alias("reporter_country"),
            text("occr_country").alias("occurrence_country"),
            "reactions",
            "outcomes",
            "report_sources",
        )
        .distinct()
        .withColumn("_srid", sha_key("quarter", "primary_id"))
    )
    case = conform_genomics(add_genomics_provenance(case, "_srid", RID), "faers_case")

# COMMAND ----------

# DBTITLE 1,Write Silver -- faers_case
if selected("faers_case"):
    write_silver(case, "faers_case", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- faers_case
if selected("faers_case"):
    written = spark.table(genomics_table("faers_case"))
    blocks = inspect_table(
        written,
        "faers_case",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["quarter", "primary_id"],
        extra_checks={
            "quarters": written.select("quarter").distinct().count(),
            "dates_not_parsed": written.filter(
                F.col("event_date_raw").isNotNull() & F.col("event_date").isNull()
            ).count(),
            "case_ids_with_several_versions": written.groupBy("case_id")
            .count()
            .filter(F.col("count") > 1)
            .count(),
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

# COMMAND ----------

# DBTITLE 1,Export findings -- faers_case
if selected("faers_case"):
    write_silver_findings(
        FINDINGS, f"{COMPONENT.split('/')[-1]}__faers_case", "faers_case", blocks
    )

# COMMAND ----------

# DBTITLE 1,Read Silver -- drug ids and names
if selected("faers_case_drug"):
    xwalk = read_silver("entity_id_crosswalk")
    names = read_silver("entity_name")
    drug_map = entity_id_map(xwalk, "drug").unionByName(
        term_map(
            names,
            "drug",
            ("name", "generic_name", "trade_name", "synonym", "brand_mixture"),
            "drug_name",
        )
    )

# COMMAND ----------

# DBTITLE 1,Transform -- indications and therapies per drug sequence
if selected("faers_case_drug"):
    DRUG = "faers.drug"

    def text(column, dataset=DRUG):
        return clean_placeholders(column, RULES, f"{dataset}.{column}")

    indications = (
        read_genomics_bronze("faers_indi")
        .groupBy("quarter", "primaryid", F.col("indi_drug_seq").alias("drug_seq"))
        .agg(
            F.sort_array(F.collect_set(text("indi_pt", "faers.indi"))).alias(
                "indications"
            )
        )
    )
    therapies = (
        read_genomics_bronze("faers_ther")
        .groupBy("quarter", "primaryid", F.col("dsg_drug_seq").alias("drug_seq"))
        .agg(
            F.sort_array(
                F.collect_set(
                    F.struct(
                        text("start_dt", "faers.ther").alias("start_dt"),
                        text("end_dt", "faers.ther").alias("end_dt"),
                        text("dur", "faers.ther").alias("duration"),
                        text("dur_cod", "faers.ther").alias("duration_unit"),
                    )
                )
            ).alias("therapies")
        )
    )

# COMMAND ----------

# DBTITLE 1,Transform -- drug records, keys and provenance
if selected("faers_case_drug"):
    drug = (
        read_genomics_bronze("faers_drug")
        .join(indications, ["quarter", "primaryid", "drug_seq"], "left")
        .join(therapies, ["quarter", "primaryid", "drug_seq"], "left")
    )
    drug = link_entities(
        drug,
        [
            (F.upper(F.trim(text("drugname"))), "drug_name"),
            (F.upper(F.trim(text("prod_ai"))), "drug_name"),
        ],
        drug_map,
        "drug_key",
        "drug_link_method",
    )
    case_drug = (
        drug.select(
            "quarter",
            F.col("primaryid").alias("primary_id"),
            text("caseid").alias("case_id"),
            F.col("drug_seq").alias("drug_sequence"),
            text("role_cod").alias("role_code"),
            text("drugname").alias("drug_name"),
            text("prod_ai").alias("active_ingredient"),
            text("val_vbm").alias("drug_name_validation_code"),
            text("route").alias("route"),
            text("dose_vbm").alias("dose_verbatim_text"),
            typed_number("cum_dose_chr", "double", RULES, f"{DRUG}.cum_dose_chr").alias(
                "cumulative_dose"
            ),
            text("cum_dose_unit").alias("cumulative_dose_unit"),
            text("dechal").alias("dechallenge"),
            text("rechal").alias("rechallenge"),
            text("lot_num").alias("lot_number"),
            text("exp_dt").alias("expiry_date_raw"),
            text("nda_num").alias("new_drug_application_number"),
            typed_number("dose_amt", "double", RULES, f"{DRUG}.dose_amt").alias(
                "dose_amount"
            ),
            text("dose_unit").alias("dose_unit"),
            text("dose_form").alias("dose_form"),
            text("dose_freq").alias("dose_frequency"),
            "drug_key",
            "drug_link_method",
            "indications",
            "therapies",
        )
        .distinct()
        .withColumn(
            "_srid", sha_key("quarter", "primary_id", "drug_sequence", "drug_name")
        )
    )
    case_drug = conform_genomics(
        add_genomics_provenance(case_drug, "_srid", RID), "faers_case_drug"
    )

# COMMAND ----------

# DBTITLE 1,Write Silver -- faers_case_drug
if selected("faers_case_drug"):
    write_silver(
        case_drug, "faers_case_drug", source=SOURCE, component=COMPONENT, rid=RID
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- faers_case_drug
if selected("faers_case_drug"):
    written = spark.table(genomics_table("faers_case_drug"))
    blocks = inspect_table(
        written,
        "faers_case_drug",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["quarter", "primary_id", "drug_sequence"],
        extra_checks={
            "drugs_without_a_drug_key": written.filter(
                F.col("drug_key").isNull()
            ).count(),
            "roles": str(
                {
                    r["role_code"]: r["count"]
                    for r in written.groupBy("role_code").count().collect()
                }
            ),
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

# COMMAND ----------

# DBTITLE 1,Export findings -- faers_case_drug
if selected("faers_case_drug"):
    write_silver_findings(
        FINDINGS,
        f"{COMPONENT.split('/')[-1]}__faers_case_drug",
        "faers_case_drug",
        blocks,
    )
