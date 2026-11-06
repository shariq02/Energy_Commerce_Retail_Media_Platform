# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
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
# MAGIC **Purpose:** tumor samples, somatic mutations, and perturbation samples and
# MAGIC expression.

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
COMPONENT = "silver/genomics/09_facts"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Build -- tumor_sample
# -- Transform -- barcodes and their parts
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
sample = conform_genomics(add_genomics_provenance(sample, "_srid", RID), "tumor_sample")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- tumor_sample
write_silver(sample, "tumor_sample", source=SOURCE, component=COMPONENT, rid=RID)

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

write_silver_findings(
    FINDINGS, f"{COMPONENT.split('/')[-1]}__tumor_sample", "tumor_sample", blocks
)

# COMMAND ----------

# DBTITLE 1,Build -- somatic_mutation
# -- Read Silver -- id maps and protein changes
xwalk = read_silver("entity_id_crosswalk")
names = read_silver("entity_name")
gene_map = entity_id_map(xwalk, "gene").unionByName(
    term_map(names, "gene", ("symbol", "official_symbol"), "gene_symbol")
)
transcript_map = entity_id_map(xwalk, "transcript")
variant_map = entity_id_map(xwalk, "variant")
protein_changes = (
    current_snapshot(read_silver("variant_hgvs"), RULES)
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

# -- Transform -- columns, parsed protein change and keys
DATASET = "tcga.mc3_somatic_mutations"

text = text_for(DATASET)

number = number_for(DATASET)

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

# -- Transform -- record and provenance
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
somatic = conform_genomics(
    add_genomics_provenance(somatic, "_srid", RID), "somatic_mutation"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- somatic_mutation
write_silver(somatic, "somatic_mutation", source=SOURCE, component=COMPONENT, rid=RID)

written = spark.table(genomics_table("somatic_mutation"))
blocks = inspect_table(
    written,
    "somatic_mutation",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "records_in_bronze": read_genomics_bronze("tcga_mc3_somatic_mutations").count(),
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

write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__somatic_mutation",
    "somatic_mutation",
    blocks,
)

# COMMAND ----------

# DBTITLE 1,Build -- perturbation_sample
# -- Read Silver -- drug ids
xwalk = read_silver("entity_id_crosswalk")
drug_map = entity_id_map(xwalk, "drug")

# -- Transform -- sample record and provenance
DATASET = "tahoe.sample_metadata"

text = text_for(DATASET)

number = number_for(DATASET)

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
sample = conform_genomics(
    add_genomics_provenance(sample, "_srid", RID), "perturbation_sample"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- perturbation_sample
write_silver(sample, "perturbation_sample", source=SOURCE, component=COMPONENT, rid=RID)

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

write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__perturbation_sample",
    "perturbation_sample",
    blocks,
)

# COMMAND ----------

# DBTITLE 1,Build -- perturbation_expression
# -- Read Silver -- id maps
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

# -- Transform -- typed statistics, gene and drug keys
DATASET = "tahoe.pseudobulk_differential_expression"

text = text_for(DATASET)

number = number_for(DATASET)

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

# -- Transform -- columns and provenance
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
    add_genomics_provenance(perturbation, "_srid", RID), "perturbation_expression"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- perturbation_expression
write_silver(
    perturbation,
    "perturbation_expression",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

written = spark.table(genomics_table("perturbation_expression"))
blocks = inspect_table(
    written,
    "perturbation_expression",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "rows_without_a_gene_key": written.filter(F.col("gene_key").isNull()).count(),
        "rows_without_a_drug_key": written.filter(F.col("drug_key").isNull()).count(),
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

write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__perturbation_expression",
    "perturbation_expression",
    blocks,
)
