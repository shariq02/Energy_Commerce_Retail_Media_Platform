# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- FACTS AND FAERS (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** tumor samples, somatic mutations, perturbation samples and expression,
# MAGIC and the FAERS cases with their drugs and their reaction, outcome, report source,
# MAGIC indication and therapy rows.

# COMMAND ----------

# DBTITLE 1,Genomics library
# MAGIC %run ../_genomics_common

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F

SOURCE = "genomics"
COMPONENT = "silver/genomics/06_facts_and_faers"
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
write_inspect_export(
    sample,
    "tumor_sample",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["sample_barcode"],
    extra_checks={
        "unparsed_barcodes": lambda t: t.filter(
            F.col("parse_status") == "unparsed"
        ).count(),
        "projects": lambda t: t.select("project").distinct().count(),
        "participants": lambda t: t.select("participant_barcode").distinct().count(),
    },
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
            "filter_status",
            "tumor_depth",
            "tumor_ref_count",
            "tumor_alt_count",
        ),
    )
)
somatic = conform_genomics(
    add_genomics_provenance(somatic, "_srid", RID), "somatic_mutation"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- somatic_mutation
write_inspect_export(
    somatic,
    "somatic_mutation",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "records_in_bronze": read_genomics_bronze("tcga_mc3_somatic_mutations").count(),
        "records_without_a_gene_key": lambda t: t.filter(
            F.col("gene_key").isNull()
        ).count(),
        "variant_links": lambda t: str(
            {
                r["variant_link_method"]: r["count"]
                for r in t.groupBy("variant_link_method").count().collect()
            }
        ),
        "unparsed_protein_changes": lambda t: t.filter(
            F.col("protein_change_short_notation").isNotNull()
            & F.col("protein_position").isNull()
        ).count(),
    },
    link_method_counts=True,
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
write_inspect_export(
    sample,
    "perturbation_sample",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "samples_without_a_drug_key": lambda t: t.filter(
            F.col("drug_key").isNull()
        ).count(),
        "plates": lambda t: t.select("plate").distinct().count(),
    },
    link_method_counts=True,
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
write_inspect_export(
    perturbation,
    "perturbation_expression",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "rows_without_a_gene_key": lambda t: t.filter(
            F.col("gene_key").isNull()
        ).count(),
        "rows_without_a_drug_key": lambda t: t.filter(
            F.col("drug_key").isNull()
        ).count(),
        "rows_without_a_p_value": lambda t: t.filter(F.col("p_value").isNull()).count(),
    },
    link_method_counts=True,
)

# COMMAND ----------

# DBTITLE 1,Build -- faers_case
# -- Transform -- demographics, dates and provenance (one row per case version)
DEMO = "faers.demo"
text = text_for(DEMO)
case = (
    read_genomics_bronze("faers_demo")
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
        text("auth_num").alias("authority_number"),
        text("lit_ref").alias("literature_reference"),
        text("to_mfr").alias("sent_to_manufacturer"),
    )
    .distinct()
    .withColumn("_srid", sha_key("quarter", "primary_id"))
)
case_ids = case.select("quarter", "primary_id", "case_id").distinct()
case = conform_genomics(add_genomics_provenance(case, "_srid", RID), "faers_case")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- faers_case
write_inspect_export(
    case,
    "faers_case",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["quarter", "primary_id"],
    extra_checks={
        "quarters": lambda t: t.select("quarter").distinct().count(),
        "dates_not_parsed": lambda t: t.filter(
            F.col("event_date_raw").isNotNull() & F.col("event_date").isNull()
        ).count(),
        "case_ids_with_several_versions": lambda t: (
            t.groupBy("case_id").count().filter(F.col("count") > 1).count()
        ),
    },
)

# COMMAND ----------

# DBTITLE 1,Build -- faers_case_drug
# -- Read Silver -- drug ids and names
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

# -- Transform -- drug records, keys and provenance
DRUG = "faers.drug"
text = text_for(DRUG)
drug = link_entities(
    read_genomics_bronze("faers_drug"),
    [
        (F.upper(F.trim(text("drugname"))), "drug_name"),
        (F.upper(F.trim(text("prod_ai"))), "drug_name"),
    ],
    drug_map,
    "drug_key",
    "drug_link_method",
)
case_drug = drug.select(
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
    typed_number("dose_amt", "double", RULES, f"{DRUG}.dose_amt").alias("dose_amount"),
    text("dose_unit").alias("dose_unit"),
    text("dose_form").alias("dose_form"),
    text("dose_freq").alias("dose_frequency"),
    "drug_key",
    "drug_link_method",
).distinct()
# A drug sequence can hold several dose records; number them inside the sequence.
SEQUENCE = ["quarter", "primary_id", "drug_sequence"]
case_drug = (
    within_group_ordinal(
        case_drug, SEQUENCE, [c for c in case_drug.columns if c not in SEQUENCE]
    )
    .withColumnRenamed("_source_id_ordinal", "dose_record_number")
    .drop("_source_id_disambiguated")
    .withColumn("_srid", sha_key(*SEQUENCE, "dose_record_number"))
)
case_drug = conform_genomics(
    add_genomics_provenance(case_drug, "_srid", RID), "faers_case_drug"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- faers_case_drug
write_inspect_export(
    case_drug,
    "faers_case_drug",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=[*SEQUENCE, "dose_record_number"],
    extra_checks={
        "drugs_without_a_drug_key": lambda t: t.filter(
            F.col("drug_key").isNull()
        ).count(),
        "roles": lambda t: str(
            {
                r["role_code"]: r["count"]
                for r in t.groupBy("role_code").count().collect()
            }
        ),
    },
    link_method_counts=True,
)

# COMMAND ----------

# DBTITLE 1,Build -- faers_case_item
# -- Helpers -- one row per reaction, outcome, report source, indication or therapy
none = F.lit(None).cast("string")


def item(df, kind, dataset, value, drug_sequence=None, **extra):
    """Rows in the columns of `faers_case_item`; a case-level item has no drug sequence."""
    text_of = text_for(dataset)
    return df.select(
        "quarter",
        F.col("primaryid").alias("primary_id"),
        (drug_sequence if drug_sequence is not None else none).alias("drug_sequence"),
        F.lit(kind).alias("item_kind"),
        (text_of(value) if value else none).alias("item_value"),
        extra.get("drug_recurrence_action", none).alias("drug_recurrence_action"),
        extra.get("therapy_start_date_raw", none).alias("therapy_start_date_raw"),
        extra.get("therapy_end_date_raw", none).alias("therapy_end_date_raw"),
        extra.get("therapy_duration", none).alias("therapy_duration"),
        extra.get("therapy_duration_unit", none).alias("therapy_duration_unit"),
    )


# -- Transform -- case-level items (no drug sequence)
reac = read_genomics_bronze("faers_reac")
reactions = item(
    reac,
    "reaction",
    "faers.reac",
    "pt",
    drug_recurrence_action=source_text("drug_rec_act", "faers.reac"),
)
outcomes = item(read_genomics_bronze("faers_outc"), "outcome", "faers.outc", "outc_cod")
report_sources = item(
    read_genomics_bronze("faers_rpsr"), "report_source", "faers.rpsr", "rpsr_cod"
)

# -- Transform -- drug-sequence items
indications = item(
    read_genomics_bronze("faers_indi"),
    "indication",
    "faers.indi",
    "indi_pt",
    drug_sequence=F.col("indi_drug_seq"),
)
therapies = item(
    read_genomics_bronze("faers_ther"),
    "therapy",
    "faers.ther",
    None,
    drug_sequence=F.col("dsg_drug_seq"),
    therapy_start_date_raw=source_text("start_dt", "faers.ther"),
    therapy_end_date_raw=source_text("end_dt", "faers.ther"),
    therapy_duration=source_text("dur", "faers.ther"),
    therapy_duration_unit=source_text("dur_cod", "faers.ther"),
)

# -- Transform -- union, case id and provenance
case_items = (
    _union([reactions, outcomes, report_sources, indications, therapies])
    .distinct()
    .join(case_ids, ["quarter", "primary_id"], "left")
    .withColumn(
        "_srid",
        sha_key(
            "quarter",
            "primary_id",
            "drug_sequence",
            "item_kind",
            "item_value",
            "drug_recurrence_action",
            "therapy_start_date_raw",
            "therapy_end_date_raw",
            "therapy_duration",
            "therapy_duration_unit",
        ),
    )
)
case_items = conform_genomics(
    add_genomics_provenance(case_items, "_srid", RID), "faers_case_item"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- faers_case_item
write_inspect_export(
    case_items,
    "faers_case_item",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "items_by_kind": lambda t: str(
            {
                r["item_kind"]: r["count"]
                for r in t.groupBy("item_kind").count().collect()
            }
        ),
        "items_without_a_case_id": lambda t: t.filter(
            F.col("case_id").isNull()
        ).count(),
        "items_without_a_value": lambda t: t.filter(
            (F.col("item_kind") != "therapy") & F.col("item_value").isNull()
        ).count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Check and export -- rejected values of the lenient number reads
export_rejected_values(FINDINGS, COMPONENT, read_genomics_bronze)
