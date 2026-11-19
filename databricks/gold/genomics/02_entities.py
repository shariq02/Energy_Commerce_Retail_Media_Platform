# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GOLD GENOMICS -- ENTITIES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** 18 `genomics_gold` tables: ten entity profiles (the entity rows
# MAGIC with every attribute of their type; somatic mutations are keyed by their Silver
# MAGIC record id), the gene-disease relations, structural
# MAGIC variants (variant rows and variant x gene rows), somatic sites, the ClinVar
# MAGIC class changes in snapshot order and four typed tables that hold the many-side
# MAGIC Silver rows (`record_kind` names the Silver table of a row).

# COMMAND ----------

# DBTITLE 1,Gold library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "genomics"
COMPONENT = "gold/genomics/02_entities"
RID = gold_run_id()
GATE = {"source": SOURCE, "component": COMPONENT, "rid": RID}
COMMON = {**GATE, "schema": GENOMICS_GOLD_SCHEMA}
MASTER_TYPES = ("transcript", "protein", "disease", "drug")
# profile table -> (entity type, key, Silver table or None for entity_master, grain)
PROFILES = {
    **{f"{t}_profile": (t, f"{t}_key", None, [f"{t}_key"]) for t in MASTER_TYPES},
    "cell_line_profile": (
        "cell_line",
        "cell_line_key",
        "cell_line",
        ["cell_line_key", "source_record_id"],
    ),
    "genetic_test_profile": (
        "genetic_test",
        "test_key",
        "genetic_test",
        ["test_key", "source_record_id"],
    ),
    "clinical_trial_profile": (
        "clinical_trial",
        "nct_id",
        "clinical_trial",
        ["nct_id"],
    ),
    "epitope_profile": ("epitope", "epitope_id", "epitope", ["epitope_id"]),
    "tumor_sample_profile": (
        "tumor_sample",
        "sample_barcode",
        "tumor_sample",
        ["sample_barcode"],
    ),
    "somatic_mutation_profile": (
        "somatic_mutation",
        "source_record_id",
        "somatic_mutation",
        ["source_record_id"],
    ),
}
# Gold site column -> Silver `somatic_mutation` column (the parts of the site key)
SITE_PARTS = {
    "assembly": "assembly",
    "contig": "contig",
    "start_position": "start_position",
    "reference_allele": "reference_allele",
    "tumor_allele": "tumor_allele2",
}
CHANGE_COLUMNS = [
    "variant_key",
    "classification_type",
    "term",
    "clinical_significance_simple",
    "first_source_snapshot",
    "last_source_snapshot",
]
SNAPSHOT_DATES = load_yaml("src/schemas/mappings/genomics_rules.yml")[
    "clinvar_snapshot_dates"
]
# typed table -> Silver table -> the columns that make one row of it unique
SEQUENCE = ["quarter", "primary_id", "drug_sequence", "dose_record_number"]
RECORD = ["source_record_id"]
MANY_SIDE = {
    "gene_measurement": {
        "gene_dependency": RECORD,
        "gene_expression_median": ["gene_ensembl_id", "tissue_key"],
        "perturbation_expression": RECORD,
        "perturbation_sample": RECORD,
        "gene_disease_score_year": RECORD,
        "regulatory_element_gene_link": RECORD,
    },
    "protein_variant_record": {
        "protein_residue_score": RECORD,
        "protein_feature": RECORD,
        "protein_gene_link": RECORD,
        "variant_hgvs": RECORD,
        "variant_condition": RECORD,
        "variant_feature_overlap": RECORD,
    },
    "case_assay_record": {
        "faers_case": ["quarter", "primary_id"],
        "faers_case_drug": SEQUENCE,
        "faers_case_item": RECORD,
        "epitope_mhc_assay": ["assay_id"],
    },
    "entity_identity_record": {
        "entity_id_crosswalk": [
            "entity_type",
            "entity_key",
            "id_type",
            "id_value",
            "source_dataset",
            "source_field",
        ],
        "entity_name": RECORD,
        "entity_claim": RECORD,
        "entity_coordinate": RECORD,
        "ontology_relation": RECORD,
    },
}
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Read Silver
attributes = read_silver("entity_derived_attribute")
master = read_silver("entity_master")
relation = read_silver("entity_relation")
classification = read_silver("variant_classification")
variant_labels = read_silver("variant").select(
    "variant_key", "assembly", "contig", "position", "end_position", "variant_type"
)
bases = {
    table: (
        master_rows(master, entity_type, key)
        if silver is None
        else from_silver(read_silver(silver))
    )
    for table, (entity_type, key, silver, _) in PROFILES.items()
}

# COMMAND ----------

# DBTITLE 1,Transform -- the ten entity profiles
for table, (entity_type, key, _, grain) in PROFILES.items():
    TABLES[table] = (
        full_profile(bases[table], attributes, entity_type, key=key, **GATE),
        grain,
    )

# COMMAND ----------

# DBTITLE 1,Transform -- gene_disease_profile (relation rows, gene symbol, disease name)
has_gene = F.col("gene_key").isNotNull() | F.col("source_gene_id").isNotNull()
has_disease = F.col("disease_key").isNotNull() | F.col("source_disease_id").isNotNull()
names = {
    entity_type: master.filter(F.col("entity_type") == entity_type).select(
        F.col("entity_key").alias(f"{entity_type}_key"), F.col(column).alias(alias)
    )
    for entity_type, column, alias in (
        ("gene", "symbol", "gene_symbol"),
        ("disease", "name", "disease_name"),
    )
}
TABLES["gene_disease_profile"] = (
    add_provenance(
        drop_empty_columns(from_silver(relation).filter(has_gene & has_disease))
        .join(names["gene"], "gene_key", "left")
        .join(names["disease"], "disease_key", "left"),
        SOURCE,
        RID,
        ecosystem="genomics",
    ),
    ["source_record_id"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- structural_variant_profile (variant rows, variant x gene rows)
variant_attributes = pivot_attributes(
    attributes,
    "variant",
    attribute_columns(attributes, "variant"),
    key="variant_key",
    rules=STRUCTURAL_RULES,
    **GATE,
)
variant_rows = add_provenance(
    join_profile(variant_labels, variant_attributes, "variant_key", "right"),
    SOURCE,
    RID,
    ecosystem="genomics",
).withColumn("record_kind", F.lit("variant"))
pair_rows = pivot_attributes(
    attributes,
    "structural_variant_gene",
    attribute_columns(attributes, "structural_variant_gene"),
    key="pair_key",
    **GATE,
)
pair_rows = add_provenance(
    pair_rows.withColumn("variant_key", F.substring_index("pair_key", "|", 1))
    .withColumn("overlapping_gene_key", F.substring_index("pair_key", "|", -1))
    .drop("pair_key")
    .join(variant_labels, "variant_key", "left")
    .withColumn("record_kind", F.lit("variant_gene")),
    SOURCE,
    RID,
    ecosystem="genomics",
)
TABLES["structural_variant_profile"] = (
    variant_rows.withColumn(
        "overlapping_gene_key", F.lit(None).cast("string")
    ).unionByName(pair_rows, allowMissingColumns=True),
    ["record_kind", "variant_key", "overlapping_gene_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- somatic_site_profile (site attributes, site key parts)
# The Silver site key skips an empty allele, so its parts cannot be split by
# position; the parts come from `somatic_mutation` (a site key with more than one
# distinct set of parts keeps them empty).
sites = pivot_attributes(
    attributes,
    "somatic_site",
    attribute_columns(attributes, "somatic_site"),
    key="site_key",
    **GATE,
)
site_parts = (
    read_silver("somatic_mutation")
    .groupBy(
        F.concat_ws(
            ":",
            "assembly",
            "contig",
            F.col("start_position").cast("string"),
            "reference_allele",
            "tumor_allele2",
        ).alias("site_key")
    )
    .agg(
        F.countDistinct(F.struct(*SITE_PARTS.values())).alias("_part_sets"),
        *[F.first(source).alias(name) for name, source in SITE_PARTS.items()],
    )
    .select(
        "site_key",
        *[
            F.when(F.col("_part_sets") == 1, F.col(name)).alias(name)
            for name in SITE_PARTS
        ],
    )
)
TABLES["somatic_site_profile"] = (
    add_provenance(
        sites.join(site_parts, "site_key", "left").select(
            "site_key",
            *SITE_PARTS,
            *[c for c in sites.columns if c != "site_key"],
        ),
        SOURCE,
        RID,
        ecosystem="genomics",
    ),
    ["site_key"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- clinvar_classification_change (term and the term before it)
labels = {
    r[0]
    for r in classification.select("first_source_snapshot")
    .union(classification.select("last_source_snapshot"))
    .distinct()
    .collect()
    if r[0]
}
order = sorted(labels, key=lambda label: str(SNAPSHOT_DATES.get(label, label)))
rank = F.create_map(*[F.lit(p) for i, label in enumerate(order) for p in (label, i)])
runs = (
    classification.select(*CHANGE_COLUMNS)
    .distinct()
    .withColumn("_rank", F.try_element_at(rank, F.col("first_source_snapshot")))
)
in_order = Window.partitionBy("variant_key", "classification_type").orderBy(
    "_rank", "term"
)
for column in CHANGE_COLUMNS[2:]:
    runs = runs.withColumn(f"previous_{column}", F.lag(column).over(in_order))
TABLES["clinvar_classification_change"] = (
    add_provenance(
        runs.filter(
            F.col("previous_term").isNotNull()
            & (F.col("previous_term") != F.col("term"))
        ).drop("_rank"),
        "clinvar",
        RID,
        ecosystem="genomics",
    ),
    CHANGE_COLUMNS,
)

# COMMAND ----------

# DBTITLE 1,Transform -- the typed tables of the many-side Silver rows
for table, members in MANY_SIDE.items():
    frame, grain = typed_table(
        {name: (from_silver(read_silver(name)), keys) for name, keys in members.items()}
    )
    TABLES[table] = (add_provenance(frame, SOURCE, RID, ecosystem="genomics"), grain)

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
