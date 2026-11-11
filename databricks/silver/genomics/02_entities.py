# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- ENTITIES (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the entity master (gene, transcript, protein, disease and drug in one
# MAGIC table), the protein-gene links, the protein features, the cell lines, the genetic
# MAGIC tests and the ontology relations.

# COMMAND ----------

# DBTITLE 1,Genomics library
# MAGIC %run ../_genomics_common

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F

SOURCE = "genomics"
COMPONENT = "silver/genomics/02_entities"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Build -- entity_master
# -- Read Silver -- claims, crosswalk and the primary GRCh38 intervals
claims = read_silver("entity_claim")
xwalk = read_silver("entity_id_crosswalk")
coordinates = read_silver("entity_coordinate").filter(
    (F.col("assembly") == "GRCh38") & F.col("is_primary")
)

# -- Transform -- one frame per entity type, then one union
frames = []
conflict_frames = []
for entity_type in MASTER_FIELDS:
    rows, conflicts = build_master(
        entity_type,
        claims,
        xwalk,
        coordinates if entity_type in ("gene", "transcript") else None,
    )
    frames.append(
        conform_union(add_genomics_provenance(rows, "_srid", RID), "entity_master")
    )
    conflict_frames.append(conflicts.withColumn("entity_type", F.lit(entity_type)))
master = _union(frames)
master_conflicts = _union(conflict_frames)
master_keys = (
    xwalk.filter(F.col("entity_type").isin(*MASTER_FIELDS))
    .select("entity_type", "entity_key")
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- entity_master
conflict_counts = {
    f"{row['entity_type']}_{row['field']}_conflicts": row["count"]
    for row in master_conflicts.groupBy("entity_type", "field").count().collect()
}
write_inspect_export(
    master,
    "entity_master",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["entity_type", "entity_key"],
    df_before=master_keys,
    extra_checks={
        "entities_with_an_own_key": lambda t: t.filter(
            F.col("key_source").startswith("own_")
        ).count(),
        "genes_without_a_symbol": lambda t: t.filter(
            (F.col("entity_type") == "gene") & F.col("symbol").isNull()
        ).count(),
        "genes_without_a_grch38_interval": lambda t: t.filter(
            (F.col("entity_type") == "gene") & F.col("grch38_contig").isNull()
        ).count(),
        "transcripts_without_a_gene": lambda t: t.filter(
            (F.col("entity_type") == "transcript") & F.col("gene_key").isNull()
        ).count(),
        "diseases_and_drugs_without_a_name": lambda t: t.filter(
            F.col("entity_type").isin("disease", "drug") & F.col("name").isNull()
        ).count(),
        "rows_per_entity_type": lambda t: str(
            {
                r["entity_type"]: r["count"]
                for r in t.groupBy("entity_type").count().collect()
            }
        ),
        **conflict_counts,
    },
)

# COMMAND ----------

# DBTITLE 1,Build -- protein_gene_link
# -- Read Silver -- claims and names
claims = read_silver("entity_claim")
names = read_silver("entity_name")

# -- Transform -- exact links through transcripts
tx_claims = claims.filter(F.col("entity_type") == "transcript")
genes = tx_claims.filter(F.col("field") == "gene_key").select(
    "entity_key",
    "source_dataset",
    F.col("value_text").alias("gene_key"),
    "source_field",
)
proteins = tx_claims.filter(F.col("field") == "protein_key").select(
    "entity_key", "source_dataset", F.col("value_text").alias("protein_key")
)
exact = genes.join(proteins, ["entity_key", "source_dataset"]).select(
    "protein_key",
    "gene_key",
    F.lit("transcript_exact").alias("link_method"),
    F.lit(None).cast("int").alias("genes_sharing_symbol"),
    "source_dataset",
    "source_field",
)

# -- Transform -- links by gene symbol
symbols = (
    names.filter(
        (F.col("entity_type") == "gene")
        & F.col("term_type").isin("symbol", "official_symbol")
    )
    .select(
        F.upper(F.col("term")).alias("symbol_key"),
        F.col("entity_key").alias("gene_key"),
    )
    .distinct()
)
sharing = symbols.groupBy("symbol_key").agg(
    F.countDistinct("gene_key").alias("genes_sharing_symbol")
)
by_symbol = (
    claims.filter(
        (F.col("entity_type") == "protein") & (F.col("field") == "gene_symbol")
    )
    .select(
        F.col("entity_key").alias("protein_key"),
        F.upper(F.trim(F.col("value_text"))).alias("symbol_key"),
        "source_dataset",
        "source_field",
    )
    .join(symbols, "symbol_key")
    .join(sharing, "symbol_key")
    .select(
        "protein_key",
        "gene_key",
        F.lit("symbol").alias("link_method"),
        "genes_sharing_symbol",
        "source_dataset",
        "source_field",
    )
)

# -- Transform -- union and provenance
links = (
    exact.unionByName(by_symbol)
    .distinct()
    .withColumn(
        "_srid",
        sha_key(
            "protein_key",
            "gene_key",
            "link_method",
            "source_dataset",
            "source_field",
        ),
    )
)
links = conform_genomics(
    add_genomics_provenance(links, "_srid", RID), "protein_gene_link"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- protein_gene_link
write_inspect_export(
    links,
    "protein_gene_link",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "exact_links": lambda t: t.filter(
            F.col("link_method") == "transcript_exact"
        ).count(),
        "symbol_links": lambda t: t.filter(F.col("link_method") == "symbol").count(),
        "symbol_links_with_several_genes": lambda t: t.filter(
            F.col("genes_sharing_symbol") > 1
        ).count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Build -- protein_feature
# -- Read Silver -- id crosswalk
xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "protein")

# -- Transform -- parse the feature records
features = with_entity_key(
    read_genomics_bronze("uniprot_features"),
    "accession",
    xwalk,
    "uniprot.entries",
    "uniprot",
)
location = F.trim(F.col("location"))
LOCATION = r"^(\d+)(?:\.\.(\d+))?$"
start = F.nullif(F.regexp_extract(location, LOCATION, 1), F.lit(""))
end = F.nullif(F.regexp_extract(location, LOCATION, 2), F.lit(""))
qualifiers = F.col("qualifiers")
parsed = (
    features.select(
        F.col("entity_key").alias("protein_key"),
        F.col("feature_type"),
        location.alias("location_raw"),
        start.cast("int").alias("start_position"),
        F.coalesce(end, start).cast("int").alias("end_position"),
        F.nullif(F.regexp_extract(qualifiers, r'/note="([^"]*)"', 1), F.lit("")).alias(
            "note"
        ),
        qualifiers.alias("qualifiers"),
    )
    .distinct()
    .withColumn(
        "feature_id",
        sha_key("protein_key", "feature_type", "location_raw", "qualifiers"),
    )
)

# -- Transform -- one row per feature and reference (evidence code, PubMed id, dbSNP id)
REFERENCE_PATTERNS = {
    "evidence_code": "(ECO:[0-9]+)",
    "pubmed_id": "PubMed:([0-9]+)",
    "dbsnp_id": "dbSNP:(rs[0-9]+)",
}
references = _union(
    [
        parsed.select(
            "feature_id",
            F.lit(kind).alias("reference_kind"),
            F.explode(
                F.array_distinct(
                    F.expr(f"regexp_extract_all(qualifiers, '{pattern}', 1)")
                )
            ).alias("reference_value"),
        )
        for kind, pattern in REFERENCE_PATTERNS.items()
    ]
)
feature = parsed.join(references, "feature_id", "left").withColumn(
    "_srid", sha_key("feature_id", "reference_kind", "reference_value")
)
feature = conform_genomics(
    add_genomics_provenance(feature, "_srid", RID), "protein_feature"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- protein_feature
write_inspect_export(
    feature,
    "protein_feature",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "feature_records_in_bronze": read_genomics_bronze("uniprot_features").count(),
        "features": lambda t: t.select("feature_id").distinct().count(),
        "features_without_positions": lambda t: (
            t.filter(F.col("start_position").isNull())
            .select("feature_id")
            .distinct()
            .count()
        ),
        "feature_types": lambda t: t.select("feature_type").distinct().count(),
        "rows_per_reference_kind": lambda t: str(
            {
                r["reference_kind"]: r["count"]
                for r in t.groupBy("reference_kind").count().collect()
            }
        ),
    },
    link_method_counts=True,
)

# COMMAND ----------

# DBTITLE 1,Build -- cell_line
# -- Read Bronze -- Tahoe cell lines
DATASET = "tahoe.cell_line_metadata"
cells = read_genomics_bronze("tahoe_cell_line_metadata")
clean = text_for(DATASET)
keyed = cells.filter(clean("Cell_ID_DepMap").isNotNull())

# -- Transform -- one row per cell line and driver
cell_line = (
    keyed.groupBy(
        clean("Cell_ID_DepMap").alias("cell_line_key"),
        clean("Driver_Gene_Symbol").alias("driver_gene_symbol"),
        clean("Driver_VarZyg").alias("driver_variant_zygosity"),
        clean("Driver_VarType").alias("driver_variant_type"),
        clean("Driver_ProtEffect_or_CdnaEffect").alias("driver_effect"),
        clean("Driver_Mech_InferDM").alias("driver_mechanism"),
        clean("Driver_GeneType_DM").alias("driver_gene_type"),
    )
    .agg(
        F.min(clean("cell_name")).alias("cell_name"),
        F.min(clean("Cell_ID_Cellosaur")).alias("cellosaur_id"),
        F.min(clean("Organ")).alias("organ"),
    )
    .withColumn(
        "_srid",
        sha_key(
            "cell_line_key",
            "driver_gene_symbol",
            "driver_variant_zygosity",
            "driver_variant_type",
            "driver_effect",
            "driver_mechanism",
            "driver_gene_type",
        ),
    )
)
cell_line = conform_genomics(
    add_genomics_provenance(cell_line, "_srid", RID), "cell_line"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- cell_line
write_inspect_export(
    cell_line,
    "cell_line",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "rows_without_a_depmap_id": cells.count() - keyed.count(),
        "cell_lines": lambda t: t.select("cell_line_key").distinct().count(),
        "cell_lines_without_drivers": lambda t: t.filter(
            F.col("driver_gene_symbol").isNull()
        ).count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Build -- genetic_test
# -- Read Silver -- gene and disease ids
xwalk = read_silver("entity_id_crosswalk")
gene_ids = (
    xwalk.filter((F.col("entity_type") == "gene") & (F.col("id_type") == "ncbi_gene"))
    .select(F.col("id_value").alias("_gene_id"), F.col("entity_key").alias("_gene_key"))
    .distinct()
)
disease_ids = (
    xwalk.filter((F.col("entity_type") == "disease") & (F.col("id_type") == "medgen"))
    .select(F.col("id_value").alias("_cui"), F.col("entity_key").alias("_disease_key"))
    .distinct()
)

# -- Transform -- latest version of each test
DATASET = "gtr.test_condition_gene"
clean = text_for(DATASET)
rows = (
    genetic_test()
    .withColumn(
        "version",
        F.nullif(F.regexp_extract("accession_version", r"\.(\d+)$", 1), F.lit("")).cast(
            "int"
        ),
    )
    .withColumn("kind", F.lower(F.trim(F.col("object"))))
)
latest = rows.groupBy("test_key").agg(
    F.max("version").alias("latest_number"),
    F.countDistinct("version").alias("versions"),
)
current = rows.join(latest, "test_key").filter(
    F.col("version").eqNullSafe(F.col("latest_number"))
)
heads = current.groupBy("test_key").agg(
    F.max("latest_number").cast("string").alias("latest_version"),
    F.max("versions").alias("versions"),
    F.min(clean("test_type")).alias("test_type"),
)

# -- Transform -- one row per test and gene, one per test and condition
gene_rows = (
    current.filter(F.col("kind") == "gene")
    .join(gene_ids, clean("gene_or_SNOMED_CT_ID") == F.col("_gene_id"), "left")
    .select(
        "test_key",
        F.lit("gene").alias("link_kind"),
        F.col("_gene_key").alias("gene_key"),
        clean("gene_symbol").alias("gene_symbol"),
    )
    .distinct()
)
condition_rows = (
    current.filter(F.col("kind") == "condition")
    .join(disease_ids, clean("GTR_identifier") == F.col("_cui"), "left")
    .select(
        "test_key",
        F.lit("condition").alias("link_kind"),
        F.col("_disease_key").alias("disease_key"),
        clean("object_name").alias("condition_name"),
        clean("MIM_number").alias("mim_number"),
        clean("gene_or_SNOMED_CT_ID").alias("snomed_ct_id"),
    )
    .distinct()
)
linked = gene_rows.unionByName(condition_rows, allowMissingColumns=True)

# -- Transform -- a test without any link keeps one row
tests = heads.join(linked, "test_key", "left").withColumn(
    "_srid",
    sha_key(
        "test_key",
        "link_kind",
        "gene_key",
        "gene_symbol",
        "disease_key",
        "condition_name",
        "mim_number",
        "snomed_ct_id",
    ),
)
tests = conform_genomics(add_genomics_provenance(tests, "_srid", RID), "genetic_test")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- genetic_test
write_inspect_export(
    tests,
    "genetic_test",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "tests_in_bronze": rows.select("test_key").distinct().count(),
        "tests": lambda t: t.select("test_key").distinct().count(),
        "tests_without_genes": lambda t: (
            t.select("test_key").distinct().count()
            - t.filter(F.col("link_kind") == "gene")
            .select("test_key")
            .distinct()
            .count()
        ),
        "tests_without_conditions": lambda t: (
            t.select("test_key").distinct().count()
            - t.filter(F.col("link_kind") == "condition")
            .select("test_key")
            .distinct()
            .count()
        ),
        "rows_with_an_unknown_object": rows.filter(
            ~F.col("kind").isin("gene", "condition")
        ).count(),
    },
)

# COMMAND ----------

# DBTITLE 1,Build -- ontology_relation
# -- Read Silver -- id crosswalk
xwalk = read_silver("entity_id_crosswalk")
disease_keys = (
    xwalk.filter(
        (F.col("entity_type") == "disease")
        & (F.col("id_type") == "opentargets_disease")
    )
    .select(F.col("id_value").alias("_ot_id"), F.col("entity_key").alias("_ot_key"))
    .distinct()
)

# -- Transform -- Open Targets disease parents (one row per parent)
terms = (
    read_genomics_bronze("opentargets_disease")
    .withColumn("curie", curie_expr("id"))
    .withColumn("ontology", F.regexp_extract("curie", r"^([A-Za-z]+):", 1))
)
parents = terms.select(
    "id",
    "ontology",
    F.explode(F.from_json("parents", "array<string>")).alias("parent_id"),
)
child_map = disease_keys.select(
    F.col("_ot_id").alias("id"), F.col("_ot_key").alias("child_mapped")
)
parent_map = disease_keys.select(
    F.col("_ot_id").alias("parent_id"), F.col("_ot_key").alias("parent_mapped")
)
opentargets = (
    parents.join(child_map, "id", "left")
    .join(parent_map, "parent_id", "left")
    .select(
        F.nullif("ontology", F.lit("")).alias("ontology"),
        F.coalesce(F.col("parent_mapped"), F.col("parent_id")).alias("parent_key"),
        F.coalesce(F.col("child_mapped"), F.col("id")).alias("child_key"),
        F.lit("parent").alias("relation"),
        F.lit("parent_child").alias("direction"),
        F.lit("opentargets.disease").alias("source_dataset"),
        F.lit("parents").alias("source_field"),
    )
)

# -- Transform -- HPO and Reactome
hpo = read_genomics_bronze("hpo_term_relations").select(
    F.lit("HP").alias("ontology"),
    F.when(F.col("pred") == "is_a", curie_expr("obj"))
    .otherwise(curie_expr("sub"))
    .alias("parent_key"),
    F.when(F.col("pred") == "is_a", curie_expr("sub"))
    .otherwise(curie_expr("obj"))
    .alias("child_key"),
    F.col("pred").alias("relation"),
    F.when(F.col("pred") == "is_a", "parent_child")
    .otherwise("source_order")
    .alias("direction"),
    F.lit("hpo.term_relations").alias("source_dataset"),
    F.lit("sub,pred,obj").alias("source_field"),
)
reactome = read_genomics_bronze("reactome_pathway_relations").select(
    F.lit("reactome").alias("ontology"),
    F.col("parent_pathway_id").alias("parent_key"),
    F.col("child_pathway_id").alias("child_key"),
    F.lit("parent").alias("relation"),
    F.lit("parent_child").alias("direction"),
    F.lit("reactome.pathway_relations").alias("source_dataset"),
    F.lit("parent_pathway_id,child_pathway_id").alias("source_field"),
)

# -- Transform -- MedGen relations (direction not stated)
medgen = (
    read_genomics_bronze("medgen_relations")
    .filter(F.col("SUPPRESS") == "N")
    .select(
        F.col("SAB").alias("ontology"),
        F.col("CUI1").alias("parent_key"),
        F.col("CUI2").alias("child_key"),
        F.coalesce(F.nullif(F.col("RELA"), F.lit("")), F.col("REL")).alias("relation"),
        F.lit("source_order").alias("direction"),
        F.lit("medgen.relations").alias("source_dataset"),
        F.lit("CUI1,CUI2,REL,RELA").alias("source_field"),
    )
)

# -- Transform -- union and provenance
relations = (
    opentargets.unionByName(hpo)
    .unionByName(reactome)
    .unionByName(medgen)
    .filter(F.col("parent_key").isNotNull() & F.col("child_key").isNotNull())
    .distinct()
    .withColumn(
        "_srid",
        sha_key(
            "ontology",
            "parent_key",
            "child_key",
            "relation",
            "direction",
            "source_dataset",
            "source_field",
        ),
    )
)
relations = conform_genomics(
    add_genomics_provenance(relations, "_srid", RID), "ontology_relation"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- ontology_relation
write_inspect_export(
    relations,
    "ontology_relation",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "source_order_relations": lambda t: t.filter(
            F.col("direction") == "source_order"
        ).count(),
        "parent_child_relations": lambda t: t.filter(
            F.col("direction") == "parent_child"
        ).count(),
        "self_relations": lambda t: t.filter(
            F.col("parent_key") == F.col("child_key")
        ).count(),
        "ontologies": lambda t: t.select("ontology").distinct().count(),
    },
)
