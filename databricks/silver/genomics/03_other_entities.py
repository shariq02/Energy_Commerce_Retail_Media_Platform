# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- OTHER ENTITIES (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the disease, drug, cell line and genetic test masters and the ontology
# MAGIC relations.
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
COMPONENT = "silver/genomics/03_other_entities"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()
AVAILABLE = ["disease", "drug", "cell_line", "genetic_test", "ontology_relation"]
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

# DBTITLE 1,Build -- disease
if selected("disease"):
    # -- Read Silver -- claims and crosswalk
    claims = read_silver("entity_claim")
    xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "disease")

    # -- Transform -- resolve the fields
    masters, conflicts = resolve_master(
        claims, entity_type="disease", fields=DISEASE_FIELDS
    )
    keys = xwalk.groupBy("entity_key").agg(
        F.max(F.when(F.col("id_type") == "medgen", F.lit(True))).alias("has_medgen")
    )

    # -- Transform -- master and provenance
    disease = (
        keys.join(masters, "entity_key", "left")
        .withColumn("disease_key", F.col("entity_key"))
        .withColumn(
            "key_source",
            F.when(F.col("has_medgen"), "medgen_cui").otherwise("own_opentargets_id"),
        )
        .withColumn("_srid", F.col("disease_key"))
    )
    disease = conform_genomics(add_genomics_provenance(disease, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- disease
if selected("disease"):
    write_silver(disease, "disease", source=SOURCE, component=COMPONENT, rid=RID)

    written = spark.table(genomics_table("disease"))
    conflict_counts = {
        f"{row['field']}_conflicts": row["count"]
        for row in conflicts.groupBy("field").count().collect()
    }
    blocks = inspect_table(
        written,
        "disease",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["disease_key"],
        df_before=keys,
        extra_checks={
            "diseases_with_an_own_key": written.filter(
                F.col("key_source") == "own_opentargets_id"
            ).count(),
            "diseases_without_a_name": written.filter(F.col("name").isNull()).count(),
            **conflict_counts,
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
        FINDINGS, f"{COMPONENT.split('/')[-1]}__disease", "disease", blocks
    )

# COMMAND ----------

# DBTITLE 1,Build -- drug
if selected("drug"):
    # -- Read Silver -- claims and crosswalk
    claims = read_silver("entity_claim")
    xwalk = read_silver("entity_id_crosswalk").filter(F.col("entity_type") == "drug")

    # -- Transform -- resolve the fields
    masters, conflicts = resolve_master(claims, entity_type="drug", fields=DRUG_FIELDS)
    keys = xwalk.groupBy("entity_key").agg(
        F.max(F.when(F.col("id_type") == "chembl", F.lit(True))).alias("has_chembl"),
        F.max(F.when(F.col("id_type") == "pharmgkb", F.lit(True))).alias(
            "has_pharmgkb"
        ),
    )

    # -- Transform -- master and provenance
    drug = (
        keys.join(masters, "entity_key", "left")
        .withColumn("drug_key", F.col("entity_key"))
        .withColumn(
            "key_source",
            F.when(F.col("has_chembl"), "chembl_id")
            .when(F.col("has_pharmgkb"), "pharmgkb_accession")
            .otherwise("own_tahoe_name"),
        )
        .withColumn("_srid", F.col("drug_key"))
    )
    drug = conform_genomics(add_genomics_provenance(drug, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- drug
if selected("drug"):
    write_silver(drug, "drug", source=SOURCE, component=COMPONENT, rid=RID)

    written = spark.table(genomics_table("drug"))
    conflict_counts = {
        f"{row['field']}_conflicts": row["count"]
        for row in conflicts.groupBy("field").count().collect()
    }
    blocks = inspect_table(
        written,
        "drug",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["drug_key"],
        df_before=keys,
        extra_checks={
            "drugs_with_an_own_key": written.filter(
                F.col("key_source") != "chembl_id"
            ).count(),
            "drugs_without_a_name": written.filter(F.col("name").isNull()).count(),
            **conflict_counts,
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

    write_silver_findings(FINDINGS, f"{COMPONENT.split('/')[-1]}__drug", "drug", blocks)

# COMMAND ----------

# DBTITLE 1,Build -- cell_line
if selected("cell_line"):
    # -- Read Bronze -- Tahoe cell lines
    DATASET = "tahoe.cell_line_metadata"
    cells = read_genomics_bronze("tahoe_cell_line_metadata")

    clean = text_for(DATASET)

    keyed = cells.filter(clean("Cell_ID_DepMap").isNotNull())

    # -- Transform -- group the drivers per cell line
    driver = F.struct(
        clean("Driver_Gene_Symbol").alias("gene_symbol"),
        clean("Driver_VarZyg").alias("variant_zygosity"),
        clean("Driver_VarType").alias("variant_type"),
        clean("Driver_ProtEffect_or_CdnaEffect").alias("effect"),
        clean("Driver_Mech_InferDM").alias("mechanism"),
        clean("Driver_GeneType_DM").alias("gene_type"),
    )
    cell_line = (
        keyed.withColumn(
            "driver", F.when(clean("Driver_Gene_Symbol").isNotNull(), driver)
        )
        .groupBy(clean("Cell_ID_DepMap").alias("cell_line_key"))
        .agg(
            F.min(clean("cell_name")).alias("cell_name"),
            F.min(clean("Cell_ID_Cellosaur")).alias("cellosaur_id"),
            F.min(clean("Organ")).alias("organ"),
            F.sort_array(F.collect_set("driver")).alias("drivers"),
        )
        .withColumn("_srid", F.col("cell_line_key"))
    )
    cell_line = conform_genomics(
        add_genomics_provenance(cell_line, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- cell_line
if selected("cell_line"):
    write_silver(cell_line, "cell_line", source=SOURCE, component=COMPONENT, rid=RID)

    written = spark.table(genomics_table("cell_line"))
    blocks = inspect_table(
        written,
        "cell_line",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["cell_line_key"],
        extra_checks={
            "rows_without_a_depmap_id": cells.count() - keyed.count(),
            "cell_lines_without_drivers": written.filter(
                F.size("drivers") == 0
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
        FINDINGS, f"{COMPONENT.split('/')[-1]}__cell_line", "cell_line", blocks
    )

# COMMAND ----------

# DBTITLE 1,Build -- genetic_test
if selected("genetic_test"):
    # -- Read Silver -- gene and disease ids
    xwalk = read_silver("entity_id_crosswalk")
    gene_ids = (
        xwalk.filter(
            (F.col("entity_type") == "gene") & (F.col("id_type") == "ncbi_gene")
        )
        .select(
            F.col("id_value").alias("_gene_id"), F.col("entity_key").alias("_gene_key")
        )
        .distinct()
    )
    disease_ids = (
        xwalk.filter(
            (F.col("entity_type") == "disease") & (F.col("id_type") == "medgen")
        )
        .select(
            F.col("id_value").alias("_cui"), F.col("entity_key").alias("_disease_key")
        )
        .distinct()
    )

    # -- Transform -- latest version of each test
    DATASET = "gtr.test_condition_gene"

    clean = text_for(DATASET)

    rows = (
        genetic_test()
        .withColumn(
            "version",
            F.nullif(
                F.regexp_extract("accession_version", r"\.(\d+)$", 1), F.lit("")
            ).cast("int"),
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

    # -- Transform -- genes and conditions as arrays
    genes = (
        current.filter(F.col("kind") == "gene")
        .join(gene_ids, clean("gene_or_SNOMED_CT_ID") == F.col("_gene_id"), "left")
        .groupBy("test_key")
        .agg(
            F.sort_array(
                F.collect_set(
                    F.struct(
                        F.col("_gene_key").alias("gene_key"),
                        clean("gene_symbol").alias("gene_symbol"),
                    )
                )
            ).alias("genes")
        )
    )
    conditions = (
        current.filter(F.col("kind") == "condition")
        .join(disease_ids, clean("GTR_identifier") == F.col("_cui"), "left")
        .groupBy("test_key")
        .agg(
            F.sort_array(
                F.collect_set(
                    F.struct(
                        F.col("_disease_key").alias("disease_key"),
                        clean("object_name").alias("condition_name"),
                        clean("MIM_number").alias("mim_number"),
                        clean("gene_or_SNOMED_CT_ID").alias("snomed_ct_id"),
                    )
                )
            ).alias("conditions")
        )
    )

    # -- Transform -- test record and provenance
    GENE_ARRAY = "array<struct<gene_key:string,gene_symbol:string>>"
    CONDITION_ARRAY = (
        "array<struct<disease_key:string,condition_name:string,"
        "mim_number:string,snomed_ct_id:string>>"
    )
    tests = (
        current.groupBy("test_key")
        .agg(
            F.max("latest_number").cast("string").alias("latest_version"),
            F.max("versions").alias("versions"),
            F.min(clean("test_type")).alias("test_type"),
        )
        .join(genes, "test_key", "left")
        .join(conditions, "test_key", "left")
        .withColumn("genes", F.coalesce(F.col("genes"), F.array().cast(GENE_ARRAY)))
        .withColumn(
            "conditions",
            F.coalesce(F.col("conditions"), F.array().cast(CONDITION_ARRAY)),
        )
        .withColumn("_srid", F.col("test_key"))
    )
    tests = conform_genomics(add_genomics_provenance(tests, "_srid", RID), TABLE)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- genetic_test
if selected("genetic_test"):
    write_silver(tests, "genetic_test", source=SOURCE, component=COMPONENT, rid=RID)

    written = spark.table(genomics_table("genetic_test"))
    blocks = inspect_table(
        written,
        "genetic_test",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["test_key"],
        extra_checks={
            "tests_in_bronze": rows.select("test_key").distinct().count(),
            "tests_without_genes": written.filter(F.size("genes") == 0).count(),
            "tests_without_conditions": written.filter(
                F.size("conditions") == 0
            ).count(),
            "rows_with_an_unknown_object": rows.filter(
                ~F.col("kind").isin("gene", "condition")
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
        FINDINGS, f"{COMPONENT.split('/')[-1]}__genetic_test", "genetic_test", blocks
    )

# COMMAND ----------

# DBTITLE 1,Build -- ontology_relation
if selected("ontology_relation"):
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

    # -- Transform -- Open Targets disease parents
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
            F.coalesce(F.nullif(F.col("RELA"), F.lit("")), F.col("REL")).alias(
                "relation"
            ),
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
        add_genomics_provenance(relations, "_srid", RID), TABLE
    )

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- ontology_relation
if selected("ontology_relation"):
    write_silver(
        relations, "ontology_relation", source=SOURCE, component=COMPONENT, rid=RID
    )

    written = spark.table(genomics_table("ontology_relation"))
    blocks = inspect_table(
        written,
        "ontology_relation",
        source=FINDINGS,
        component=COMPONENT,
        rid=RID,
        key_cols=["source_record_id"],
        extra_checks={
            "source_order_relations": written.filter(
                F.col("direction") == "source_order"
            ).count(),
            "parent_child_relations": written.filter(
                F.col("direction") == "parent_child"
            ).count(),
            "self_relations": written.filter(
                F.col("parent_key") == F.col("child_key")
            ).count(),
            "ontologies": written.select("ontology").distinct().count(),
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
        f"{COMPONENT.split('/')[-1]}__ontology_relation",
        "ontology_relation",
        blocks,
    )
