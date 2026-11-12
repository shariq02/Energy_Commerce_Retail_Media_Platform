# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- SHARED PLUMBING (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** every id, name, claim and interval of every genomics entity: the base
# MAGIC of all other tables.

# COMMAND ----------

# DBTITLE 1,Genomics library
# MAGIC %run ../_genomics_common

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import Window
from pyspark.sql import functions as F

SOURCE = "genomics"
COMPONENT = "silver/genomics/01_shared_plumbing"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Datasets
NCBI = "ncbi_gene.gene_info"
G2R = "ncbi_gene.gene2refseq"
PHARMGKB = "pharmgkb.genes"
PHARMGKB_DRUGS = "pharmgkb.drugs"
GENCODE = "gencode.annotation"
REFSEQ = "refseq.annotation"
OPENTARGETS = "opentargets.target"
OT_DISEASE = "opentargets.disease"
OT_MOLECULE = "opentargets.drug_molecule"
TAHOE_GENE = "tahoe.gene_metadata"
TAHOE_DRUG = "tahoe.drug_metadata"
TAHOE_CELL = "tahoe.cell_line_metadata"
DEPMAP_MODELS = "depmap.models"
UNIPROT = "uniprot.entries"
CHEMBL = "chembl.compounds"
CHEMBL_MAP = "chembl.uniprot_mapping"
STRING_INFO = "string.protein_info"
STRING_ALIASES = "string.protein_aliases"
MEDGEN = "medgen.concepts"
HPO = "hpo.terms"
REACTOME = "reactome.pathways"
GTR = "gtr.test_condition_gene"
ENCODE = "encode.ccre_registry"
GTEX = "gtex.gene_median_tpm"

# COMMAND ----------

# DBTITLE 1,Helpers


def _gencode(feature):
    return _bronze("gencode_annotation").filter(F.col("feature") == feature)


def _attribute(name):
    """Value of a quoted GTF attribute from `attributes`; null when absent."""
    pattern = name + r' "([^"]+)"'
    return F.nullif(F.regexp_extract("attributes", pattern, 1), F.lit(""))


def _human(df, column="tax_id"):
    return df.filter(F.col(column) == "9606")


def _id_rows(df, *, entity_type, rules, dataset, key, specs, method="exact_id"):
    """Crosswalk rows of several id columns; `specs` lists (id type, column, field)."""
    return _union(
        [
            single_id_frame(
                df,
                key=key,
                value=value,
                id_type=id_type,
                entity_type=entity_type,
                dataset=dataset,
                field=field,
                rules=rules,
                method=method,
            )
            for id_type, value, field in specs
        ]
    )


def _symbol_unique_genes(names):
    """(symbol_key, gene_key): upper-case gene symbols that one gene holds."""
    return (
        names.filter(
            (F.col("entity_type") == "gene")
            & F.col("term_type").isin("symbol", "official_symbol")
            & (F.col("entities_sharing_term") == 1)
        )
        .select(
            F.upper(F.col("term")).alias("symbol_key"),
            F.col("entity_key").alias("gene_key"),
        )
        .distinct()
    )


# COMMAND ----------

# DBTITLE 1,Crosswalk: gene


def gene_crosswalk(rules):
    gene = {"entity_type": "gene", "rules": rules}
    info = _human(_bronze("ncbi_gene_gene_info"))
    ncbi = single_id_frame(
        info,
        key="GeneID",
        value="GeneID",
        id_type="ncbi_gene",
        dataset=NCBI,
        field="GeneID",
        **gene,
    ).unionByName(
        crosswalk_frame(
            xref_pairs_frame(info, "GeneID", "dbXrefs", "|"),
            dataset=NCBI,
            field="dbXrefs",
            **gene,
        )
    )
    # an id held by one NCBI gene only can link another source's record to it
    id_map = unique_id_map(finish_crosswalk(ncbi), ["ensembl", "hgnc"])

    pharmgkb = _bronze("pharmgkb_genes").withColumn(
        "hgnc_clean",
        hgnc_expr(clean_placeholders("HGNC_ID", rules, f"{PHARMGKB}.HGNC_ID")),
    )
    pharmgkb = resolve_gene_key(
        pharmgkb,
        id_map,
        geneid=clean_placeholders("NCBI_Gene_ID", rules, f"{PHARMGKB}.NCBI_Gene_ID"),
        ensembl=clean_placeholders("Ensembl_Id", rules, f"{PHARMGKB}.Ensembl_Id"),
        hgnc=F.col("hgnc_clean"),
        accession=F.col("PharmGKB_Accession_Id"),
    )
    how = F.col("key_method")
    pharmgkb_rows = crosswalk_frame(
        xref_pairs_frame(pharmgkb, "gene_key", "Cross_references", ",", method=how),
        dataset=PHARMGKB,
        field="Cross_references",
        **gene,
    ).unionByName(
        _id_rows(
            pharmgkb,
            entity_type="gene",
            rules=rules,
            dataset=PHARMGKB,
            key="gene_key",
            method=how,
            specs=[
                ("pharmgkb", "PharmGKB_Accession_Id", "PharmGKB_Accession_Id"),
                ("ncbi_gene", "NCBI_Gene_ID", "NCBI_Gene_ID"),
                ("ensembl", "Ensembl_Id", "Ensembl_Id"),
                ("hgnc", "hgnc_clean", "HGNC_ID"),
            ],
        )
    )

    gencode = (
        _gencode("gene")
        .withColumn("ensembl_clean", ensembl_unversioned_expr(F.col("gene_id")))
        .withColumn("hgnc_clean", hgnc_expr(_attribute("hgnc_id")))
    )
    gencode = resolve_gene_key(
        gencode, id_map, ensembl=F.col("ensembl_clean"), hgnc=F.col("hgnc_clean")
    )
    gencode_rows = _id_rows(
        gencode,
        entity_type="gene",
        rules=rules,
        dataset=GENCODE,
        key="gene_key",
        method=F.col("key_method"),
        specs=[
            ("ensembl", "ensembl_clean", "gene_id"),
            ("hgnc", "hgnc_clean", "attributes"),
        ],
    )

    opentargets = _bronze("opentargets_target").withColumn(
        "xrefs", F.from_json("dbXrefs", "array<struct<id:string,source:string>>")
    )
    hgnc_item = F.try_element_at(
        F.filter("xrefs", lambda item: item["source"] == "HGNC"), F.lit(1)
    )
    opentargets = opentargets.withColumn("hgnc_clean", hgnc_expr(hgnc_item["id"]))
    opentargets = resolve_gene_key(
        opentargets,
        id_map,
        ensembl=clean_placeholders("id", rules, f"{OPENTARGETS}.id"),
        hgnc=F.col("hgnc_clean"),
    )
    pairs = opentargets.select(
        F.col("gene_key").alias("entity_key"),
        F.explode("xrefs").alias("item"),
        F.col("key_method").alias("method"),
    ).select(
        "entity_key",
        F.col("item.source").alias("label"),
        F.col("item.id").alias("value"),
        "method",
    )
    opentargets_rows = _id_rows(
        opentargets,
        entity_type="gene",
        rules=rules,
        dataset=OPENTARGETS,
        key="gene_key",
        method=F.col("key_method"),
        specs=[("ensembl", "id", "id")],
    ).unionByName(crosswalk_frame(pairs, dataset=OPENTARGETS, field="dbXrefs", **gene))

    tahoe = resolve_gene_key(
        _bronze("tahoe_gene_metadata"),
        id_map,
        ensembl=clean_placeholders("ensembl_id", rules, f"{TAHOE_GENE}.ensembl_id"),
    )
    tahoe_rows = _id_rows(
        tahoe,
        entity_type="gene",
        rules=rules,
        dataset=TAHOE_GENE,
        key="gene_key",
        method=F.col("key_method"),
        specs=[
            ("ensembl", "ensembl_id", "ensembl_id"),
            ("tahoe_token", "token_id", "token_id"),
        ],
    )
    return _union([ncbi, pharmgkb_rows, gencode_rows, opentargets_rows, tahoe_rows])


# COMMAND ----------

# DBTITLE 1,Crosswalk: transcript and protein


def _refseq_transcripts():
    """RefSeq annotation transcripts with `transcript_key`: the unversioned
    accession, else `refseq:<name>` (mitochondrial RNAs are named by symbol)."""
    transcript = F.regexp_replace(F.trim(F.col("transcript_id")), r"^rna-", "")
    accession = transcript.rlike(r"^[NX][MR]_\d+")
    return (
        _bronze("refseq_annotation")
        .filter(F.col("feature") == "transcript")
        .withColumn("rna_name", transcript)
        .withColumn("is_accession", accession)
        .withColumn(
            "transcript_key",
            F.when(accession, F.regexp_replace(transcript, r"\.\d+$", "")).otherwise(
                F.concat(F.lit("refseq:"), transcript)
            ),
        )
    )


def _gene2refseq(rules):
    return (
        _human(_bronze("ncbi_gene_gene2refseq"))
        .withColumn(
            "rna",
            accession_unversioned_expr(
                "RNA_nucleotide_accession_version",
                rules,
                f"{G2R}.RNA_nucleotide_accession_version",
            ),
        )
        .withColumn(
            "np",
            accession_unversioned_expr(
                "protein_accession_version", rules, f"{G2R}.protein_accession_version"
            ),
        )
    )


def transcript_crosswalk(rules):
    tx = {"entity_type": "transcript", "rules": rules}
    gencode = _gencode("transcript").withColumn(
        "enst", ensembl_unversioned_expr(F.col("transcript_id"))
    )
    rows = _id_rows(
        gencode,
        dataset=GENCODE,
        key="enst",
        specs=[
            ("ensembl_transcript", "enst", "transcript_id"),
            ("ensembl_transcript_versioned", "transcript_id", "transcript_id"),
        ],
        **tx,
    )
    g2r = _gene2refseq(rules)
    rows = rows.unionByName(
        _id_rows(
            g2r,
            dataset=G2R,
            key="rna",
            specs=[
                ("refseq_rna", "rna", "RNA_nucleotide_accession_version"),
                (
                    "refseq_rna_versioned",
                    "RNA_nucleotide_accession_version",
                    "RNA_nucleotide_accession_version",
                ),
                ("genbank_gi", "RNA_nucleotide_gi", "RNA_nucleotide_gi"),
            ],
            **tx,
        )
    )
    refseq = _refseq_transcripts()
    accession = F.col("is_accession")
    return rows.unionByName(
        single_id_frame(
            refseq,
            key="transcript_key",
            value=F.when(accession, F.col("transcript_key")),
            id_type="refseq_rna",
            dataset=REFSEQ,
            field="transcript_id",
            **tx,
        )
    ).unionByName(
        single_id_frame(
            refseq,
            key="transcript_key",
            value=F.when(~accession, F.col("rna_name")),
            id_type="refseq_annotation_transcript",
            dataset=REFSEQ,
            field="transcript_id",
            **tx,
        )
    )


def protein_crosswalk(rules):
    protein = {"entity_type": "protein", "rules": rules}
    uniprot = _human(_bronze("uniprot_entries"))
    rows = _id_rows(
        uniprot,
        dataset=UNIPROT,
        key="accession",
        specs=[
            ("uniprot", "accession", "accession"),
            ("uniprot_entry_name", "entry_name", "entry_name"),
        ],
        **protein,
    )
    uniprot_map = unique_id_map(finish_crosswalk(rows), ["uniprot"])
    chembl = _bronze("chembl_uniprot_mapping")
    rows = rows.unionByName(
        _id_rows(
            chembl,
            dataset=CHEMBL_MAP,
            key="uniprot_accession",
            specs=[
                ("uniprot", "uniprot_accession", "uniprot_accession"),
                ("chembl_target", "chembl_target_id", "chembl_target_id"),
            ],
            **protein,
        )
    )

    info = _bronze("string_protein_info").withColumn(
        "ensp", F.regexp_replace(F.trim(F.col("string_protein_id")), r"^\d+\.", "")
    )
    aliases = _bronze("string_protein_aliases").withColumn(
        "ensp", F.regexp_replace(F.trim(F.col("string_protein_id")), r"^\d+\.", "")
    )
    alias_type = F.lit(None).cast("string")
    for entry in reversed(rules["string_alias_id_types"]):
        alias_type = F.when(
            F.col("source").rlike(entry["pattern"]), F.lit(entry["id_type"])
        ).otherwise(alias_type)
    alias_ids = (
        aliases.withColumn("id_type", alias_type)
        .filter(F.col("id_type").isNotNull())
        .select(
            F.col("ensp").alias("own"),
            "id_type",
            F.trim(F.col("alias")).alias("id_value"),
        )
    )
    links = link_by_ids(alias_ids, uniprot_map)
    info = (
        info.join(links, info["ensp"] == links["own"], "left")
        .withColumn("protein_key", F.coalesce(F.col("entity_key"), F.col("ensp")))
        .withColumn(
            "key_method",
            F.when(F.col("entity_key").isNotNull(), F.lit("uniprot_alias")).otherwise(
                F.lit("own_ensembl_protein")
            ),
        )
        .select("string_protein_id", "ensp", "protein_key", "key_method")
    )
    rows = rows.unionByName(
        _id_rows(
            info,
            dataset=STRING_INFO,
            key="protein_key",
            method=F.col("key_method"),
            specs=[
                ("ensembl_protein", "ensp", "string_protein_id"),
                ("string", "string_protein_id", "string_protein_id"),
            ],
            **protein,
        )
    )
    keyed_aliases = alias_ids.join(
        info.select(F.col("ensp").alias("_a_ensp"), "protein_key"),
        alias_ids["own"] == F.col("_a_ensp"),
        "inner",
    ).select(
        F.col("protein_key").alias("entity_key"),
        F.col("id_type").alias("label"),
        F.col("id_value").alias("value"),
        F.lit("string_alias").alias("method"),
    )
    rows = rows.unionByName(
        crosswalk_frame(keyed_aliases, dataset=STRING_ALIASES, field="alias", **protein)
    )

    string_keys = info.select(
        F.col("ensp").alias("_s_ensp"), F.col("protein_key").alias("_s_key")
    )
    gencode = (
        _gencode("transcript")
        .withColumn("ensp_g", ensembl_unversioned_expr(_attribute("protein_id")))
        .filter(F.col("ensp_g").isNotNull())
        .select("ensp_g")
        .distinct()
        .join(string_keys, F.col("ensp_g") == F.col("_s_ensp"), "left")
        .withColumn("protein_key", F.coalesce(F.col("_s_key"), F.col("ensp_g")))
        .withColumn(
            "key_method",
            F.when(
                F.col("_s_key").isNotNull(), F.lit("ensembl_protein_via_string")
            ).otherwise(F.lit("own_ensembl_protein")),
        )
    )
    rows = rows.unionByName(
        _id_rows(
            gencode,
            dataset=GENCODE,
            key="protein_key",
            method=F.col("key_method"),
            specs=[("ensembl_protein", "ensp_g", "protein_id")],
            **protein,
        )
    )
    g2r = (
        _gene2refseq(rules)
        .select(
            "np",
            "protein_accession_version",
            "protein_gi",
            "mature_peptide_accession_version",
            "mature_peptide_gi",
        )
        .distinct()
    )
    return rows.unionByName(
        _id_rows(
            g2r,
            dataset=G2R,
            key="np",
            specs=[
                ("refseq_protein", "np", "protein_accession_version"),
                (
                    "refseq_protein_versioned",
                    "protein_accession_version",
                    "protein_accession_version",
                ),
                ("genbank_gi", "protein_gi", "protein_gi"),
                (
                    "refseq_mature_peptide",
                    "mature_peptide_accession_version",
                    "mature_peptide_accession_version",
                ),
                ("genbank_gi_mature_peptide", "mature_peptide_gi", "mature_peptide_gi"),
            ],
            **protein,
        )
    )


# COMMAND ----------

# DBTITLE 1,Crosswalk: disease, phenotype, pathway, tissue, regulatory element


def _medgen():
    return _bronze("medgen_concepts").filter(F.col("SUPPRESS") == "N")


def disease_crosswalk(rules):
    disease = {"entity_type": "disease", "rules": rules}
    medgen = _medgen()
    rows = _id_rows(
        medgen.select("CUI").distinct(),
        dataset=MEDGEN,
        key="CUI",
        specs=[("medgen", "CUI", "CUI"), ("umls", "CUI", "CUI")],
        **disease,
    )
    codes = (
        medgen.filter(
            F.col("CODE").isNotNull()
            & (F.col("CODE") != F.col("CUI"))
            & (F.col("CODE") != "NOCODE")
        )
        .select(
            F.col("CUI").alias("entity_key"),
            F.col("SAB").alias("label"),
            F.col("CODE").alias("value"),
            F.lit("exact_id").alias("method"),
        )
        .distinct()
    )
    rows = rows.unionByName(
        crosswalk_frame(codes, dataset=MEDGEN, field="CODE", **disease)
    )
    # the atom and the source atom, concept and descriptor ids of every MedGen row
    rows = rows.unionByName(
        _id_rows(
            medgen,
            dataset=MEDGEN,
            key="CUI",
            specs=[
                ("medgen_atom", "AUI", "AUI"),
                ("source_atom", "SAUI", "SAUI"),
                ("source_concept", "SCUI", "SCUI"),
                ("source_descriptor", "SDUI", "SDUI"),
            ],
            **disease,
        )
    )
    id_map = unique_id_map(finish_crosswalk(rows))

    terms = (
        _bronze("opentargets_disease")
        .withColumn("curie", curie_expr("id"))
        .withColumn(
            "prefix",
            F.nullif(F.regexp_extract("curie", r"^([A-Za-z]+):", 1), F.lit("")),
        )
    )
    own = terms.filter(F.col("prefix").isNotNull()).select(
        F.col("id").alias("entity_key"),
        F.col("prefix").alias("label"),
        F.col("curie").alias("value"),
        F.lit("exact_id").alias("method"),
    )
    xrefs = terms.select(
        F.col("id").alias("entity_key"),
        F.explode(F.from_json("dbXRefs", "array<string>")).alias("item"),
    ).select(
        "entity_key",
        F.regexp_extract("item", r"^([^:]+):", 1).alias("label"),
        F.regexp_extract("item", r"^[^:]+:(.*)$", 1).alias("value"),
        F.lit("exact_id").alias("method"),
    )
    links = link_by_ids(typed_pairs(own.unionByName(xrefs), rules), id_map)
    keyed = (
        terms.join(links, terms["id"] == links["own"], "left")
        .withColumn("disease_key", F.coalesce(F.col("entity_key"), F.col("id")))
        .withColumn(
            "key_method",
            F.when(F.col("entity_key").isNotNull(), F.lit("ids_via_medgen")).otherwise(
                F.lit("own_opentargets_id")
            ),
        )
    )
    how = F.col("key_method")
    pairs = (
        keyed.filter(F.col("prefix").isNotNull())
        .select(
            F.col("disease_key").alias("entity_key"),
            F.col("prefix").alias("label"),
            F.col("curie").alias("value"),
            how.alias("method"),
        )
        .unionByName(
            keyed.select(
                F.col("disease_key").alias("entity_key"),
                F.explode(F.from_json("dbXRefs", "array<string>")).alias("item"),
                how.alias("method"),
            ).select(
                "entity_key",
                F.regexp_extract("item", r"^([^:]+):", 1).alias("label"),
                F.regexp_extract("item", r"^[^:]+:(.*)$", 1).alias("value"),
                "method",
            )
        )
    )
    obsolete = keyed.select(
        F.col("disease_key").alias("entity_key"),
        F.explode(F.from_json("obsoleteXRefs", "array<string>")).alias("item"),
    ).select(
        "entity_key",
        F.regexp_extract("item", r"^([^:]+):", 1).alias("label"),
        F.regexp_extract("item", r"^[^:]+:(.*)$", 1).alias("value"),
        F.lit("obsolete_cross_reference").alias("method"),
    )
    opentargets = (
        _id_rows(
            keyed,
            dataset=OT_DISEASE,
            key="disease_key",
            method=how,
            specs=[("opentargets_disease", "id", "id")],
            **disease,
        )
        .unionByName(
            crosswalk_frame(
                pairs, dataset=OT_DISEASE, field="id_and_dbXRefs", **disease
            )
        )
        .unionByName(
            crosswalk_frame(
                obsolete, dataset=OT_DISEASE, field="obsoleteXRefs", **disease
            )
        )
    )
    return rows.unionByName(opentargets)


def phenotype_crosswalk(rules):
    terms = _bronze("hpo_terms").withColumn("curie", curie_expr("id"))
    return _id_rows(
        terms,
        entity_type="phenotype",
        rules=rules,
        dataset=HPO,
        key="curie",
        specs=[("hpo", "curie", "id")],
    )


def pathway_crosswalk(rules):
    return _id_rows(
        _bronze("reactome_pathways"),
        entity_type="pathway",
        rules=rules,
        dataset=REACTOME,
        key="pathway_id",
        specs=[("reactome", "pathway_id", "pathway_id")],
    )


def tissue_list():
    """One row per GTEx tissue: the tissue columns of the wide median table."""
    columns = _bronze("gtex_gene_median_tpm").columns[2:]
    return spark.createDataFrame([(c,) for c in columns], "tissue string")


def tissue_crosswalk(rules):
    return _id_rows(
        tissue_list(),
        entity_type="tissue",
        rules=rules,
        dataset=GTEX,
        key="tissue",
        specs=[("gtex_tissue", "tissue", "column_name")],
    )


def regulatory_crosswalk(rules):
    return _id_rows(
        _bronze("encode_ccre_registry"),
        entity_type="regulatory_element",
        rules=rules,
        dataset=ENCODE,
        key="ccre_accession",
        specs=[
            ("encode_ccre", "ccre_accession", "ccre_accession"),
            ("encode_dhs", "dhs_accession", "dhs_accession"),
        ],
    )


def cell_line_crosswalk(rules):
    """Tahoe cell lines and DepMap models; both are keyed by the DepMap model id."""
    return _union(
        [
            _id_rows(
                _bronze("tahoe_cell_line_metadata"),
                entity_type="cell_line",
                rules=rules,
                dataset=TAHOE_CELL,
                key="Cell_ID_DepMap",
                specs=[
                    ("depmap", "Cell_ID_DepMap", "Cell_ID_DepMap"),
                    ("cellosaur", "Cell_ID_Cellosaur", "Cell_ID_Cellosaur"),
                ],
            ),
            _id_rows(
                _bronze("depmap_models"),
                entity_type="cell_line",
                rules=rules,
                dataset=DEPMAP_MODELS,
                key="ModelID",
                specs=[
                    ("depmap", "ModelID", "ModelID"),
                    ("cellosaur", "RRID", "RRID"),
                    ("sanger_model", "SangerModelID", "SangerModelID"),
                    ("cosmic", "COSMICID", "COSMICID"),
                ],
            ),
        ]
    )


def test_crosswalk(rules):
    return _id_rows(
        genetic_test(),
        entity_type="genetic_test",
        rules=rules,
        dataset=GTR,
        key="test_key",
        specs=[
            ("gtr_accession", "test_key", "accession_version"),
            ("gtr_accession_version", "accession_version", "accession_version"),
        ],
    )


# COMMAND ----------

# DBTITLE 1,Crosswalk: drug


def _tahoe_drugs():
    return _bronze("tahoe_drug_metadata").withColumn(
        "tahoe_key", F.concat(F.lit("tahoe:"), F.lower(F.trim(F.col("drug"))))
    )


def drug_crosswalk(rules):
    drug = {"entity_type": "drug", "rules": rules}
    chembl = _bronze("chembl_compounds")
    rows = _id_rows(
        chembl,
        dataset=CHEMBL,
        key="chembl_id",
        specs=[
            ("chembl", "chembl_id", "chembl_id"),
            ("inchikey", "standard_inchi_key", "standard_inchi_key"),
        ],
        **drug,
    )
    molecule = _bronze("opentargets_drug_molecule")
    refs = molecule.select(
        F.col("id").alias("entity_key"),
        F.explode(
            F.from_json(
                "crossReferences", "array<struct<source:string,ids:array<string>>>"
            )
        ).alias("ref"),
    ).select(
        "entity_key",
        F.col("ref.source").alias("label"),
        F.explode("ref.ids").alias("value"),
        F.lit("exact_id").alias("method"),
    )
    rows = rows.unionByName(
        _id_rows(
            molecule,
            dataset=OT_MOLECULE,
            key="id",
            specs=[("chembl", "id", "id"), ("inchikey", "inchiKey", "inchiKey")],
            **drug,
        )
    ).unionByName(
        crosswalk_frame(refs, dataset=OT_MOLECULE, field="crossReferences", **drug)
    )
    id_map = unique_id_map(finish_crosswalk(rows))

    pharmgkb = _bronze("pharmgkb_drugs")
    accession = "PharmGKB_Accession_Id"
    xref_pairs = xref_pairs_frame(pharmgkb, accession, "Cross_references", ",")
    lists = _union(
        [
            id_list_pairs(pharmgkb, accession, id_type, column, rules, PHARMGKB_DRUGS)
            for id_type, column in (
                ("rxnorm", "RxNorm_Identifiers"),
                ("atc", "ATC_Identifiers"),
                ("pubchem_compound", "PubChem_Compound_Identifiers"),
            )
        ]
    )
    candidates = typed_pairs(xref_pairs, rules).unionByName(lists)
    links = link_by_ids(candidates, id_map)
    pharmgkb = (
        pharmgkb.join(links, pharmgkb[accession] == links["own"], "left")
        .withColumn("drug_key", F.coalesce(F.col("entity_key"), F.col(accession)))
        .withColumn(
            "key_method",
            F.when(F.col("entity_key").isNotNull(), F.lit("ids_via_chembl")).otherwise(
                F.lit("own_pharmgkb_id")
            ),
        )
    )
    how = F.col("key_method")
    pharmgkb_rows = _id_rows(
        pharmgkb,
        dataset=PHARMGKB_DRUGS,
        key="drug_key",
        method=how,
        specs=[("pharmgkb", accession, accession)],
        **drug,
    ).unionByName(
        crosswalk_frame(
            xref_pairs_frame(pharmgkb, "drug_key", "Cross_references", ",", method=how),
            dataset=PHARMGKB_DRUGS,
            field="Cross_references",
            **drug,
        )
    )
    for id_type, column in (
        ("rxnorm", "RxNorm_Identifiers"),
        ("atc", "ATC_Identifiers"),
        ("pubchem_compound", "PubChem_Compound_Identifiers"),
    ):
        listed = id_list_pairs(
            pharmgkb, "drug_key", id_type, column, rules, PHARMGKB_DRUGS
        )
        pharmgkb_rows = pharmgkb_rows.unionByName(
            crosswalk_frame(
                listed.select(
                    F.col("own").alias("entity_key"),
                    F.col("id_type").alias("label"),
                    F.col("id_value").alias("value"),
                    F.lit("pharmgkb_list").alias("method"),
                ),
                dataset=PHARMGKB_DRUGS,
                field=column,
                **drug,
            )
        )
    id_map = unique_id_map(finish_crosswalk(rows.unionByName(pharmgkb_rows)))

    tahoe = _tahoe_drugs().withColumn(
        "pubchem", F.regexp_replace(F.trim(F.col("pubchem_cid")), r"\.0$", "")
    )
    tahoe_pairs = tahoe.select(
        F.col("tahoe_key").alias("own"),
        F.lit("pubchem_compound").alias("id_type"),
        F.col("pubchem").alias("id_value"),
    ).filter(F.col("id_value").isNotNull() & (F.col("id_value") != ""))
    links = link_by_ids(tahoe_pairs, id_map)
    tahoe = (
        tahoe.join(links, tahoe["tahoe_key"] == links["own"], "left")
        .withColumn("drug_key", F.coalesce(F.col("entity_key"), F.col("tahoe_key")))
        .withColumn(
            "key_method",
            F.when(F.col("entity_key").isNotNull(), F.lit("ids_via_pubchem")).otherwise(
                F.lit("own_tahoe_name")
            ),
        )
    )
    tahoe_rows = _id_rows(
        tahoe,
        dataset=TAHOE_DRUG,
        key="drug_key",
        method=F.col("key_method"),
        specs=[
            ("tahoe_drug", "tahoe_key", "drug"),
            ("pubchem_compound", "pubchem", "pubchem_cid"),
        ],
        **drug,
    )
    return _union([rows, pharmgkb_rows, tahoe_rows])


# COMMAND ----------

# DBTITLE 1,Crosswalk: all entity types


def crosswalk_all(rules):
    return _union(
        [
            gene_crosswalk(rules),
            transcript_crosswalk(rules),
            protein_crosswalk(rules),
            disease_crosswalk(rules),
            phenotype_crosswalk(rules),
            pathway_crosswalk(rules),
            tissue_crosswalk(rules),
            regulatory_crosswalk(rules),
            cell_line_crosswalk(rules),
            test_crosswalk(rules),
            drug_crosswalk(rules),
            variant_crosswalk(rules),
        ]
    )


# COMMAND ----------

# DBTITLE 1,Names


def _symbol_free(df):
    """Open Targets: a symbol equal to the Ensembl id means no symbol."""
    return df.withColumn(
        "approvedSymbol",
        F.when(F.col("approvedSymbol") != F.col("id"), F.col("approvedSymbol")),
    )


def names_all(rules, xwalk):
    """Name rows of every entity type."""
    keyed = {"key_col": "entity_key", "rules": rules}
    by_type = {
        t: xwalk.filter(F.col("entity_type") == t)
        for t in (
            "gene",
            "transcript",
            "protein",
            "disease",
            "drug",
            "cell_line",
            "phenotype",
            "pathway",
            "tissue",
        )
    }
    gene = {"entity_type": "gene", **keyed}
    gene_x = by_type["gene"]
    parts = [
        names_frame(
            _human(_bronze("ncbi_gene_gene_info")).withColumn(
                "entity_key", F.col("GeneID")
            ),
            dataset=NCBI,
            specs=[
                ("Symbol", "symbol", "text"),
                ("Symbol_from_nomenclature_authority", "official_symbol", "text"),
                ("Full_name_from_nomenclature_authority", "full_name", "text"),
                ("description", "description", "text"),
                ("Synonyms", "synonym", "pipe"),
                ("Other_designations", "designation", "pipe"),
                ("LocusTag", "locus_tag", "text"),
            ],
            **gene,
        ),
        names_frame(
            with_entity_key(
                _symbol_free(_bronze("opentargets_target")),
                "id",
                gene_x,
                OPENTARGETS,
                "ensembl",
            ),
            dataset=OPENTARGETS,
            specs=[
                ("approvedSymbol", "symbol", "text"),
                ("approvedName", "full_name", "text"),
                ("synonyms", "synonym", "json"),
                ("symbolSynonyms", "symbol_synonym", "json"),
                ("nameSynonyms", "name_synonym", "json"),
                ("obsoleteSymbols", "obsolete_symbol", "json"),
                ("obsoleteNames", "obsolete_name", "json"),
            ],
            **gene,
        ),
        names_frame(
            with_entity_key(
                _bronze("pharmgkb_genes"),
                "PharmGKB_Accession_Id",
                gene_x,
                PHARMGKB,
                "pharmgkb",
            ),
            dataset=PHARMGKB,
            specs=[
                ("Symbol", "symbol", "text"),
                ("Name", "full_name", "text"),
                ("Alternate_Symbols", "alternate_symbol", "comma"),
                # full names contain commas, so this list is not split
                ("Alternate_Names", "alternate_name", "text"),
            ],
            **gene,
        ),
        names_frame(
            with_entity_key(
                _gencode("gene").withColumn(
                    "ensembl_clean", ensembl_unversioned_expr(F.col("gene_id"))
                ),
                "ensembl_clean",
                gene_x,
                GENCODE,
                "ensembl",
            ),
            dataset=GENCODE,
            specs=[("gene_name", "symbol", "text")],
            **gene,
        ),
        names_frame(
            with_entity_key(
                _bronze("tahoe_gene_metadata"),
                "ensembl_id",
                gene_x,
                TAHOE_GENE,
                "ensembl",
            ),
            dataset=TAHOE_GENE,
            specs=[("gene_symbol", "symbol", "text")],
            **gene,
        ),
    ]
    tx = {"entity_type": "transcript", **keyed}
    parts.append(
        names_frame(
            with_entity_key(
                _gencode("transcript").withColumn(
                    "enst", ensembl_unversioned_expr(F.col("transcript_id"))
                ),
                "enst",
                by_type["transcript"],
                GENCODE,
                "ensembl_transcript",
            ),
            dataset=GENCODE,
            specs=[("transcript_name", "transcript_name", "text")],
            **tx,
        )
    )
    protein = {"entity_type": "protein", **keyed}
    protein_x = by_type["protein"]
    string_info = _bronze("string_protein_info").withColumn(
        "ensp", F.regexp_replace(F.trim(F.col("string_protein_id")), r"^\d+\.", "")
    )
    string_aliases = _bronze("string_protein_aliases").withColumn(
        "ensp", F.regexp_replace(F.trim(F.col("string_protein_id")), r"^\d+\.", "")
    )
    parts += [
        names_frame(
            with_entity_key(
                _human(_bronze("uniprot_entries")),
                "accession",
                protein_x,
                UNIPROT,
                "uniprot",
            ),
            dataset=UNIPROT,
            specs=[
                ("protein_name", "name", "text"),
                ("entry_name", "entry_name", "text"),
                ("gene_name", "gene_symbol", "text"),
            ],
            **protein,
        ),
        names_frame(
            with_entity_key(
                string_info, "ensp", protein_x, STRING_INFO, "ensembl_protein"
            ),
            dataset=STRING_INFO,
            specs=[("preferred_name", "gene_symbol", "text")],
            **protein,
        ),
        names_frame(
            with_entity_key(
                string_aliases, "ensp", protein_x, STRING_INFO, "ensembl_protein"
            ),
            dataset=STRING_ALIASES,
            specs=[("alias", F.lower(F.col("source")), "text")],
            **protein,
        ),
        names_frame(
            with_entity_key(
                _bronze("chembl_uniprot_mapping"),
                "uniprot_accession",
                protein_x,
                CHEMBL_MAP,
                "uniprot",
            ),
            dataset=CHEMBL_MAP,
            specs=[("target_name", "name", "text")],
            **protein,
        ),
    ]
    disease = {"entity_type": "disease", **keyed}
    disease_x = by_type["disease"]
    medgen = _medgen().withColumn("entity_key", F.col("CUI"))
    parts += [
        names_frame(
            medgen,
            dataset=MEDGEN,
            specs=[("STR", F.lower(F.col("TTY")), "text", "SAB")],
            **disease,
        ),
        names_frame(
            with_entity_key(
                _bronze("opentargets_disease"),
                "id",
                disease_x,
                OT_DISEASE,
                "opentargets_disease",
            ),
            dataset=OT_DISEASE,
            specs=[
                ("name", "name", "text"),
                ("exactSynonyms", "exact_synonym", "jsonstr"),
                ("relatedSynonyms", "related_synonym", "jsonstr"),
                ("narrowSynonyms", "narrow_synonym", "jsonstr"),
                ("broadSynonyms", "broad_synonym", "jsonstr"),
                ("obsoleteTerms", "obsolete_term", "jsonstr"),
            ],
            **disease,
        ),
    ]
    drug = {"entity_type": "drug", **keyed}
    drug_x = by_type["drug"]
    parts += [
        names_frame(
            with_entity_key(
                _bronze("pharmgkb_drugs"),
                "PharmGKB_Accession_Id",
                drug_x,
                PHARMGKB_DRUGS,
                "pharmgkb",
            ),
            dataset=PHARMGKB_DRUGS,
            specs=[
                ("Name", "name", "text"),
                ("Generic_Names", "generic_name", "comma"),
                ("Trade_Names", "trade_name", "comma"),
                ("Brand_Mixtures", "brand_mixture", "comma"),
            ],
            **drug,
        ),
        names_frame(
            with_entity_key(
                _bronze("opentargets_drug_molecule"),
                "id",
                drug_x,
                OT_MOLECULE,
                "chembl",
            ),
            dataset=OT_MOLECULE,
            specs=[
                ("name", "name", "text"),
                ("synonyms", "synonym", "json"),
                ("tradeNames", "trade_name", "jsonmix"),
            ],
            **drug,
        ),
        names_frame(
            with_entity_key(
                _tahoe_drugs(), "tahoe_key", drug_x, TAHOE_DRUG, "tahoe_drug"
            ),
            dataset=TAHOE_DRUG,
            specs=[("drug", "name", "text")],
            **drug,
        ),
        names_frame(
            with_entity_key(
                _bronze("tahoe_cell_line_metadata"),
                "Cell_ID_DepMap",
                by_type["cell_line"],
                TAHOE_CELL,
                "depmap",
            ),
            dataset=TAHOE_CELL,
            specs=[("cell_name", "name", "text")],
            entity_type="cell_line",
            **keyed,
        ),
        names_frame(
            _bronze("depmap_models").withColumn("entity_key", F.col("ModelID")),
            dataset=DEPMAP_MODELS,
            specs=[
                ("CellLineName", "name", "text"),
                ("StrippedCellLineName", "stripped_name", "text"),
                ("CCLEName", "ccle_name", "text"),
            ],
            entity_type="cell_line",
            **keyed,
        ),
        names_frame(
            _bronze("hpo_terms").withColumn("entity_key", curie_expr("id")),
            dataset=HPO,
            specs=[("label", "name", "text")],
            entity_type="phenotype",
            **keyed,
        ),
        names_frame(
            _bronze("reactome_pathways").withColumn("entity_key", F.col("pathway_id")),
            dataset=REACTOME,
            specs=[("pathway_name", "name", "text")],
            entity_type="pathway",
            **keyed,
        ),
        names_frame(
            tissue_list()
            .withColumn("entity_key", F.col("tissue"))
            .withColumn("display_name", F.regexp_replace("tissue", "_", " ")),
            dataset=GTEX,
            specs=[
                ("tissue", "source_column", "text"),
                ("display_name", "display_name", "text"),
            ],
            entity_type="tissue",
            **keyed,
        ),
        # ClinVar HGVS names, gnomAD ids, PharmGKB names and synonyms of variants
        variant_names(rules, variant_id_map(xwalk)),
    ]
    return _union(parts)


# COMMAND ----------

# DBTITLE 1,Claims: gene


def gene_claims(rules, xwalk):
    base = {"entity_type": "gene", "key_col": "entity_key", "rules": rules}
    gene_x = xwalk.filter(F.col("entity_type") == "gene")
    ncbi = _human(_bronze("ncbi_gene_gene_info")).withColumn(
        "entity_key", F.col("GeneID")
    )
    # a symbol equal to the Ensembl id means the gene has no symbol
    opentargets = (
        _symbol_free(_bronze("opentargets_target"))
        .withColumn("function_descriptions_json", F.col("functionDescriptions"))
        .withColumn(
            "functionDescriptions",
            F.try_element_at(
                F.from_json("functionDescriptions", "array<string>"), F.lit(1)
            ),
        )
    )
    opentargets = with_entity_key(opentargets, "id", gene_x, OPENTARGETS, "ensembl")
    pharmgkb = with_entity_key(
        _bronze("pharmgkb_genes"), "PharmGKB_Accession_Id", gene_x, PHARMGKB, "pharmgkb"
    )
    gencode = with_entity_key(
        _gencode("gene").withColumn(
            "ensembl_clean", ensembl_unversioned_expr(F.col("gene_id"))
        ),
        "ensembl_clean",
        gene_x,
        GENCODE,
        "ensembl",
    )
    tahoe = with_entity_key(
        _bronze("tahoe_gene_metadata"), "ensembl_id", gene_x, TAHOE_GENE, "ensembl"
    )
    return _union(
        [
            claims_frame(
                ncbi,
                fields=[
                    ("symbol", "Symbol_from_nomenclature_authority"),
                    ("symbol", "Symbol"),
                    ("full_name", "Full_name_from_nomenclature_authority"),
                    ("description", "description"),
                    ("gene_type", "type_of_gene"),
                    ("chromosome", "chromosome"),
                    ("map_location", "map_location"),
                    ("nomenclature_status", "Nomenclature_status"),
                    ("modification_date", "Modification_date"),
                    ("feature_type", "Feature_type"),
                ],
                source_dataset=NCBI,
                **base,
            ),
            claims_frame(
                opentargets,
                fields=[
                    ("symbol", "approvedSymbol"),
                    ("full_name", "approvedName"),
                    ("gene_type", "biotype"),
                    ("description", "functionDescriptions"),
                    ("function_descriptions", "function_descriptions_json"),
                    ("genomic_location", "genomicLocation"),
                    ("subcellular_locations", "subcellularLocations"),
                    ("target_class", "targetClass"),
                    ("go_terms", "go"),
                    ("hallmarks", "hallmarks"),
                    ("constraint", "constraint"),
                    ("protein_ids", "proteinIds"),
                    ("pathways", "pathways"),
                    ("tss", "tss"),
                ],
                source_dataset=OPENTARGETS,
                numeric_fields=("tss",),
                **base,
            ),
            claims_frame(
                pharmgkb,
                fields=[
                    ("full_name", "Name"),
                    ("is_vip", "Is_VIP"),
                    ("has_variant_annotation", "Has_Variant_Annotation"),
                    ("has_cpic_dosing_guideline", "Has_CPIC_Dosing_Guideline"),
                ],
                source_dataset=PHARMGKB,
                **base,
            ),
            claims_frame(
                gencode,
                fields=[("symbol", "gene_name"), ("gene_type", "gene_type")],
                source_dataset=GENCODE,
                **base,
            ),
            claims_frame(
                tahoe,
                fields=[("symbol", "gene_symbol")],
                source_dataset=TAHOE_GENE,
                **base,
            ),
            xwalk_claims(
                gene_x, rules, "gene", {"ensembl_id": "ensembl", "hgnc_id": "hgnc"}
            ),
        ]
    )


# COMMAND ----------

# DBTITLE 1,Claims: transcript and protein


def transcript_claims(rules, xwalk, names):
    base = {"entity_type": "transcript", "key_col": "entity_key", "rules": rules}
    by_type = {
        t: xwalk.filter(F.col("entity_type") == t)
        for t in ("gene", "transcript", "protein")
    }
    genes = (
        by_type["gene"]
        .filter((F.col("id_type") == "ensembl") & (F.col("entities_sharing_id") == 1))
        .select(F.col("id_value").alias("_g_ens"), F.col("entity_key").alias("_g_key"))
        .distinct()
    )
    proteins = (
        by_type["protein"]
        .filter(
            (F.col("id_type") == "ensembl_protein")
            & (F.col("entities_sharing_id") == 1)
        )
        .select(F.col("id_value").alias("_p_ens"), F.col("entity_key").alias("_p_key"))
        .distinct()
    )
    symbols = _symbol_unique_genes(names).select(
        F.col("symbol_key").alias("_s_sym"), F.col("gene_key").alias("_s_key")
    )
    gencode = with_entity_key(
        _gencode("transcript").withColumn(
            "enst", ensembl_unversioned_expr(F.col("transcript_id"))
        ),
        "enst",
        by_type["transcript"],
        GENCODE,
        "ensembl_transcript",
    )
    gencode = (
        gencode.withColumn("gene_ens", ensembl_unversioned_expr(F.col("gene_id")))
        .withColumn("protein_ens", ensembl_unversioned_expr(_attribute("protein_id")))
        .withColumn(
            "version", F.regexp_extract("transcript_id", r"\.(\d+)(?:_PAR_Y)?$", 1)
        )
        .withColumn("level", _attribute("level"))
        .withColumn(
            "tag_list",
            F.regexp_extract_all(F.col("attributes"), F.lit(r'tag "([^"]+)"'), 1),
        )
        .join(genes, F.col("gene_ens") == F.col("_g_ens"), "left")
        .join(proteins, F.col("protein_ens") == F.col("_p_ens"), "left")
        .withColumn("gene_id", F.col("_g_key"))
        .withColumn("protein_id", F.col("_p_key"))
    )
    g2r = _gene2refseq(rules).filter(F.col("rna").isNotNull())
    g2r = with_entity_key(g2r, "rna", by_type["transcript"], G2R, "refseq_rna")
    g2r = (
        g2r.withColumn(
            "rna_version",
            F.regexp_extract("RNA_nucleotide_accession_version", r"\.(\d+)$", 1),
        )
        .join(
            by_type["protein"]
            .filter(
                (F.col("source_dataset") == G2R)
                & (F.col("id_type") == "refseq_protein")
            )
            .select(
                F.col("id_value").alias("_n_np"), F.col("entity_key").alias("_n_key")
            )
            .distinct(),
            F.col("np") == F.col("_n_np"),
            "left",
        )
        .withColumn("protein_accession_version", F.col("_n_key"))
    )
    refseq = (
        _refseq_transcripts()
        .withColumn("entity_key", F.col("transcript_key"))
        .join(symbols, F.upper(F.col("gene_id")) == F.col("_s_sym"), "left")
        .withColumn("gene_id", F.col("_s_key"))
    )
    return _union(
        [
            claims_frame(
                gencode,
                fields=[
                    ("gene_key", "gene_id"),
                    ("protein_key", "protein_id"),
                    ("transcript_type", "transcript_type"),
                    ("transcript_name", "transcript_name"),
                    ("version", "version"),
                    ("level", "level"),
                ],
                source_dataset=GENCODE,
                **base,
            ),
            array_claims(
                gencode,
                entity_type="transcript",
                key_col="entity_key",
                field="tag",
                array=F.col("tag_list"),
                dataset=GENCODE,
                source_field="attributes",
            ),
            claims_frame(
                g2r,
                fields=[
                    ("gene_key", "GeneID"),
                    ("protein_key", "protein_accession_version"),
                    ("version", "rna_version"),
                ],
                source_dataset=G2R,
                **base,
            ),
            claims_frame(
                refseq,
                fields=[
                    ("gene_key", "gene_id"),
                    ("transcript_type", "transcript_type"),
                ],
                source_dataset=REFSEQ,
                **base,
            ),
        ]
    )


def protein_claims(rules, xwalk):
    base = {"entity_type": "protein", "key_col": "entity_key", "rules": rules}
    protein_x = xwalk.filter(F.col("entity_type") == "protein")
    string_info = _bronze("string_protein_info").withColumn(
        "ensp", F.regexp_replace(F.trim(F.col("string_protein_id")), r"^\d+\.", "")
    )
    return _union(
        [
            claims_frame(
                with_entity_key(
                    _human(_bronze("uniprot_entries")),
                    "accession",
                    protein_x,
                    UNIPROT,
                    "uniprot",
                ),
                fields=[
                    ("name", "protein_name"),
                    ("length", "sequence_length"),
                    ("gene_symbol", "gene_name"),
                    ("entry_name", "entry_name"),
                    ("tax_id", "tax_id"),
                ],
                source_dataset=UNIPROT,
                numeric_fields=("length",),
                **base,
            ),
            claims_frame(
                with_entity_key(
                    _bronze("chembl_uniprot_mapping"),
                    "uniprot_accession",
                    protein_x,
                    CHEMBL_MAP,
                    "uniprot",
                ),
                fields=[("name", "target_name"), ("target_type", "target_type")],
                source_dataset=CHEMBL_MAP,
                **base,
            ),
            claims_frame(
                with_entity_key(
                    string_info, "ensp", protein_x, STRING_INFO, "ensembl_protein"
                ),
                fields=[
                    ("description", "annotation"),
                    ("length", "protein_size"),
                    ("gene_symbol", "preferred_name"),
                ],
                source_dataset=STRING_INFO,
                numeric_fields=("length",),
                **base,
            ),
        ]
    )


# COMMAND ----------

# DBTITLE 1,Claims: disease, drug, cell line, phenotype, pathway, regulatory element


def disease_claims(rules, xwalk):
    base = {"entity_type": "disease", "key_col": "entity_key", "rules": rules}
    disease_x = xwalk.filter(F.col("entity_type") == "disease")
    preferred = (
        _medgen()
        .filter(
            (F.col("TS") == "P") & (F.col("STT") == "PF") & (F.col("ISPREF") == "Y")
        )
        .withColumn("entity_key", F.col("CUI"))
    )
    terms = with_entity_key(
        _bronze("opentargets_disease").withColumn(
            "ontology_prefix", F.regexp_extract(curie_expr("id"), r"^([A-Za-z]+):", 1)
        ),
        "id",
        disease_x,
        OT_DISEASE,
        "opentargets_disease",
    )
    return _union(
        [
            claims_frame(
                preferred, fields=[("name", "STR")], source_dataset=MEDGEN, **base
            ),
            claims_frame(
                terms,
                fields=[
                    ("name", "name"),
                    ("description", "description"),
                    ("ontology_prefix", "ontology_prefix"),
                    ("therapeutic_areas", "therapeuticAreas"),
                ],
                source_dataset=OT_DISEASE,
                **base,
            ),
        ]
    )


def drug_claims(rules, xwalk):
    base = {"entity_type": "drug", "key_col": "entity_key", "rules": rules}
    drug_x = xwalk.filter(F.col("entity_type") == "drug")
    pharmgkb = with_entity_key(
        _bronze("pharmgkb_drugs"),
        "PharmGKB_Accession_Id",
        drug_x,
        PHARMGKB_DRUGS,
        "pharmgkb",
    )
    chembl = with_entity_key(
        _bronze("chembl_compounds"), "chembl_id", drug_x, CHEMBL, "chembl"
    )
    molecule = with_entity_key(
        _bronze("opentargets_drug_molecule"), "id", drug_x, OT_MOLECULE, "chembl"
    )
    tahoe = with_entity_key(
        _tahoe_drugs(), "tahoe_key", drug_x, TAHOE_DRUG, "tahoe_drug"
    )
    facts = [
        ("dosing_guideline", "Dosing_Guideline"),
        ("clinical_annotation_count", "Clinical_Annotation_Count"),
        ("variant_annotation_count", "Variant_Annotation_Count"),
        ("pathway_count", "Pathway_Count"),
        ("vip_count", "VIP_Count"),
        ("dosing_guideline_sources", "Dosing_Guideline_Sources"),
        ("top_clinical_annotation_level", "Top_Clinical_Annotation_Level"),
        ("top_fda_label_testing_level", "Top_FDA_Label_Testing_Level"),
        ("top_any_drug_label_testing_level", "Top_Any_Drug_Label_Testing_Level"),
        ("label_has_dosing_info", "Label_Has_Dosing_Info"),
        ("top_cpic_pairs_level", "Top_CPIC_Pairs_Level"),
        ("fda_label_has_prescribing_info", "FDA_Label_has_Prescribing_Info"),
        ("in_fda_pgx_association_sections", "In_FDA_PGx_Association_Sections"),
    ]
    return _union(
        [
            claims_frame(
                pharmgkb,
                fields=[
                    ("name", "Name"),
                    ("type", "Type"),
                    ("smiles", "SMILES"),
                    ("inchi", "InChI"),
                    ("external_vocabulary", "External_Vocabulary"),
                    *facts,
                ],
                source_dataset=PHARMGKB_DRUGS,
                **base,
            ),
            claims_frame(
                chembl,
                fields=[
                    ("smiles", "canonical_smiles"),
                    ("inchi", "standard_inchi"),
                    ("inchi_key", "standard_inchi_key"),
                ],
                source_dataset=CHEMBL,
                **base,
            ),
            claims_frame(
                molecule,
                fields=[
                    ("name", "name"),
                    ("type", "drugType"),
                    ("smiles", "canonicalSmiles"),
                    ("inchi_key", "inchiKey"),
                    ("max_clinical_stage", "maximumClinicalStage"),
                    ("description", "description"),
                    ("parent_chembl_id", "parentId"),
                    ("child_chembl_ids", "childChemblIds"),
                    ("molfile", "molblock"),
                ],
                source_dataset=OT_MOLECULE,
                **base,
            ),
            claims_frame(
                tahoe,
                fields=[
                    ("name", "drug"),
                    ("smiles", "canonical_smiles"),
                    ("targets", "targets"),
                    ("moa_broad", "moa_broad"),
                    ("moa_fine", "moa_fine"),
                    ("human_approved", "human_approved"),
                    ("clinical_trials", "clinical_trials"),
                ],
                source_dataset=TAHOE_DRUG,
                **base,
            ),
            claims_frame(
                tahoe,
                fields=[("approval_note", "gpt_notes_approval")],
                source_dataset=TAHOE_DRUG,
                source_type="llm_generated",
                **base,
            ),
        ]
    )


def small_claims(rules):
    """Claims of the entity types that have no crosswalk resolution: cell line,
    phenotype, pathway and regulatory element."""
    keyed = {"key_col": "entity_key", "rules": rules}
    cells = _bronze("tahoe_cell_line_metadata").withColumn(
        "entity_key", F.col("Cell_ID_DepMap")
    )
    terms = _bronze("hpo_terms").withColumn("entity_key", curie_expr("id"))
    pathways = _bronze("reactome_pathways").withColumn(
        "entity_key", F.col("pathway_id")
    )
    elements = _bronze("encode_ccre_registry").withColumn(
        "entity_key", F.col("ccre_accession")
    )
    return _union(
        [
            claims_frame(
                cells,
                fields=[("cell_name", "cell_name"), ("organ", "Organ")],
                entity_type="cell_line",
                source_dataset=TAHOE_CELL,
                **keyed,
            ),
            claims_frame(
                terms,
                fields=[("definition", "definition"), ("is_deprecated", "deprecated")],
                entity_type="phenotype",
                source_dataset=HPO,
                **keyed,
            ),
            claims_frame(
                pathways,
                fields=[("species", "species")],
                entity_type="pathway",
                source_dataset=REACTOME,
                **keyed,
            ),
            claims_frame(
                elements,
                fields=[("ccre_class", "ccre_class")],
                entity_type="regulatory_element",
                source_dataset=ENCODE,
                **keyed,
            ),
        ]
    )


# COMMAND ----------

# DBTITLE 1,Coordinates


def coordinate_all(rules, xwalk):
    gene_x = xwalk.filter(F.col("entity_type") == "gene")

    def number(column, dataset):
        return clean_placeholders(column, rules, f"{dataset}.{column}")

    def strand(column):
        return F.when(F.col(column).isin("+", "-"), F.col(column))

    gencode = with_entity_key(
        _gencode("gene").withColumn(
            "ensembl_clean", ensembl_unversioned_expr(F.col("gene_id"))
        ),
        "ensembl_clean",
        gene_x,
        GENCODE,
        "ensembl",
    )
    gene2refseq = (
        _human(_bronze("ncbi_gene_gene2refseq"))
        .withColumn("assembly_label", assembly_expr("assembly", rules))
        .withColumn("contig_name", number("genomic_nucleotide_accession_version", G2R))
        .withColumn(
            "start_number",
            number("start_position_on_the_genomic_accession", G2R).cast("long"),
        )
        .withColumn(
            "end_number",
            number("end_position_on_the_genomic_accession", G2R).cast("long"),
        )
        .filter(
            F.col("contig_name").isNotNull()
            & F.col("start_number").isNotNull()
            & F.col("end_number").isNotNull()
        )
    )
    genes = gene2refseq.groupBy(
        "GeneID",
        "contig_name",
        "assembly_label",
        "orientation",
        "genomic_nucleotide_gi",
    ).agg(
        F.min("start_number").alias("start_number"),
        F.max("end_number").alias("end_number"),
    )
    pharmgkb = with_entity_key(
        _bronze("pharmgkb_genes"), "PharmGKB_Accession_Id", gene_x, PHARMGKB, "pharmgkb"
    )
    opentargets = with_entity_key(
        _bronze("opentargets_target"), "id", gene_x, OPENTARGETS, "ensembl"
    ).withColumn(
        "location",
        F.from_json(
            "genomicLocation",
            "struct<chromosome:string,start:long,end:long,strand:int>",
        ),
    )
    frames = [
        coordinate_rows(
            gencode,
            entity_type="gene",
            key="entity_key",
            level="gene",
            dataset=GENCODE,
            assembly=F.lit("GRCh38"),
            contig=chromosome_expr("seqname"),
            start=F.col("start"),
            end=F.col("end"),
            strand=strand("strand"),
            system="gtf_1_based_closed",
        ),
        coordinate_rows(
            genes,
            entity_type="gene",
            key="GeneID",
            level="rna_record_span",
            dataset=G2R,
            assembly=F.col("assembly_label"),
            contig=F.col("contig_name"),
            start=F.col("start_number"),
            end=F.col("end_number"),
            strand=strand("orientation"),
            system="ncbi_1_based_closed",
            contig_gi=F.col("genomic_nucleotide_gi"),
        ),
        coordinate_rows(
            opentargets,
            entity_type="gene",
            key="entity_key",
            level="gene",
            dataset=OPENTARGETS,
            assembly=F.lit("GRCh38"),
            contig=chromosome_expr(F.col("location.chromosome")),
            start=F.col("location.start"),
            end=F.col("location.end"),
            strand=F.when(F.col("location.strand") == 1, F.lit("+")).when(
                F.col("location.strand") == -1, F.lit("-")
            ),
            system="ensembl_1_based_closed",
        ),
    ]
    for assembly, start, end in (
        ("GRCh38", "Chromosomal_Start_GRCh38", "Chromosomal_Stop_GRCh38"),
        ("GRCh37", "Chromosomal_Start_GRCh37", "Chromosomal_Stop_GRCh37"),
    ):
        frames.append(
            coordinate_rows(
                pharmgkb,
                entity_type="gene",
                key="entity_key",
                level="gene",
                dataset=PHARMGKB,
                assembly=F.lit(assembly),
                contig=chromosome_expr(number("Chromosome", PHARMGKB)),
                start=number(start, PHARMGKB),
                end=number(end, PHARMGKB),
                strand=F.lit(None).cast("string"),
                system="unstated",
            )
        )
    gencode_tx = _gencode("transcript").withColumn(
        "enst", ensembl_unversioned_expr(F.col("transcript_id"))
    )
    frames.append(
        coordinate_rows(
            gencode_tx,
            entity_type="transcript",
            key="enst",
            level="transcript",
            dataset=GENCODE,
            assembly=F.lit("GRCh38"),
            contig=chromosome_expr("seqname"),
            start=F.col("start"),
            end=F.col("end"),
            strand=strand("strand"),
            system="gtf_1_based_closed",
        )
    )
    refseq_tx = _refseq_transcripts()
    frames.append(
        coordinate_rows(
            refseq_tx,
            entity_type="transcript",
            key="transcript_key",
            level="transcript",
            dataset=REFSEQ,
            assembly=F.lit("GRCh38"),
            contig=chromosome_expr("seqname"),
            start=F.col("start"),
            end=F.col("end"),
            strand=strand("strand"),
            system="gtf_1_based_closed",
        )
    )
    records = (
        _gene2refseq(rules)
        .withColumn("assembly_label", assembly_expr("assembly", rules))
        .withColumn("contig_name", number("genomic_nucleotide_accession_version", G2R))
        .withColumn(
            "start_number",
            number("start_position_on_the_genomic_accession", G2R).cast("long"),
        )
        .withColumn(
            "end_number",
            number("end_position_on_the_genomic_accession", G2R).cast("long"),
        )
        .filter(
            F.col("rna").isNotNull()
            & F.col("contig_name").isNotNull()
            & F.col("start_number").isNotNull()
            & F.col("end_number").isNotNull()
        )
        .groupBy(
            "rna",
            "contig_name",
            "assembly_label",
            "orientation",
            "genomic_nucleotide_gi",
        )
        .agg(
            F.min("start_number").alias("start_number"),
            F.max("end_number").alias("end_number"),
        )
    )
    frames.append(
        coordinate_rows(
            records,
            entity_type="transcript",
            key="rna",
            level="rna_record",
            dataset=G2R,
            assembly=F.col("assembly_label"),
            contig=F.col("contig_name"),
            start=F.col("start_number"),
            end=F.col("end_number"),
            strand=strand("orientation"),
            system="ncbi_1_based_closed",
            contig_gi=F.col("genomic_nucleotide_gi"),
        )
    )
    frames.append(
        coordinate_rows(
            _bronze("encode_ccre_registry"),
            entity_type="regulatory_element",
            key="ccre_accession",
            level="regulatory_element",
            dataset=ENCODE,
            assembly=F.lit("GRCh38"),
            contig=chromosome_expr("chrom"),
            start=F.col("start").cast("long") + 1,
            end=F.col("end"),
            strand=F.lit(None).cast("string"),
            system="bed_0_based",
            conversion="bed_0_based_converted",
        )
    )
    return _union(frames).filter(
        F.col("contig").isNotNull()
        & F.col("start_position").isNotNull()
        & F.col("end_position").isNotNull()
    )


# COMMAND ----------


# DBTITLE 1,Claims: Open Targets nested columns
def opentargets_nested_claims(rules, xwalk):
    """Claim rows of the Open Targets nested columns, one row per element (the
    unparsed JSON stays as claims of the original fields)."""
    gene_x = xwalk.filter(F.col("entity_type") == "gene")
    target = with_entity_key(
        _bronze("opentargets_target"), "id", gene_x, OPENTARGETS, "ensembl"
    )
    none_text = F.lit(None).cast("string")
    none_number = F.lit(None).cast("double")

    def claim_rows(df, field, source_field, text=None, number=None):
        field_column = F.lit(field) if isinstance(field, str) else field
        source = F.lit(source_field) if isinstance(source_field, str) else source_field
        return df.select(
            F.lit("gene").alias("entity_type"),
            F.col("entity_key"),
            field_column.alias("field"),
            (none_text if text is None else text).alias("value_text"),
            (none_number if number is None else number).alias("value_number"),
            F.lit(None).cast("date").alias("value_date"),
            F.lit("text" if number is None else "number").alias("value_type"),
            F.lit(OPENTARGETS).alias("source_dataset"),
            source.alias("source_field"),
            F.lit(None).cast("int").alias("source_rank"),
            F.lit("source").alias("source_type"),
            F.lit(None).cast("string").alias("source_snapshot"),
        ).filter(F.col("value_text").isNotNull() | F.col("value_number").isNotNull())

    def exploded(column, schema):
        return target.select(
            "entity_key", F.explode(F.from_json(F.col(column), schema)).alias("e")
        )

    go = exploded(
        "go",
        "array<struct<id:string,source:string,evidence:string,aspect:string,"
        "geneProduct:string,ecoId:string>>",
    )
    aspect = F.col("e.aspect")
    go_field = (
        F.when(aspect == "P", "go_biological_process")
        .when(aspect == "F", "go_molecular_function")
        .when(aspect == "C", "go_cellular_component")
        .otherwise("go_term")
    )
    pathways = exploded(
        "pathways", "array<struct<pathwayId:string,pathway:string,topLevelTerm:string>>"
    )
    locations = exploded(
        "subcellularLocations",
        "array<struct<location:string,source:string,termSL:string,labelSL:string>>",
    )
    classes = exploded(
        "targetClass", "array<struct<id:string,label:string,level:string>>"
    )
    proteins = exploded("proteinIds", "array<struct<id:string,source:string>>")
    constraint = exploded(
        "constraint",
        "array<struct<constraintType:string,score:double,exp:double,obs:double,oe:double,"
        "oeLower:double,oeUpper:double,upperRank:int,upperBin:int,upperBin6:int>>",
    )
    hallmarks = target.select(
        "entity_key",
        F.from_json(
            F.col("hallmarks"),
            "struct<attributes:array<struct<pmid:string,description:string,attribute_name:string>>,"
            "cancerHallmarks:array<struct<description:string,impact:string,label:string,pmid:string>>>",
        ).alias("h"),
    )
    parts = [
        claim_rows(
            go,
            go_field,
            F.concat_ws(":", F.lit("go"), F.col("e.source")),
            text=F.col("e.id"),
        ),
        claim_rows(pathways, "reactome_pathway", "pathways", text=F.col("e.pathwayId")),
        claim_rows(pathways, "pathway_name", "pathways", text=F.col("e.pathway")),
        claim_rows(
            pathways, "pathway_top_level_term", "pathways", text=F.col("e.topLevelTerm")
        ),
        claim_rows(
            locations,
            "subcellular_location",
            F.concat_ws(":", F.lit("subcellularLocations"), F.col("e.source")),
            text=F.col("e.location"),
        ),
        claim_rows(
            classes,
            "target_class",
            F.concat_ws(":", F.lit("targetClass"), F.col("e.level")),
            text=F.col("e.label"),
        ),
        claim_rows(
            proteins,
            "protein_id",
            F.concat_ws(":", F.lit("proteinIds"), F.col("e.source")),
            text=F.col("e.id"),
        ),
        claim_rows(
            hallmarks.select("entity_key", F.explode("h.attributes").alias("a")),
            "hallmark_attribute",
            "hallmarks",
            text=F.col("a.attribute_name"),
        ),
        claim_rows(
            hallmarks.select("entity_key", F.explode("h.cancerHallmarks").alias("c")),
            "cancer_hallmark",
            "hallmarks",
            text=F.col("c.label"),
        ),
    ]
    metrics = [
        ("score", "score"),
        ("expected", "exp"),
        ("observed", "obs"),
        ("observed_over_expected", "oe"),
        ("observed_over_expected_lower", "oeLower"),
        ("observed_over_expected_upper", "oeUpper"),
        ("upper_rank", "upperRank"),
        ("upper_bin", "upperBin"),
        ("upper_bin_six", "upperBin6"),
    ]
    for name, member in metrics:
        parts.append(
            claim_rows(
                constraint,
                F.concat_ws(
                    "_",
                    F.lit("constraint"),
                    F.lower(F.col("e.constraintType")),
                    F.lit(name),
                ),
                "constraint",
                number=F.col(f"e.{member}").cast("double"),
            )
        )
    return _union(parts)


# COMMAND ----------

# DBTITLE 1,Build -- entity_id_crosswalk
# -- Transform -- ids of every entity type
crosswalk = finish_crosswalk(keep_best_match_method(crosswalk_all(RULES), RULES))

# -- Transform -- keys and provenance
crosswalk = crosswalk.withColumn(
    "_srid",
    sha_key(
        "entity_type",
        "entity_key",
        "id_type",
        "id_value",
        "source_dataset",
        "source_field",
    ),
)
crosswalk = conform_genomics(
    add_genomics_provenance(crosswalk, "_srid", RID), "entity_id_crosswalk"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- entity_id_crosswalk
write_silver(
    crosswalk, "entity_id_crosswalk", source=SOURCE, component=COMPONENT, rid=RID
)

written = spark.table(genomics_table("entity_id_crosswalk"))
blocks = inspect_table(
    written,
    "entity_id_crosswalk",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=[
        "entity_type",
        "entity_key",
        "id_type",
        "id_value",
        "source_dataset",
        "source_field",
    ],
    extra_checks={
        "entity_types": written.select("entity_type").distinct().count(),
        "methods_without_a_rank": str(
            sorted(
                row["match_method"]
                for row in written.select("match_method").distinct().collect()
                if row["match_method"] not in RULES["crosswalk_method_precedence"]
            )
        ),
        "entities_with_an_own_key": written.filter(
            F.col("match_method").startswith("own_")
        )
        .select("entity_type", "entity_key")
        .distinct()
        .count(),
        "ids_held_by_several_entities": written.filter(F.col("entities_sharing_id") > 1)
        .select("entity_type", "id_type", "id_value")
        .distinct()
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

write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__entity_id_crosswalk",
    "entity_id_crosswalk",
    blocks,
)

print(
    "STRING alias sources (add the ones that hold UniProt ids to string_alias_id_types):"
)
read_genomics_bronze("string_protein_aliases").groupBy("source").count().orderBy(
    F.desc("count")
).show(60, truncate=False)

# COMMAND ----------

# DBTITLE 1,Build -- entity_name
# -- Read Silver -- id crosswalk
xwalk = read_silver("entity_id_crosswalk")

# -- Transform -- names, sharing count and provenance
names = finish_names(names_all(RULES, xwalk)).withColumn(
    "_srid",
    sha_key(
        "entity_type",
        "entity_key",
        "term",
        "term_type",
        "source_dataset",
        "source_field",
    ),
)
names = conform_genomics(add_genomics_provenance(names, "_srid", RID), "entity_name")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- entity_name
write_silver(names, "entity_name", source=SOURCE, component=COMPONENT, rid=RID)

written = spark.table(genomics_table("entity_name"))
blocks = inspect_table(
    written,
    "entity_name",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "entity_types": written.select("entity_type").distinct().count(),
        "terms_held_by_several_entities": written.filter(
            F.col("entities_sharing_term") > 1
        )
        .select("entity_type", F.upper("term"))
        .distinct()
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

write_silver_findings(
    FINDINGS, f"{COMPONENT.split('/')[-1]}__entity_name", "entity_name", blocks
)

# COMMAND ----------

# DBTITLE 1,Build -- entity_claim
# -- Read Silver -- crosswalk and names
xwalk = read_silver("entity_id_crosswalk")
names = read_silver("entity_name")

# -- Transform -- transcript genes for the variant claims
tx_claims = transcript_claims(RULES, xwalk, names)
transcript_genes = (
    tx_claims.filter(F.col("field") == "gene_key")
    .groupBy(F.col("entity_key").alias("transcript_key"))
    .agg(
        F.countDistinct("value_text").alias("genes"),
        F.min("value_text").alias("gene_key"),
    )
    .filter(F.col("genes") == 1)
    .select("transcript_key", "gene_key")
)

# -- Transform -- unique gene ids and symbols for the PharmGKB variant genes
gene_ids = entity_id_map(xwalk, "gene").filter(F.col("id_type") == "pharmgkb")
gene_map = gene_ids.select("id_type", "id_value", "entity_key").unionByName(
    term_map(names, "gene", ["symbol", "official_symbol"], "symbol")
)

# -- Transform -- claims of every entity type (only ClinVar claims carry a snapshot)
claims = _reduce(
    lambda left, right: left.unionByName(right, allowMissingColumns=True),
    [
        gene_claims(RULES, xwalk),
        tx_claims,
        protein_claims(RULES, xwalk),
        disease_claims(RULES, xwalk),
        drug_claims(RULES, xwalk),
        small_claims(RULES),
        variant_claims(RULES, variant_id_map(xwalk), transcript_genes, gene_map),
        opentargets_nested_claims(RULES, xwalk),
    ],
)

# -- Transform -- keys and provenance
claims = claims.distinct().withColumn(
    "_srid",
    sha_key(
        "entity_type",
        "entity_key",
        "field",
        "source_dataset",
        "source_field",
        "value_text",
        "value_number",
        "source_snapshot",
    ),
)
claims = conform_genomics(add_genomics_provenance(claims, "_srid", RID), "entity_claim")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- entity_claim
write_silver(claims, "entity_claim", source=SOURCE, component=COMPONENT, rid=RID)

written = spark.table(genomics_table("entity_claim"))
blocks = inspect_table(
    written,
    "entity_claim",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "entity_types": written.select("entity_type").distinct().count(),
        "claims_without_rank": written.filter(F.col("source_rank").isNull()).count(),
        "llm_generated_claims": written.filter(
            F.col("source_type") == "llm_generated"
        ).count(),
        "claims_per_clinvar_snapshot": str(
            {
                r["source_snapshot"]: r["count"]
                for r in written.filter(F.col("source_snapshot").isNotNull())
                .groupBy("source_snapshot")
                .count()
                .collect()
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

write_silver_findings(
    FINDINGS, f"{COMPONENT.split('/')[-1]}__entity_claim", "entity_claim", blocks
)

# COMMAND ----------

# DBTITLE 1,Build -- entity_coordinate
# -- Read Silver -- id crosswalk
xwalk = read_silver("entity_id_crosswalk")

# -- Transform -- intervals, length, primary interval and provenance
# one primary interval per entity and assembly, by the coordinate source order
primary = Window.partitionBy("entity_type", "entity_key", "assembly").orderBy(
    dataset_rank_expr(RULES).asc_nulls_last(),
    F.col("source_dataset"),
    F.col("contig"),
    F.col("start_position"),
)
coordinates = (
    coordinate_all(RULES, xwalk)
    .distinct()
    .withColumn("interval_length", F.col("end_position") - F.col("start_position") + 1)
    .withColumn("is_primary", F.row_number().over(primary) == 1)
    .withColumn(
        "_srid",
        sha_key(
            "entity_type",
            "entity_key",
            "feature_level",
            "source_dataset",
            "assembly",
            "contig",
            "start_position",
            "end_position",
            "strand",
        ),
    )
)
coordinates = conform_genomics(
    add_genomics_provenance(coordinates, "_srid", RID), "entity_coordinate"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- entity_coordinate
write_silver(
    coordinates, "entity_coordinate", source=SOURCE, component=COMPONENT, rid=RID
)

written = spark.table(genomics_table("entity_coordinate"))
blocks = inspect_table(
    written,
    "entity_coordinate",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "entity_types": written.select("entity_type").distinct().count(),
        "intervals_with_start_after_end": written.filter(
            F.col("start_position") > F.col("end_position")
        ).count(),
        "genes_without_a_grch38_interval": xwalk.filter(F.col("entity_type") == "gene")
        .select("entity_key")
        .distinct()
        .join(
            written.filter(
                (F.col("entity_type") == "gene") & (F.col("assembly") == "GRCh38")
            )
            .select("entity_key")
            .distinct(),
            "entity_key",
            "left_anti",
        )
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

write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__entity_coordinate",
    "entity_coordinate",
    blocks,
)
