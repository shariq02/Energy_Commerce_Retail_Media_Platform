# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- VARIANTS AND SCORES (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** variants with their notation, conditions and classifications; the scores
# MAGIC of variants and genome positions; population frequencies and feature overlaps.

# COMMAND ----------

# DBTITLE 1,Genomics library
# MAGIC %run ../_genomics_common

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F

SOURCE = "genomics"
COMPONENT = "silver/genomics/03_variants_and_scores"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()
FRAMES = dict(variant_frames(RULES))

# COMMAND ----------

# DBTITLE 1,Build -- variant
# -- Read Silver -- claims and crosswalk
claims = read_silver("entity_claim")
xwalk = read_silver("entity_id_crosswalk")
id_map = variant_id_map(xwalk)

# -- Transform -- coordinate rows of every source
CORE = [
    "variant_key",
    "key_method",
    "v_assembly",
    "v_contig",
    "v_position",
    "v_reference",
    "v_alternate",
    "v_end",
    "v_source",
]
none = F.lit(None).cast("string")
pharmgkb = pharmgkb_variant_frame(RULES, id_map).select(
    "variant_key",
    "key_method",
    F.lit("unspecified").alias("v_assembly"),
    none.alias("v_contig"),
    none.alias("v_position"),
    none.alias("v_reference"),
    none.alias("v_alternate"),
    none.alias("v_end"),
    "v_source",
)
core = _union([frame.select(*CORE) for frame in FRAMES.values()] + [pharmgkb])


# -- Transform -- one record per variant, one boolean column per source dataset
def number(column):
    return F.when(F.col(column).rlike(r"^\d+$"), F.col(column).cast("bigint"))


records = (
    core.groupBy("variant_key")
    .agg(
        F.max(F.when(F.col("key_method") == "vcf_alleles", F.lit(1))).alias(
            "has_exact"
        ),
        F.max(F.when(F.col("v_assembly") != "unspecified", F.col("v_assembly"))).alias(
            "known_assembly"
        ),
        F.max("v_contig").alias("contig"),
        F.max(number("v_position")).alias("position"),
        F.max("v_reference").alias("reference_allele"),
        F.max("v_alternate").alias("alternate_allele"),
        F.max(number("v_end")).alias("end_position"),
        *[
            F.max(F.col("v_source") == dataset).alias(flag)
            for dataset, flag in VARIANT_SOURCE_FLAGS.items()
        ],
    )
    .withColumn("assembly", F.coalesce(F.col("known_assembly"), F.lit("unspecified")))
)

# -- Transform -- resolve the fields, master and provenance
masters, conflicts = resolve_master(
    claims, entity_type="variant", fields=VARIANT_FIELDS
)
masters = masters.withColumnRenamed("entity_key", "variant_key")
variant = (
    records.join(masters, "variant_key", "left")
    .withColumn(
        "key_source",
        F.when(
            F.col("variant_key").rlike(r"^(clinvar_allele|dbvar|pharmgkb_variant):"),
            "own_source_id",
        )
        .when(F.col("has_exact").isNotNull(), "vcf_alleles")
        .otherwise("vcf_alleles_assumed_grch38"),
    )
    .withColumn("_srid", F.col("variant_key"))
)
variant = conform_genomics(add_genomics_provenance(variant, "_srid", RID), "variant")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- variant
conflict_counts = {
    f"{row['field']}_conflicts": row["count"]
    for row in conflicts.groupBy("field").count().collect()
}
write_inspect_export(
    variant,
    "variant",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["variant_key"],
    df_before=records,
    extra_checks={
        "variants_with_an_own_key": lambda t: t.filter(
            F.col("key_source") == "own_source_id"
        ).count(),
        "variants_without_a_position": lambda t: t.filter(
            F.col("position").isNull()
        ).count(),
        "variants_without_a_gene": lambda t: t.filter(
            F.col("gene_key").isNull()
        ).count(),
        "variants_by_assembly": lambda t: str(
            {r["assembly"]: r["count"] for r in t.groupBy("assembly").count().collect()}
        ),
        **{
            f"variants_{flag}": (lambda t, flag=flag: t.filter(F.col(flag)).count())
            for flag in VARIANT_SOURCE_FLAGS.values()
        },
        **conflict_counts,
    },
    link_method_counts=True,
)

# COMMAND ----------

# DBTITLE 1,Build -- variant_hgvs
# -- Read Silver -- transcript ids
xwalk = read_silver("entity_id_crosswalk")
transcripts = entity_id_map(xwalk, "transcript")

# -- Transform -- ClinVar names
name_column = clean_placeholders("Name", RULES, "clinvar.variant_summary.Name")
parsed = (
    FRAMES[CLINVAR]
    .select("variant_key", name_column.alias("name"), "snapshot")
    .filter(F.col("name").isNotNull())
    .withColumn("parsed", hgvs_name_udf()(F.col("name")))
)
clinvar = parsed.select(
    "variant_key",
    *[F.col(f"parsed.{n}").alias(n) for n, _ in HGVS_NAME_FIELDS],
    F.lit(CLINVAR).alias("source_dataset"),
    F.lit("Name").alias("source_field"),
    F.col("snapshot").alias("source_snapshot"),
)

# -- Transform -- AlphaMissense protein changes
alpha = FRAMES[ALPHAMISSENSE].select("variant_key", "transcript_id", "protein_variant")
change = F.regexp_extract("protein_variant", r"^([A-Z*])(\d+)([A-Z*])$", 0)
alphamissense = alpha.select(
    "variant_key",
    ensembl_unversioned_expr(F.col("transcript_id")).alias("transcript_accession"),
    F.nullif(F.regexp_extract("transcript_id", r"\.(\d+)$", 1), F.lit(""))
    .cast("int")
    .alias("transcript_version"),
    F.lit(None).cast("string").alias("gene_symbol"),
    *[F.lit(None).cast(kind).alias(n) for n, kind in HGVS_NAME_FIELDS[3:13]],
    F.col("protein_variant").alias("protein_notation"),
    F.regexp_extract("protein_variant", r"^([A-Z*])(\d+)([A-Z*])$", 1).alias(
        "protein_reference_amino_acid"
    ),
    F.nullif(
        F.regexp_extract("protein_variant", r"^([A-Z*])(\d+)([A-Z*])$", 2),
        F.lit(""),
    )
    .cast("int")
    .alias("protein_position"),
    F.lit(None).cast("string").alias("protein_end_reference_amino_acid"),
    F.lit(None).cast("int").alias("protein_end_position"),
    F.when(change != "", F.lit("missense")).alias("protein_edit_type"),
    F.regexp_extract("protein_variant", r"^([A-Z*])(\d+)([A-Z*])$", 3).alias(
        "protein_alternate_amino_acid"
    ),
    F.lit(None).cast("string").alias("protein_inserted_amino_acids"),
    F.when(change != "", F.lit("ok"))
    .otherwise(F.lit("unparsed"))
    .alias("parse_status"),
    F.lit(ALPHAMISSENSE).alias("source_dataset"),
    F.lit("protein_variant").alias("source_field"),
    F.lit(None).cast("string").alias("source_snapshot"),
)

# -- Transform -- transcript links and provenance
hgvs = clinvar.unionByName(alphamissense)
hgvs = link_entities(
    hgvs,
    [
        (F.col("transcript_accession"), "refseq_rna"),
        (F.col("transcript_accession"), "ensembl_transcript"),
    ],
    transcripts,
    "transcript_key",
    "transcript_link_method",
).drop("transcript_link_method")
hgvs = hgvs.distinct().withColumn(
    "_srid",
    sha_key(
        "variant_key",
        "transcript_accession",
        "transcript_version",
        "coding_notation",
        "protein_notation",
        "source_dataset",
        "source_snapshot",
    ),
)
hgvs = conform_genomics(add_genomics_provenance(hgvs, "_srid", RID), "variant_hgvs")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- variant_hgvs
write_inspect_export(
    hgvs,
    "variant_hgvs",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "clinvar_unparsed": lambda t: t.filter(
            (F.col("source_dataset") == CLINVAR) & (F.col("parse_status") == "unparsed")
        ).count(),
        "clinvar_partial": lambda t: t.filter(
            (F.col("source_dataset") == CLINVAR) & (F.col("parse_status") == "partial")
        ).count(),
        "rows_without_a_transcript": lambda t: t.filter(
            F.col("transcript_key").isNull()
        ).count(),
        "edit_types": lambda t: str(
            {
                r["protein_edit_type"]: r["count"]
                for r in t.groupBy("protein_edit_type").count().collect()
            }
        ),
    },
    link_method_counts=True,
)

# COMMAND ----------

# DBTITLE 1,Build -- variant_condition
# -- Read Silver -- disease ids
xwalk = read_silver("entity_id_crosswalk")
disease_map = entity_id_map(xwalk, "disease")

# -- Transform -- pair ids, names and accessions
DATASET = "clinvar.variant_summary"
text = text_for(DATASET)
parsed = FRAMES[CLINVAR].select(
    "variant_key",
    F.col("snapshot").alias("source_snapshot"),
    F.explode(
        clinvar_conditions_udf()(
            text("PhenotypeIDS"), text("PhenotypeList"), text("RCVaccession")
        )
    ).alias("c"),
)

# -- Transform -- one row per condition id (a condition without an id keeps one row)
conditions = parsed.select(
    "variant_key",
    "source_snapshot",
    F.col("c.position").alias("condition_position"),
    F.col("c.name").alias("condition_name"),
    F.col("c.accession").alias("clinvar_condition_accession"),
    F.explode_outer("c.ids").alias("condition_id"),
    F.col("c.pairing_status").alias("pairing_status"),
).distinct()

# -- Transform -- disease links through the exact id
id_type, id_value = split_curie("condition_id", RULES)
condition = (
    conditions.withColumn("id_type", id_type)
    .withColumn("id_value", id_value)
    .join(disease_map, ["id_type", "id_value"], "left")
    .withColumnRenamed("entity_key", "disease_key")
    .withColumn(
        "disease_link_method",
        F.when(F.col("disease_key").isNotNull(), "ids")
        .when(F.col("condition_id").isNull(), "no_id")
        .otherwise("unlinked"),
    )
    .withColumn(
        "is_generic_label",
        F.lower(F.trim(F.col("condition_name"))).isin(
            RULES["generic_condition_labels"]
        ),
    )
    .withColumn("source_dataset", F.lit(CLINVAR))
    .withColumn(
        "_srid",
        sha_key(
            "variant_key",
            "source_snapshot",
            "condition_position",
            "condition_id",
            "clinvar_condition_accession",
            "condition_name",
        ),
    )
)
condition = conform_genomics(
    add_genomics_provenance(condition, "_srid", RID), "variant_condition"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- variant_condition
write_inspect_export(
    condition,
    "variant_condition",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "pairing_mismatches": lambda t: t.filter(
            F.col("pairing_status") == "count_mismatch"
        ).count(),
        "conditions_without_a_disease_key": lambda t: t.filter(
            F.col("disease_key").isNull()
        ).count(),
        "generic_labels": lambda t: t.filter(F.col("is_generic_label")).count(),
    },
    link_method_counts=True,
)

# COMMAND ----------

# DBTITLE 1,Build -- variant_classification
# -- Transform -- the three classification types
DATASET = "clinvar.variant_summary"
clinvar = FRAMES[CLINVAR]
stars = F.create_map(
    *[F.lit(x) for pair in RULES["clinvar_review_stars"].items() for x in pair]
)
text = text_for(DATASET)


def date_of(column):
    return F.expr(f"cast(try_to_timestamp({column}, 'MMM dd, yyyy') as date)")


TYPES = [
    (
        "germline",
        "ClinicalSignificance",
        "ReviewStatus",
        "LastEvaluated",
        "SCVsForAggregateGermlineClassification",
    ),
    (
        "somatic_clinical_impact",
        "SomaticClinicalImpact",
        "ReviewStatusClinicalImpact",
        "SomaticClinicalImpactLastEvaluated",
        "SCVsForAggregateSomaticClinicalImpact",
    ),
    (
        "oncogenicity",
        "Oncogenicity",
        "ReviewStatusOncogenicity",
        "OncogenicityLastEvaluated",
        "SCVsForAggregateOncogenicityClassification",
    ),
]
parts = []
for kind, term, review, evaluated, scv in TYPES:
    germline = kind == "germline"
    parts.append(
        clinvar.select(
            "variant_key",
            F.lit(kind).alias("classification_type"),
            text(term).alias("term"),
            text(review).alias("review_status"),
            F.try_element_at(stars, F.lower(text(review))).alias("review_stars"),
            date_of(evaluated).alias("last_evaluated"),
            (
                typed_number(
                    "NumberSubmitters", "int", RULES, f"{DATASET}.NumberSubmitters"
                )
                if germline
                else F.lit(None).cast("int")
            ).alias("number_submitters"),
            (text("ClinSigSimple") if germline else F.lit(None).cast("string")).alias(
                "clinical_significance_simple"
            ),
            F.explode_outer(split_list(text(scv), r"[|,;]")).alias(
                "submission_accession"
            ),
            F.lit(CLINVAR).alias("source_dataset"),
            F.col("snapshot").alias("source_snapshot"),
        ).filter(F.col("term").isNotNull())
    )
classification = _union(parts)

# -- Transform -- keys and provenance
classification = classification.distinct().withColumn(
    "_srid",
    sha_key(
        "variant_key",
        "source_snapshot",
        "classification_type",
        "term",
        "review_status",
        "last_evaluated",
        "submission_accession",
    ),
)
classification = conform_genomics(
    add_genomics_provenance(classification, "_srid", RID), "variant_classification"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- variant_classification
write_inspect_export(
    classification,
    "variant_classification",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "types": lambda t: str(
            {
                r["classification_type"]: r["count"]
                for r in t.groupBy("classification_type").count().collect()
            }
        ),
        "review_statuses_without_stars": lambda t: t.filter(
            F.col("review_status").isNotNull() & F.col("review_stars").isNull()
        ).count(),
        "rows_without_a_date": lambda t: t.filter(
            F.col("last_evaluated").isNull()
        ).count(),
        "rows_without_a_submission": lambda t: t.filter(
            F.col("submission_accession").isNull()
        ).count(),
    },
    link_method_counts=True,
)

# COMMAND ----------

# DBTITLE 1,Build -- locus_score
# -- Transform -- scores of a variant (variant_key set, no position columns)
number = source_double
none = F.lit(None).cast("string")
no_position = [
    none.alias("assembly"),
    none.alias("contig"),
    F.lit(None).cast("bigint").alias("position"),
]
alpha = FRAMES[ALPHAMISSENSE].select(
    "variant_key",
    *no_position,
    F.lit("alphamissense_pathogenicity").alias("score_name"),
    number("am_pathogenicity", ALPHAMISSENSE).alias("score_value"),
    clean_placeholders("am_class", RULES, f"{ALPHAMISSENSE}.am_class").alias(
        "score_label"
    ),
    clean_placeholders("transcript_id", RULES, f"{ALPHAMISSENSE}.transcript_id").alias(
        "context"
    ),
    F.lit(ALPHAMISSENSE).alias("source_dataset"),
    F.lit("am_pathogenicity").alias("source_field"),
)
cadd = FRAMES[CADD].select(
    "variant_key",
    *no_position,
    F.lit("cadd_phred").alias("score_name"),
    number("phred", CADD).alias("score_value"),
    none.alias("score_label"),
    none.alias("context"),
    F.lit(CADD).alias("source_dataset"),
    F.lit("phred").alias("source_field"),
)
gpn = [
    FRAMES[CLINVAR_BENIGN].select(
        "variant_key",
        *no_position,
        F.lit(column.lower()).alias("score_name"),
        number(column, CLINVAR_BENIGN).alias("score_value"),
        none.alias("score_label"),
        none.alias("context"),
        F.lit(CLINVAR_BENIGN).alias("source_dataset"),
        F.lit(column).alias("source_field"),
    )
    for column in ("GPN_Star_V100", "GPN_Star_P243", "GPN_Star_M447")
]

# -- Transform -- scores of a genome position (no variant key)
POSITION_SOURCES = [
    (
        "ucsc_conservation_phylop_100way",
        "ucsc_conservation.phylop_100way",
        "phylop_100way",
    ),
    (
        "ucsc_conservation_phastcons_100way",
        "ucsc_conservation.phastcons_100way",
        "phastcons_100way",
    ),
]
positions = [
    read_genomics_bronze(table).select(
        F.lit(None).cast("string").alias("variant_key"),
        F.lit("unspecified").alias("assembly"),
        chromosome_expr(
            clean_placeholders("chromosome", RULES, f"{dataset}.chromosome")
        ).alias("contig"),
        typed_number("position", "bigint", RULES, f"{dataset}.position").alias(
            "position"
        ),
        F.lit(name).alias("score_name"),
        typed_number("score", "double", RULES, f"{dataset}.score").alias("score_value"),
        none.alias("score_label"),
        none.alias("context"),
        F.lit(dataset).alias("source_dataset"),
        F.lit("score").alias("source_field"),
    )
    for table, dataset, name in POSITION_SOURCES
]

# -- Transform -- union, keys and provenance
scores = (
    _union([alpha, cadd, *gpn])
    .filter(F.col("score_value").isNotNull())
    .unionByName(
        _union(positions).filter(
            F.col("contig").isNotNull()
            & F.col("position").isNotNull()
            & F.col("score_value").isNotNull()
        )
    )
    .distinct()
    .withColumn(
        "_srid",
        sha_key(
            "variant_key",
            "assembly",
            "contig",
            "position",
            "score_name",
            "source_dataset",
            "context",
            "score_value",
        ),
    )
)
scores = conform_genomics(add_genomics_provenance(scores, "_srid", RID), "locus_score")

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- locus_score
write_inspect_export(
    scores,
    "locus_score",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "variant_scores": lambda t: t.filter(F.col("variant_key").isNotNull()).count(),
        "position_scores": lambda t: t.filter(F.col("variant_key").isNull()).count(),
        "scores": lambda t: str(
            {
                r["score_name"]: r["count"]
                for r in t.groupBy("score_name").count().collect()
            }
        ),
        "contigs": lambda t: (
            t.filter(F.col("variant_key").isNull()).select("contig").distinct().count()
        ),
    },
    link_method_counts=True,
)

# COMMAND ----------

# DBTITLE 1,Build -- variant_population_frequency
# -- Transform -- long rows per population
gnomad = FRAMES[GNOMAD]
none_long = F.lit(None).cast("bigint")
number = number_for(GNOMAD)
status = clean_placeholders("gnomad_filters", RULES, f"{GNOMAD}.gnomad_filters")
POPULATIONS = [
    "afr",
    "ami",
    "amr",
    "asj",
    "eas",
    "fin",
    "mid",
    "nfe",
    "sas",
    "remaining",
]


def rows(
    population,
    measure,
    frequency,
    count=none_long,
    total=none_long,
    homozygotes=none_long,
):
    return gnomad.select(
        "variant_key",
        population.alias("population"),
        F.lit(measure).alias("measure"),
        frequency.alias("allele_frequency"),
        count.alias("allele_count"),
        total.alias("allele_number"),
        homozygotes.alias("homozygote_count"),
        status.alias("filter_status"),
        F.lit(GNOMAD).alias("source_dataset"),
    )


group = clean_placeholders("gnomad_grpmax", RULES, f"{GNOMAD}.gnomad_grpmax")
parts = [
    rows(
        F.lit("all"),
        "allele_frequency",
        number("gnomad_af"),
        number("gnomad_ac", "bigint"),
        number("gnomad_an", "bigint"),
        number("gnomad_nhomalt", "bigint"),
    )
]
parts += [
    rows(F.lit(pop), "allele_frequency", number(f"gnomad_af_{pop}"))
    for pop in POPULATIONS
]
parts += [
    rows(group, "highest_group_allele_frequency", number("gnomad_af_grpmax")),
    rows(group, "filtering_allele_frequency_95", number("gnomad_faf95_max")),
    rows(
        F.lit("max_ancestry"),
        "highest_ancestry_allele_frequency",
        number("gnomad_af_max_ancestry"),
    ),
]
frequency = _union(parts).filter(
    F.col("allele_frequency").isNotNull() & F.col("population").isNotNull()
)

# -- Transform -- keys and provenance
frequency = frequency.distinct().withColumn(
    "_srid", sha_key("variant_key", "population", "measure", "source_dataset")
)
frequency = conform_genomics(
    add_genomics_provenance(frequency, "_srid", RID), "variant_population_frequency"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- variant_population_frequency
write_inspect_export(
    frequency,
    "variant_population_frequency",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "measures": lambda t: str(
            {r["measure"]: r["count"] for r in t.groupBy("measure").count().collect()}
        ),
        "populations": lambda t: t.select("population").distinct().count(),
        "frequencies_above_one": lambda t: t.filter(
            F.col("allele_frequency") > 1
        ).count(),
    },
    link_method_counts=True,
)

# COMMAND ----------

# DBTITLE 1,Build -- variant_feature_overlap
# -- Read Silver -- variants, intervals, protein positions
BIN = 100000
variants = (
    read_silver("variant")
    .filter(
        (F.col("assembly") == "GRCh38")
        & F.col("position").isNotNull()
        & F.col("contig").isNotNull()
    )
    .select(
        "variant_key",
        "contig",
        F.col("position").alias("start_position"),
        (
            F.col("position") + F.greatest(F.length("reference_allele"), F.lit(1)) - 1
        ).alias("end_position"),
    )
    .withColumn("bin", F.floor(F.col("start_position") / BIN))
)
coordinates = read_silver("entity_coordinate").filter(
    (F.col("assembly") == "GRCh38") & F.col("is_primary")
)
master = read_silver("entity_master")
gene_types = master.filter(F.col("entity_type") == "gene").select(
    F.col("entity_key").alias("gene_key"), F.col("gene_type").alias("feature_type")
)
transcript_types = master.filter(F.col("entity_type") == "transcript").select(
    F.col("entity_key").alias("transcript_key"),
    F.col("transcript_type").alias("feature_type"),
)
element_types = (
    read_silver("entity_claim")
    .filter(
        (F.col("entity_type") == "regulatory_element")
        & (F.col("field") == "ccre_class")
    )
    .select(
        F.col("entity_key").alias("element_key"),
        F.col("value_text").alias("feature_type"),
    )
    .distinct()
)


# -- Transform -- interval overlaps (binned range join)
def features_of(entity_type, types, key_column):
    frame = coordinates.filter(F.col("entity_type") == entity_type).select(
        F.col("entity_key").alias("feature_key"),
        F.col("contig").alias("f_contig"),
        F.col("start_position").alias("f_start"),
        F.col("end_position").alias("f_end"),
    )
    if types is None:
        return frame.withColumn("feature_type", F.lit(None).cast("string"))
    return frame.join(types, frame["feature_key"] == types[key_column], "left").drop(
        key_column
    )


def overlaps(features, kind, source):
    spread = features.withColumn(
        "bin",
        F.explode(
            F.sequence(F.floor(F.col("f_start") / BIN), F.floor(F.col("f_end") / BIN))
        ),
    )
    joined = variants.join(
        spread,
        (variants["contig"] == spread["f_contig"])
        & (variants["bin"] == spread["bin"])
        & (variants["start_position"] <= spread["f_end"])
        & (variants["end_position"] >= spread["f_start"]),
    )
    return joined.select(
        "variant_key",
        F.lit(kind).alias("feature_kind"),
        "feature_key",
        "feature_type",
        F.lit("interval").alias("overlap_method"),
        F.lit(source).alias("source_dataset"),
    ).distinct()


parts = [
    overlaps(features_of("gene", gene_types, "gene_key"), "gene", "entity_coordinate"),
    overlaps(
        features_of("transcript", transcript_types, "transcript_key"),
        "transcript",
        "entity_coordinate",
    ),
    overlaps(
        features_of("regulatory_element", element_types, "element_key"),
        "regulatory_element",
        "entity_coordinate",
    ),
]

# -- Transform -- exon, CDS and UTR rows (when Bronze holds them)
GENCODE = "gencode.annotation"
structure = read_genomics_bronze("gencode_annotation").filter(
    F.col("feature").isin("exon", "CDS", "UTR")
)
structure_features = structure.select(
    ensembl_unversioned_expr(F.col("transcript_id")).alias("feature_key"),
    chromosome_expr("seqname").alias("f_contig"),
    typed_number("start", "bigint", RULES, f"{GENCODE}.start").alias("f_start"),
    typed_number("end", "bigint", RULES, f"{GENCODE}.end").alias("f_end"),
    F.col("feature").alias("feature_type"),
).filter(F.col("f_start").isNotNull() & F.col("f_end").isNotNull())
for kind in ("exon", "CDS", "UTR"):
    parts.append(
        overlaps(
            structure_features.filter(F.col("feature_type") == kind),
            kind.lower(),
            GENCODE,
        )
    )

# -- Transform -- protein features by protein position
hgvs = (
    current_snapshot(read_silver("variant_hgvs"), RULES)
    .filter(F.col("protein_position").isNotNull() & F.col("transcript_key").isNotNull())
    .select("variant_key", "transcript_key", "protein_position")
)
proteins = master.filter(
    (F.col("entity_type") == "transcript") & F.col("protein_key").isNotNull()
).select(F.col("entity_key").alias("transcript_key"), "protein_key")
protein_features = (
    read_silver("protein_feature")
    .select(
        "protein_key",
        F.col("feature_id").alias("feature_key"),
        "feature_type",
        "start_position",
        "end_position",
    )
    .distinct()
)
parts.append(
    hgvs.join(proteins, "transcript_key")
    .join(protein_features, "protein_key")
    .filter(
        (F.col("start_position") <= F.col("protein_position"))
        & (F.col("end_position") >= F.col("protein_position"))
    )
    .select(
        "variant_key",
        F.lit("protein_feature").alias("feature_kind"),
        "feature_key",
        "feature_type",
        F.lit("protein_position").alias("overlap_method"),
        F.lit("protein_feature").alias("source_dataset"),
    )
    .distinct()
)

# -- Transform -- union and provenance
overlap = _union(parts).withColumn(
    "_srid", sha_key("variant_key", "feature_kind", "feature_key", "overlap_method")
)
overlap = conform_genomics(
    add_genomics_provenance(overlap, "_srid", RID), "variant_feature_overlap"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- variant_feature_overlap
write_inspect_export(
    overlap,
    "variant_feature_overlap",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    extra_checks={
        "kinds": lambda t: str(
            {
                r["feature_kind"]: r["count"]
                for r in t.groupBy("feature_kind").count().collect()
            }
        ),
        "exon_rows_in_bronze": structure.count(),
    },
    link_method_counts=True,
)
