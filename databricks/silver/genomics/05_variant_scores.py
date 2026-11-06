# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- VARIANT SCORES (GENOMICS)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** variant scores, position scores, population frequencies and feature
# MAGIC overlaps.

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
COMPONENT = "silver/genomics/05_variant_scores"
RID = run_id()
FINDINGS = "genomics"
RULES = load_genomics_rules()

# COMMAND ----------

# DBTITLE 1,Build -- variant_score
# -- Transform -- scores of every source
frames = dict(variant_frames(RULES))
none = F.lit(None).cast("string")

number = source_double

alpha = frames[ALPHAMISSENSE].select(
    "variant_key",
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
cadd = frames[CADD].select(
    "variant_key",
    F.lit("cadd_phred").alias("score_name"),
    number("phred", CADD).alias("score_value"),
    none.alias("score_label"),
    none.alias("context"),
    F.lit(CADD).alias("source_dataset"),
    F.lit("phred").alias("source_field"),
)
gpn = [
    frames[CLINVAR_BENIGN].select(
        "variant_key",
        F.lit(column.lower()).alias("score_name"),
        number(column, CLINVAR_BENIGN).alias("score_value"),
        none.alias("score_label"),
        none.alias("context"),
        F.lit(CLINVAR_BENIGN).alias("source_dataset"),
        F.lit(column).alias("source_field"),
    )
    for column in ("GPN_Star_V100", "GPN_Star_P243", "GPN_Star_M447")
]
scores = _union([alpha, cadd, *gpn]).filter(F.col("score_value").isNotNull())

# -- Transform -- keys and provenance
scores = scores.distinct().withColumn(
    "_srid",
    sha_key("variant_key", "score_name", "source_dataset", "context", "score_value"),
)
scores = conform_genomics(
    add_genomics_provenance(scores, "_srid", RID), "variant_score"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- variant_score
write_silver(scores, "variant_score", source=SOURCE, component=COMPONENT, rid=RID)

written = spark.table(genomics_table("variant_score"))
blocks = inspect_table(
    written,
    "variant_score",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "scores": str(
            {
                r["score_name"]: r["count"]
                for r in written.groupBy("score_name").count().collect()
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
    FINDINGS, f"{COMPONENT.split('/')[-1]}__variant_score", "variant_score", blocks
)

# COMMAND ----------

# DBTITLE 1,Build -- position_score
# -- Transform -- both conservation scores
SOURCES = [
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
parts = []
for table, dataset, name in SOURCES:
    parts.append(
        read_genomics_bronze(table).select(
            F.lit("unspecified").alias("assembly"),
            chromosome_expr(
                clean_placeholders("chromosome", RULES, f"{dataset}.chromosome")
            ).alias("contig"),
            typed_number("position", "bigint", RULES, f"{dataset}.position").alias(
                "position"
            ),
            F.lit(name).alias("score_name"),
            typed_number("score", "double", RULES, f"{dataset}.score").alias(
                "score_value"
            ),
            F.lit(dataset).alias("source_dataset"),
        )
    )
position = _union(parts).filter(
    F.col("contig").isNotNull()
    & F.col("position").isNotNull()
    & F.col("score_value").isNotNull()
)

# -- Transform -- keys and provenance
position = position.distinct().withColumn(
    "_srid", sha_key("assembly", "contig", "position", "score_name")
)
position = conform_genomics(
    add_genomics_provenance(position, "_srid", RID), "position_score"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- position_score
write_silver(position, "position_score", source=SOURCE, component=COMPONENT, rid=RID)

written = spark.table(genomics_table("position_score"))
blocks = inspect_table(
    written,
    "position_score",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "scores": str(
            {
                r["score_name"]: r["count"]
                for r in written.groupBy("score_name").count().collect()
            }
        ),
        "contigs": written.select("contig").distinct().count(),
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
    f"{COMPONENT.split('/')[-1]}__position_score",
    "position_score",
    blocks,
)

# COMMAND ----------

# DBTITLE 1,Build -- variant_population_frequency
# -- Transform -- long rows per population
frames = dict(variant_frames(RULES))
gnomad = frames[GNOMAD]
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
write_silver(
    frequency,
    "variant_population_frequency",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

written = spark.table(genomics_table("variant_population_frequency"))
blocks = inspect_table(
    written,
    "variant_population_frequency",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "measures": str(
            {
                r["measure"]: r["count"]
                for r in written.groupBy("measure").count().collect()
            }
        ),
        "populations": written.select("population").distinct().count(),
        "frequencies_above_one": written.filter(F.col("allele_frequency") > 1).count(),
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
    f"{COMPONENT.split('/')[-1]}__variant_population_frequency",
    "variant_population_frequency",
    blocks,
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
gene_types = read_silver("gene").select(
    "gene_key", F.col("gene_type").alias("feature_type")
)
transcript_types = read_silver("transcript").select(
    "transcript_key", F.col("transcript_type").alias("feature_type")
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


gene_overlap = overlaps(
    features_of("gene", gene_types, "gene_key"), "gene", "entity_coordinate"
)
transcript_overlap = overlaps(
    features_of("transcript", transcript_types, "transcript_key"),
    "transcript",
    "entity_coordinate",
)
element_overlap = overlaps(
    features_of("regulatory_element", element_types, "element_key"),
    "regulatory_element",
    "entity_coordinate",
)
parts = [gene_overlap, transcript_overlap, element_overlap]

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
proteins = (
    read_silver("transcript")
    .filter(F.col("protein_key").isNotNull())
    .select("transcript_key", "protein_key")
)
protein_features = read_silver("protein_feature").select(
    "protein_key",
    F.col("source_record_id").alias("feature_key"),
    "feature_type",
    "start_position",
    "end_position",
)
protein_overlap = (
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
parts.append(protein_overlap)

# -- Transform -- union and provenance
overlap = _union(parts).withColumn(
    "_srid", sha_key("variant_key", "feature_kind", "feature_key", "overlap_method")
)
overlap = conform_genomics(
    add_genomics_provenance(overlap, "_srid", RID), "variant_feature_overlap"
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- variant_feature_overlap
write_silver(
    overlap, "variant_feature_overlap", source=SOURCE, component=COMPONENT, rid=RID
)

written = spark.table(genomics_table("variant_feature_overlap"))
blocks = inspect_table(
    written,
    "variant_feature_overlap",
    source=FINDINGS,
    component=COMPONENT,
    rid=RID,
    key_cols=["source_record_id"],
    extra_checks={
        "kinds": str(
            {
                r["feature_kind"]: r["count"]
                for r in written.groupBy("feature_kind").count().collect()
            }
        ),
        "exon_rows_in_bronze": structure.count(),
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
    f"{COMPONENT.split('/')[-1]}__variant_feature_overlap",
    "variant_feature_overlap",
    blocks,
)
