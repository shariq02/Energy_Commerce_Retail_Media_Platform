# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GENOMICS SILVER ENTITY BUILDERS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** per entity type, the crosswalk, name, claim and coordinate rows
# MAGIC built from Bronze. Pulled in with `%run` after `_genomics_common`.

# COMMAND ----------

# DBTITLE 1,Basics: Bronze reads and unions


def _bronze(table):
    return read_genomics_bronze(table)


def _union(frames):
    return _reduce(lambda left, right: left.unionByName(right), frames)


def genetic_test(rules=None):
    """GTR rows with `test_key` (the accession without its version)."""
    return _bronze("gtr_test_condition_gene").withColumn(
        "test_key", F.regexp_replace(F.trim(F.col("accession_version")), r"\.\d+$", "")
    )


# COMMAND ----------

# DBTITLE 1,Derived attribute rows


def derived_rows(
    df,
    *,
    entity_type,
    key,
    attribute,
    rule_name,
    method,
    version,
    as_of,
    value_text=None,
    value_number=None,
    value_boolean=None,
    matched=None,
    text_source=None,
):
    """Rows in the column order of `entity_derived_attribute`; `attribute`,
    `value_*` and `matched` are Columns or texts, `key` is a column name."""

    def column(value, kind):
        return (
            (F.lit(value) if isinstance(value, str) else value)
            if value is not None
            else F.lit(None).cast(kind)
        )

    if value_boolean is not None:
        value_type = "boolean"
    elif value_number is not None:
        value_type = "number"
    else:
        value_type = "text"
    return df.select(
        F.lit(entity_type).alias("entity_type"),
        F.col(key).cast("string").alias("entity_key"),
        column(attribute, "string").alias("attribute"),
        column(value_text, "string").alias("value_text"),
        column(value_number, "double").cast("double").alias("value_number"),
        column(value_boolean, "boolean").alias("value_boolean"),
        F.lit(value_type).alias("value_type"),
        F.lit(method).alias("method"),
        column(matched, "string").alias("matched_text"),
        F.lit(text_source).cast("string").alias("text_source"),
        F.lit(rule_name).alias("rule_name"),
        F.lit(version).alias("rule_version"),
        as_of.alias("as_of_date"),
    )


def keyword_rows(
    df,
    *,
    entity_type,
    key,
    text,
    group,
    rules,
    text_source,
    version,
    as_of,
    patterns=None,
    suffix=None,
):
    """One true-flag row per entity and keyword rule that matches the text column;
    the first match is kept in `matched_text`. The rules are `patterns` (a dict) or
    the text rules of `group`; a `suffix` text is added to each attribute name."""
    parts = []
    for attribute, pattern in (patterns or rules["text_rules"][group]).items():
        hit = df.filter(F.col(text).rlike("(?i)" + pattern))
        parts.append(
            derived_rows(
                hit,
                entity_type=entity_type,
                key=key,
                attribute=attribute + (suffix or ""),
                rule_name=group,
                method="keyword",
                version=version,
                as_of=as_of,
                value_boolean=F.lit(True),
                matched=F.regexp_extract(F.col(text), "(?i)(" + pattern + ")", 1),
                text_source=text_source,
            )
        )
    return _union(parts)


def captured_rows(
    df, *, entity_type, key, text, group, rules, text_source, version, as_of, numbers=()
):
    """One row per entity and capture rule of `group`: the first capture group
    of the first match, as text or (for `numbers`) as a number."""
    parts = []
    for attribute, pattern in rules["text_rules"][group].items():
        regex = "(?i)" + pattern
        value = F.nullif(F.regexp_extract(F.col(text), regex, 1), F.lit(""))
        hit = df.filter(F.col(text).rlike(regex)).withColumn("_captured", value)
        hit = hit.filter(F.col("_captured").isNotNull())
        extra = (
            {"value_number": F.col("_captured").cast("double")}
            if attribute in numbers
            else {"value_text": F.col("_captured")}
        )
        parts.append(
            derived_rows(
                hit,
                entity_type=entity_type,
                key=key,
                attribute=attribute,
                rule_name=group,
                method="keyword",
                version=version,
                as_of=as_of,
                matched=F.regexp_extract(F.col(text), regex, 0),
                text_source=text_source,
                **extra,
            )
        )
    return _union(parts)


def first_match_expr(entries, flags, value_key, default):
    """Column expr: the `value_key` of the first rule whose `all` flags are in
    the array column `flags` and whose `none` flags are not; else `default`."""
    expr = F.lit(default)
    for entry in reversed(entries):
        condition = F.lit(True)
        for name in entry.get("all", []):
            condition = condition & F.array_contains(flags, name)
        for name in entry.get("none", []):
            condition = condition & ~F.array_contains(flags, name)
        expr = F.when(condition, F.lit(entry[value_key])).otherwise(expr)
    return expr


# COMMAND ----------

# DBTITLE 1,Variants: datasets
CLINVAR = "clinvar.variant_summary"
GNOMAD = "gnomad.variants"
ALPHAMISSENSE = "alphamissense.variant_scores"
CADD = "cadd.scores"
DBVAR = "dbvar.variants"
PHARMGKB_VARIANTS = "pharmgkb.variants"
LR_CODING = "long_range_benchmark.pathogenic_coding"
LR_NON_CODING = "long_range_benchmark.pathogenic_non_coding"
LR_EQTL = "long_range_benchmark.causal_eqtl"
CLINVAR_BENIGN = "clinvar_vs_benign.variants"
GNOMAD_BALANCED = "gnomad_balanced.variants"

# COMMAND ----------

# DBTITLE 1,Variants: one frame per source, with its key and ids


ID_ARRAY = "array<struct<id_type:string,id_value:string,source_field:string>>"


def _clean(name, rules, dataset):
    return clean_placeholders(name, rules, f"{dataset}.{name}")


def _id_item(id_type, value, field):
    return F.struct(
        F.lit(id_type).alias("id_type"),
        value.cast("string").alias("id_value"),
        F.lit(field).alias("source_field"),
    )


def _variant_frame(
    df, *, dataset, assembly, chrom, pos, ref, alt, end, ids, method, own=None
):
    """Add the common variant columns to a source frame; the source columns stay.
    `own` is the key of a row without usable alleles (else the key stays null)."""
    ids_column = F.array(*ids) if ids else F.array().cast(ID_ARRAY)
    key = variant_key_expr(assembly, chrom, pos, ref, alt)
    how = F.lit(method)
    if own is not None:
        how = F.when(key.isNotNull(), how).otherwise(F.lit("own_source_id"))
        key = F.coalesce(key, own)
    return (
        df.withColumn("variant_key", key)
        .withColumn("key_method", how)
        .withColumn(
            "v_assembly", assembly if not isinstance(assembly, str) else F.lit(assembly)
        )
        .withColumn("v_contig", chromosome_expr(chrom))
        .withColumn("v_position", pos)
        .withColumn("v_reference", F.upper(F.trim(ref)))
        .withColumn("v_alternate", F.upper(F.trim(alt)))
        .withColumn("v_end", end)
        .withColumn("v_source", F.lit(dataset))
        .withColumn(
            "v_ids", F.filter(ids_column, lambda item: item["id_value"].isNotNull())
        )
        .filter(F.col("variant_key").isNotNull())
    )


def _rsid(column, rules, dataset):
    digits = _clean(column, rules, dataset)
    return F.when(digits.rlike(r"^\d+$"), F.concat(F.lit("rs"), digits))


def clinvar_frame(rules):
    d = CLINVAR
    assembly = F.coalesce(_clean("Assembly", rules, d), F.lit("unspecified"))
    frame = _bronze("clinvar_variant_summary")
    return _variant_frame(
        frame.withColumn("RS_dbSNP", _rsid("RS_dbSNP", rules, d)).withColumn(
            "GeneID",
            F.when(
                _clean("GeneID", rules, d).rlike(r"^\d+$"), _clean("GeneID", rules, d)
            ),
        ),
        dataset=d,
        assembly=assembly,
        chrom=_clean("Chromosome", rules, d),
        pos=F.coalesce(_clean("PositionVCF", rules, d), _clean("Start", rules, d)),
        ref=_clean("ReferenceAlleleVCF", rules, d),
        alt=_clean("AlternateAlleleVCF", rules, d),
        end=_clean("Stop", rules, d),
        ids=[
            _id_item("clinvar_allele", F.col("AlleleID"), "AlleleID"),
            _id_item("clinvar_variation", F.col("VariationID"), "VariationID"),
            _id_item("rsid", F.col("RS_dbSNP"), "RS_dbSNP"),
            _id_item("dbvar", _clean("nsv_esv_dbVar", rules, d), "nsv_esv_dbVar"),
        ],
        method="vcf_alleles",
        own=F.concat(F.lit("clinvar_allele:"), F.col("AlleleID"), F.lit(":"), assembly),
    )


def _vcf_frame(
    table,
    dataset,
    rules,
    *,
    chrom,
    pos,
    ref,
    alt,
    assembly="GRCh38",
    method="vcf_alleles_assumed_grch38",
    ids=(),
):
    return _variant_frame(
        _bronze(table),
        dataset=dataset,
        assembly=assembly,
        chrom=_clean(chrom, rules, dataset),
        pos=_clean(pos, rules, dataset),
        ref=_clean(ref, rules, dataset),
        alt=_clean(alt, rules, dataset),
        end=F.lit(None).cast("string"),
        ids=list(ids),
        method=method,
    )


def alphamissense_frame(rules):
    d = ALPHAMISSENSE
    names = rules["ucsc_assembly_names"]
    mapped = F.lit(None).cast("string")
    for label, canonical in names.items():
        mapped = F.when(
            F.lower(F.trim(F.col("genome"))) == label, F.lit(canonical)
        ).otherwise(mapped)
    assembly = F.coalesce(mapped, _clean("genome", rules, d), F.lit("unspecified"))
    return _variant_frame(
        _bronze("alphamissense_variant_scores"),
        dataset=d,
        assembly=assembly,
        chrom=_clean("CHROM", rules, d),
        pos=_clean("POS", rules, d),
        ref=_clean("REF", rules, d),
        alt=_clean("ALT", rules, d),
        end=F.lit(None).cast("string"),
        ids=[],
        method="vcf_alleles",
    )


def dbvar_frame(rules):
    d = DBVAR
    own = F.concat(F.lit("dbvar:"), F.col("study"), F.lit(":"), F.col("ID"))
    return _variant_frame(
        _bronze("dbvar_variants"),
        dataset=d,
        assembly=F.lit("unspecified"),
        chrom=_clean("CHROM", rules, d),
        pos=_clean("POS", rules, d),
        ref=F.lit(None).cast("string"),
        alt=F.lit(None).cast("string"),
        end=_clean("END", rules, d),
        ids=[_id_item("dbvar", _clean("ID", rules, d), "ID")],
        method="vcf_alleles",
        own=own,
    )


def variant_frames(rules):
    """(dataset, frame) of every source that has genomic coordinates."""
    return [
        (CLINVAR, clinvar_frame(rules)),
        (
            GNOMAD,
            _vcf_frame(
                "gnomad_variants",
                GNOMAD,
                rules,
                chrom="chr",
                pos="pos",
                ref="ref",
                alt="alt",
                method="vcf_alleles",
                ids=[_id_item("gnomad_variant", F.col("variant_id"), "variant_id")],
            ),
        ),
        (ALPHAMISSENSE, alphamissense_frame(rules)),
        (
            CADD,
            _vcf_frame(
                "cadd_scores",
                CADD,
                rules,
                chrom="chrom",
                pos="pos",
                ref="ref",
                alt="alt",
            ),
        ),
        (DBVAR, dbvar_frame(rules)),
        (
            LR_CODING,
            _vcf_frame(
                "long_range_benchmark_pathogenic_coding",
                LR_CODING,
                rules,
                chrom="CHROM",
                pos="POS",
                ref="REF",
                alt="ALT",
            ),
        ),
        (
            LR_NON_CODING,
            _vcf_frame(
                "long_range_benchmark_pathogenic_non_coding",
                LR_NON_CODING,
                rules,
                chrom="CHROM",
                pos="POS",
                ref="REF",
                alt="ALT",
            ),
        ),
        (
            LR_EQTL,
            _vcf_frame(
                "long_range_benchmark_causal_eqtl",
                LR_EQTL,
                rules,
                chrom="CHROM",
                pos="POS",
                ref="REF",
                alt="ALT",
            ),
        ),
        (
            CLINVAR_BENIGN,
            _vcf_frame(
                "clinvar_vs_benign_variants",
                CLINVAR_BENIGN,
                rules,
                chrom="chrom",
                pos="pos",
                ref="ref",
                alt="alt",
            ),
        ),
        (
            GNOMAD_BALANCED,
            _vcf_frame(
                "gnomad_balanced_variants",
                GNOMAD_BALANCED,
                rules,
                chrom="chrom",
                pos="pos",
                ref="ref",
                alt="alt",
            ),
        ),
    ]


def _variant_id_rows(frame, dataset):
    rows = frame.select(
        F.col("variant_key").alias("entity_key"),
        F.explode("v_ids").alias("item"),
        F.col("key_method").alias("match_method"),
    )
    return rows.select(
        F.lit("variant").alias("entity_type"),
        "entity_key",
        F.col("item.id_type").alias("id_type"),
        F.col("item.id_value").alias("id_value"),
        F.lit(dataset).alias("source_dataset"),
        F.col("item.source_field").alias("source_field"),
        "match_method",
    )


def pharmgkb_variant_frame(rules, id_map):
    """PharmGKB variants keyed by an exact, unique rsID of another source, else
    by their own accession (`id_map`: unique rsID rows of the other sources)."""
    d = PHARMGKB_VARIANTS
    frame = _bronze("pharmgkb_variants").withColumn(
        "rsid_text",
        F.when(
            _clean("Variant_Name", rules, d).rlike(r"^rs\d+$"),
            _clean("Variant_Name", rules, d),
        ),
    )
    pairs = frame.select(
        F.col("Variant_ID").alias("own"),
        F.lit("rsid").alias("id_type"),
        F.col("rsid_text").alias("id_value"),
    ).filter(F.col("id_value").isNotNull())
    links = link_by_ids(pairs, id_map)
    frame = frame.join(links, frame["Variant_ID"] == links["own"], "left")
    own = F.concat(F.lit("pharmgkb_variant:"), F.col("Variant_ID"))
    return (
        frame.withColumn("variant_key", F.coalesce(F.col("entity_key"), own))
        .withColumn(
            "key_method",
            F.when(
                F.col("entity_key").isNotNull(), F.lit("rsid_via_other_source")
            ).otherwise(F.lit("own_source_id")),
        )
        .withColumn("v_source", F.lit(d))
        .withColumn(
            "v_ids",
            F.filter(
                F.array(
                    _id_item("pharmgkb_variant", F.col("Variant_ID"), "Variant_ID"),
                    _id_item("rsid", F.col("rsid_text"), "Variant_Name"),
                ),
                lambda item: item["id_value"].isNotNull(),
            ),
        )
        .filter(F.col("variant_key").isNotNull())
    )


def variant_crosswalk(rules):
    """Variant id rows; PharmGKB variants link through rsIDs that one variant of
    the other sources holds."""
    frames = variant_frames(rules)
    rows = _union([_variant_id_rows(frame, dataset) for dataset, frame in frames])
    id_map = unique_id_map(finish_crosswalk(rows), ["rsid"])
    pharmgkb = pharmgkb_variant_frame(rules, id_map)
    return rows.unionByName(_variant_id_rows(pharmgkb, PHARMGKB_VARIANTS))


def variant_id_map(xwalk):
    """The unique rsID rows of the variant crosswalk, without the PharmGKB rows
    (the map the PharmGKB link was made with)."""
    rows = xwalk.filter(
        (F.col("entity_type") == "variant")
        & (F.col("source_dataset") != PHARMGKB_VARIANTS)
    ).select(
        "entity_type",
        "entity_key",
        "id_type",
        "id_value",
        "source_dataset",
        "source_field",
        "match_method",
    )
    return unique_id_map(finish_crosswalk(rows), ["rsid"])


# COMMAND ----------

# DBTITLE 1,Variants: names and claims


def variant_names(rules, id_map):
    keyed = {"key_col": "entity_key", "rules": rules, "entity_type": "variant"}
    clinvar = clinvar_frame(rules).withColumn("entity_key", F.col("variant_key"))
    gnomad = variant_frames(rules)[1][1].withColumn("entity_key", F.col("variant_key"))
    pharmgkb = pharmgkb_variant_frame(rules, id_map).withColumn(
        "entity_key", F.col("variant_key")
    )
    return _union(
        [
            names_frame(
                clinvar, dataset=CLINVAR, specs=[("Name", "hgvs_name", "text")], **keyed
            ),
            names_frame(
                gnomad,
                dataset=GNOMAD,
                specs=[("variant_id", "gnomad_variant_id", "text")],
                **keyed,
            ),
            names_frame(
                pharmgkb,
                dataset=PHARMGKB_VARIANTS,
                specs=[
                    ("Variant_Name", "name", "text"),
                    ("Synonyms", "synonym", "comma"),
                ],
                **keyed,
            ),
        ]
    )


def variant_claims(rules, id_map, transcript_genes):
    """Claims of the variant fields. `transcript_genes` (transcript_key,
    gene_key) turns the AlphaMissense transcript into a gene claim."""
    base = {"entity_type": "variant", "key_col": "entity_key", "rules": rules}
    clinvar = clinvar_frame(rules).withColumn("entity_key", F.col("variant_key"))
    alpha = (
        alphamissense_frame(rules)
        .withColumn("entity_key", F.col("variant_key"))
        .withColumn("tx_key", ensembl_unversioned_expr(F.col("transcript_id")))
        .join(
            F.broadcast(transcript_genes),
            F.col("tx_key") == F.col("transcript_key"),
            "inner",
        )
        .withColumn("transcript_id", F.col("gene_key"))
    )
    dbvar = dbvar_frame(rules).withColumn("entity_key", F.col("variant_key"))
    pharmgkb = pharmgkb_variant_frame(rules, id_map).withColumn(
        "entity_key", F.col("variant_key")
    )
    return _union(
        [
            claims_frame(
                clinvar,
                fields=[
                    ("variant_type", "Type"),
                    ("origin", "OriginSimple"),
                    ("rsid", "RS_dbSNP"),
                    ("gene_key", "GeneID"),
                    ("name", "Name"),
                ],
                source_dataset=CLINVAR,
                **base,
            ),
            claims_frame(
                alpha,
                fields=[("gene_key", "transcript_id")],
                source_dataset=ALPHAMISSENSE,
                **base,
            ),
            claims_frame(
                dbvar, fields=[("variant_type", "SVTYPE")], source_dataset=DBVAR, **base
            ),
            claims_frame(
                pharmgkb,
                fields=[("name", "Variant_Name")],
                source_dataset=PHARMGKB_VARIANTS,
                **base,
            ),
        ]
    )


# COMMAND ----------

# DBTITLE 1,Linking helpers: id maps, symbol maps, links


def entity_id_map(xwalk, entity_type):
    """(id_type, id_value, entity_key) of the unique ids of one entity type."""
    return unique_id_map(xwalk.filter(F.col("entity_type") == entity_type))


def term_map(names, entity_type, term_types, id_type):
    """(id_type, id_value, entity_key) of the upper-case terms (of `term_types`)
    that exactly one entity of the type holds."""
    terms = (
        names.filter(
            (F.col("entity_type") == entity_type) & F.col("term_type").isin(*term_types)
        )
        .select(F.upper(F.col("term")).alias("id_value"), F.col("entity_key"))
        .distinct()
    )
    return (
        terms.groupBy("id_value")
        .agg(
            F.countDistinct("entity_key").alias("holders"),
            F.min("entity_key").alias("entity_key"),
        )
        .filter(F.col("holders") == 1)
        .select(F.lit(id_type).alias("id_type"), "id_value", "entity_key")
    )


def link_entities(df, specs, id_map, key_out, method_out):
    """Add `key_out` (the entity key of the first spec that finds one) and
    `method_out` (its id type, `unlinked` or `no_id`). A spec is (value Column,
    id type text or Column); `id_map` holds unique ids only, so no row is copied."""
    out = df
    keys, methods, values = [], [], []
    for index, (value, id_type) in enumerate(specs):
        tmp_value, tmp_key = f"_le_value_{index}", f"_le_key_{index}"
        tmp_id, tmp_type = f"_le_id_{index}", f"_le_type_{index}"
        out = out.withColumn(tmp_value, value)
        if isinstance(id_type, str):
            lookup = id_map.filter(F.col("id_type") == id_type).select(
                F.col("id_value").alias(tmp_id), F.col("entity_key").alias(tmp_key)
            )
            condition = F.col(tmp_value) == F.col(tmp_id)
            label = F.lit(id_type)
        else:
            out = out.withColumn(tmp_type, id_type)
            lookup = id_map.select(
                F.col("id_value").alias(tmp_id),
                F.col("id_type").alias(f"_le_maptype_{index}"),
                F.col("entity_key").alias(tmp_key),
            )
            condition = (F.col(tmp_value) == F.col(tmp_id)) & (
                F.col(tmp_type) == F.col(f"_le_maptype_{index}")
            )
            label = F.col(tmp_type)
        out = out.join(lookup, condition, "left")
        keys.append(F.col(tmp_key))
        methods.append(F.when(F.col(tmp_key).isNotNull(), label))
        values.append(F.col(tmp_value).isNotNull())
    out = out.withColumn(key_out, F.coalesce(*keys)).withColumn(
        method_out,
        F.coalesce(
            *methods,
            F.when(_reduce(lambda a, b: a | b, values), F.lit("unlinked")),
            F.lit("no_id"),
        ),
    )
    return out.drop(*[c for c in out.columns if c.startswith("_le_")])


def split_curie(column, rules):
    """(id_type, id_value) Columns of a `LABEL:value` text; `OMIM:123` -> omim, 123."""
    text = F.col(column) if isinstance(column, str) else column
    label = F.regexp_extract(text, r"^([^:]+):", 1)
    value = F.regexp_extract(text, r"^[^:]+:(.*)$", 1)
    id_type = id_type_expr(label, rules)
    return id_type, id_value_expr(id_type, value)


def band_expr(entries, value, default):
    """Column expr: the label of the first band whose `below` is above `value`."""
    expr = F.lit(default)
    for entry in reversed(entries):
        expr = F.when(
            value < F.lit(float(entry["below"])), F.lit(entry["label"])
        ).otherwise(expr)
    return F.when(value.isNotNull(), expr)


def split_list(column, separators=r"[;|,]"):
    """Array of the trimmed, non-empty parts of a list text; null for no part."""
    text = F.col(column) if isinstance(column, str) else column
    parts = F.filter(
        F.transform(F.split(text, separators), lambda x: F.trim(x)), lambda x: x != ""
    )
    return F.when(F.size(parts) > 0, parts)


# COMMAND ----------

# DBTITLE 1,Parser UDFs (small text parsers over Spark rows)

HGVS_NAME_FIELDS = [
    ("transcript_accession", "string"),
    ("transcript_version", "int"),
    ("gene_symbol", "string"),
    ("coding_notation", "string"),
    ("coding_region", "string"),
    ("coding_start", "int"),
    ("coding_start_offset", "int"),
    ("coding_end", "int"),
    ("coding_end_offset", "int"),
    ("coding_edit_type", "string"),
    ("coding_reference_base", "string"),
    ("coding_alternate_base", "string"),
    ("coding_inserted_bases", "string"),
    ("protein_notation", "string"),
    ("protein_reference_amino_acid", "string"),
    ("protein_position", "int"),
    ("protein_end_reference_amino_acid", "string"),
    ("protein_end_position", "int"),
    ("protein_edit_type", "string"),
    ("protein_alternate_amino_acid", "string"),
    ("protein_inserted_amino_acids", "string"),
    ("parse_status", "string"),
]
HGVS_PROTEIN_FIELDS = [
    ("protein_notation", "string"),
    ("protein_reference_amino_acid", "string"),
    ("protein_position", "int"),
    ("protein_end_reference_amino_acid", "string"),
    ("protein_end_position", "int"),
    ("protein_edit_type", "string"),
    ("protein_alternate_amino_acid", "string"),
    ("protein_inserted_amino_acids", "string"),
]
TCGA_FIELDS = [
    ("project", "string"),
    ("tissue_source_site", "string"),
    ("participant_barcode", "string"),
    ("sample_barcode", "string"),
    ("sample_type_code", "string"),
    ("vial", "string"),
    ("portion", "string"),
    ("analyte", "string"),
    ("plate", "string"),
    ("center", "string"),
    ("parse_status", "string"),
]
PARSER_KEY = {
    "coding_notation": "c_notation",
    "coding_region": "c_region",
    "coding_start_offset": "c_start_offset",
    "coding_start": "c_start",
    "coding_end_offset": "c_end_offset",
    "coding_end": "c_end",
    "coding_edit_type": "c_edit_type",
    "coding_reference_base": "c_reference",
    "coding_alternate_base": "c_alternate",
    "coding_inserted_bases": "c_inserted",
    "protein_notation": "p_notation",
    "protein_reference_amino_acid": "p_reference",
    "protein_position": "p_position",
    "protein_end_reference_amino_acid": "p_end_reference",
    "protein_end_position": "p_end_position",
    "protein_edit_type": "p_edit_type",
    "protein_alternate_amino_acid": "p_alternate",
    "protein_inserted_amino_acids": "p_inserted",
}

CONDITION_DDL = "array<struct<position:int,ids:array<string>,name:string,accession:string,pairing_status:string>>"


def _ddl(fields):
    return "struct<" + ",".join(f"{name}:{kind}" for name, kind in fields) + ">"


def hgvs_name_udf():
    names = [name for name, _ in HGVS_NAME_FIELDS]
    return F.udf(
        lambda text: tuple(parse_hgvs_name(text)[PARSER_KEY.get(n, n)] for n in names),
        _ddl(HGVS_NAME_FIELDS),
    )


def hgvs_protein_udf():
    names = [name for name, _ in HGVS_PROTEIN_FIELDS]
    return F.udf(
        lambda text: tuple(
            parse_hgvs_protein(text)[PARSER_KEY.get(n, n)] for n in names
        ),
        _ddl(HGVS_PROTEIN_FIELDS),
    )


def tcga_barcode_udf():
    names = [name for name, _ in TCGA_FIELDS]
    return F.udf(
        lambda text: tuple(parse_tcga_barcode(text)[n] for n in names),
        _ddl(TCGA_FIELDS),
    )


def clinvar_conditions_udf():
    keys = ("position", "ids", "name", "accession", "pairing_status")

    def parse(ids, names, accessions):
        return [
            tuple(row[k] for k in keys)
            for row in parse_clinvar_conditions(ids, names, accessions)
        ]

    return F.udf(parse, CONDITION_DDL)


def faers_date(column):
    """A FAERS `yyyymmdd` text as a date; null for a partial date."""
    text = F.col(column)
    return F.when(
        text.rlike(r"^\d{8}$"),
        F.expr(f"cast(try_to_timestamp({column}, 'yyyyMMdd') as date)"),
    )


# COMMAND ----------

# DBTITLE 1,Band helpers for the old cutoffs and weights


def from_band_expr(entries, value, default):
    """Column expr: the label of the first entry (list falling by `from`) whose
    `from` the value reaches; `default` when none."""
    expr = F.lit(default)
    for entry in reversed(entries):
        expr = F.when(value >= F.lit(entry["from"]), F.lit(entry["label"])).otherwise(
            expr
        )
    return F.when(value.isNotNull(), expr)


def points_expr(entries, value):
    """Column expr: the points of the first entry (list falling by `from`) whose
    `from` the value reaches; 0 when none."""
    expr = F.lit(0)
    for entry in reversed(entries):
        expr = F.when(value >= F.lit(entry["from"]), F.lit(entry["points"])).otherwise(
            expr
        )
    return F.coalesce(expr, F.lit(0))


def any_match_expr(entries, flags, test_names, default):
    """Column expr: the label of the first entry with any test whose flag name
    is in the array column `flags`; `test_names` turns a test into a flag name."""
    expr = F.lit(default)
    for entry in reversed(entries):
        condition = F.lit(False)
        for test in entry["any"]:
            condition = condition | F.array_contains(flags, test_names(test))
        expr = F.when(condition, F.lit(entry["label"])).otherwise(expr)
    return expr


# COMMAND ----------

# DBTITLE 1,Source column helpers and row shaping (use the notebook's RULES)


def source_text(column, dataset):
    """Trimmed text of a Bronze column; null for a placeholder."""
    return clean_placeholders(column, RULES, f"{dataset}.{column}")


def source_number(column, kind, dataset):
    """A Bronze column as a number (`int`, `bigint` or `double`)."""
    return typed_number(column, kind, RULES, f"{dataset}.{column}")


def source_double(column, dataset):
    return source_number(column, "double", dataset)


def source_typed(source, kind, dataset):
    """A Bronze column as the declared Spark type (text, number or boolean)."""
    text = source_text(source, dataset)
    if kind == "boolean":
        return F.when(text.isNotNull(), F.lower(text) == "true")
    if kind in ("int", "bigint", "double"):
        return source_number(source, kind, dataset)
    return text


def text_for(dataset):
    """`text(column)` of one dataset."""
    return lambda column: source_text(column, dataset)


def number_for(dataset, default_kind="double"):
    """`number(column, kind)` of one dataset."""
    return lambda column, kind=default_kind: source_number(column, kind, dataset)


def shape_rows(df, columns, dataset, fixed=None, from_source=False, **given):
    """The frame in the declared columns of a table: `fixed` literals, `given`
    Columns, else the same-named column of `df` (when `from_source`), else null."""
    fixed = fixed or {}
    values = []
    for name, kind in columns:
        if name == "source_dataset":
            value = F.lit(dataset)
        elif name in fixed:
            value = F.lit(fixed[name])
        elif name in given:
            value = given[name]
        elif from_source and name in df.columns:
            value = F.col(name)
        else:
            value = F.lit(None)
        values.append(value.cast(kind).alias(name))
    return df.select(*values)


def rows_factory(columns, label_column=None, from_source=False):
    """`rows(df, [label,] dataset, **given)` of one table."""

    def rows(df, *args, **given):
        if label_column is None:
            (dataset,) = args
            fixed = {}
        else:
            label, dataset = args
            fixed = {label_column: label}
        return shape_rows(df, columns, dataset, fixed, from_source, **given)

    return rows


def mapped_rows_factory(columns, label_column):
    """`rows(df, label, dataset, mapping)`: `mapping` gives the source column of a target column."""

    def rows(df, label, dataset, mapping):
        values = []
        for name, kind in columns:
            if name == label_column:
                value = F.lit(label)
            elif name == "source_dataset":
                value = F.lit(dataset)
            elif name in mapping:
                value = source_typed(mapping[name], kind, dataset)
            elif name in df.columns:
                value = F.col(name)
            else:
                value = F.lit(None)
            values.append(value.cast(kind).alias(name))
        return df.select(*values)

    return rows


def pair_linker(first_map, first_name, second_map, second_name):
    """`linked(df, first_specs, second_specs)`: two entity links, each with its key
    and its link method column."""

    def linked(df, first_specs, second_specs):
        df = link_entities(
            df, first_specs, first_map, f"{first_name}_key", f"{first_name}_link_method"
        )
        return link_entities(
            df,
            second_specs,
            second_map,
            f"{second_name}_key",
            f"{second_name}_link_method",
        )

    return linked
