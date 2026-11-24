# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GENOMICS BRONZE LOAD
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** load the chunk files of each genomics source from the landing Volume
# MAGIC into its `genomics_bronze` tables (datasets of one `at_load` merge go into one
# MAGIC table), check each table, and remove the files of a source whose tables all
# MAGIC passed. A dataset with no file is skipped. One source folder in the Volume is
# MAGIC one run. ClinVar keeps the snapshots already loaded and appends the new ones.

# COMMAND ----------

# DBTITLE 1,Shared libraries
# MAGIC %run ./_bronze_common

# COMMAND ----------

# DBTITLE 1,Imports
import re
from functools import reduce

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SCHEMA = "genomics_bronze"

# source folder in data/staging -> datasets. One Bronze table per dataset, named
# <source>_<dataset> like its staged chunk files.
SOURCES = {
    "aact": [
        "browse_conditions",
        "browse_interventions",
        "conditions",
        "eligibilities",
        "interventions",
        "keywords",
        "studies",
    ],
    "acmg": ["secondary_findings_genes"],
    "alphafold": ["residue_confidence"],
    "alphamissense": ["variant_scores"],
    "biogrid": ["interactions"],
    "blosum62": ["scores"],
    "cadd": ["scores"],
    "chembl": ["compounds", "uniprot_mapping"],
    "clingen": ["dosage_sensitivity", "gene_validity"],
    "clinvar": ["variant_position_grch37", "variant_summary", "variation_allele"],
    "clinvar_vs_benign": ["variants"],
    "collectri": ["regulons"],
    "cpic": ["pairs"],
    "dbvar": ["variants"],
    "ddinter": ["interactions"],
    "depmap": ["gene_effect", "models"],
    "encode": ["ccre_registry", "enhancer_gene_links"],
    "ensembl_compara": ["orthologues", "paralogues"],
    "faers": ["demo", "drug", "indi", "outc", "reac", "rpsr", "ther"],
    "fda": ["pgx_biomarkers"],
    "gencode": ["annotation"],
    "gene_ages": ["consensus"],
    "genome_sequence": ["chunk"],
    "gnomad": ["variants"],
    "gnomad_balanced": ["variants"],
    "gnomad_constraint": ["metrics"],
    "gnomad_mtdna": ["variants"],
    "gtex": ["gene_median_tpm"],
    "gtr": ["test_condition_gene"],
    "hpo": ["genes_to_phenotype", "phenotype_annotations", "terms", "term_relations"],
    "iedb": ["epitopes", "mhc_ligands"],
    "long_range_benchmark": [
        "causal_eqtl",
        "pathogenic_coding",
        "pathogenic_non_coding",
    ],
    "maps_embeddings": ["metadata"],
    "medgen": ["concepts", "relations"],
    "ncbi_gene": ["gene2refseq", "gene_info", "mim2gene_medgen"],
    "opentargets": [
        "association_by_datasource_direct",
        "association_overall_direct",
        "disease",
        "drug_mechanism_of_action",
        "drug_molecule",
        "evidence_gwas_credible_sets",
        "target",
    ],
    "pharmgkb": ["drugs", "genes", "relationships", "variants"],
    "proteingym": [
        "clinical_reference",
        "clinical_substitutions",
        "dms_reference",
        "dms_substitutions",
    ],
    "reactome": ["pathways", "pathway_mappings", "pathway_relations"],
    "refseq": ["annotation"],
    "spectralbio": ["variants"],
    "string": ["protein_aliases", "protein_info", "protein_links"],
    "tahoe": [
        "cell_line_metadata",
        "drug_metadata",
        "gene_metadata",
        "pseudobulk_differential_expression",
        "sample_metadata",
    ],
    "tcga": ["mc3_somatic_mutations", "site_codes"],
    "thousand_genomes": ["variants"],
    "ucsc_conservation": ["phastcons_100way", "phylop_100way"],
    "uniprot": ["entries", "features"],
}

# Datasets with a text value that spans lines; the others are read line by line.
MULTILINE_TABLES = {"hpo_terms", "opentargets_disease", "opentargets_drug_molecule"}

RUN_SOURCES = []  # empty = every source with a file in the landing Volume
WRITE_MODE = "overwrite"  # "append" for the second and later parts of a split source

# The `current` ClinVar rows in Bronze are the release of 2026-10-26 (release record)
# and get that tag; the staged 2026-01 and 2025-01 files equal the loaded ones.
CLINVAR_TABLE = "clinvar_variant_summary"
CLINVAR_PREVIOUS_CURRENT = "2026-10-26"
CLINVAR_LOADED_SNAPSHOTS = ["2026-01", "2025-01"]

GROUPS = load_groups(SCHEMA)

if RUN_SOURCES:
    unknown = [s for s in RUN_SOURCES if s not in SOURCES]
    if unknown:
        raise RuntimeError(f"unknown source(s): {unknown}")

# COMMAND ----------

# DBTITLE 1,Helpers -- table names, chunk files and column names


def table_name(source: str, dataset: str) -> str:
    return f"{source}_{dataset}"


def file_pattern(source: str, dataset: str) -> re.Pattern:
    """Matches `<source>_<dataset>_chunk_NNNNN.csv` and `.csv.gz`."""
    name = re.escape(table_name(source, dataset))
    return re.compile(rf"^{name}_chunk_\d{{5}}\.csv(\.gz)?$")


def clean_columns(columns: list[str]) -> list[str]:
    """Replace the characters Delta rejects in a column name with `_`.
    A name that is already clean is kept as it is."""
    cleaned = [
        re.sub(r"[^0-9A-Za-z_]+", "_", name).strip("_")
        if re.search(r"[^0-9A-Za-z_]", name)
        else name
        for name in columns
    ]
    lowered = [name.lower() for name in cleaned]
    if "" in cleaned or len(set(lowered)) != len(lowered):
        raise RuntimeError(f"column names collide after cleaning: {columns}")
    return cleaned


# COMMAND ----------

# DBTITLE 1,Map each dataset to its uploaded files
# A file counts when its name matches a dataset, in any folder of the landing root.
paths = landing_files()
seen = set(paths)
plan: dict[str, dict[str, list[str]]] = {}
for source in RUN_SOURCES or SOURCES:
    found: dict[str, list[str]] = {}
    for dataset in SOURCES[source]:
        pattern = file_pattern(source, dataset)
        matches = sorted(p for p in paths if pattern.match(p.rsplit("/", 1)[-1]))
        if matches:
            found[dataset] = matches
    plan[source] = found
selected = list(RUN_SOURCES) or [s for s in plan if plan[s]]
if not selected:
    raise RuntimeError(f"FAIL  no genomics file in {landing_root()}")
plan = {source: plan[source] for source in selected}
claim_files(plan, seen)
for source in selected:
    for dataset in SOURCES[source]:
        if dataset not in plan[source]:
            print(f"SKIP  {source}_{dataset}: no file")
    count = sum(len(v) for v in plan[source].values())
    print(f"OK    {source}: {count} file(s) to load")

# COMMAND ----------

# DBTITLE 1,Helpers -- read one dataset, keep the ClinVar snapshots


def read_dataset(name: str, files: list[str]):
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("enforceSchema", "false")  # verify every chunk's header
        .option("multiLine", str(name in MULTILINE_TABLES).lower())
        .option("quote", '"')
        .option("escape", '"')  # the staged CSV doubles an inner quote
        .csv(files)
    )
    df = df.toDF(*clean_columns(df.columns))
    _, column, value = GROUPS.get(name, (name, None, None))
    if column:
        columns = df.columns
        df = df.withColumn(column, F.lit(value)).select(column, *columns)
    return df


def keep_clinvar_snapshots(df, table: str):
    """Tag the earlier `current` rows once, drop the snapshots already loaded."""
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"{table} must exist: the loaded snapshots are kept")
    tagged = spark.table(table).filter(F.col("snapshot") == CLINVAR_PREVIOUS_CURRENT)
    if tagged.limit(1).count() == 0:
        spark.sql(
            f"UPDATE {table} SET snapshot = '{CLINVAR_PREVIOUS_CURRENT}' "
            "WHERE snapshot = 'current'"
        )
        print(f"OK    {table}: earlier current rows tagged {CLINVAR_PREVIOUS_CURRENT}")
    return df.filter(~F.col("snapshot").isin(CLINVAR_LOADED_SNAPSHOTS))


# COMMAND ----------

# DBTITLE 1,Load each Bronze table
load_results: list[dict] = []

for source, datasets in plan.items():
    targets: dict[str, list[str]] = {}
    for dataset in datasets:
        name = table_name(source, dataset)
        targets.setdefault(GROUPS.get(name, (name,))[0], []).append(name)
    for target, names in targets.items():
        table = f"{CATALOG}.{SCHEMA}.{target}"
        expected = sorted(p for p, group in GROUPS.items() if group[0] == target)
        files = [f for n in names for f in datasets[n.removeprefix(f"{source}_")]]
        result = {
            "source": source,
            "table": table,
            "files": len(files),
            "rows": None,
            "validated": False,
            "error": None,
            "paths": files,
            "discriminator": GROUPS[names[0]][1] if expected else None,
            "expected": {GROUPS[n][2] for n in expected},
        }
        if expected and sorted(names) != expected:
            result["error"] = f"no file for {sorted(set(expected) - set(names))}"
            print(f"FAIL  {target}: {result['error']}")
            load_results.append(result)
            continue
        try:
            frames = [
                read_dataset(n, datasets[n.removeprefix(f"{source}_")]) for n in names
            ]
            df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
            mode = WRITE_MODE
            if target == CLINVAR_TABLE:
                df, mode = keep_clinvar_snapshots(df, table), "append"
            result["rows"] = write_bronze(df, table, mode)
            print(f"OK    {target}: {result['rows']} rows from {len(files)} file(s)")
        except Exception as exc:
            result["error"] = str(exc)
            print(f"FAIL  {target}: {exc}")
        load_results.append(result)

# COMMAND ----------

# DBTITLE 1,Validate each table
for result in load_results:
    if result["rows"] is None:
        continue
    error = check_table(result["table"], result["rows"])
    if not error and result["discriminator"]:
        column = result["discriminator"]
        found_values = {
            row[0]
            for row in spark.table(result["table"]).select(column).distinct().collect()
        }
        if result["expected"] - found_values:
            error = f"{column} lacks {sorted(result['expected'] - found_values)}"
    result["error"] = error
    result["validated"] = error is None
    if error:
        print(f"FAIL  {result['table']}: {error}")

# COMMAND ----------

# DBTITLE 1,Remove the files of every source that passed
for source in plan:
    results = [r for r in load_results if r["source"] == source]
    if results and all(r["validated"] for r in results):
        remove_loaded([p for r in results for p in r["paths"]])
        print(f"REMOVED    files of {source}")
    else:
        print(f"PRESERVED  files of {source}")

# COMMAND ----------

# DBTITLE 1,Summary
print_summary("GENOMICS BRONZE LOAD", load_results)
if not all(r["validated"] for r in load_results):
    raise RuntimeError("FAIL  at least one table did not load; see the lines above")
