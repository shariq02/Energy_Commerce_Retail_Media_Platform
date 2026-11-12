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

# MAGIC %run ./_genomics_bronze_common

# COMMAND ----------

# DBTITLE 1,Imports
from functools import reduce

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
RUN_SOURCES = []  # empty = every source folder found in the landing Volume
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
    selected = RUN_SOURCES
else:
    selected = [s for s in landing_sources() if s in SOURCES]
if not selected:
    raise RuntimeError(f"FAIL  no genomics source folder in {landing_root()}")
print(f"Sources selected: {selected}")

# COMMAND ----------

# DBTITLE 1,Map each dataset to its uploaded files
plan: dict[str, dict[str, list[str]]] = {}
for source in selected:
    paths = list_files(landing_path(source))
    found: dict[str, list[str]] = {}
    for dataset in SOURCES[source]:
        pattern = file_pattern(source, dataset)
        matches = sorted(p for p in paths if pattern.match(p.rsplit("/", 1)[-1]))
        if matches:
            found[dataset] = matches
        else:
            print(f"SKIP  {source}_{dataset}: no file")
    plan[source] = found
    print(f"OK    {source}: {sum(len(v) for v in found.values())} file(s) to load")

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
        remove_files([p for r in results for p in r["paths"]])
        print(f"REMOVED    files of {source}")
    else:
        print(f"PRESERVED  files of {source}")

# COMMAND ----------

# DBTITLE 1,Summary
print_summary("GENOMICS BRONZE LOAD", load_results)
if not all(r["validated"] for r in load_results):
    raise RuntimeError("FAIL  at least one table did not load; see the lines above")
