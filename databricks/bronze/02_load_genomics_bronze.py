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
# MAGIC into its `genomics_bronze` tables, check each table, and remove the files of a
# MAGIC source whose tables all passed. A dataset with no file is skipped. One source
# MAGIC folder in the Volume is one run. Safe to run again.

# COMMAND ----------

# DBTITLE 1,Shared libraries
# MAGIC %run ./_bronze_common

# COMMAND ----------

# MAGIC %run ./_genomics_bronze_common

# COMMAND ----------

# DBTITLE 1,Configuration
RUN_SOURCES = []  # empty = every source folder found in the landing Volume
WRITE_MODE = "overwrite"  # "append" for the second and later parts of a split source

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

# DBTITLE 1,Load each dataset into its Bronze table
load_results: list[dict] = []

for source, datasets in plan.items():
    for dataset, files in datasets.items():
        name = table_name(source, dataset)
        table = f"{CATALOG}.{SCHEMA}.{name}"
        result = {
            "source": source,
            "table": table,
            "files": len(files),
            "rows": None,
            "validated": False,
            "error": None,
            "paths": files,
        }
        try:
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
            result["rows"] = write_bronze(df, table, WRITE_MODE)
            print(f"OK    {name}: {result['rows']} rows from {len(files)} file(s)")
        except Exception as exc:
            result["error"] = str(exc)
            print(f"FAIL  {name}: {exc}")
        load_results.append(result)

# COMMAND ----------

# DBTITLE 1,Validate each table
for result in load_results:
    if result["rows"] is None:
        continue
    result["error"] = check_table(result["table"], result["rows"])
    result["validated"] = result["error"] is None
    if result["error"]:
        print(f"FAIL  {result['table']}: {result['error']}")

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
