# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
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
# MAGIC **Purpose:** load the uploaded chunk files of each genomics source into its
# MAGIC `genomics_bronze` tables, check each table, and drop the volume of a source whose
# MAGIC tables all passed. A source with no volume, or with a dataset that has no file
# MAGIC yet, is skipped. Safe to run again.

# COMMAND ----------

# DBTITLE 1,Shared config
# MAGIC %run ./_genomics_bronze_common

# COMMAND ----------

# DBTITLE 1,Configuration
RUN_SOURCES = []  # empty = all sources; for example ["clinvar", "faers"]

selected = RUN_SOURCES or list(SOURCES)
unknown = [s for s in selected if s not in SOURCES]
if unknown:
    raise RuntimeError(f"unknown source(s): {unknown}")
print(f"Sources selected: {len(selected)}")

# COMMAND ----------

# DBTITLE 1,Map each dataset to its uploaded files
live_volumes = {
    row.volume_name
    for row in spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}").collect()
}

plan: dict[str, dict[str, list[str]]] = {}
for source in selected:
    if source not in live_volumes:
        print(f"SKIP  {source}: no volume (loaded already, or not created)")
        continue
    paths = [f.path for f in dbutils.fs.ls(volume_path(source)) if not f.isDir()]
    found: dict[str, list[str]] = {}
    missing = []
    for dataset in SOURCES[source]:
        pattern = file_pattern(source, dataset)
        matches = sorted(p for p in paths if pattern.match(p.rsplit("/", 1)[-1]))
        if matches:
            found[dataset] = matches
        else:
            missing.append(dataset)
    if missing:
        print(f"WAIT  {source}: no file yet for {missing}")
        continue
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
            "status": "FAILED",
            "validated": False,
            "error": None,
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
            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(table)
            result["rows"] = spark.table(table).count()
            result["status"] = "LOADED"
            print(f"OK    {name}: {result['rows']} rows from {len(files)} file(s)")
        except Exception as exc:
            result["error"] = str(exc)
            print(f"FAIL  {name}: {exc}")
        load_results.append(result)

# COMMAND ----------

# DBTITLE 1,Validate each table
for result in load_results:
    if result["status"] != "LOADED":
        continue
    if not spark.table(result["table"]).columns:
        result["error"] = "table has an empty schema"
    elif not result["rows"]:
        result["error"] = "table has zero rows after load"
    else:
        result["validated"] = True
    if result["error"]:
        print(f"FAIL  {result['table']}: {result['error']}")

# COMMAND ----------

# DBTITLE 1,Drop the volume of every source that passed
volume_cleanup: dict[str, str] = {}
for source in plan:
    results = [r for r in load_results if r["source"] == source]
    if all(r["validated"] for r in results):
        spark.sql(f"DROP VOLUME IF EXISTS {CATALOG}.{SCHEMA}.{source}")
        volume_cleanup[source] = "DELETED"
    else:
        volume_cleanup[source] = "PRESERVED"
    print(f"{volume_cleanup[source]:<10} volume {source}")

# COMMAND ----------

# DBTITLE 1,Summary
loaded = sum(1 for r in load_results if r["validated"])
print("=" * 70)
print(f"Tables loaded and validated: {loaded}/{len(load_results)}")
print(
    f"Sources loaded             : {sum(v == 'DELETED' for v in volume_cleanup.values())}/{len(plan)}"
)
print("=" * 70)
if loaded != len(load_results):
    raise RuntimeError("FAIL  at least one table did not load; see the lines above")
