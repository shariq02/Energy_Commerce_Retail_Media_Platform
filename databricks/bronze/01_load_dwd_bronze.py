# Databricks notebook source
# MAGIC %md
# MAGIC # BRONZE DATA LOADING
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Load uploaded Unity Catalog Volume data into the
# MAGIC frozen Bronze table structure.

# COMMAND ----------

# 1. IMPORTS

from pyspark.sql.utils import AnalysisException
import re

# COMMAND ----------

# 2. CONFIGURATION
#
# One catalog, one Bronze schema, two DWD Volume upload units
# (dwd_analytical, dwd_metadata). Each of the 12 DWD_DATASETS below maps
# to exactly one Bronze table -- physical chunks inside a Volume are a
# file-count detail, never a table-count driver. The regex used later
# to collect a dataset's files accepts either a single unchunked file
# (<dataset>.csv, as produced by scripts/ingestion/stage_dwd.py) or a
# chunked upload (dwd_<dataset>_chunk_00001.csv, ...) so both staging
# outcomes are handled without changing this notebook.

CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"

DWD_ANALYTICAL_VOLUME = "dwd_analytical"
DWD_METADATA_VOLUME = "dwd_metadata"

DWD_ANALYTICAL_DATASETS = [
    "air_temperature",
    "cloudiness",
    "moisture",
    "precipitation",
    "pressure",
    "sun",
    "wind",
]

DWD_METADATA_DATASETS = [
    "station_geography",
    "station_name_history",
    "device_instrument",
    "parameter_unit",
    "missing_value_periods",
]

# (dataset_name, source_volume) for all 12 Bronze upload units.
DWD_DATASETS = [(d, DWD_ANALYTICAL_VOLUME) for d in DWD_ANALYTICAL_DATASETS] + \
               [(d, DWD_METADATA_VOLUME) for d in DWD_METADATA_DATASETS]

VOLUMES = [DWD_ANALYTICAL_VOLUME, DWD_METADATA_VOLUME]


def volume_path(volume: str) -> str:
    return f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{volume}"


def bronze_table(dataset: str) -> str:
    return f"{CATALOG}.{BRONZE_SCHEMA}.dwd_{dataset}"


def dataset_file_pattern(dataset: str) -> re.Pattern:
    # Matches "<dataset>.csv" (single staged file) or
    # "dwd_<dataset>_chunk_00001.csv" (physically chunked upload).
    return re.compile(rf"^(?:dwd_)?{re.escape(dataset)}(?:_chunk_\d{{5}})?\.(?:csv|txt)$")

# COMMAND ----------

# 3. VOLUME EXISTENCE CHECKS
#
# Both DWD Volumes must exist and be listable before any dataset file
# is looked for. Missing/inaccessible Volumes fail the notebook
# immediately -- there is nothing downstream that can safely proceed.

# DBTITLE 1,Verify DWD Volumes exist and are accessible
found_volumes = {
    row.volume_name
    for row in spark.sql(f"SHOW VOLUMES IN {CATALOG}.{BRONZE_SCHEMA}").collect()
}
missing_volumes = [v for v in VOLUMES if v not in found_volumes]
if missing_volumes:
    raise RuntimeError(
        f"FAIL  missing required DWD Volume(s) in {CATALOG}.{BRONZE_SCHEMA}: {missing_volumes}"
    )

inaccessible_volumes = []
for volume in VOLUMES:
    try:
        dbutils.fs.ls(volume_path(volume))
        print(f"OK  volume accessible: {volume_path(volume)}")
    except Exception as exc:
        inaccessible_volumes.append((volume_path(volume), str(exc)))

if inaccessible_volumes:
    detail = "; ".join(f"{path} -> {err}" for path, err in inaccessible_volumes)
    raise RuntimeError(f"FAIL  inaccessible DWD Volume(s): {detail}")

print(f"OK  both DWD Volumes present and accessible: {VOLUMES}")

# COMMAND ----------

# 4. DATASET-TO-FILE MAPPING
#
# For each of the 12 DWD datasets, collect every physical file
# belonging to it out of its source Volume. Any dataset with zero
# matching files fails the notebook immediately -- a missing dataset
# file must not silently become a missing/empty Bronze table.

# DBTITLE 1,Map each DWD dataset to its source files
dataset_files: dict[str, list[str]] = {}
missing_dataset_files: list[str] = []

for dataset, volume in DWD_DATASETS:
    all_files = [f.path for f in dbutils.fs.ls(volume_path(volume)) if not f.isDir()]
    pattern = dataset_file_pattern(dataset)
    matches = sorted(
        path for path in all_files if pattern.match(path.rsplit("/", 1)[-1])
    )
    dataset_files[dataset] = matches
    if not matches:
        missing_dataset_files.append(f"{dataset} (expected in {volume_path(volume)})")
    else:
        print(f"OK  {dataset}: {len(matches)} file(s) found in {volume_path(volume)}")

if missing_dataset_files:
    raise RuntimeError(
        f"FAIL  no source file(s) found for dataset(s): {missing_dataset_files}"
    )

print(f"OK  all {len(DWD_DATASETS)} DWD datasets have at least one source file")

# COMMAND ----------

# 5. BRONZE LOADING
#
# Each dataset's file(s) -- one physical file or many chunks -- are
# read together as a single DataFrame and written to exactly one
# Bronze table, preserving the staged column structure. Columns are
# read as strings (no schema inference) so Bronze faithfully mirrors
# the staged CSV content rather than reinterpreting types. A failure
# loading one dataset does not stop the others -- every dataset is
# attempted so the Final Summary and Volume cleanup step can report
# and decide on the complete picture.

# DBTITLE 1,Load each DWD dataset into its Bronze table
load_results: list[dict] = []

for dataset, volume in DWD_DATASETS:
    files = dataset_files[dataset]
    table = bronze_table(dataset)
    result = {
        "dataset": dataset,
        "volume": volume,
        "table": table,
        "files": len(files),
        "rows": None,
        "columns": None,
        "status": "FAILED",
        "error": None,
    }
    try:
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .csv(files)
        )
        row_count = df.count()
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        result["rows"] = row_count
        result["columns"] = len(df.columns)
        result["status"] = "LOADED"
        print(f"OK  {dataset}: {row_count} rows from {len(files)} file(s) -> {table}")
    except Exception as exc:
        result["error"] = str(exc)
        print(f"FAIL  {dataset}: could not load into {table} -- {exc}")
    load_results.append(result)

# COMMAND ----------

# 6. VALIDATION
#
# For every dataset that loaded successfully, confirm the Bronze table
# exists, is describable, has a non-empty schema, and its row count
# matches what was reported during loading. Datasets that failed to
# load are recorded as failed validation too -- they never reach this
# check with a usable table.

# DBTITLE 1,Validate each Bronze table
for result in load_results:
    if result["status"] != "LOADED":
        result["validated"] = False
        result["validation_error"] = "skipped -- dataset failed to load"
        continue
    table = result["table"]
    try:
        schema = spark.table(table).schema
        if len(schema.fields) == 0:
            raise RuntimeError("table has an empty schema")
        actual_rows = spark.table(table).count()
        if actual_rows != result["rows"]:
            raise RuntimeError(
                f"row count mismatch: loaded {result['rows']}, table has {actual_rows}"
            )
        result["validated"] = True
        result["validation_error"] = None
        print(f"OK  {table}: schema has {len(schema.fields)} column(s), {actual_rows} rows verified")
    except (AnalysisException, RuntimeError) as exc:
        result["validated"] = False
        result["validation_error"] = str(exc)
        print(f"FAIL  {table}: validation error -- {exc}")

all_loaded = all(r["status"] == "LOADED" for r in load_results)
all_validated = all(r.get("validated") for r in load_results)
all_files_ok = len(missing_dataset_files) == 0
overall_success = all_loaded and all_validated and all_files_ok

# COMMAND ----------

# 7. FINAL SUMMARY

# DBTITLE 1,DWD Bronze load summary
print("=" * 70)
print("DWD BRONZE LOAD SUMMARY")
print("=" * 70)
for result in load_results:
    print(
        f"{result['dataset']:<24} files={result['files']:<3} "
        f"rows={str(result['rows']):<10} status={result['status']:<7} "
        f"validated={result.get('validated')}"
    )
    if result["error"]:
        print(f"    load error:       {result['error']}")
    if result.get("validation_error"):
        print(f"    validation error:  {result['validation_error']}")
print("-" * 70)
print(f"Datasets loaded    : {sum(1 for r in load_results if r['status'] == 'LOADED')}/{len(DWD_DATASETS)}")
print(f"Datasets validated : {sum(1 for r in load_results if r.get('validated'))}/{len(DWD_DATASETS)}")
print(f"Overall result     : {'PASS' if overall_success else 'FAIL'}")
print("=" * 70)

# COMMAND ----------

# 8. VOLUME CLEANUP
#
# The two DWD Volumes are temporary landing storage, deleted only after
# all 12 Bronze tables are confirmed loaded and validated with no
# failed files/chunks. Any failure anywhere above -- missing Volume,
# missing dataset file, failed load, failed validation -- preserves
# both Volumes untouched.

# DBTITLE 1,Delete DWD Volumes only if every dataset passed
volume_cleanup: dict[str, str] = {}

if overall_success:
    for volume in VOLUMES:
        spark.sql(f"DROP VOLUME IF EXISTS {CATALOG}.{BRONZE_SCHEMA}.{volume}")
        volume_cleanup[volume] = "DELETED"
        print(f"OK  volume deleted: {volume_path(volume)}")
else:
    for volume in VOLUMES:
        volume_cleanup[volume] = "PRESERVED"
        print(f"PRESERVED  volume kept (load/validation did not fully pass): {volume_path(volume)}")

print("-" * 70)
print("VOLUME CLEANUP RESULT")
for volume, status in volume_cleanup.items():
    print(f"  {volume}: {status}")
print("=" * 70)

if not overall_success:
    raise RuntimeError(
        "FAIL  DWD Bronze load did not fully pass -- see summary above. "
        "Both DWD Volumes were preserved."
    )
