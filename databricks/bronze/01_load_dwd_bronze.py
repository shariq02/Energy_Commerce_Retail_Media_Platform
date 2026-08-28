# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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

# DBTITLE 1,Imports
import re

from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# DBTITLE 1,Write-performance tuning
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set(
    "spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", "8"
)

# COMMAND ----------

# DBTITLE 1,Configuration
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

# Source column names containing characters Delta rejects in identifiers
# (e.g. "[", "]", "(", ")", ".") for the affected DWD datasets only.
# Renaming happens right before the Bronze write; values are untouched.
COLUMN_RENAME_MAP: dict[str, dict[str, str]] = {
    "device_instrument": {
        "Geo. Laenge [Grad]": "geo_longitude_deg",
        "Geo. Breite [Grad]": "geo_latitude_deg",
        "Stationshoehe [m]": "station_elevation_m",
        "Geberhoehe ueber Grund [m]": "sensor_height_m",
        "Geraetetyp Name": "device_type_name",
    },
    "parameter_unit": {
        "Datenquelle (Strukturversion=SV)": "data_source",
    },
}

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
            .option("enforceSchema", "false")  # verify every chunk's header, fail loud on mismatch
            .option("multiLine", "false")      # keep CSV splittable
            .csv(files)
        )
        rename_map = COLUMN_RENAME_MAP.get(dataset, {})
        applied_renames = {old: new for old, new in rename_map.items() if old in df.columns}
        for old, new in applied_renames.items():
            df = df.withColumnRenamed(old, new)
        if applied_renames:
            print(f"OK  {dataset}: normalized column name(s) -- {applied_renames}")

        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        # Row count from the Delta transaction log (metadata only). Taking it
        # after the write avoids the second full parse of every source file
        # that a pre-write df.count() would force.
        row_count = spark.table(table).count()
        result["rows"] = row_count
        result["columns"] = len(df.columns)
        result["status"] = "LOADED"
        print(f"OK  {dataset}: {row_count} rows from {len(files)} file(s) -> {table}")
    except Exception as exc:
        result["error"] = str(exc)
        print(f"FAIL  {dataset}: could not load into {table} -- {exc}")
    load_results.append(result)

# COMMAND ----------

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
        if actual_rows == 0:
            raise RuntimeError("table has zero rows after load")
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