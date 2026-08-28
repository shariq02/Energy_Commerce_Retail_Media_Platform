# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # BRONZE DATA LOADING -- iPINYOU (RETAIL MEDIA / RTB)
# MAGIC
# MAGIC **Energy Commerce and Retail Media Analytics Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Load uploaded Unity Catalog Volume data for the iPinYou
# MAGIC RTB dataset (retail_media domain) into the frozen Bronze table
# MAGIC structure -- `ipinyou_training`, `ipinyou_leaderboard`,
# MAGIC `ipinyou_reference`.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports
import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"

SOURCE_PREFIX = "ipinyou"

ANALYTICAL_VOLUME = "ipinyou_analytical"
REFERENCE_VOLUME = "ipinyou_reference"

# Pass-through datasets: (staging dataset name, source Volume, Bronze table).
# One staged dataset -> one Bronze table, no transformation.
DATASETS: list[tuple[str, str, str]] = [
    ("training", ANALYTICAL_VOLUME, f"{CATALOG}.{BRONZE_SCHEMA}.ipinyou_training"),
    ("leaderboard", ANALYTICAL_VOLUME, f"{CATALOG}.{BRONZE_SCHEMA}.ipinyou_leaderboard"),
]

# Merged reference table: the three staged lookup files each become one
# lookup_type partition of a single Bronze table. Their id column is named
# differently per file (city_id / region_id / tag_id) and is renamed to a
# common `lookup_id`; `lookup_type` is added as an explicit discriminator.
REFERENCE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.ipinyou_reference"
# (staged file stem, id column in that file, lookup_type value)
REFERENCE_LOOKUPS: list[tuple[str, str, str]] = [
    ("city", "city_id", "city"),
    ("region", "region_id", "region"),
    ("user_profile_tags", "tag_id", "tag"),
]

VOLUMES = sorted({ANALYTICAL_VOLUME, REFERENCE_VOLUME})

CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "false",
    "enforceSchema": "false",  # validate each file's header, fail loud on a real mismatch
    "multiLine": "false",      # keep CSV splittable so large files read in parallel
}
FILE_EXT_PATTERN = r"csv"

# Characters Delta rejects in column identifiers. Any offending column is
# renamed (offending chars -> "_") right before the Bronze write; values
# are never touched.
_ILLEGAL_COL_CHARS = re.compile(r"[ ,;{}()\[\]\n\t=.]")


def volume_path(volume: str) -> str:
    return f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{volume}"


def dataset_file_pattern(stem: str) -> re.Pattern:
    # Matches "<stem>.csv", an optionally "ipinyou_"-prefixed variant, or a
    # physically chunked upload "ipinyou_<stem>_chunk_00001.csv". Chunk
    # boundaries disappear at Bronze.
    return re.compile(
        rf"^(?:{re.escape(SOURCE_PREFIX)}_)?{re.escape(stem)}"
        rf"(?:_chunk_\d{{5}})?\.(?:{FILE_EXT_PATTERN})$"
    )


def find_files(volume: str, stem: str) -> list[str]:
    all_files = [f.path for f in dbutils.fs.ls(volume_path(volume)) if not f.isDir()]
    pattern = dataset_file_pattern(stem)
    return sorted(p for p in all_files if pattern.match(p.rsplit("/", 1)[-1]))


def sanitize_columns(df: DataFrame) -> tuple[DataFrame, dict[str, str]]:
    renames: dict[str, str] = {}
    for col in df.columns:
        clean = _ILLEGAL_COL_CHARS.sub("_", col)
        clean = re.sub(r"_+", "_", clean).strip("_")
        if clean != col:
            renames[col] = clean
    final_names = [renames.get(c, c) for c in df.columns]
    if len(set(final_names)) != len(final_names):
        raise RuntimeError(f"column sanitization would collide: {final_names}")
    for old, new in renames.items():
        df = df.withColumnRenamed(old, new)
    return df, renames


def read_dataset(files: list[str]) -> DataFrame:
    reader = spark.read
    for key, value in CSV_OPTIONS.items():
        reader = reader.option(key, value)
    return reader.csv(files)

# COMMAND ----------

# DBTITLE 1,Verify source Volume(s) exist and are accessible
found_volumes = {
    row.volume_name
    for row in spark.sql(f"SHOW VOLUMES IN {CATALOG}.{BRONZE_SCHEMA}").collect()
}
missing_volumes = [v for v in VOLUMES if v not in found_volumes]
if missing_volumes:
    raise RuntimeError(
        f"FAIL  missing required Volume(s) in {CATALOG}.{BRONZE_SCHEMA}: {missing_volumes}"
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
    raise RuntimeError(f"FAIL  inaccessible Volume(s): {detail}")

print(f"OK  all {len(VOLUMES)} required Volume(s) present and accessible: {VOLUMES}")

# COMMAND ----------

# DBTITLE 1,Inspect source Volume file details
for volume in VOLUMES:
    vpath = volume_path(volume)
    items = dbutils.fs.ls(vpath)
    print(f"Volume: {vpath}")
    print(f"Files found: {len(items)}")
    print()
    for item in items:
        print(f"Name : {item.name}")
        print(f"Path : {item.path}")
        print(f"Size : {item.size / (1024 * 1024):.2f} MB")
        print("-" * 70)
    print()

# COMMAND ----------

# DBTITLE 1,Map each staged dataset to its source files
dataset_files: dict[str, list[str]] = {}
missing_dataset_files: list[str] = []

for dataset, volume, _table in DATASETS:
    matches = find_files(volume, dataset)
    dataset_files[dataset] = matches
    if not matches:
        missing_dataset_files.append(f"{dataset} (expected in {volume_path(volume)})")
    else:
        print(f"OK  {dataset}: {len(matches)} file(s) found in {volume_path(volume)}")

reference_files: dict[str, list[str]] = {}
for stem, _id_col, _lookup_type in REFERENCE_LOOKUPS:
    matches = find_files(REFERENCE_VOLUME, stem)
    reference_files[stem] = matches
    if not matches:
        missing_dataset_files.append(f"reference/{stem} (expected in {volume_path(REFERENCE_VOLUME)})")
    else:
        print(f"OK  reference/{stem}: {len(matches)} file(s) found in {volume_path(REFERENCE_VOLUME)}")

if missing_dataset_files:
    raise RuntimeError(
        f"FAIL  no source file(s) found for dataset(s): {missing_dataset_files}"
    )

print(f"OK  all {len(DATASETS)} pass-through dataset(s) and "
      f"{len(REFERENCE_LOOKUPS)} reference lookup(s) have at least one source file")

# COMMAND ----------

# DBTITLE 1,Load the pass-through datasets into their Bronze tables
load_results: list[dict] = []

for dataset, volume, table in DATASETS:
    files = dataset_files[dataset]
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
        df = read_dataset(files)

        df, sanitized = sanitize_columns(df)
        if sanitized:
            print(f"OK  {dataset}: sanitized column name(s) -- {sanitized}")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table)
        )
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

# DBTITLE 1,Build and load the merged reference table
reference_result = {
    "dataset": "reference",
    "volume": REFERENCE_VOLUME,
    "table": REFERENCE_TABLE,
    "files": sum(len(reference_files[stem]) for stem, _, _ in REFERENCE_LOOKUPS),
    "rows": None,
    "columns": None,
    "status": "FAILED",
    "error": None,
}
try:
    merged: DataFrame | None = None
    for stem, id_col, lookup_type in REFERENCE_LOOKUPS:
        part = read_dataset(reference_files[stem])
        if id_col in part.columns:
            part = part.withColumnRenamed(id_col, "lookup_id")
        elif "lookup_id" not in part.columns:
            raise RuntimeError(
                f"reference/{stem}: expected id column '{id_col}' not present -- "
                f"columns are {part.columns}"
            )
        part = part.withColumn("lookup_type", F.lit(lookup_type))
        ordered = ["lookup_type", "lookup_id"] + [
            c for c in part.columns if c not in ("lookup_type", "lookup_id")
        ]
        part = part.select(*ordered)
        merged = part if merged is None else merged.unionByName(part, allowMissingColumns=True)
        print(f"OK  reference/{stem}: {len(reference_files[stem])} file(s), "
              f"columns={part.columns}")

    merged, sanitized = sanitize_columns(merged)
    if sanitized:
        print(f"OK  reference: sanitized column name(s) -- {sanitized}")

    (
        merged.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(REFERENCE_TABLE)
    )
    row_count = spark.table(REFERENCE_TABLE).count()
    reference_result["rows"] = row_count
    reference_result["columns"] = len(merged.columns)
    reference_result["status"] = "LOADED"
    print(f"OK  reference: {row_count} rows from {reference_result['files']} file(s) "
          f"-> {REFERENCE_TABLE}")
except Exception as exc:
    reference_result["error"] = str(exc)
    print(f"FAIL  reference: could not load into {REFERENCE_TABLE} -- {exc}")

load_results.append(reference_result)

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

# DBTITLE 1,iPinYou Bronze load summary
print("=" * 70)
print("iPINYOU BRONZE LOAD SUMMARY")
print("=" * 70)
for result in load_results:
    print(
        f"{result['dataset']:<28} files={result['files']:<3} "
        f"rows={str(result['rows']):<12} status={result['status']:<7} "
        f"validated={result.get('validated')}"
    )
    if result["error"]:
        print(f"    load error:        {result['error']}")
    if result.get("validation_error"):
        print(f"    validation error:  {result['validation_error']}")
print("-" * 70)
print(f"Datasets loaded    : {sum(1 for r in load_results if r['status'] == 'LOADED')}/{len(load_results)}")
print(f"Datasets validated : {sum(1 for r in load_results if r.get('validated'))}/{len(load_results)}")
print(f"Overall result     : {'PASS' if overall_success else 'FAIL'}")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Delete source Volume(s) only if every dataset passed
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
        "FAIL  iPinYou Bronze load did not fully pass -- see summary above. "
        "Source Volume(s) were preserved."
    )