# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # BRONZE DATA LOADING -- HONDA IOT (IOT / ENERGY CONSUMPTION)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** August 2026
# MAGIC
# MAGIC **Purpose:** Load uploaded Unity Catalog Volume data for the Honda
# MAGIC Research Institute Smart Building dataset (iot domain) into the frozen
# MAGIC Bronze table structure.

# COMMAND ----------

# DBTITLE 1,Imports
import re
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
BRONZE_SCHEMA = "bronze"

SOURCE_PREFIX = "honda_iot"

# (staging dataset name, source Volume, fully-qualified Bronze table name)
# Staging dataset names keep the source's P/W casing; Bronze table names are
# lower-case. The P and W datasets of a metric share one Bronze table.
DATASETS: list[tuple[str, str, str]] = [
    (
        "electricity_P",
        "honda_iot_analytical",
        f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_electricity",
    ),
    (
        "electricity_W",
        "honda_iot_analytical",
        f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_electricity",
    ),
    (
        "heating_P",
        "honda_iot_analytical",
        f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_heating",
    ),
    (
        "heating_W",
        "honda_iot_analytical",
        f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_heating",
    ),
    (
        "cooling_P",
        "honda_iot_analytical",
        f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_cooling",
    ),
    (
        "cooling_W",
        "honda_iot_analytical",
        f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_cooling",
    ),
    ("weather", "honda_iot_analytical", f"{CATALOG}.{BRONZE_SCHEMA}.honda_iot_weather"),
]

# Column added, as the first column, to a table that holds several datasets,
# and the value each dataset gets in it.
DISCRIMINATOR_COLUMN = "measurement_type"
DISCRIMINATOR_VALUE = {
    "electricity_P": "p",
    "electricity_W": "w",
    "heating_P": "p",
    "heating_W": "w",
    "cooling_P": "p",
    "cooling_W": "w",
}

VOLUMES = sorted({volume for _, volume, _ in DATASETS})

# Read settings for this source's staged files.
READ_FORMAT = "csv"  # "csv" or "json"
CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "false",
    "enforceSchema": "false",  # validate each file's header, fail loud on a real mismatch
    "multiLine": "false",
}  # used only when READ_FORMAT == "csv"
FILE_EXT_PATTERN = r"csv|csv\.gz"

COLUMN_RENAME_MAP: dict[str, dict[str, str]] = {}

# Characters Delta rejects in column identifiers. Any offending column is
# renamed (offending chars -> "_") right before the Bronze write; values
# are never touched. Honda's dotted sensor column names (e.g.
# "WeatherStation.Weather.Ta") are handled here.
_ILLEGAL_COL_CHARS = re.compile(r"[ ,;{}()\[\]\n\t=.]")


def volume_path(volume: str) -> str:
    return f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{volume}"


def dataset_file_pattern(dataset: str) -> re.Pattern:
    # Matches "<dataset>.<ext>" (single staged file), an optionally
    # "<source>_"-prefixed variant, or a physically chunked upload
    # "<dataset>_chunk_00001.<ext>". Chunk boundaries disappear at Bronze.
    return re.compile(
        rf"^(?:{re.escape(SOURCE_PREFIX)}_)?{re.escape(dataset)}"
        rf"(?:_chunk_\d{{5}})?\.(?:{FILE_EXT_PATTERN})$"
    )


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
    if READ_FORMAT == "json":
        return spark.read.option("multiLine", "true").json(files)
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
    all_files = [f.path for f in dbutils.fs.ls(volume_path(volume)) if not f.isDir()]
    pattern = dataset_file_pattern(dataset)
    matches = sorted(p for p in all_files if pattern.match(p.rsplit("/", 1)[-1]))
    dataset_files[dataset] = matches
    if not matches:
        missing_dataset_files.append(f"{dataset} (expected in {volume_path(volume)})")
    else:
        print(f"OK  {dataset}: {len(matches)} file(s) found in {volume_path(volume)}")

if missing_dataset_files:
    raise RuntimeError(
        f"FAIL  no source file(s) found for dataset(s): {missing_dataset_files}"
    )

print(f"OK  all {len(DATASETS)} dataset(s) have at least one source file")

# COMMAND ----------

# DBTITLE 1,Group the staged datasets by Bronze table
table_groups: dict[str, list[tuple[str, str]]] = {}
for dataset, volume, table in DATASETS:
    table_groups.setdefault(table, []).append((dataset, volume))

print(f"{len(DATASETS)} dataset(s) -> {len(table_groups)} Bronze table(s)")

# COMMAND ----------


# DBTITLE 1,Helper -- read and normalise one staged dataset
def prepare_frame(dataset: str, files: list[str]) -> DataFrame:
    df = read_dataset(files)

    explicit = COLUMN_RENAME_MAP.get(dataset, {})
    applied = {old: new for old, new in explicit.items() if old in df.columns}
    for old, new in applied.items():
        df = df.withColumnRenamed(old, new)

    df, sanitized = sanitize_columns(df)
    if applied or sanitized:
        print(
            f"OK  {dataset}: normalized column name(s) -- explicit={applied} sanitized={sanitized}"
        )

    value = DISCRIMINATOR_VALUE.get(dataset)
    if value is not None:
        cols = df.columns
        df = df.withColumn(DISCRIMINATOR_COLUMN, F.lit(value)).select(
            DISCRIMINATOR_COLUMN, *cols
        )
    return df


# COMMAND ----------

# DBTITLE 1,Load each Bronze table from its staged dataset(s)
load_results: list[dict] = []

for table, members in table_groups.items():
    datasets = [dataset for dataset, _volume in members]
    files_total = sum(len(dataset_files[d]) for d in datasets)
    result = {
        "dataset": table.rsplit(".", 1)[-1],
        "volume": members[0][1],
        "table": table,
        "files": files_total,
        "rows": None,
        "columns": None,
        "discriminator_values": {
            DISCRIMINATOR_VALUE[d] for d in datasets if d in DISCRIMINATOR_VALUE
        },
        "status": "FAILED",
        "error": None,
    }
    try:
        frames = [prepare_frame(d, dataset_files[d]) for d in datasets]
        df = reduce(lambda a, b: a.unionByName(b), frames)

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
        print(f"OK  {datasets}: {row_count} rows from {files_total} file(s) -> {table}")
    except Exception as exc:
        result["error"] = str(exc)
        print(f"FAIL  {datasets}: could not load into {table} -- {exc}")
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
        expected = result["discriminator_values"]
        if expected:
            found = {
                r[0]
                for r in spark.table(table)
                .select(DISCRIMINATOR_COLUMN)
                .distinct()
                .collect()
            }
            if found != expected:
                raise RuntimeError(
                    f"{DISCRIMINATOR_COLUMN} values {sorted(found)} != expected {sorted(expected)}"
                )
        result["validated"] = True
        result["validation_error"] = None
        print(
            f"OK  {table}: schema has {len(schema.fields)} column(s), {actual_rows} rows verified"
        )
    except (AnalysisException, RuntimeError) as exc:
        result["validated"] = False
        result["validation_error"] = str(exc)
        print(f"FAIL  {table}: validation error -- {exc}")

all_loaded = all(r["status"] == "LOADED" for r in load_results)
all_validated = all(r.get("validated") for r in load_results)
all_files_ok = len(missing_dataset_files) == 0
overall_success = all_loaded and all_validated and all_files_ok

# COMMAND ----------

# DBTITLE 1,Honda IoT Bronze load summary
print("=" * 70)
print("HONDA IOT BRONZE LOAD SUMMARY")
print("=" * 70)
for result in load_results:
    print(
        f"{result['dataset']:<28} files={result['files']:<3} "
        f"rows={result['rows']!s:<12} status={result['status']:<7} "
        f"validated={result.get('validated')}"
    )
    if result["error"]:
        print(f"    load error:        {result['error']}")
    if result.get("validation_error"):
        print(f"    validation error:  {result['validation_error']}")
print("-" * 70)
print(
    f"Tables loaded      : {sum(1 for r in load_results if r['status'] == 'LOADED')}/{len(table_groups)}"
)
print(
    f"Tables validated   : {sum(1 for r in load_results if r.get('validated'))}/{len(table_groups)}"
)
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
        print(
            f"PRESERVED  volume kept (load/validation did not fully pass): {volume_path(volume)}"
        )

print("-" * 70)
print("VOLUME CLEANUP RESULT")
for volume, status in volume_cleanup.items():
    print(f"  {volume}: {status}")
print("=" * 70)

if not overall_success:
    raise RuntimeError(
        "FAIL  Honda IoT Bronze load did not fully pass -- see summary above. "
        "Source Volume(s) were preserved."
    )
