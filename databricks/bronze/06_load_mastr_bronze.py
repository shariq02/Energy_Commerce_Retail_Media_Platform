# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # BRONZE DATA LOADING -- MASTR (ENERGY)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** Load uploaded Unity Catalog Volume data for MaStR (energy
# MAGIC domain) into the frozen Bronze table structure.

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

SOURCE_PREFIX = "mastr"

MASTR_ANALYTICAL_VOLUME = "mastr_analytical"
MASTR_REFERENCE_VOLUME = "mastr_reference"

ANALYTICAL_DATASETS = [
    "einheiten_wind",
    "einheiten_biomasse",
    "einheiten_wasser",
    "einheiten_verbrennung",
    "einheiten_kernkraft",
    "einheiten_geothermie_gsgk",
    "anlagen_eeg_wind",
    "anlagen_eeg_biomasse",
    "anlagen_eeg_wasser",
    "anlagen_eeg_geothermie_gsgk",
    "anlagen_kwk",
    "marktakteure",
    "marktakteure_und_rollen",
    "netzanschlusspunkte",
    "netze",
    "lokationen",
    "bilanzierungsgebiete",
    "einheiten_genehmigung",
    "geloeschte_deaktivierte_einheiten",
    "geloeschte_deaktivierte_marktakteure",
    "einheiten_aenderung_netzbetreiberzuordnungen",
    "ertuechtigungen",
]

REFERENCE_DATASETS = [
    "einheitentypen",
    "katalogkategorien",
    "katalogwerte",
    "lokationstypen",
    "marktfunktionen",
    "marktrollen",
]

# (dataset_name, source_volume, fully-qualified Bronze table name)
DATASETS: list[tuple[str, str, str]] = [
    (d, MASTR_ANALYTICAL_VOLUME, f"{CATALOG}.{BRONZE_SCHEMA}.mastr_{d}")
    for d in ANALYTICAL_DATASETS
] + [
    (d, MASTR_REFERENCE_VOLUME, f"{CATALOG}.{BRONZE_SCHEMA}.mastr_{d}")
    for d in REFERENCE_DATASETS
]

VOLUMES = sorted({volume for _, volume, _ in DATASETS})

# MaStR free-text fields (turbine descriptions, company names, addresses)
# routinely contain embedded newlines -- the staging writer quotes them
# correctly, but Spark with multiLine=false treats the newline as a record
# terminator, splitting one logical record into several physical rows and
# shifting every field after the break (observed on einheiten_wind: 2,128
# newline-containing records became ~2,170 extra misaligned rows, with plant
# names in the status column and a bogus year-3220 commissioning date).
# multiLine=true costs file-split parallelism; MaStR chunk files are <=150 MB
# and the large tables have 19-23 of them, so they still read in parallel.
CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "false",
    "enforceSchema": "false",  # validate each file's header, fail loud on a real mismatch
    "multiLine": "true",
    "quote": '"',
    "escape": '"',  # RFC-4180 doubled-quote escaping used by the staging writer
}

# Per-dataset read-option overrides, if a specific table ever needs one.
CSV_OPTIONS_OVERRIDE: dict[str, dict[str, str]] = {}

# After load, a name column must not read as numeric and a short-code column
# must not carry long free text -- either means the field alignment is wrong.
STRUCTURAL_CHECKS: dict[str, dict[str, list[str]]] = {
    "marktakteure": {
        "name_cols": ["MarktakteurNachname", "Firmenname"],
        "code_cols": ["Personenart", "MarktakteurAnrede", "Marktfunktion"],
    },
}

COLUMN_RENAME_MAP: dict[str, dict[str, str]] = {}

# Characters Delta rejects in column identifiers. Any offending column is
# renamed (offending chars -> "_") right before the Bronze write; values
# are never touched.
_ILLEGAL_COL_CHARS = re.compile(r"[ ,;{}()\[\]\n\t=.]")


def volume_path(volume: str) -> str:
    return f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{volume}"


def dataset_file_pattern(dataset: str) -> re.Pattern:
    # Matches "<source>_<dataset>_chunk_00001.csv" (MaStR staging always
    # writes through the chunk writer, even when a dataset fits in one
    # chunk).
    return re.compile(
        rf"^{re.escape(SOURCE_PREFIX)}_{re.escape(dataset)}_chunk_\d{{5}}\.csv$"
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


def read_dataset(files: list[str], dataset: str) -> DataFrame:
    # Read each chunk file separately and combine by column name rather than
    # passing all files to one reader.csv(files) call. Staged MaStR chunks
    # can have differently ordered headers -- optional XML fields are
    # sometimes absent from a record, shifting the remaining tags' order --
    # and Spark's multi-file CSV reader requires every file's header to
    # match the first file's column order exactly, failing the whole
    # dataset on any such drift (observed on marktakteure and lokationen).
    opts = {**CSV_OPTIONS, **CSV_OPTIONS_OVERRIDE.get(dataset, {})}
    reader = spark.read
    for key, value in opts.items():
        reader = reader.option(key, value)
    frames = [reader.csv(f) for f in files]
    df = frames[0]
    for other in frames[1:]:
        df = df.unionByName(other, allowMissingColumns=True)
    return df


# Every MaStR record's first column is a MastrNummer: 2-4 uppercase letters
# then a long digit run. A row split by an embedded newline leaves a free-text
# fragment (or empty) in column 0, so a non-trivial share of column-0 values
# not matching this pattern means the CSV field alignment is broken.
_MASTR_ID_RE = r"^[A-Z]{2,4}[0-9]{6,}$"


def id_column_error(dataset: str, df: DataFrame) -> str | None:
    if not df.columns:
        return None
    first = df.columns[0]
    # Only the analytical tables lead with a MastrNummer; the reference/catalog
    # tables lead with a plain integer Id, which this pattern must not flag.
    if not first.lower().endswith(("mastrnummer", "mastrnr")):
        return None
    s = F.trim(F.col(first).cast("string"))
    present = s.isNotNull() & (s != "")
    ok = present & s.rlike(_MASTR_ID_RE)
    r = df.agg(
        F.sum(present.cast("long")).alias("p"),
        F.sum(ok.cast("long")).alias("ok"),
    ).first()
    if not r["p"]:
        return None
    bad_rate = 1 - (r["ok"] / r["p"])
    if bad_rate > 0.005:
        return (
            f"{bad_rate:.1%} of column {first!r} values are not a MastrNummer "
            f"({r['p'] - r['ok']} of {r['p']}) -- records split on an embedded newline"
        )
    return None


def structural_alignment_error(dataset: str, df: DataFrame) -> str | None:
    # Returns a message if a name column reads as numeric or a code column
    # carries long free text -- both mean the CSV field alignment is wrong.
    spec = STRUCTURAL_CHECKS.get(dataset)
    if not spec:
        return None
    lower = {c.lower(): c for c in df.columns}
    problems: list[str] = []
    for want in spec.get("name_cols", []):
        col = lower.get(want.lower())
        if not col:
            continue
        s = F.col(col).cast("string")
        present = s.isNotNull() & (F.trim(s) != "")
        numeric = present & F.regexp_replace(F.trim(s), r"[\d.,\-]", "").eqNullSafe("")
        r = df.agg(
            F.sum(present.cast("long")).alias("p"),
            F.sum(numeric.cast("long")).alias("n"),
        ).first()
        if r["p"] and r["n"] / r["p"] > 0.3:
            problems.append(
                f"name column {col!r} is {r['n'] / r['p']:.0%} numeric (expected ~0%)"
            )
    for want in spec.get("code_cols", []):
        col = lower.get(want.lower())
        if not col:
            continue
        r = df.agg(F.max(F.length(F.col(col).cast("string"))).alias("mx")).first()
        if r["mx"] and r["mx"] > 40:
            problems.append(
                f"code column {col!r} has a {r['mx']}-char value (expected a short code)"
            )
    return "; ".join(problems) or None


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

# DBTITLE 1,Load each dataset into its Bronze table
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
        df = read_dataset(files, dataset)

        alignment_error = id_column_error(dataset, df) or structural_alignment_error(
            dataset, df
        )
        if alignment_error:
            raise RuntimeError(
                f"structural check failed -- CSV field alignment is wrong: {alignment_error}. "
                "Adjust the read options for this dataset and re-stage the raw export."
            )

        explicit = COLUMN_RENAME_MAP.get(dataset, {})
        applied = {old: new for old, new in explicit.items() if old in df.columns}
        for old, new in applied.items():
            df = df.withColumnRenamed(old, new)

        df, sanitized = sanitize_columns(df)
        if applied or sanitized:
            print(
                f"OK  {dataset}: normalized column name(s) -- explicit={applied} sanitized={sanitized}"
            )

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

# DBTITLE 1,MaStR Bronze load summary
print("=" * 70)
print("MASTR BRONZE LOAD SUMMARY")
print("=" * 70)
for result in load_results:
    print(
        f"{result['dataset']:<44} files={result['files']:<3} "
        f"rows={result['rows']!s:<10} status={result['status']:<7} "
        f"validated={result.get('validated')}"
    )
    if result["error"]:
        print(f"    load error:        {result['error']}")
    if result.get("validation_error"):
        print(f"    validation error:  {result['validation_error']}")
print("-" * 70)
print(
    f"Datasets loaded    : {sum(1 for r in load_results if r['status'] == 'LOADED')}/{len(DATASETS)}"
)
print(
    f"Datasets validated : {sum(1 for r in load_results if r.get('validated'))}/{len(DATASETS)}"
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
        "FAIL  MaStR Bronze load did not fully pass -- see summary above. "
        "Source Volume(s) were preserved."
    )
