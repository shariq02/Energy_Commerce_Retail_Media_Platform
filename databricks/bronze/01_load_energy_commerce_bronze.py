# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ENERGY AND COMMERCE BRONZE LOAD
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** load the files of each energy and commerce source from the landing
# MAGIC Volume into its `bronze` tables, check each table, and remove the files of a
# MAGIC source whose tables all passed. One source folder in the Volume is one run.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ./_bronze_common

# COMMAND ----------

# DBTITLE 1,Imports
import re
from functools import reduce

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
RUN_SOURCES = []  # empty = every source with a file in the landing Volume
WRITE_MODE = "overwrite"  # "append" for the second and later parts of a split source
BRONZE_SCHEMA = "bronze"

CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "false",
    "enforceSchema": "false",  # verify every file's header, fail loud on a mismatch
    "multiLine": "false",  # keep the CSV splittable
}
CSV_EXTENSIONS = r"csv|csv\.gz|txt"

# COMMAND ----------

# DBTITLE 1,Sources
DWD_DATASETS = [
    "air_temperature",
    "cloudiness",
    "moisture",
    "precipitation",
    "pressure",
    "sun",
    "wind",
    "dew_point",
    "soil_temperature",
    "visibility",
    "cloud_type",
    "wind_synop",
    "extreme_wind",
    "weather_phenomena",
    "solar",
    "station_geography",
    "station_name_history",
    "device_instrument",
    "parameter_unit",
    "missing_value_periods",
]

MASTR_DATASETS = [
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
    "katalogkategorien",
    "katalogwerte",
]
# Four small code lists with the same Id / Wert shape share one table.
MASTR_CODE_LISTS = [
    "einheitentypen",
    "lokationstypen",
    "marktfunktionen",
    "marktrollen",
]

# source folder in the landing Volume ->
#   datasets: (file dataset name, Bronze table, value for the discriminator column)
#   discriminator: column added first when several datasets share one table
#   renames: source column names that Delta rejects, per dataset
#   options, per_file, format: how the files are read
SOURCES = {
    "dwd": {
        "datasets": [(d, f"dwd_{d}", None) for d in DWD_DATASETS],
        "renames": {
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
        },
    },
    "smard": {
        "datasets": [
            ("energy_timeseries", "smard_energy_timeseries", None),
            ("balancing_energy", "smard_balancing_energy", None),
            ("commercial_exchanges", "smard_commercial_exchanges", None),
        ],
    },
    "energy_charts": {
        "datasets": [("physical_flows", "energy_charts_physical_flows", None)],
    },
    "honda_iot": {
        "datasets": [
            (f"{kind}_{unit}", f"honda_iot_{kind}", unit.lower())
            for kind in ("electricity", "heating", "cooling")
            for unit in ("P", "W")
        ]
        + [("weather", "honda_iot_weather", None)],
        "discriminator": "measurement_type",
    },
    "rees46": {
        "datasets": [("events", "rees46_events", None)],
    },
    "mastr": {
        "datasets": [(d, f"mastr_{d}", None) for d in MASTR_DATASETS]
        + [(d, "mastr_code_lookup", d) for d in MASTR_CODE_LISTS],
        "discriminator": "catalog_kind",
        # MaStR text fields hold line breaks, and the column order of a chunk can differ
        "options": {"multiLine": "true", "quote": '"', "escape": '"'},
        "per_file": True,
    },
    "power_plant_list": {
        "datasets": [
            ("kraftwerksliste", "power_plant_list", None),
            ("zu_und_rueckbau", "power_plant_capacity_additions", None),
        ],
    },
    "redispatch": {
        "datasets": [("redispatch_measures", "redispatch_measures", None)],
    },
    "ga4": {
        "datasets": [("events", "ga4_events", None)],
        "format": "json",
    },
}

# COMMAND ----------

# DBTITLE 1,Helper -- field alignment checks
# A MaStR record starts with a MastrNummer. A row split by a line break leaves a
# text fragment in column 0, and a name column that reads as numbers means the
# fields moved.
MASTR_ID_PATTERN = r"^[A-Z]{2,4}[0-9]{6,}$"
MASTR_NAME_COLUMNS = {"marktakteure": ["MarktakteurNachname", "Firmenname"]}
MASTR_CODE_COLUMNS = {
    "marktakteure": ["Personenart", "MarktakteurAnrede", "Marktfunktion"]
}


def share(df, condition, present):
    row = df.agg(
        F.sum(present.cast("long")).alias("present"),
        F.sum(condition.cast("long")).alias("hit"),
    ).first()
    return row["present"], row["hit"]


def alignment_error(source: str, dataset: str, df) -> str | None:
    if source == "power_plant_list":
        unnamed = [c for c in df.columns if c.lower().startswith(("unnamed", "_c"))]
        if len(unnamed) > 0.2 * len(df.columns):
            return f"{len(unnamed)} of {len(df.columns)} columns are unnamed: the header was not applied"
    if source != "mastr":
        return None
    problems = []
    first = df.columns[0]
    if first.lower().endswith(("mastrnummer", "mastrnr")):
        text = F.trim(F.col(first).cast("string"))
        present = text.isNotNull() & (text != "")
        total, valid = share(df, present & text.rlike(MASTR_ID_PATTERN), present)
        if total and 1 - valid / total > 0.005:
            problems.append(
                f"{total - valid} of {total} values of {first!r} are no MastrNummer"
            )
    lower = {c.lower(): c for c in df.columns}
    for wanted in MASTR_NAME_COLUMNS.get(dataset, []):
        column = lower.get(wanted.lower())
        if column:
            text = F.col(column).cast("string")
            present = text.isNotNull() & (F.trim(text) != "")
            digits = F.regexp_replace(F.trim(text), r"[\d.,\-]", "").eqNullSafe("")
            total, numeric = share(df, present & digits, present)
            if total and numeric / total > 0.3:
                problems.append(
                    f"name column {column!r} is {numeric / total:.0%} numeric"
                )
    for wanted in MASTR_CODE_COLUMNS.get(dataset, []):
        column = lower.get(wanted.lower())
        if column:
            longest = df.agg(F.max(F.length(F.col(column).cast("string")))).first()[0]
            if longest and longest > 40:
                problems.append(
                    f"code column {column!r} has a {longest}-character value"
                )
    return "; ".join(problems) or None


# COMMAND ----------

# DBTITLE 1,Helper -- read one dataset


def read_dataset(spec: dict, files: list[str]):
    if spec.get("format") == "json":
        return spark.read.option("multiLine", "false").json(files)
    options = {**CSV_OPTIONS, **spec.get("options", {})}

    def read(paths):
        reader = spark.read
        for key, value in options.items():
            reader = reader.option(key, value)
        return reader.csv(paths)

    if spec.get("per_file"):
        frames = [read(path) for path in files]
        return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
    return read(files)


def prepare_frame(source: str, dataset: str, value, files: list[str]):
    spec = SOURCES[source]
    df = read_dataset(spec, files)
    error = alignment_error(source, dataset, df)
    if error:
        raise RuntimeError(f"field alignment is wrong: {error}")
    for old, new in spec.get("renames", {}).get(dataset, {}).items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    df, _ = sanitize_columns(df)
    if value is not None:
        columns = df.columns
        df = df.withColumn(spec["discriminator"], F.lit(value)).select(
            spec["discriminator"], *columns
        )
    return df


# COMMAND ----------

# DBTITLE 1,Select the sources and map each dataset to its files
# A file counts when it sits in its source folder or loose in the landing root.
if RUN_SOURCES:
    unknown = [s for s in RUN_SOURCES if s not in SOURCES]
    if unknown:
        raise RuntimeError(f"not an energy or commerce source: {unknown}")
folders, loose = set(landing_sources()), loose_files()

plan: dict[str, dict[str, list[str]]] = {}
seen: set[str] = set()
for source in RUN_SOURCES or SOURCES:
    paths = candidate_files(source, folders, loose)
    seen.update(paths)
    ext = r"jsonl" if SOURCES[source].get("format") == "json" else CSV_EXTENSIONS
    plan[source] = {}
    for dataset, _table, _value in SOURCES[source]["datasets"]:
        pattern = re.compile(
            rf"^(?:{re.escape(source)}_)?{re.escape(dataset)}(?:_chunk_\d{{5}})?\.(?:{ext})$"
        )
        plan[source][dataset] = sorted(
            p for p in paths if pattern.match(p.rsplit("/", 1)[-1])
        )
selected = list(RUN_SOURCES) or [s for s in plan if any(plan[s].values())]
if not selected:
    raise RuntimeError(f"FAIL  no energy or commerce file in {landing_root()}")
plan = {source: plan[source] for source in selected}
claim_files(plan, seen)
for source in selected:
    found = sum(1 for files in plan[source].values() if files)
    print(f"{source}: {found} of {len(plan[source])} datasets have files")

# COMMAND ----------

# DBTITLE 1,Load each Bronze table
load_results: list[dict] = []

for source in selected:
    groups: dict[str, list[tuple[str, object]]] = {}
    for dataset, table, value in SOURCES[source]["datasets"]:
        groups.setdefault(table, []).append((dataset, value))
    for table, members in groups.items():
        full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{table}"
        files = [f for d, _ in members for f in plan[source][d]]
        missing = [d for d, _ in members if not plan[source][d]]
        result = {
            "source": source,
            "table": full_name,
            "files": len(files),
            "rows": None,
            "validated": False,
            "error": None,
            "expected": {v for _, v in members if v is not None},
            "paths": files,
        }
        if missing:
            result["error"] = f"no file for {missing}"
        else:
            try:
                frames = [
                    prepare_frame(source, d, v, plan[source][d]) for d, v in members
                ]
                df = reduce(lambda a, b: a.unionByName(b), frames)
                result["rows"] = write_bronze(df, full_name, WRITE_MODE)
                print(f"OK    {table}: {result['rows']} rows from {len(files)} file(s)")
            except Exception as exc:
                result["error"] = str(exc)
                print(f"FAIL  {table}: {exc}")
        load_results.append(result)

# COMMAND ----------

# DBTITLE 1,Validate each table
for result in load_results:
    if result["rows"] is None:
        continue
    error = check_table(result["table"], result["rows"])
    if not error and result["expected"]:
        column = SOURCES[result["source"]]["discriminator"]
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
for source in selected:
    results = [r for r in load_results if r["source"] == source]
    if all(r["validated"] for r in results):
        remove_source(source, [p for r in results for p in r["paths"]])
        print(f"REMOVED    files of {source}")
    else:
        print(f"PRESERVED  files of {source} (a table did not pass)")

# COMMAND ----------

# DBTITLE 1,Summary
print_summary("ENERGY AND COMMERCE BRONZE LOAD", load_results)
if not all(r["validated"] for r in load_results):
    raise RuntimeError("FAIL  at least one table did not load; see the lines above")
