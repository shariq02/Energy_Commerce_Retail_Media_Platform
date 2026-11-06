# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # LAYER RETIREMENT -- DROP A LAYER AFTER ITS SUCCESSOR IS CHECKED
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** One notebook retires old layers from the catalog. The
# MAGIC `RETIREMENTS` list in the configuration names, for each layer, the schemas to
# MAGIC drop and the schemas that must hold its data first (the successor). A layer is
# MAGIC dropped only when every successor schema exists, holds tables, and none of them
# MAGIC is empty. A schema that does not exist is skipped, so the list may name layers
# MAGIC that are not built yet.
# MAGIC
# MAGIC The default is a dry run: the notebook lists what it would drop and writes the
# MAGIC findings file. The three drop switches stay off until you have read the plan.
# MAGIC Safe to re-run. The staged source files stay on the external disk.

# COMMAND ----------

# DBTITLE 1,Imports
import os
from datetime import UTC, datetime

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
FINDINGS_SUBDIR = "src/inventory"

# name -> schemas to drop, and the schemas that must hold the data before the drop
RETIREMENTS = {
    "bronze (energy and commerce)": {
        "drop": ["bronze"],
        "successor": ["energy_silver", "energy_silver_reference", "commerce_silver"],
    },
    "genomics bronze": {
        "drop": ["genomics_bronze"],
        "successor": ["genomics_silver", "genomics_silver_reference"],
    },
}

# successor tables that may be empty without blocking the drop, as "schema.table"
ALLOW_EMPTY = set()

# Destructive steps stay off until the plan has been read.
DROP_TABLES = False  # True
DROP_VOLUMES = False  # True
DROP_SCHEMAS = False  # True

print(f"Catalog : {CATALOG}")
print(f"Drop tables and views : {DROP_TABLES}")
print(f"Drop Volumes : {DROP_VOLUMES}")
print(f"Drop schemas : {DROP_SCHEMAS}")

# COMMAND ----------

# DBTITLE 1,Utility library
# MAGIC %run ./_utility_common

# COMMAND ----------

# DBTITLE 1,Find the findings directory
FINDINGS_DIR = os.path.join(find_repo_root(), FINDINGS_SUBDIR)
os.makedirs(FINDINGS_DIR, exist_ok=True)
print(f"Findings directory: {FINDINGS_DIR}")


# COMMAND ----------

# DBTITLE 1,Find the schemas of the catalog
existing_schemas = {
    r["databaseName"] for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()
}
print(f"Schemas in the catalog: {len(existing_schemas)}")

# COMMAND ----------

# DBTITLE 1,List what each layer holds (tables, views, Volumes)
inventory = {}  # schema -> {"tables": [...], "views": [...], "volumes": [...]}
for name, spec in RETIREMENTS.items():
    for schema in spec["drop"]:
        if schema not in existing_schemas:
            print(f"SKIP  {name}: schema {schema} does not exist")
            continue
        kinds = {
            r["table_name"]: r["table_type"]
            for r in spark.sql(
                f"SELECT table_name, table_type FROM {CATALOG}.information_schema.tables "
                f"WHERE table_schema = '{schema}'"
            ).collect()
        }
        inventory[schema] = {
            "tables": sorted(t for t, k in kinds.items() if k != "VIEW"),
            "views": sorted(t for t, k in kinds.items() if k == "VIEW"),
            "volumes": sorted(
                r["volume_name"]
                for r in spark.sql(f"SHOW VOLUMES IN {CATALOG}.{schema}").collect()
            ),
        }
        held = inventory[schema]
        print(
            f"{name}: {schema} holds {len(held['tables'])} tables, "
            f"{len(held['views'])} views, {len(held['volumes'])} Volumes"
        )

# COMMAND ----------

# DBTITLE 1,Check the successor schemas of each layer
checks = []  # (layer, successor schema, tables, empty tables, passes)
layer_passes = {}
for name, spec in RETIREMENTS.items():
    if not any(schema in inventory for schema in spec["drop"]):
        continue
    ok = True
    for schema in spec["successor"]:
        if schema not in existing_schemas:
            checks.append((name, schema, 0, "schema missing", False))
            ok = False
            continue
        tables = sorted(
            r["tableName"]
            for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()
        )
        empty = [
            t
            for t in tables
            if f"{schema}.{t}" not in ALLOW_EMPTY
            and spark.table(f"{CATALOG}.{schema}.{t}").isEmpty()
        ]
        passes = bool(tables) and not empty
        ok = ok and passes
        detail = ", ".join(empty) if empty else ("no tables" if not tables else "")
        checks.append((name, schema, len(tables), detail, passes))
    layer_passes[name] = ok
    print(f"{'PASS' if ok else 'BLOCKED'}  {name}")
for name, schema, n, detail, passes in checks:
    print(f"  {schema}: {n} tables  {'ok' if passes else 'NOT ok: ' + detail}")

# COMMAND ----------

# DBTITLE 1,Write the findings file
generated_at = datetime.now(UTC)
plan_rows = [
    (
        name,
        schema,
        len(inventory[schema]["tables"]),
        len(inventory[schema]["views"]),
        len(inventory[schema]["volumes"]),
        "drop" if layer_passes.get(name) else "keep (successor not ready)",
    )
    for name, spec in RETIREMENTS.items()
    for schema in spec["drop"]
    if schema in inventory
]
findings_text = "\n".join(
    [
        "# Layer retirement plan",
        "",
        f"_Generated: {generated_at.strftime('%Y-%m-%dT%H:%MZ')}_",
        "",
        (
            f"Switches: tables and views {DROP_TABLES}, Volumes {DROP_VOLUMES}, "
            f"schemas {DROP_SCHEMAS}"
        ),
        "",
        "## Schemas to drop",
        "",
        markdown_table(
            ["layer", "schema", "tables", "views", "volumes", "decision"], plan_rows
        ),
        "",
        "## Successor check",
        "",
        markdown_table(
            ["layer", "successor schema", "tables", "problem", "passes"], checks
        ),
        "",
    ]
)
findings_path = os.path.join(
    FINDINGS_DIR, f"retirement_{generated_at.strftime('%Y%m%d')}.md"
)
with open(findings_path, "w", encoding="utf-8") as fh:
    fh.write(findings_text)
print(f"Written: {findings_path}")

# COMMAND ----------

# DBTITLE 1,Drop the tables and views of the layers that pass
for name, spec in RETIREMENTS.items():
    if not layer_passes.get(name):
        continue
    for schema in spec["drop"]:
        if schema not in inventory:
            continue
        for kind, sql_kind in (("views", "VIEW"), ("tables", "TABLE")):
            for obj in inventory[schema][kind]:
                if DROP_TABLES:
                    spark.sql(f"DROP {sql_kind} IF EXISTS {CATALOG}.{schema}.{obj}")
                    print(f"DROP  {sql_kind.lower()} {schema}.{obj}")
                else:
                    print(f"WOULD DROP  {sql_kind.lower()} {schema}.{obj}")

# COMMAND ----------

# DBTITLE 1,Drop the Volumes of the layers that pass
for name, spec in RETIREMENTS.items():
    if not layer_passes.get(name):
        continue
    for schema in spec["drop"]:
        if schema not in inventory:
            continue
        for volume in inventory[schema]["volumes"]:
            if DROP_VOLUMES:
                spark.sql(f"DROP VOLUME IF EXISTS {CATALOG}.{schema}.{volume}")
                print(f"DROP  volume {schema}.{volume}")
            else:
                print(f"WOULD DROP  volume {schema}.{volume}")

# COMMAND ----------

# DBTITLE 1,Drop the schemas of the layers that pass
for name, spec in RETIREMENTS.items():
    if not layer_passes.get(name):
        continue
    for schema in spec["drop"]:
        if schema not in inventory:
            continue
        if not DROP_SCHEMAS:
            print(f"WOULD DROP  schema {schema}")
            continue
        try:
            spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema}")
            print(f"DROP  schema {schema}")
        except Exception as exc:
            print(f"KEEP  schema {schema}: {str(exc)[:200]}")

# COMMAND ----------

# DBTITLE 1,Result
schemas_now = {
    r["databaseName"] for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()
}
for schema in inventory:
    if schema not in schemas_now:
        print(f"{schema}: dropped")
        continue
    left = len(spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect())
    volumes_left = len(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{schema}").collect())
    print(f"{schema}: {left} tables and {volumes_left} Volumes left")

# COMMAND ----------

# MAGIC %md
# MAGIC After a real drop, run `databricks/utility/03_catalog_inventory.py` for the new
# MAGIC object and Volume counts, and `databricks/schema_registry/01_snapshot_schema.py`
# MAGIC for the schema snapshot.
