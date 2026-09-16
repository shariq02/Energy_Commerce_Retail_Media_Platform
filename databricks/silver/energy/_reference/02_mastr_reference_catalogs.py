# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR REFERENCE CATALOGS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the six MaStR reference-catalog Bronze tables into
# MAGIC source-scoped Silver at their source grain. `mastr_katalogwerte` (with
# MAGIC `mastr_katalogkategorien`) is the decode authority every other MaStR
# MAGIC notebook joins to, so this notebook runs first.


# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "silver/energy/_reference/02_mastr_reference_catalogs"
RID = run_id()

CONTRACT = load_contract(SOURCE)
TABLES = contract_tables(CONTRACT)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_katalogkategorien
_katalogkategorien_bronze = read_bronze("mastr_katalogkategorien")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_katalogwerte
_katalogwerte_bronze = read_bronze("mastr_katalogwerte")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_einheitentypen
_einheitentypen_bronze = read_bronze("mastr_einheitentypen")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_lokationstypen
_lokationstypen_bronze = read_bronze("mastr_lokationstypen")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_marktfunktionen
_marktfunktionen_bronze = read_bronze("mastr_marktfunktionen")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_marktrollen
_marktrollen_bronze = read_bronze("mastr_marktrollen")

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_katalogkategorien
_katalogkategorien_pk = TABLES["mastr_katalogkategorien"]["grain"]["key"][0]
katalogkategorien = _katalogkategorien_bronze.withColumn(
    "_srid", F.col(_katalogkategorien_pk).cast("string")
)
katalogkategorien = add_provenance(katalogkategorien, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_katalogkategorien
write_silver(
    katalogkategorien,
    "mastr_katalogkategorien",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_katalogkategorien + export findings
_findings_blocks = inspect_table(
    katalogkategorien,
    "mastr_katalogkategorien",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_katalogkategorien_pk],
    df_before=_katalogkategorien_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_katalogkategorien",
    "mastr_katalogkategorien",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_katalogwerte
_katalogwerte_pk = TABLES["mastr_katalogwerte"]["grain"]["key"][0]
katalogwerte = _katalogwerte_bronze.withColumn(
    "_srid", F.col(_katalogwerte_pk).cast("string")
)
katalogwerte = add_provenance(katalogwerte, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_katalogwerte
write_silver(
    katalogwerte, "mastr_katalogwerte", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_katalogwerte + export findings
_findings_blocks = inspect_table(
    katalogwerte,
    "mastr_katalogwerte",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_katalogwerte_pk],
    df_before=_katalogwerte_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_katalogwerte",
    "mastr_katalogwerte",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_einheitentypen
_einheitentypen_pk = TABLES["mastr_einheitentypen"]["grain"]["key"][0]
einheitentypen = _einheitentypen_bronze.withColumn(
    "_srid", F.col(_einheitentypen_pk).cast("string")
)
einheitentypen = add_provenance(einheitentypen, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_einheitentypen
write_silver(
    einheitentypen, "mastr_einheitentypen", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_einheitentypen + export findings
_findings_blocks = inspect_table(
    einheitentypen,
    "mastr_einheitentypen",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_einheitentypen_pk],
    df_before=_einheitentypen_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_einheitentypen",
    "mastr_einheitentypen",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_lokationstypen
_lokationstypen_pk = TABLES["mastr_lokationstypen"]["grain"]["key"][0]
lokationstypen = _lokationstypen_bronze.withColumn(
    "_srid", F.col(_lokationstypen_pk).cast("string")
)
lokationstypen = add_provenance(lokationstypen, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_lokationstypen
write_silver(
    lokationstypen, "mastr_lokationstypen", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_lokationstypen + export findings
_findings_blocks = inspect_table(
    lokationstypen,
    "mastr_lokationstypen",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_lokationstypen_pk],
    df_before=_lokationstypen_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_lokationstypen",
    "mastr_lokationstypen",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_marktfunktionen
_marktfunktionen_pk = TABLES["mastr_marktfunktionen"]["grain"]["key"][0]
marktfunktionen = _marktfunktionen_bronze.withColumn(
    "_srid", F.col(_marktfunktionen_pk).cast("string")
)
marktfunktionen = add_provenance(marktfunktionen, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_marktfunktionen
write_silver(
    marktfunktionen,
    "mastr_marktfunktionen",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_marktfunktionen + export findings
_findings_blocks = inspect_table(
    marktfunktionen,
    "mastr_marktfunktionen",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_marktfunktionen_pk],
    df_before=_marktfunktionen_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_marktfunktionen",
    "mastr_marktfunktionen",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_marktrollen
_marktrollen_pk = TABLES["mastr_marktrollen"]["grain"]["key"][0]
marktrollen = _marktrollen_bronze.withColumn(
    "_srid", F.col(_marktrollen_pk).cast("string")
)
marktrollen = add_provenance(marktrollen, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_marktrollen
write_silver(
    marktrollen, "mastr_marktrollen", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_marktrollen + export findings
_findings_blocks = inspect_table(
    marktrollen,
    "mastr_marktrollen",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[_marktrollen_pk],
    df_before=_marktrollen_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_marktrollen",
    "mastr_marktrollen",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Summary
audit(
    COMPONENT,
    SOURCE,
    "catalog_tables_written",
    float(6),
    status="PASS",
    rid=RID,
)
