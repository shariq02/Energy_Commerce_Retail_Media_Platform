# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR GRID TOPOLOGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `mastr_lokationen`, `mastr_netzanschlusspunkte`,
# MAGIC `mastr_netze` and `mastr_bilanzierungsgebiete` into source-scoped Silver
# MAGIC at their own grain, plus two additive bridges from the location link
# MAGIC arrays (`mastr_location_unit_bridge`,
# MAGIC `mastr_location_connection_bridge`). Runs after
# MAGIC `02_mastr_reference_catalogs`.


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
COMPONENT = "silver/energy/grid/02_mastr_grid_topology"
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_lokationen
_lok_bronze = read_bronze("mastr_lokationen")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_netzanschlusspunkte
_nap_bronze = read_bronze("mastr_netzanschlusspunkte")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_netze
_netze_bronze = read_bronze("mastr_netze")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_bilanzierungsgebiete
_bil_bronze = read_bronze("mastr_bilanzierungsgebiete")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_lokationen (source for both bridges)
_lok = read_bronze("mastr_lokationen")

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_lokationen
lok = mastr_standardise(_lok_bronze, NAME_MAP, CODED, source=SOURCE)
lok = lok.withColumn(
    "_srid", F.col(NAME_MAP.get("MastrNummer", "MastrNummer")).cast("string")
)
lok = add_provenance(lok, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_lokationen
write_silver(lok, "mastr_lokationen", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_lokationen + export findings
_findings_blocks = inspect_table(
    lok,
    "mastr_lokationen",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[NAME_MAP.get("MastrNummer", "MastrNummer")],
    df_before=_lok_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_lokationen",
    "mastr_lokationen",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_netzanschlusspunkte
nap = mastr_standardise(_nap_bronze, NAME_MAP, CODED, source=SOURCE)
nap = nap.withColumn(
    "_srid",
    F.col(
        NAME_MAP.get("NetzanschlusspunktMastrNummer", "NetzanschlusspunktMastrNummer")
    ).cast("string"),
)
nap = add_provenance(nap, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_netzanschlusspunkte
write_silver(
    nap, "mastr_netzanschlusspunkte", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_netzanschlusspunkte + export findings
_findings_blocks = inspect_table(
    nap,
    "mastr_netzanschlusspunkte",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[
        NAME_MAP.get("NetzanschlusspunktMastrNummer", "NetzanschlusspunktMastrNummer")
    ],
    df_before=_nap_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_netzanschlusspunkte",
    "mastr_netzanschlusspunkte",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_netze
netze = mastr_standardise(_netze_bronze, NAME_MAP, CODED, source=SOURCE)
netze = netze.withColumn(
    "_srid", F.col(NAME_MAP.get("MastrNummer", "MastrNummer")).cast("string")
)
netze = add_provenance(netze, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_netze
write_silver(netze, "mastr_netze", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_netze + export findings
_findings_blocks = inspect_table(
    netze,
    "mastr_netze",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[NAME_MAP.get("MastrNummer", "MastrNummer")],
    df_before=_netze_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_netze",
    "mastr_netze",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_bilanzierungsgebiete
bil = _bil_bronze.dropDuplicates()
bil = mastr_standardise(bil, NAME_MAP, CODED, source=SOURCE)
bil = bil.withColumn("_srid", F.col(NAME_MAP.get("Id", "Id")).cast("string"))
bil = add_provenance(bil, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_bilanzierungsgebiete
write_silver(
    bil, "mastr_bilanzierungsgebiete", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_bilanzierungsgebiete + export findings
_findings_blocks = inspect_table(
    bil,
    "mastr_bilanzierungsgebiete",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=[NAME_MAP.get("Id", "Id")],
    df_before=_bil_bronze,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_bilanzierungsgebiete",
    "mastr_bilanzierungsgebiete",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Write mastr_location_unit_bridge (additive)
_bridge1 = explode_link_bridge(
    _lok,
    "MastrNummer",
    "VerknuepfteEinheitenMaStRNummern",
    "mastr_location_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    bronze_table="mastr_lokationen",
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_location_unit_bridge + export findings
_findings_blocks = inspect_table(
    _bridge1,
    "mastr_location_unit_bridge",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_location_unit_bridge",
    "mastr_location_unit_bridge",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Write mastr_location_connection_bridge (additive)
_bridge2 = explode_link_bridge(
    _lok,
    "MastrNummer",
    "NetzanschlusspunkteMaStRNummern",
    "mastr_location_connection_bridge",
    source=SOURCE,
    component=COMPONENT,
    bronze_table="mastr_lokationen",
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect mastr_location_connection_bridge + export findings
_findings_blocks = inspect_table(
    _bridge2,
    "mastr_location_connection_bridge",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["parent_id", "linked_id"],
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_location_connection_bridge",
    "mastr_location_connection_bridge",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Coordinate-conflict check -- read the generation-unit Silver tables
# Reads the generation-unit Silver tables (must run first). Flags conflicting
# coordinates per location_id; never picks a "correct" one.
_GENERATION_UNIT_TABLES = [
    "mastr_einheiten_wind",
    "mastr_einheiten_biomasse",
    "mastr_einheiten_wasser",
    "mastr_einheiten_verbrennung",
    "mastr_einheiten_kernkraft",
    "mastr_einheiten_geothermie_gsgk",
]

_coords = None
for _t in _GENERATION_UNIT_TABLES:
    try:
        _part = (
            read_silver(_t)
            .filter(
                F.col("location_id").isNotNull()
                & F.col("latitude").isNotNull()
                & F.col("longitude").isNotNull()
            )
            .select(
                "location_id",
                F.round("latitude", 2).alias("lat2dp"),
                F.round("longitude", 2).alias("lon2dp"),
            )
        )
    except Exception as exc:
        print(f"SKIP {_t} in coordinate-conflict check: {exc}")
        continue
    _coords = _part if _coords is None else _coords.unionByName(_part)

# COMMAND ----------

# DBTITLE 1,Build + write mastr_location_coordinate_conflict (additive)
if _coords is not None:
    _conflict = (
        _coords.dropDuplicates(["location_id", "lat2dp", "lon2dp"])
        .groupBy("location_id")
        .agg(
            F.countDistinct(F.concat_ws(",", "lat2dp", "lon2dp")).alias(
                "distinct_coords"
            )
        )
        .withColumn("_coordinate_conflict", F.col("distinct_coords") > 1)
    )
    _conflict = _conflict.withColumn("_srid", F.col("location_id").cast("string"))
    _conflict = add_provenance(_conflict, SOURCE, "_srid", RID)
    write_silver(
        _conflict,
        "mastr_location_coordinate_conflict",
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
    )

# COMMAND ----------

# DBTITLE 1,Inspect mastr_location_coordinate_conflict + export findings
if _coords is not None:
    _findings_blocks = inspect_table(
        _conflict,
        "mastr_location_coordinate_conflict",
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
        key_cols=["location_id"],
        extra_checks={
            "conflicting_location_count": _conflict.filter(
                F.col("_coordinate_conflict")
            ).count(),
        },
    )
    write_silver_findings(
        SOURCE,
        f"{COMPONENT.split('/')[-1]}__mastr_location_coordinate_conflict",
        "mastr_location_coordinate_conflict",
        _findings_blocks,
    )
else:
    print("no generation-unit Silver tables available yet -- coordinate check skipped.")
