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

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "mastr"
COMPONENT = "silver/energy/grid/02_mastr_grid_topology"
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

PRIMARY_TABLES = {
    "mastr_lokationen": "MastrNummer",
    "mastr_netzanschlusspunkte": "NetzanschlusspunktMastrNummer",
    "mastr_netze": "MastrNummer",
    "mastr_bilanzierungsgebiete": "Id",
}

# COMMAND ----------

# DBTITLE 1,Primary topology tables -> Silver


def process(bt: str, pk: str) -> None:
    bronze_df = read_bronze(bt)
    df = bronze_df
    if bt == "mastr_bilanzierungsgebiete":
        df = df.dropDuplicates()
    df = mastr_standardise(df, NAME_MAP, CODED, source=SOURCE)
    id_col = NAME_MAP.get(pk, pk)
    df = df.withColumn("_srid", F.col(id_col).cast("string"))
    df = add_provenance(df, SOURCE, "_srid", RID)
    write_silver(df, bt, source=SOURCE, component=COMPONENT, rid=RID)
    _findings_blocks = inspect_table(
        df,
        bt,
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
        key_cols=[id_col],
        df_before=bronze_df,
    )
    write_silver_findings(
        SOURCE,
        f"{COMPONENT.split('/')[-1]}__{bt}",
        bt,
        _findings_blocks,
    )


for _bt, _pk in PRIMARY_TABLES.items():
    process(_bt, _pk)

# COMMAND ----------

# DBTITLE 1,Additive location bridges
_lok = read_bronze("mastr_lokationen")
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

# DBTITLE 1,MASTR-3 -- location coordinate-conflict flag (additive)
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
        print(f"SKIP {_t} in MASTR-3 coordinate check: {exc}")
        continue
    _coords = _part if _coords is None else _coords.unionByName(_part)

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
    print("MASTR-3: no generation-unit Silver tables available yet -- skipped.")