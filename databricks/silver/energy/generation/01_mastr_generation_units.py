# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- MASTR GENERATION UNITS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the six MaStR `einheiten_*` carrier Bronze tables into
# MAGIC source-scoped Silver at their unit grain (`EinheitMastrNummer`), one
# MAGIC block per table (read / transform / write / inspect each their own
# MAGIC cell) -- catalog decode, English business names, capacities in kW,
# MAGIC coordinates unchanged (out-of-Germany points recorded to quarantine),
# MAGIC AGS from the Gemeindeschluessel prefix. Runs after
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
COMPONENT = "silver/energy/generation/01_mastr_generation_units"
RID = run_id()

MAPPING = load_mapping(SOURCE)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# Drop only columns constant BY CONSTRUCTION (single-carrier tables,
# German-only register). Other flagged constants may be incidental -- kept.
STRUCTURAL_CONSTANT_COLS = ["Energietraeger", "Land"]

# COMMAND ----------

# DBTITLE 1,Helper -- drop structural constants (definition only)


def _drop_structural_constants(df):
    """Drop STRUCTURAL_CONSTANT_COLS by whichever name (raw or business-
    renamed) is actually present -- mastr_standardise may have already
    renamed them. Silently no-ops for a column that isn't present."""
    for raw in STRUCTURAL_CONSTANT_COLS:
        target = NAME_MAP.get(raw, raw)
        if target in df.columns:
            df = df.drop(target)
        elif raw in df.columns:
            df = df.drop(raw)
    return df


# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_einheiten_wind
_bronze_wind = read_bronze("mastr_einheiten_wind")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_einheiten_biomasse
_bronze_biomasse = read_bronze("mastr_einheiten_biomasse")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_einheiten_wasser
_bronze_wasser = read_bronze("mastr_einheiten_wasser")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_einheiten_verbrennung
_bronze_verbrennung = read_bronze("mastr_einheiten_verbrennung")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_einheiten_kernkraft
_bronze_kernkraft = read_bronze("mastr_einheiten_kernkraft")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- mastr_einheiten_geothermie_gsgk
_bronze_geothermie_gsgk = read_bronze("mastr_einheiten_geothermie_gsgk")

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_einheiten_wind
_df_wind = mastr_standardise(_bronze_wind, NAME_MAP, CODED, source=SOURCE)
_df_wind = _drop_structural_constants(_df_wind)

_bad_wind = _df_wind.filter(bbox_outside_de("latitude", "longitude"))
write_quarantine(
    _q_rows(
        _bad_wind,
        "coord_outside_de_bbox",
        "unit coordinate outside the Germany bounding box",
        "latitude,longitude",
        "latitude",
        "unit_id",
        SOURCE,
        "mastr_einheiten_wind",
    ),
    RID,
)

_df_wind = attach_ags_prefix(_df_wind, "municipality_key_ags")
_df_wind = _df_wind.withColumn("_srid", F.col("unit_id").cast("string"))
_df_wind = add_provenance(_df_wind, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_einheiten_wind
write_silver(
    _df_wind, "mastr_einheiten_wind", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_einheiten_wind
_findings_blocks = inspect_table(
    _df_wind,
    "mastr_einheiten_wind",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["unit_id"],
    df_before=_bronze_wind,
    extra_checks={
        "structural_constants_dropped": ",".join(
            c
            for c in STRUCTURAL_CONSTANT_COLS
            if NAME_MAP.get(c, c) not in _df_wind.columns and c not in _df_wind.columns
        )
    },
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_einheiten_wind",
    "mastr_einheiten_wind",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_einheiten_biomasse
_df_biomasse = mastr_standardise(_bronze_biomasse, NAME_MAP, CODED, source=SOURCE)
_df_biomasse = _drop_structural_constants(_df_biomasse)

_bad_biomasse = _df_biomasse.filter(bbox_outside_de("latitude", "longitude"))
write_quarantine(
    _q_rows(
        _bad_biomasse,
        "coord_outside_de_bbox",
        "unit coordinate outside the Germany bounding box",
        "latitude,longitude",
        "latitude",
        "unit_id",
        SOURCE,
        "mastr_einheiten_biomasse",
    ),
    RID,
)

_df_biomasse = attach_ags_prefix(_df_biomasse, "municipality_key_ags")
_df_biomasse = _df_biomasse.withColumn("_srid", F.col("unit_id").cast("string"))
_df_biomasse = add_provenance(_df_biomasse, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_einheiten_biomasse
write_silver(
    _df_biomasse,
    "mastr_einheiten_biomasse",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_einheiten_biomasse
_findings_blocks = inspect_table(
    _df_biomasse,
    "mastr_einheiten_biomasse",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["unit_id"],
    df_before=_bronze_biomasse,
    extra_checks={
        "structural_constants_dropped": ",".join(
            c
            for c in STRUCTURAL_CONSTANT_COLS
            if NAME_MAP.get(c, c) not in _df_biomasse.columns
            and c not in _df_biomasse.columns
        )
    },
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_einheiten_biomasse",
    "mastr_einheiten_biomasse",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_einheiten_wasser
_df_wasser = mastr_standardise(_bronze_wasser, NAME_MAP, CODED, source=SOURCE)
_df_wasser = _drop_structural_constants(_df_wasser)

_bad_wasser = _df_wasser.filter(bbox_outside_de("latitude", "longitude"))
write_quarantine(
    _q_rows(
        _bad_wasser,
        "coord_outside_de_bbox",
        "unit coordinate outside the Germany bounding box",
        "latitude,longitude",
        "latitude",
        "unit_id",
        SOURCE,
        "mastr_einheiten_wasser",
    ),
    RID,
)

_df_wasser = attach_ags_prefix(_df_wasser, "municipality_key_ags")
_df_wasser = _df_wasser.withColumn("_srid", F.col("unit_id").cast("string"))
_df_wasser = add_provenance(_df_wasser, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_einheiten_wasser
write_silver(
    _df_wasser, "mastr_einheiten_wasser", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_einheiten_wasser
_findings_blocks = inspect_table(
    _df_wasser,
    "mastr_einheiten_wasser",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["unit_id"],
    df_before=_bronze_wasser,
    extra_checks={
        "structural_constants_dropped": ",".join(
            c
            for c in STRUCTURAL_CONSTANT_COLS
            if NAME_MAP.get(c, c) not in _df_wasser.columns
            and c not in _df_wasser.columns
        )
    },
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_einheiten_wasser",
    "mastr_einheiten_wasser",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_einheiten_verbrennung
_df_verbrennung = mastr_standardise(_bronze_verbrennung, NAME_MAP, CODED, source=SOURCE)
_df_verbrennung = _drop_structural_constants(_df_verbrennung)

_bad_verbrennung = _df_verbrennung.filter(bbox_outside_de("latitude", "longitude"))
write_quarantine(
    _q_rows(
        _bad_verbrennung,
        "coord_outside_de_bbox",
        "unit coordinate outside the Germany bounding box",
        "latitude,longitude",
        "latitude",
        "unit_id",
        SOURCE,
        "mastr_einheiten_verbrennung",
    ),
    RID,
)

_df_verbrennung = attach_ags_prefix(_df_verbrennung, "municipality_key_ags")
_df_verbrennung = _df_verbrennung.withColumn("_srid", F.col("unit_id").cast("string"))
_df_verbrennung = add_provenance(_df_verbrennung, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_einheiten_verbrennung
write_silver(
    _df_verbrennung,
    "mastr_einheiten_verbrennung",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_einheiten_verbrennung
_findings_blocks = inspect_table(
    _df_verbrennung,
    "mastr_einheiten_verbrennung",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["unit_id"],
    df_before=_bronze_verbrennung,
    extra_checks={
        "structural_constants_dropped": ",".join(
            c
            for c in STRUCTURAL_CONSTANT_COLS
            if NAME_MAP.get(c, c) not in _df_verbrennung.columns
            and c not in _df_verbrennung.columns
        )
    },
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_einheiten_verbrennung",
    "mastr_einheiten_verbrennung",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_einheiten_kernkraft
_df_kernkraft = mastr_standardise(_bronze_kernkraft, NAME_MAP, CODED, source=SOURCE)
_df_kernkraft = _drop_structural_constants(_df_kernkraft)

_bad_kernkraft = _df_kernkraft.filter(bbox_outside_de("latitude", "longitude"))
write_quarantine(
    _q_rows(
        _bad_kernkraft,
        "coord_outside_de_bbox",
        "unit coordinate outside the Germany bounding box",
        "latitude,longitude",
        "latitude",
        "unit_id",
        SOURCE,
        "mastr_einheiten_kernkraft",
    ),
    RID,
)

_df_kernkraft = attach_ags_prefix(_df_kernkraft, "municipality_key_ags")
_df_kernkraft = _df_kernkraft.withColumn("_srid", F.col("unit_id").cast("string"))
_df_kernkraft = add_provenance(_df_kernkraft, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_einheiten_kernkraft
write_silver(
    _df_kernkraft,
    "mastr_einheiten_kernkraft",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_einheiten_kernkraft
_findings_blocks = inspect_table(
    _df_kernkraft,
    "mastr_einheiten_kernkraft",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["unit_id"],
    df_before=_bronze_kernkraft,
    extra_checks={
        "structural_constants_dropped": ",".join(
            c
            for c in STRUCTURAL_CONSTANT_COLS
            if NAME_MAP.get(c, c) not in _df_kernkraft.columns
            and c not in _df_kernkraft.columns
        )
    },
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_einheiten_kernkraft",
    "mastr_einheiten_kernkraft",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- mastr_einheiten_geothermie_gsgk
_df_geothermie_gsgk = mastr_standardise(
    _bronze_geothermie_gsgk, NAME_MAP, CODED, source=SOURCE
)
_df_geothermie_gsgk = _drop_structural_constants(_df_geothermie_gsgk)

_bad_geothermie_gsgk = _df_geothermie_gsgk.filter(
    bbox_outside_de("latitude", "longitude")
)
write_quarantine(
    _q_rows(
        _bad_geothermie_gsgk,
        "coord_outside_de_bbox",
        "unit coordinate outside the Germany bounding box",
        "latitude,longitude",
        "latitude",
        "unit_id",
        SOURCE,
        "mastr_einheiten_geothermie_gsgk",
    ),
    RID,
)

_df_geothermie_gsgk = attach_ags_prefix(_df_geothermie_gsgk, "municipality_key_ags")
_df_geothermie_gsgk = _df_geothermie_gsgk.withColumn(
    "_srid", F.col("unit_id").cast("string")
)
_df_geothermie_gsgk = add_provenance(_df_geothermie_gsgk, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- mastr_einheiten_geothermie_gsgk
write_silver(
    _df_geothermie_gsgk,
    "mastr_einheiten_geothermie_gsgk",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- mastr_einheiten_geothermie_gsgk
_findings_blocks = inspect_table(
    _df_geothermie_gsgk,
    "mastr_einheiten_geothermie_gsgk",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["unit_id"],
    df_before=_bronze_geothermie_gsgk,
    extra_checks={
        "structural_constants_dropped": ",".join(
            c
            for c in STRUCTURAL_CONSTANT_COLS
            if NAME_MAP.get(c, c) not in _df_geothermie_gsgk.columns
            and c not in _df_geothermie_gsgk.columns
        )
    },
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__mastr_einheiten_geothermie_gsgk",
    "mastr_einheiten_geothermie_gsgk",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Summary
audit(
    COMPONENT,
    SOURCE,
    "unit_tables_written",
    6.0,
    status="PASS",
    rid=RID,
)
