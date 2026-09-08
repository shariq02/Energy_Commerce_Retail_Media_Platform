# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DWD REFERENCE / METADATA
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the 5 DWD metadata Bronze tables into source-scoped Silver
# MAGIC reference tables at their source grain, plus two curated/derived
# MAGIC reference tables (`dwd_parameter_catalog`, `dwd_city_bundesland_xref`).
# MAGIC Parser-trailer and spurious-header rows are filtered (row-quarantine +
# MAGIC audit). Runs before the DWD measurement notebooks, which read
# MAGIC `dwd_parameter_catalog`, `dwd_station_geography` and
# MAGIC `dwd_city_bundesland_xref`.
# MAGIC
# MAGIC `dwd_missing_value_periods` (~5.9M rows) is an isolated final section --
# MAGIC its own single scan, no shared work with the tiny metadata tables.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F

SOURCE = "dwd"
COMPONENT = "silver/energy/_reference/01_dwd_reference"
RID = run_id()

CONTRACT = load_contract(SOURCE)
MAPPING = load_mapping(SOURCE)
TABLES = contract_tables(CONTRACT)
META_FIELDS = (MAPPING.get("business_names", {}) or {}).get("metadata_fields", {})
STATION_IDS = [str(s) for s in CONTRACT["conventions"]["station_set"]["ids"]]

# Curated 28-station city -> (Bundesland name, Bundesland AGS). `city` is the
# DWD download folder key and also the Bronze `city` column value. Bundesland
# level only -- the conformed spine has no polygon geometry, so DWD lat/lon is
# kept unchanged and administrative attribution is this lookup.
CITY_BUNDESLAND = {
    "berlin": ("Berlin", "11"),
    "hamburg": ("Hamburg", "02"),
    "munich": ("Bayern", "09"),
    "cologne_bonn": ("Nordrhein-Westfalen", "05"),
    "frankfurt_am_main": ("Hessen", "06"),
    "stuttgart": ("Baden-Wuerttemberg", "08"),
    "essen": ("Nordrhein-Westfalen", "05"),
    "leipzig": ("Sachsen", "14"),
    "dresden": ("Sachsen", "14"),
    "nuremberg": ("Bayern", "09"),
    "hannover": ("Niedersachsen", "03"),
    "bremen": ("Bremen", "04"),
    "potsdam": ("Brandenburg", "12"),
    "magdeburg": ("Sachsen-Anhalt", "15"),
    "erfurt": ("Thueringen", "16"),
    "trier": ("Rheinland-Pfalz", "07"),
    "saarbruecken": ("Saarland", "10"),
    "kiel": ("Schleswig-Holstein", "01"),
    "rostock_warnemuende": ("Mecklenburg-Vorpommern", "13"),
    "norderney": ("Niedersachsen", "03"),
    "sylt": ("Schleswig-Holstein", "01"),
    "garmisch_partenkirchen": ("Bayern", "09"),
    "zugspitze": ("Bayern", "09"),
    "hohenpeissenberg": ("Bayern", "09"),
    "feldberg_schwarzwald": ("Baden-Wuerttemberg", "08"),
    "cottbus": ("Brandenburg", "12"),
    "goerlitz": ("Sachsen", "14"),
    "braunschweig": ("Niedersachsen", "03"),
}
assert len(CITY_BUNDESLAND) == 28, len(CITY_BUNDESLAND)

# COMMAND ----------

# DBTITLE 1,Helpers


def clean_station_id(id_col):
    return F.regexp_replace(F.trim(F.col(id_col)), r"\.0$", "")


def keep_real_stations(df, id_col, bronze_table):
    """Row-quarantine parser-trailer / spurious-header rows (id not a curated
    numeric station id). The kept frame has `station_id` normalised."""
    df = df.withColumn("station_id", clean_station_id(id_col))
    kept, q = row_quarantine(
        df,
        ~F.col("station_id").isin(STATION_IDS),
        rule_id="metadata_trailer_row",
        reason="id column not a curated numeric station id (trailer/header row)",
        field_name=id_col,
        value_col="station_id",
        sr_id_col="station_id",
        source_system=SOURCE,
        bronze_table=bronze_table,
    )
    write_quarantine(q, RID)
    return kept.drop(id_col)


def rename_meta(df, bronze_table):
    for col in TABLES[bronze_table]["columns"]:
        n = col["name"]
        tgt = META_FIELDS.get(n)
        if tgt and tgt != n and n in df.columns:
            df = df.withColumnRenamed(n, tgt)
    return df


# COMMAND ----------

# DBTITLE 1,dwd_city_bundesland_xref (curated derived reference)
xref = spark.createDataFrame(
    [(c, bl, ags, "bundesland") for c, (bl, ags) in CITY_BUNDESLAND.items()],
    "city string, bundesland_name string, ags_code string, ags_level string",
)
write_silver(
    xref, "dwd_city_bundesland_xref", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,dwd_station_geography (SCD, source grain)
BT = "dwd_station_geography"
sg = keep_real_stations(read_bronze(BT), "Stations_id", BT)
sg = (
    sg.withColumn("latitude", F.col("`Geogr.Breite`").cast("double"))
    .withColumn("longitude", F.col("`Geogr.Laenge`").cast("double"))
    .withColumn("station_elevation_m", F.col("Stationshoehe").cast("double"))
    .withColumn("valid_from", parse_ts("von_datum", ("yyyyMMdd",), "UTC"))
    .withColumn("valid_to", parse_ts("bis_datum", ("yyyyMMdd",), "UTC"))
    .withColumnRenamed("Stationsname", "station_name")
    .drop("Geogr.Breite", "Geogr.Laenge", "Stationshoehe", "von_datum", "bis_datum")
)
sg, q = value_quarantine(
    sg,
    bbox_outside_de("latitude", "longitude"),
    flag_col="_coord_outside_de_bbox",
    rule_id="coord_outside_de_bbox",
    reason="station coordinate outside the Germany bounding box",
    field_name="latitude,longitude",
    value_col="latitude",
    sr_id_col="station_id",
    source_system=SOURCE,
    bronze_table=BT,
)
write_quarantine(q, RID)
sg = add_provenance(sg, SOURCE, sha_key("station_id", "valid_from"), RID)
write_silver(sg, "dwd_station_geography", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,dwd_station_name_history (SCD, source grain)
BT = "dwd_station_name_history"
snh = keep_real_stations(read_bronze(BT), "Stations_ID", BT)
snh = (
    snh.withColumnRenamed("Stationsname", "station_name")
    .withColumn("valid_from", parse_ts("Von_Datum", ("yyyyMMdd",), "UTC"))
    .withColumn("valid_to", parse_ts("Bis_Datum", ("yyyyMMdd",), "UTC"))
    .drop("Von_Datum", "Bis_Datum")
)
snh = add_provenance(snh, SOURCE, sha_key("station_id", "valid_from"), RID)
write_silver(
    snh, "dwd_station_name_history", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,dwd_device_instrument (source grain)
BT = "dwd_device_instrument"
di = keep_real_stations(read_bronze(BT), "Stations_ID", BT)
di = rename_meta(di, BT)
di = di.withColumn("valid_from", parse_ts("valid_from", ("yyyyMMdd",), "UTC"))
di = di.withColumn("valid_to", parse_ts("valid_to", ("yyyyMMdd",), "UTC"))
di = add_provenance(
    di, SOURCE, sha_key("station_id", "parameter_category", "valid_from"), RID
)
write_silver(di, "dwd_device_instrument", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,dwd_parameter_unit (source grain) + derived dwd_parameter_catalog
BT = "dwd_parameter_unit"
pu = keep_real_stations(read_bronze(BT), "Stations_ID", BT)
pu = rename_meta(pu, BT)
pu = pu.withColumn("valid_from", parse_ts("valid_from", ("yyyyMMdd",), "UTC"))
pu = pu.withColumn("valid_to", parse_ts("valid_to", ("yyyyMMdd",), "UTC"))
pu = add_provenance(
    pu, SOURCE, sha_key("station_id", "parameter_source_code", "valid_from"), RID
)
write_silver(pu, "dwd_parameter_unit", source=SOURCE, component=COMPONENT, rid=RID)

# derived: source parameter code -> business name -> physical unit
param_bn = (MAPPING.get("business_names", {}) or {}).get("parameters", {})
bn_df = spark.createDataFrame(
    [(c, s["business_name"], s.get("unit")) for c, s in param_bn.items()],
    "parameter_source_code string, parameter_business_name string, mapped_unit string",
)
pu_units = (
    read_bronze("dwd_parameter_unit")
    .select(
        F.trim(F.col("Parameter")).alias("parameter_source_code"),
        F.trim(F.col("Einheit")).alias("parameter_unit"),
        F.trim(F.col("Parameterbeschreibung")).alias("parameter_description_de"),
    )
    .filter(F.col("parameter_source_code").isNotNull())
    .dropDuplicates(["parameter_source_code"])
)
cat = (
    bn_df.join(pu_units, "parameter_source_code", "left")
    .withColumn("parameter_unit", F.coalesce("parameter_unit", "mapped_unit"))
    .withColumn("catalog_vintage", F.lit("dwd_hourly_historical_20260904"))
    .drop("mapped_unit")
)
write_silver(cat, "dwd_parameter_catalog", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,dwd_missing_value_periods (isolated -- ~5.9M rows, one scan)
BT = "dwd_missing_value_periods"
mvp = keep_real_stations(read_bronze(BT), "Stations_ID", BT)
mvp = (
    mvp.withColumnRenamed("Stations_Name", "station_name")
    .withColumnRenamed("Parameter", "parameter_source_code")
    .withColumn("gap_start_ts", parse_ts("Von_Datum", ("dd.MM.yyyy-HH:mm",), "UTC"))
    .withColumn("gap_end_ts", parse_ts("Bis_Datum", ("dd.MM.yyyy-HH:mm",), "UTC"))
    .withColumn("missing_value_count", F.col("Anzahl_Fehlwerte").cast("bigint"))
    .withColumnRenamed("Beschreibung", "gap_description")
    .drop("Von_Datum", "Bis_Datum", "Anzahl_Fehlwerte")
    .dropDuplicates()
)
mvp = within_group_ordinal(
    mvp,
    ["station_id", "parameter_source_code", "gap_start_ts", "gap_end_ts"],
    ["missing_value_count", "gap_description", "eor"],
)
mvp = add_provenance(
    mvp,
    SOURCE,
    sha_key(
        "station_id",
        "parameter_source_code",
        "gap_start_ts",
        "gap_end_ts",
        "_src_id_ord",
    ),
    RID,
)
write_silver(
    mvp, "dwd_missing_value_periods", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Summary
audit(COMPONENT, SOURCE, "reference_tables_written", 7.0, status="PASS", rid=RID)
print("=" * 70)
print(f"DWD REFERENCE -- COMPLETE  (run_id {RID})")
print("=" * 70)
