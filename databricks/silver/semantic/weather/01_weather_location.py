# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- WEATHER LOCATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** one row per weather place across DWD stations, AccuWeather
# MAGIC cities, the Honda site and Seattle, on a continent -> country -> region ->
# MAGIC city hierarchy; DWD relocation history in `weather_location_validity`.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "silver/semantic/weather/01_weather_location"
RID = run_id()

CONTRACT = load_contract("dwd")
_stations = CONTRACT["conventions"]["station_set"]
STATION_IDS = [str(s) for s in _stations["ids"]]
STATION_CITY = dict(zip(STATION_IDS, _stations["cities"], strict=True))

# COMMAND ----------

# DBTITLE 1,Configuration -- DWD station city to Bundesland
CITY_REGION = {
    "berlin": "Berlin",
    "hamburg": "Hamburg",
    "munich": "Bayern",
    "cologne_bonn": "Nordrhein-Westfalen",
    "frankfurt_am_main": "Hessen",
    "stuttgart": "Baden-Wuerttemberg",
    "essen": "Nordrhein-Westfalen",
    "leipzig": "Sachsen",
    "dresden": "Sachsen",
    "nuremberg": "Bayern",
    "hannover": "Niedersachsen",
    "bremen": "Bremen",
    "potsdam": "Brandenburg",
    "magdeburg": "Sachsen-Anhalt",
    "erfurt": "Thueringen",
    "trier": "Rheinland-Pfalz",
    "saarbruecken": "Saarland",
    "kiel": "Schleswig-Holstein",
    "rostock_warnemuende": "Mecklenburg-Vorpommern",
    "norderney": "Niedersachsen",
    "sylt": "Schleswig-Holstein",
    "garmisch_partenkirchen": "Bayern",
    "zugspitze": "Bayern",
    "hohenpeissenberg": "Bayern",
    "feldberg_schwarzwald": "Baden-Wuerttemberg",
    "cottbus": "Brandenburg",
    "goerlitz": "Sachsen",
    "braunschweig": "Niedersachsen",
}

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_station_geography
dwd_geo_bronze = read_bronze("dwd_station_geography")

# COMMAND ----------

# DBTITLE 1,Transform -- DWD station validity rows
dwd_validity = (
    dwd_geo_bronze.withColumn(
        "source_location_id",
        F.regexp_replace(F.trim(F.col("Stations_id")), r"\.0$", ""),
    )
    .filter(F.col("source_location_id").isin(STATION_IDS))
    .select(
        "source_location_id",
        F.col("Stationsname").alias("name"),
        F.col("`Geogr.Breite`").cast("double").alias("latitude"),
        F.col("`Geogr.Laenge`").cast("double").alias("longitude"),
        F.col("Stationshoehe").cast("double").alias("elevation_m"),
        F.to_date(parse_ts("von_datum", ("yyyyMMdd",), "UTC")).alias("valid_from"),
        F.to_date(parse_ts("bis_datum", ("yyyyMMdd",), "UTC")).alias("valid_to"),
    )
    .withColumn("location_key", location_key("dwd", "source_location_id"))
)

# COMMAND ----------

# DBTITLE 1,Transform -- DWD current station rows
_latest = Window.partitionBy("source_location_id").orderBy(
    F.col("valid_to").isNull().desc(),
    F.col("valid_from").desc(),
    F.col("name").asc(),
)
_city_lookup = spark.createDataFrame(
    [(sid, slug, CITY_REGION[slug]) for sid, slug in STATION_CITY.items()],
    "source_location_id string, city_slug string, region string",
)
dwd_locations = (
    dwd_validity.withColumn("_rn", F.row_number().over(_latest))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "valid_from", "valid_to")
    .join(F.broadcast(_city_lookup), "source_location_id", "left")
    .withColumn("city", F.initcap(F.regexp_replace("city_slug", "_", " ")))
    .withColumn("location_role", F.lit("station"))
    .withColumn("country_code", F.lit("DE"))
    .withColumn("continent", F.lit("EU"))
    .withColumn("geography_basis", F.lit("curated_city_lookup"))
    .drop("city_slug")
)

# COMMAND ----------

# DBTITLE 1,Read Samples -- AccuWeather city set
aw_cities_src = spark.table("samples.accuweather.historical_hourly_metric").select(
    "city_name", "country_code", "latitude", "longitude"
)

# COMMAND ----------

# DBTITLE 1,Transform -- AccuWeather city rows
aw_locations = (
    aw_cities_src.distinct()
    .withColumn("source_location_id", F.col("city_name"))
    .withColumn("location_key", location_key("accuweather", "source_location_id"))
    .withColumn("name", F.initcap("city_name"))
    .withColumn("city", F.initcap("city_name"))
    .withColumn("country_code", F.upper("country_code"))
    .withColumn("continent", continent_of("country_code"))
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .withColumn("location_role", F.lit("city"))
    .withColumn("geography_basis", F.lit("source_field"))
    .drop("city_name")
)

# COMMAND ----------

# DBTITLE 1,Transform -- single-site rows (Honda site, Seattle)
_site_rows = [
    ("honda_iot", "honda_site", "site", "Honda site", "EU", "DE", None, None),
    (
        "weather_seattle",
        "seattle",
        "city",
        "Seattle",
        "NA",
        "US",
        "Washington",
        "Seattle",
    ),
]
site_locations = (
    spark.createDataFrame(
        _site_rows,
        "source_system string, source_location_id string, location_role string, "
        "name string, continent string, country_code string, region string, "
        "city string",
    )
    .withColumn("geography_basis", F.lit("source_documentation"))
    .withColumn("location_key", sha_key("source_system", "source_location_id"))
    .withColumn("source_dataset", F.lit("site_definition"))
    .withColumn("_silver_loaded_at", F.current_timestamp())
    .withColumn("_silver_run_id", F.lit(RID))
)

# COMMAND ----------

# DBTITLE 1,Transform -- union and conform weather_location
locations = (
    add_semantic_provenance(dwd_locations, "dwd", "dwd_station_geography", RID)
    .unionByName(
        add_semantic_provenance(
            aw_locations, "accuweather", "historical_hourly_metric", RID
        ),
        allowMissingColumns=True,
    )
    .unionByName(site_locations, allowMissingColumns=True)
    .withColumn("source_record_id", sha_key("source_system", "source_location_id"))
)
weather_location = conform(locations, WEATHER_LOCATION_COLUMNS)

# COMMAND ----------

# DBTITLE 1,Write Silver -- weather_location
write_semantic(
    weather_location,
    "weather_location",
    source="weather",
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_location_validity
weather_location_validity = conform(
    add_semantic_provenance(
        dwd_validity, "dwd", "dwd_station_geography", RID
    ).withColumn(
        "source_record_id",
        sha_key(
            "source_location_id", "valid_from", "valid_to", "latitude", "longitude"
        ),
    ),
    WEATHER_LOCATION_VALIDITY_COLUMNS,
)

# COMMAND ----------

# DBTITLE 1,Write Silver -- weather_location_validity
write_semantic(
    weather_location_validity,
    "weather_location_validity",
    source="weather",
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- weather_location
location_df = spark.table(semantic_table("weather_location"))
location_blocks = inspect_table(
    location_df,
    "weather_location",
    source=FINDINGS_SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["location_key"],
    extra_checks=structure_extra_checks(location_df),
)

# COMMAND ----------

# DBTITLE 1,Export findings -- weather_location
write_silver_findings(
    FINDINGS_SOURCE,
    f"{COMPONENT.split('/')[-1]}__weather_location",
    "weather_location",
    location_blocks,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- weather_location_validity
validity_df = spark.table(semantic_table("weather_location_validity"))
validity_blocks = inspect_table(
    validity_df,
    "weather_location_validity",
    source=FINDINGS_SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["location_key", "valid_from", "valid_to", "latitude", "longitude"],
    extra_checks=structure_extra_checks(validity_df),
)

# COMMAND ----------

# DBTITLE 1,Export findings -- weather_location_validity
write_silver_findings(
    FINDINGS_SOURCE,
    f"{COMPONENT.split('/')[-1]}__weather_location_validity",
    "weather_location_validity",
    validity_blocks,
)