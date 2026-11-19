# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GOLD ENERGY -- MARKET, WEATHER AND DEVICES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the 14 market, site, weather and device tables of `energy_gold`:
# MAGIC prices, Honda channel readings (long form), market and site energy components,
# MAGIC forecast and actual pairs, daily and four themed weather tables, devices, plant
# MAGIC samples, entity links and grid interventions.

# COMMAND ----------

# DBTITLE 1,Semantic structures library (meter channels, weather families, grain)
# MAGIC %run ../../silver/_energy_commerce_common

# COMMAND ----------

# DBTITLE 1,Gold library
# MAGIC %run ../_gold_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "smard"
COMPONENT = "gold/energy/02_market_weather_devices"
RID = gold_run_id()
GATE = {"source": SOURCE, "component": COMPONENT, "rid": RID}
COMMON = {**GATE, "schema": gold_schema_for(SOURCE)}
SITE_SOURCE = "honda_iot"
# forecast scope -> native label of its actual (other scopes: generation_<scope>)
FORECAST_ACTUAL_LABELS = {
    "consumption_total": "total_power_consumption",
    "residual_load": "residual_load",
}
# DWD precipitation form code -> text (codes 2 and 3 are undefined by the source)
PRECIPITATION_FORM_LABELS = {
    "0": "no precipitation",
    "1": "rain only",
    "4": "form unknown although precipitation was reported",
    "6": "liquid precipitation",
    "7": "solid precipitation",
    "8": "liquid and solid precipitation",
    "9": "missing value, form not determinable",
}
WEATHER_THEMES = {
    "weather_air_state": ("temperature", "humidity", "pressure"),
    "weather_wind_precipitation": ("wind", "precipitation"),
    "weather_sky": ("cloud", "visibility", "present_weather"),
    "weather_ground_sun": (
        "soil_temperature",
        "solar_radiation",
        "longwave_radiation",
        "sunshine_duration",
        "solar_geometry",
        "uv_index",
    ),
}
unthemed = set(WEATHER_FAMILIES) ^ {f for fs in WEATHER_THEMES.values() for f in fs}
if unthemed:
    raise RuntimeError(f"weather families without one theme: {sorted(unthemed)}")
# Silver table -> Gold table and grain, copied as they are
COPIES = {
    "weather_daily": ("weather_daily", ["daily_key"]),
    "device_telemetry_snapshot": ("iot_device", ["device_key"]),
    "plant_operating_sample": ("plant_sensor_observation", ["sample_key"]),
    "grid_intervention_event": ("grid_intervention_event", ["event_key"]),
}
# (parent table, relationship, parent id column, linked type, linked id column)
ATTRIBUTE_LINKS = [
    ("generation_unit", "unit_operator", "unit_id", "market_actor", "operator_id"),
    ("generation_unit", "unit_location", "unit_id", "grid_location", "location_id"),
    (
        "generation_unit",
        "unit_support_registration",
        "unit_id",
        "support_registration",
        "renewable_energy_act_support_id",
    ),
    (
        "generation_unit",
        "unit_support_registration",
        "unit_id",
        "support_registration",
        "combined_heat_and_power_support_id",
    ),
    (
        "generation_unit",
        "unit_authorisation",
        "unit_id",
        "unit_authorisation",
        "authorisation_id",
    ),
    (
        "power_plant_register",
        "plant_unit",
        "source_record_id",
        "generation_unit",
        "mastr_unit_id",
    ),
    (
        "grid_connection_point",
        "connection_point_location",
        "connection_point_id",
        "grid_location",
        "location_id",
    ),
    (
        "grid_connection_point",
        "connection_point_network",
        "connection_point_id",
        "grid_network",
        "grid_id",
    ),
    (
        "grid_connection_point",
        "connection_point_operator",
        "connection_point_id",
        "market_actor",
        "grid_operator_id",
    ),
    (
        "register_change_event",
        "operator_change_previous_operator",
        "unit_id",
        "market_actor",
        "previous_grid_operator_id",
    ),
    (
        "register_change_event",
        "operator_change_new_operator",
        "unit_id",
        "market_actor",
        "new_grid_operator_id",
    ),
]
LINK_COLUMNS = [
    "relationship_type",
    "parent_type",
    "parent_id",
    "linked_type",
    "linked_id",
    "link_basis",
    "match_confidence",
    "parent_resolved",
    "linked_resolved",
    "source_record_id",
    "source_dataset",
    "source_system",
]
OBSERVATION_CONTEXT = [name for name, _ in [*_ENERGY_HEAD, *_TIME_COLUMNS]]
PROVENANCE = [
    "measurement_basis",
    "source_system",
    "source_dataset",
    "source_record_id",
]
NO_TEXT = F.lit(None).cast("string")
NO_FLAG = F.lit(None).cast("boolean")
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Read Silver
silver = {
    name: from_silver(read_silver(name))
    for name in (
        *COPIES,
        "energy_observation",
        "energy_component",
        "weather_observation",
        "register_link",
        *{link[0] for link in ATTRIBUTE_LINKS},
    )
}
observation = silver["energy_observation"]
component = silver["energy_component"]

# COMMAND ----------

# DBTITLE 1,Transform -- the tables copied from Silver
for name, (table, grain) in COPIES.items():
    TABLES[table] = (add_provenance(silver[name], SOURCE, RID), grain)

# COMMAND ----------

# DBTITLE 1,Transform -- market_price (day-ahead and balancing prices)
TABLES["market_price"] = (
    add_provenance(
        observation.filter(
            F.col(PRICE_COLUMN).isNotNull()
            | F.col("balancing_price_eur_per_mwh").isNotNull()
        ).select(
            *OBSERVATION_CONTEXT,
            PRICE_COLUMN,
            "balancing_price_eur_per_mwh",
            f"{PRICE_COLUMN}_outlier_five_sigma",
            "sign_convention",
            *PROVENANCE,
        ),
        SOURCE,
        RID,
    ),
    ENERGY_GRAIN,
)

# COMMAND ----------

# DBTITLE 1,Transform -- channel_reading (one row per site, window and channel value)
meter_flags = {
    "cumulative": ("outlier_five_sigma", "stuck_reading", "magnitude_decrease", None),
    "increment": (None, None, None, "spans_gap"),
}
channel_specs = [
    (column, subsystem, channel, kind, "kwh", *meter_flags[kind])
    for channel, (subsystem, _) in HONDA_METER_CHANNELS.items()
    for column, kind in (
        (f"{channel}_kwh", "cumulative"),
        (f"{channel}_increment_kwh", "increment"),
    )
] + [
    (
        column,
        column.split("_")[0],
        column.removesuffix("_w"),
        "power",
        "w",
        "outlier_five_sigma",
        "stuck_reading",
        None,
        None,
    )
    for column in THERMAL_COLUMNS
]
flag_names = ("is_outlier_five_sigma", "is_stuck_reading", "is_magnitude_decrease")
readings = [
    F.struct(
        F.lit(column).alias("source_column"),
        F.lit(subsystem).alias("subsystem"),
        F.lit(channel).alias("channel"),
        F.lit(kind).alias("value_kind"),
        F.col(column).alias("value"),
        F.lit(unit).alias("unit"),
        *[
            (F.col(f"{column}_{check}") if check else NO_FLAG).alias(name)
            for check, name in zip(checks, (*flag_names, "spans_gap"), strict=True)
        ],
    )
    for column, subsystem, channel, kind, unit, *checks in channel_specs
]
keep = [
    *OBSERVATION_CONTEXT,
    "increment_derivation_rule",
    "sign_convention",
    *PROVENANCE,
]
TABLES["channel_reading"] = (
    add_provenance(
        columns_to_rows(observation, keep, readings).filter(F.col("value").isNotNull()),
        SOURCE,
        RID,
    ),
    [*ENERGY_GRAIN, "source_column"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- market_energy_component and site_energy_component
is_site = F.col("source_system") == SITE_SOURCE
for table, rows in (
    ("market_energy_component", component.filter(~is_site)),
    ("site_energy_component", component.filter(is_site)),
):
    TABLES[table] = (add_provenance(rows, SOURCE, RID), ["source_record_id"])

# COMMAND ----------

# DBTITLE 1,Transform -- forecast_actual (forecast row x actual row of the same window)
label_map = F.create_map(
    *[F.lit(part) for pair in FORECAST_ACTUAL_LABELS.items() for part in pair]
)
window = [
    "source_system",
    "market_area_code",
    "observation_timestamp_utc",
    "interval_seconds",
    "interval_reference",
]
forecast = component.filter(F.col("component_group") == "forecast").withColumn(
    "_actual_label",
    F.coalesce(
        F.try_element_at(label_map, F.col("forecast_scope")),
        F.concat(F.lit("generation_"), F.col("forecast_scope")),
    ),
)
actual = component.filter(F.col("component_group") == "balance").select(
    *[F.col(c).alias(f"_a_{c}") for c in window],
    F.col("native_label").alias("_a_label"),
    *[
        F.col(c).alias(f"actual_{c}")
        for c in (
            "energy_mwh",
            "native_label",
            "carrier_key",
            "source_filter_id",
            "source_series",
            "is_outlier_five_sigma",
            "source_record_id",
        )
    ],
)
match = F.col("_actual_label") == F.col("_a_label")
for c in window:
    match = match & F.col(c).eqNullSafe(F.col(f"_a_{c}"))
TABLES["forecast_actual"] = (
    add_provenance(
        forecast.join(actual, match, "left").drop(
            "_actual_label", "_a_label", *[f"_a_{c}" for c in window]
        ),
        SOURCE,
        RID,
    ),
    ["source_record_id", "actual_source_record_id"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- the four weather themes (columns and rows of their families)
context = [name for name, _ in WEATHER_OBSERVATION_CONTEXT]
form_text = F.create_map(
    *[F.lit(part) for pair in PRECIPITATION_FORM_LABELS.items() for part in pair]
)
for table, families in WEATHER_THEMES.items():
    blocks = [name for family in families for name, _ in _weather_family_block(family)]
    has_value = F.lit(False)
    for family in families:
        for name, _ in WEATHER_FAMILY_MEASUREMENTS[family]:
            has_value = has_value | F.col(name).isNotNull()
    rows = (
        silver["weather_observation"]
        .filter(has_value)
        .select(*context, *blocks, "source_record_id")
    )
    if "precipitation" in families:
        code = (
            F.col("precipitation_form_code").cast("double").cast("int").cast("string")
        )
        rows = rows.withColumn(
            "precipitation_form_text", F.try_element_at(form_text, code)
        )
    TABLES[table] = (add_provenance(rows, "dwd", RID), ["observation_key"])

# COMMAND ----------

# DBTITLE 1,Transform -- entity_link (register links, id columns, redispatch names)
links = [
    silver["register_link"]
    .withColumn("link_basis", F.lit("register_link"))
    .withColumn("match_confidence", NO_TEXT)
    .select(*LINK_COLUMNS)
]
for table, relationship, parent_id, linked_type, linked_id in ATTRIBUTE_LINKS:
    links.append(
        silver[table]
        .filter(F.col(linked_id).isNotNull())
        .select(
            F.lit(relationship).alias("relationship_type"),
            F.lit(table).alias("parent_type"),
            F.col(parent_id).alias("parent_id"),
            F.lit(linked_type).alias("linked_type"),
            F.col(linked_id).alias("linked_id"),
            F.lit("id_column").alias("link_basis"),
            NO_TEXT.alias("match_confidence"),
            NO_FLAG.alias("parent_resolved"),
            NO_FLAG.alias("linked_resolved"),
            "source_record_id",
            "source_dataset",
            "source_system",
        )
    )


plant_name, event_name = [
    F.upper(F.trim(F.regexp_replace(column, r"\s+", " ")))
    for column in ("plant_name", "affected_unit_match_name")
]
plants_by_name = (
    silver["power_plant_register"]
    .filter(F.col("plant_name").isNotNull())
    .groupBy(plant_name.alias("_name"))
    .agg(F.min("source_record_id").alias("_plant_id"))
)
links.append(
    silver["grid_intervention_event"]
    .withColumn("_name", event_name)
    .join(plants_by_name, "_name")
    .select(
        F.lit("redispatch_affected_plant").alias("relationship_type"),
        F.lit("grid_intervention_event").alias("parent_type"),
        F.col("event_key").alias("parent_id"),
        F.lit("power_plant_register").alias("linked_type"),
        F.col("_plant_id").alias("linked_id"),
        F.lit("exact_normalised_name").alias("link_basis"),
        F.col("affected_unit_match_confidence").alias("match_confidence"),
        NO_FLAG.alias("parent_resolved"),
        NO_FLAG.alias("linked_resolved"),
        "source_record_id",
        "source_dataset",
        "source_system",
    )
)
entity_link = links[0]
for part in links[1:]:
    entity_link = entity_link.unionByName(part)
TABLES["entity_link"] = (
    add_provenance(entity_link, "mastr", RID),
    [*LINK_COLUMNS[:6], "source_record_id"],
)

# COMMAND ----------

# DBTITLE 1,Grain gate -- every table
gate_tables(TABLES, **GATE)

# COMMAND ----------

# DBTITLE 1,Write -- every table
write_tables(TABLES, **COMMON)

# COMMAND ----------

# DBTITLE 1,Inspect and export -- every table
inspect_tables(TABLES, findings="energy", **COMMON)

# COMMAND ----------

# DBTITLE 1,Validation -- every table exists and has rows
validate_tables(TABLES, **COMMON)
