# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER SEMANTIC STRUCTURES SHARED HELPER
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** building blocks for the cross-source Silver structures --
# MAGIC time columns, place keys, column conformance, per-source replace writes.
# MAGIC Pulled in with `%run ../../_semantic_common` after `_silver_common`.

# COMMAND ----------

# DBTITLE 1,Configuration constants
# Registered target schema of every semantic structure (see the generator).
SEMANTIC_SCHEMA = "energy_silver"
PROJECT_TZ = "Europe/Berlin"

# Provenance columns shared by every semantic structure.
_PROVENANCE_HEAD = [("source_system", "string"), ("source_dataset", "string")]
_PROVENANCE_TAIL = [
    ("source_record_id", "string"),
    ("_silver_loaded_at", "timestamp"),
    ("_silver_run_id", "string"),
]

# Place/time scaffolding shared by every family; measurement columns below
# are designed per family, not shared. Grain = (source_system, place,
# observation_ts_utc, interval_seconds, interval_reference).
_PLACE_HEAD = [
    ("observation_key", "string"),
    ("location_key", "string"),
    ("source_location_id", "string"),
]
_TIME_COLUMNS = [
    ("observation_ts_native", "string"),
    ("time_basis", "string"),
    ("utc_offset_hours", "double"),
    ("observation_ts_utc", "timestamp"),
    ("observation_ts_project", "timestamp"),
    ("local_date", "date"),
    ("interval_seconds", "int"),
    ("interval_reference", "string"),
]
# DWD's per-product quality classification (QN_*); NULL for sources that
# don't carry one (Honda, AccuWeather). quality_flags as in every structure.
_QUALITY_COLUMNS = [
    ("quality_code", "string"),
    ("quality_label", "string"),
    ("quality_flags", "array<string>"),
]
_TAIL = [("measurement_basis", "string"), *_PROVENANCE_HEAD, *_PROVENANCE_TAIL]

# dwd_solar's native true-solar-time stamp (MESS_DATUM_WOZ).
_SOLAR_TIME = [("true_solar_time_native", "string")]

# One alternate estimate of a field's primary value, with its own native
# form, source and quality code.
_ALT_READING = (
    "array<struct<value:double,native_value:double,native_unit:string,"
    "source_dataset:string,quality_code:string>>"
)

WEATHER_TEMPERATURE_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("air_temperature_degc", "double"),
    ("air_temperature_alt", _ALT_READING),
    ("dew_point_temperature_degc", "double"),
    ("dew_point_temperature_alt", _ALT_READING),
    ("wet_bulb_temperature_degc", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

WEATHER_HUMIDITY_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("relative_humidity_percent", "double"),
    ("relative_humidity_alt", _ALT_READING),
    ("absolute_humidity_g_per_m3", "double"),
    ("vapour_pressure_hpa", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Native value/unit populated only where a source converts (AccuWeather Pa).
WEATHER_PRESSURE_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("pressure_station_hpa", "double"),
    ("pressure_station_native_value", "double"),
    ("pressure_station_native_unit", "string"),
    ("pressure_station_alt", _ALT_READING),
    ("pressure_sea_level_hpa", "double"),
    ("pressure_sea_level_native_value", "double"),
    ("pressure_sea_level_native_unit", "string"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# One `readings` array entry per statistic present (mean/instant/max/
# unspecified), not one row per product. Already m/s and degrees.
WEATHER_WIND_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    (
        "readings",
        (
            "array<struct<statistic:string,wind_speed_m_per_s:double,"
            "wind_direction_degrees:double,wind_direction_variable:boolean,"
            "wind_gust_m_per_s:double,source_dataset:string>>"
        ),
    ),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Amount, occurred-flag and form describe one precipitation event.
WEATHER_PRECIPITATION_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("precipitation_mm", "double"),
    ("precipitation_occurred", "boolean"),
    ("precipitation_form_code", "string"),
    ("precipitation_form_text", "string"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Layers: array of structs (0-4, variably populated), not flat columns.
WEATHER_CLOUD_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("cloud_cover_total_percent", "double"),
    ("cloud_cover_total_native_value", "double"),
    ("cloud_cover_total_native_unit", "string"),
    ("cloud_cover_total_alt", _ALT_READING),
    ("cloud_base_height_m", "double"),
    ("observation_method", "string"),
    (
        "layers",
        (
            "array<struct<layer_number:int,genus_code:string,genus_text:string,"
            "base_height_m:double,cover_percent:double,cover_native_value:double,"
            "cover_native_unit:string>>"
        ),
    ),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Separate from cloud: different physical basis (extinction distance).
WEATHER_VISIBILITY_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("visibility_m", "double"),
    ("visibility_native_value", "double"),
    ("visibility_native_unit", "string"),
    ("observation_method", "string"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Purely categorical -- no numeric value/unit columns. DWD only.
WEATHER_PRESENT_WEATHER_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("present_weather_code", "string"),
    ("present_weather_text", "string"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Six depths, always reported together -- a fixed set, so flat columns
# (unlike cloud's variably-populated layers). DWD only.
WEATHER_SOIL_TEMPERATURE_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("soil_temperature_2cm_degc", "double"),
    ("soil_temperature_5cm_degc", "double"),
    ("soil_temperature_10cm_degc", "double"),
    ("soil_temperature_20cm_degc", "double"),
    ("soil_temperature_50cm_degc", "double"),
    ("soil_temperature_100cm_degc", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# dwd_solar's one row split by meaning into four families below: flux,
# longwave flux, duration and geometry.

# Shortwave flux only (global + diffuse). Native pair on DWD's two sums.
WEATHER_SOLAR_RADIATION_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    *_SOLAR_TIME,
    ("global_radiation_w_per_m2", "double"),
    ("global_radiation_native_value", "double"),
    ("global_radiation_native_unit", "string"),
    ("diffuse_radiation_w_per_m2", "double"),
    ("diffuse_radiation_native_value", "double"),
    ("diffuse_radiation_native_unit", "string"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# Atmospheric infrared, not sunlight -- a different physical process. DWD only.
WEATHER_LONGWAVE_RADIATION_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    *_SOLAR_TIME,
    ("longwave_downward_radiation_w_per_m2", "double"),
    ("longwave_downward_radiation_native_value", "double"),
    ("longwave_downward_radiation_native_unit", "string"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# A duration, not a flux. dwd_sun (clock hour) and dwd_solar (true-solar
# hour) are different windows, so separate rows, never _alt.
WEATHER_SUNSHINE_DURATION_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    *_SOLAR_TIME,
    ("sunshine_duration_minutes", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# An astronomical position, not an atmospheric measurement. DWD only.
WEATHER_SOLAR_GEOMETRY_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    *_SOLAR_TIME,
    ("solar_zenith_angle_degrees", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# An index, not a physical flux measurement -- AccuWeather only.
WEATHER_UV_INDEX_COLUMNS = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("uv_index", "double"),
    *_QUALITY_COLUMNS,
    *_TAIL,
]

# name -> column list, one entry per Silver structure.
WEATHER_FAMILY_COLUMNS = {
    "weather_temperature": WEATHER_TEMPERATURE_COLUMNS,
    "weather_humidity": WEATHER_HUMIDITY_COLUMNS,
    "weather_pressure": WEATHER_PRESSURE_COLUMNS,
    "weather_wind": WEATHER_WIND_COLUMNS,
    "weather_precipitation": WEATHER_PRECIPITATION_COLUMNS,
    "weather_cloud": WEATHER_CLOUD_COLUMNS,
    "weather_visibility": WEATHER_VISIBILITY_COLUMNS,
    "weather_present_weather": WEATHER_PRESENT_WEATHER_COLUMNS,
    "weather_soil_temperature": WEATHER_SOIL_TEMPERATURE_COLUMNS,
    "weather_solar_radiation": WEATHER_SOLAR_RADIATION_COLUMNS,
    "weather_longwave_radiation": WEATHER_LONGWAVE_RADIATION_COLUMNS,
    "weather_sunshine_duration": WEATHER_SUNSHINE_DURATION_COLUMNS,
    "weather_solar_geometry": WEATHER_SOLAR_GEOMETRY_COLUMNS,
    "weather_uv_index": WEATHER_UV_INDEX_COLUMNS,
}
WEATHER_FAMILIES = list(WEATHER_FAMILY_COLUMNS)

WEATHER_DAILY_COLUMNS = [
    ("daily_key", "string"),
    ("location_key", "string"),
    ("source_location_id", "string"),
    ("local_date", "date"),
    ("variable", "string"),
    ("statistic", "string"),
    ("level", "string"),
    ("is_primary", "boolean"),
    ("date_native", "string"),
    ("time_basis", "string"),
    ("value_native", "double"),
    ("unit_native", "string"),
    ("value", "double"),
    ("unit", "string"),
    ("value_origin", "string"),
    ("derivation_rule", "string"),
    ("n_observations", "int"),
    ("measurement_basis", "string"),
    *_PROVENANCE_HEAD,
    ("source_column", "string"),
    *_PROVENANCE_TAIL,
]

WEATHER_LOCATION_COLUMNS = [
    ("location_key", "string"),
    ("source_location_id", "string"),
    ("location_role", "string"),
    ("name", "string"),
    ("latitude", "double"),
    ("longitude", "double"),
    ("elevation_m", "double"),
    ("continent", "string"),
    ("country_code", "string"),
    ("region", "string"),
    ("city", "string"),
    ("ags_code", "string"),
    ("geography_basis", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

# Station periods; several rows can share a start date (relocations), so
# record_ordinal keeps them apart.
WEATHER_LOCATION_VALIDITY_COLUMNS = [
    ("location_key", "string"),
    ("source_location_id", "string"),
    ("valid_from", "date"),
    ("valid_to", "date"),
    ("record_ordinal", "int"),
    ("name", "string"),
    ("latitude", "double"),
    ("longitude", "double"),
    ("elevation_m", "double"),
    ("quality_flags", "array<string>"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

# DWD station reference, one structure per DWD metadata product: their
# validity windows do not align, so they are not folded together.
_STATION_PERIOD_HEAD = [
    ("location_key", "string"),
    ("source_location_id", "string"),
    ("valid_from", "date"),
    ("valid_to", "date"),
    ("record_ordinal", "int"),
    ("name", "string"),
]
WEATHER_STATION_NAME_HISTORY_COLUMNS = [
    *_STATION_PERIOD_HEAD,
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]
WEATHER_STATION_INSTRUMENT_COLUMNS = [
    *_STATION_PERIOD_HEAD,
    ("parameter_category", "string"),
    ("latitude", "double"),
    ("longitude", "double"),
    ("elevation_m", "double"),
    ("sensor_height_m", "double"),
    ("device_type", "string"),
    ("measurement_method", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]
WEATHER_PARAMETER_PERIOD_COLUMNS = [
    *_STATION_PERIOD_HEAD,
    ("parameter_source_code", "string"),
    ("parameter_description_de", "string"),
    ("parameter_unit", "string"),
    ("parameter_data_source", "string"),
    ("parameter_extra_info", "string"),
    ("parameter_special_notes", "string"),
    ("parameter_reference", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]
WEATHER_PARAMETER_CATALOG_COLUMNS = [
    ("parameter_source_code", "string"),
    ("parameter_business_name", "string"),
    ("parameter_unit", "string"),
    ("parameter_description_de", "string"),
    ("catalog_vintage", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]
WEATHER_MISSING_VALUE_PERIOD_COLUMNS = [
    ("location_key", "string"),
    ("source_location_id", "string"),
    ("name", "string"),
    ("parameter_source_code", "string"),
    ("gap_start_ts", "timestamp"),
    ("gap_end_ts", "timestamp"),
    ("record_ordinal", "int"),
    ("missing_value_count", "bigint"),
    ("gap_description", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]
WEATHER_MISSINGNESS_RECONCILIATION_COLUMNS = [
    ("location_key", "string"),
    ("source_location_id", "string"),
    ("parameter_source_code", "string"),
    ("reconciliation_status", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

# MaStR code lists in one structure: key (catalog_kind, code_id); katalog
# values point at their category through parent_id.
MASTR_CODE_LIST_COLUMNS = [
    ("catalog_kind", "string"),
    ("code_id", "string"),
    ("parent_id", "string"),
    ("label", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

# Every MaStR link array as one typed relationship structure.
REGISTER_LINK_COLUMNS = [
    ("relationship_type", "string"),
    ("parent_type", "string"),
    ("parent_id", "string"),
    ("linked_type", "string"),
    ("linked_id", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

# Commerce: GA4 and REES46 are separate identity spaces, so separate
# structures; GA4 transaction fields sit on the event (same grain).
_EVENT_TIME = [
    ("event_ts_native", "string"),
    ("event_ts_utc", "timestamp"),
    ("event_ts_project", "timestamp"),
    ("local_date", "date"),
]
REES46_EVENT_COLUMNS = [
    ("event_key", "string"),
    *_EVENT_TIME,
    ("event_type", "string"),
    ("user_id", "string"),
    ("user_session", "string"),
    ("product_id", "string"),
    ("category_id", "string"),
    ("category_code", "string"),
    ("category_l1", "string"),
    ("category_l2", "string"),
    ("category_l3", "string"),
    ("brand", "string"),
    ("price", "double"),
    ("currency_unknown", "boolean"),
    ("quality_flags", "array<string>"),
    ("measurement_basis", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]
GA4_EVENT_COLUMNS = [
    ("event_key", "string"),
    ("event_date_native", "string"),
    *_EVENT_TIME,
    ("event_name", "string"),
    ("user_pseudo_id", "string"),
    ("session_id", "bigint"),
    ("session_number", "bigint"),
    ("page_location", "string"),
    ("page_title", "string"),
    ("search_term", "string"),
    ("unique_search_term", "string"),
    ("geo_country", "string"),
    ("transaction_id", "string"),
    ("purchase_revenue", "double"),
    ("unique_items", "bigint"),
    ("total_item_quantity", "bigint"),
    ("quality_flags", "array<string>"),
    ("measurement_basis", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]
# item_id is a campaign id on promotion events, a product id otherwise.
GA4_EVENT_ITEM_COLUMNS = [
    ("event_key", "string"),
    ("item_ordinal", "int"),
    ("event_name", "string"),
    ("event_ts_utc", "timestamp"),
    ("user_pseudo_id", "string"),
    ("item_context", "string"),
    ("item_id", "string"),
    ("item_name", "string"),
    ("item_category", "string"),
    ("price", "double"),
    ("quantity", "bigint"),
    ("item_revenue", "double"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

WEATHER_GRAIN = [
    "source_system",
    "location_key",
    "observation_ts_utc",
    "interval_seconds",
    "interval_reference",
]

# Energy families: a place is a site (location_key) or a market area
# (market_area_code, never a geography). Sign is always native.
_ENERGY_HEAD = [*_PLACE_HEAD, ("market_area_code", "string")]
_ENERGY_TAIL = [
    ("sign_convention", "string"),
    ("quality_flags", "array<string>"),
    *_TAIL,
]
ENERGY_GRAIN = [*WEATHER_GRAIN[:2], "market_area_code", *WEATHER_GRAIN[2:]]

# Market-area vocabulary (SMARD region, redispatch TSO).
MARKET_AREA_CODES = {
    "DE-LU": "de_lu",
    "50Hertz": "fifty_hertz",
    "Amprion": "amprion",
    "TenneT": "tennet_de",
    "TenneT DE": "tennet_de",
    "TransnetBW": "transnetbw",
    "PSE": "pse_poland",
}

# Terms of one electricity balance at one node over one window.
ELECTRICITY_BALANCE_COLUMNS = [
    *_ENERGY_HEAD,
    *_TIME_COLUMNS,
    (
        "components",
        (
            "array<struct<component_kind:string,carrier_code:string,"
            "native_label:string,energy_mwh:double,power_w:double,"
            "source_series:string>>"
        ),
    ),
    *_ENERGY_TAIL,
]

ELECTRICITY_PRICE_COLUMNS = [
    *_ENERGY_HEAD,
    *_TIME_COLUMNS,
    ("price_eur_per_mwh", "double"),
    *_ENERGY_TAIL,
]

# Window = the forecast target; the source has no issue time.
ELECTRICITY_GENERATION_FORECAST_COLUMNS = [
    *_ENERGY_HEAD,
    *_TIME_COLUMNS,
    ("forecast_issue_ts", "timestamp"),
    (
        "components",
        (
            "array<struct<forecast_scope:string,native_label:string,"
            "energy_mwh:double,semantic_status:string,semantic_issue_ref:string,"
            "source_series:string>>"
        ),
    ),
    *_ENERGY_TAIL,
]

THERMAL_ENERGY_COLUMNS = [
    *_ENERGY_HEAD,
    *_TIME_COLUMNS,
    ("heating_total_w", "double"),
    ("heating_chp_heat_w", "double"),
    ("cooling_total_w", "double"),
    *_ENERGY_TAIL,
]

# Cumulative register state (kWh) plus its derived forward increment.
HONDA_METER_CHANNELS = {
    "electricity_total": ("electricity", "total"),
    "electricity_pv": ("electricity", "PV"),
    "electricity_chp": ("electricity", "CHP"),
    "heating_total": ("heating", "total"),
    "heating_chp_heat": ("heating", "CHP_heat"),
    "heating_chp_elec": ("heating", "CHP_elec"),
    "cooling_total": ("cooling", "total"),
    "cooling_cool_elec": ("cooling", "cool_elec"),
}
ENERGY_METER_READING_COLUMNS = [
    *_ENERGY_HEAD,
    *_TIME_COLUMNS,
    *[
        col
        for ch in HONDA_METER_CHANNELS
        for col in ((f"{ch}_kwh", "double"), (f"{ch}_increment_kwh", "double"))
    ],
    ("increment_derivation_rule", "string"),
    *_ENERGY_TAIL,
]

ENERGY_FAMILY_COLUMNS = {
    "electricity_balance": ELECTRICITY_BALANCE_COLUMNS,
    "electricity_price": ELECTRICITY_PRICE_COLUMNS,
    "electricity_generation_forecast": ELECTRICITY_GENERATION_FORECAST_COLUMNS,
    "thermal_energy": THERMAL_ENERGY_COLUMNS,
    "energy_meter_reading": ENERGY_METER_READING_COLUMNS,
}

# One row per measure: an event with a start and an end, not a window grain.
GRID_INTERVENTION_EVENT_COLUMNS = [
    ("event_key", "string"),
    ("event_start_native", "string"),
    ("event_end_native", "string"),
    ("time_basis", "string"),
    ("event_start_utc", "timestamp"),
    ("event_end_utc", "timestamp"),
    ("event_start_project", "timestamp"),
    ("event_end_project", "timestamp"),
    ("duration_hours", "double"),
    ("reason_native", "string"),
    ("reason", "string"),
    ("direction_native", "string"),
    ("direction", "string"),
    ("mean_power_mw", "double"),
    ("max_power_mw", "double"),
    ("energy_mwh", "double"),
    ("instructing_tso_native", "string"),
    ("instructing_market_area_code", "string"),
    ("requesting_tso_native", "string"),
    ("requesting_market_area_codes", "array<string>"),
    ("affected_asset_text", "string"),
    ("affected_unit_match_name", "string"),
    ("affected_unit_match_confidence", "string"),
    ("primary_energy_type_native", "string"),
    ("primary_energy_type", "string"),
    ("quality_flags", "array<string>"),
    ("measurement_basis", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

# No time, no key: one row per documented source row.
PLANT_OPERATING_SAMPLE_COLUMNS = [
    ("sample_key", "string"),
    ("ambient_temperature_degc", "double"),
    ("exhaust_vacuum_cm_hg", "double"),
    ("ambient_pressure_mbar", "double"),
    ("relative_humidity_percent", "double"),
    ("net_electrical_output_mw", "double"),
    ("repeat_index", "int"),
    ("quality_flags", "array<string>"),
    ("measurement_basis", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

DEVICE_TELEMETRY_SNAPSHOT_COLUMNS = [
    ("device_key", "string"),
    ("source_device_id", "string"),
    ("device_name", "string"),
    ("ip_address", "string"),
    ("latitude", "double"),
    ("longitude", "double"),
    ("country_code", "string"),
    ("country_code_alpha3", "string"),
    ("country_name", "string"),
    ("observation_ts_native", "string"),
    ("observation_ts_utc", "timestamp"),
    ("temperature_degc", "double"),
    ("humidity_percent", "double"),
    ("co2_level", "double"),
    ("battery_level", "double"),
    ("lcd_label", "string"),
    ("data_origin", "string"),
    ("measurement_basis", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

# Every semantic Silver table and its columns. The field-class generator
# registers exactly these (energy_silver); notebooks conform to them.
SEMANTIC_STRUCTURES = {
    **WEATHER_FAMILY_COLUMNS,
    "weather_daily": WEATHER_DAILY_COLUMNS,
    "weather_location": WEATHER_LOCATION_COLUMNS,
    "weather_location_validity": WEATHER_LOCATION_VALIDITY_COLUMNS,
    "weather_station_name_history": WEATHER_STATION_NAME_HISTORY_COLUMNS,
    "weather_station_instrument": WEATHER_STATION_INSTRUMENT_COLUMNS,
    "weather_parameter_period": WEATHER_PARAMETER_PERIOD_COLUMNS,
    "weather_parameter_catalog": WEATHER_PARAMETER_CATALOG_COLUMNS,
    "weather_missing_value_period": WEATHER_MISSING_VALUE_PERIOD_COLUMNS,
    "weather_missingness_reconciliation": WEATHER_MISSINGNESS_RECONCILIATION_COLUMNS,
    **ENERGY_FAMILY_COLUMNS,
    "grid_intervention_event": GRID_INTERVENTION_EVENT_COLUMNS,
    "plant_operating_sample": PLANT_OPERATING_SAMPLE_COLUMNS,
    "device_telemetry_snapshot": DEVICE_TELEMETRY_SNAPSHOT_COLUMNS,
    "mastr_code_list": MASTR_CODE_LIST_COLUMNS,
    "register_link": REGISTER_LINK_COLUMNS,
    "rees46_event": REES46_EVENT_COLUMNS,
    "ga4_event": GA4_EVENT_COLUMNS,
    "ga4_event_item": GA4_EVENT_ITEM_COLUMNS,
}

# Register structures whose wide column set comes from the MaStR / plant-list
# contracts and mappings: registered as the union of their member tables'
# registry rows (minus `ecosystem`) plus the listed discriminators.
SEMANTIC_MEMBER_STRUCTURES = {
    "generation_unit": (
        [
            "mastr_einheiten_wind",
            "mastr_einheiten_biomasse",
            "mastr_einheiten_wasser",
            "mastr_einheiten_verbrennung",
            "mastr_einheiten_kernkraft",
            "mastr_einheiten_geothermie_gsgk",
        ],
        ["unit_type"],
    ),
    "support_registration": (
        [
            "mastr_anlagen_eeg_wind",
            "mastr_anlagen_eeg_biomasse",
            "mastr_anlagen_eeg_wasser",
            "mastr_anlagen_eeg_geothermie_gsgk",
            "mastr_anlagen_kwk",
        ],
        ["support_scheme", "unit_type", "support_registration_id"],
    ),
    "unit_authorisation": (["mastr_einheiten_genehmigung"], []),
    "unit_repowering": (["mastr_ertuechtigungen"], []),
    "market_actor": (["mastr_marktakteure"], []),
    "market_actor_role": (["mastr_marktakteure_und_rollen"], []),
    "grid_location": (["mastr_lokationen"], []),
    "grid_connection_point": (["mastr_netzanschlusspunkte"], []),
    "grid_network": (["mastr_netze"], []),
    "balancing_area": (["mastr_bilanzierungsgebiete"], []),
    "grid_location_coordinate_conflict": (["mastr_location_coordinate_conflict"], []),
    "unit_deletion_event": (["mastr_unit_deletion_events"], []),
    "actor_deletion_event": (["mastr_actor_deletion_events"], []),
    "grid_operator_change_event": (["mastr_grid_operator_change_events"], []),
    "power_plant_register": (["power_plant_list"], []),
    "power_plant_capacity_plan": (["power_plant_capacity_additions"], []),
}

# Schema per structure (per-schema table quota): reference and master data
# in energy_silver_reference, Commerce in commerce_silver, the rest in
# SEMANTIC_SCHEMA.
REFERENCE_STRUCTURES = {
    "weather_location",
    "weather_location_validity",
    "weather_station_name_history",
    "weather_station_instrument",
    "weather_parameter_period",
    "weather_parameter_catalog",
    "weather_missing_value_period",
    "weather_missingness_reconciliation",
    "mastr_code_list",
    "market_actor",
    "market_actor_role",
    "grid_location",
    "grid_connection_point",
    "grid_network",
    "balancing_area",
}
COMMERCE_STRUCTURES = {"rees46_event", "ga4_event", "ga4_event_item"}


def semantic_target_schema(table: str) -> str:
    if table in REFERENCE_STRUCTURES:
        return f"{SEMANTIC_SCHEMA}_reference"
    if table in COMMERCE_STRUCTURES:
        return "commerce_silver"
    return SEMANTIC_SCHEMA


# Continent by ISO 3166-1 alpha-2 (UN geoscheme; Russia -> Europe, Turkey and
# Cyprus -> Asia).
_CONTINENT_CODES = {
    "AF": (
        "DZ AO BJ BW BF BI CV CM CF TD KM CG CD CI DJ EG GQ ER SZ ET GA GM GH GN GW "
        "KE LS LR LY MG MW ML MR MU YT MA MZ NA NE NG RE RW SH ST SN SC SL SO ZA SS "
        "SD TZ TG TN UG EH ZM ZW"
    ),
    "AS": (
        "AF AM AZ BH BD BT BN KH CN CY GE HK IN ID IR IQ IL JP JO KZ KW KG LA LB MO "
        "MY MV MN MM NP KP OM PK PS PH QA SA SG KR LK SY TW TJ TH TL TR TM AE UZ VN "
        "YE"
    ),
    "EU": (
        "AL AD AT BY BE BA BG HR CZ DK EE FO FI FR DE GI GR GG VA HU IS IE IM IT JE "
        "XK LV LI LT LU MT MD MC ME NL MK NO PL PT RO RU SM RS SK SI ES SJ SE CH UA "
        "GB AX"
    ),
    "NA": (
        "AG BS BB BZ CA CR CU DM DO SV GD GT HT HN JM MX NI PA KN LC VC TT US AI AW "
        "BM BQ VG KY CW GL GP MQ MS PR BL MF PM SX TC VI"
    ),
    "SA": "AR BO BR CL CO EC FK GF GY PY PE SR UY VE",
    "OC": (
        "AU FJ KI MH FM NR NZ PW PG WS SB TO TV VU CK PF GU NC NU NF MP PN TK WF AS"
    ),
    "AN": "AQ",
}
CONTINENT_BY_ISO2 = {
    iso: cont for cont, codes in _CONTINENT_CODES.items() for iso in codes.split()
}

# COMMAND ----------

# DBTITLE 1,Helper -- semantic table name and session time zone


def semantic_table(name: str) -> str:
    """Full name via the field-class registry, like read_silver()."""
    return f"{CATALOG}.{target_schema_for(name)}.{name}"


def ensure_utc_session() -> None:
    """Timestamps are handled as UTC wall-clock; pin the session zone once."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")


# COMMAND ----------

# DBTITLE 1,Helper -- time columns


def add_project_time(df, utc_col: str = "observation_ts_utc"):
    """Project-time timestamp (Europe/Berlin wall clock) and local date from a
    UTC instant. NULL where the source has no instant."""
    local = F.from_utc_timestamp(F.col(utc_col), PROJECT_TZ)
    return df.withColumn("observation_ts_project", local).withColumn(
        "local_date", F.to_date(local)
    )


def local_time_to_utc(local_ts_col: str, offset_hours_col: str):
    """UTC instant from a local wall-clock timestamp and its GMT offset (hours)."""
    return (
        F.col(local_ts_col).cast("timestamp").cast("long")
        - (F.col(offset_hours_col) * 3600).cast("long")
    ).cast("timestamp")


# COMMAND ----------

# DBTITLE 1,Helper -- place keys and continent lookup


def location_key(source_system: str, source_location_id):
    """Deterministic place key; `source_location_id` is a column name or Column."""
    return sha_key(F.lit(source_system), source_location_id)


def continent_of(country_code_col: str):
    m = F.create_map([F.lit(x) for kv in CONTINENT_BY_ISO2.items() for x in kv])
    return m[F.upper(F.col(country_code_col))]


# COMMAND ----------

# DBTITLE 1,Helper -- keep rows where at least one named column is populated


def lit_map(d: dict):
    """Literal dict as a Spark map column; index it with a Column."""
    return F.create_map([F.lit(x) for kv in d.items() for x in kv])


def flag_array(flags: dict):
    """quality_flags column: the names in {name: bool Column} that are true."""
    return F.filter(
        F.array(*[F.when(cond, F.lit(name)) for name, cond in flags.items()]),
        lambda x: x.isNotNull(),
    )


def any_present(df, cols: list):
    """Filter to rows where at least one of `cols` is non-NULL, so a source
    row that populates none of a family's fields doesn't produce a junk row."""
    cond = F.lit(False)
    for c in cols:
        cond = cond | F.col(c).isNotNull()
    return df.filter(cond)


# COMMAND ----------

# DBTITLE 1,Helper -- provenance and column conformance


def add_semantic_provenance(
    df, source_system: str, source_dataset, rid: str, sr_id_col=None
):
    """Provenance columns; `sr_id_col`, when given, becomes source_record_id.
    `source_dataset=None` keeps a per-row source_dataset already on the frame."""
    if sr_id_col and sr_id_col != "source_record_id":
        df = df.withColumnRenamed(sr_id_col, "source_record_id")
    if source_dataset is not None:
        df = df.withColumn("source_dataset", F.lit(source_dataset))
    return (
        df.withColumn("source_system", F.lit(source_system))
        .withColumn("_silver_loaded_at", F.current_timestamp())
        .withColumn("_silver_run_id", F.lit(rid))
    )


def conform(df, columns: list):
    """Select every structure column in order, casting; absent ones become NULL,
    so rows from different sources union cleanly."""
    return df.select(
        *[
            (F.col(n) if n in df.columns else F.lit(None)).cast(t).alias(n)
            for n, t in columns
        ]
    )


# COMMAND ----------

# DBTITLE 1,Helper -- write (full overwrite or replace one source's rows)


def _schema_signature(schema) -> set:
    """(name, type) pairs, ignoring field order and nullability -- the things
    that make a replaceWhere genuinely incompatible with the existing table."""
    return {(f.name, f.dataType.simpleString()) for f in schema.fields}


def write_semantic(
    df, table: str, *, source: str, component: str, rid: str, replace_where=None
) -> int | None:
    """Overwrite the table, or with `replace_where` replace only those rows.
    On a schema change, rows `replace_where` would leave untouched are
    conformed onto the new schema and committed with the incoming rows in
    one atomic write (Delta forbids combining `replaceWhere` with a schema
    change)."""
    started = now_utc()
    full = f"{CATALOG}.{target_schema_for(table, source=source)}.{table}"
    unclassified = validate_field_classes(df, full)
    exists = spark.catalog.tableExists(full)
    if exists:
        v = spark.sql(f"DESCRIBE HISTORY {full} LIMIT 1").first()["version"]
        print(f"ROLLBACK IF NEEDED: RESTORE TABLE {full} TO VERSION AS OF {v}")

    if exists and replace_where:
        existing_schema = spark.table(full).schema
        if _schema_signature(existing_schema) != _schema_signature(df.schema):
            target_columns = [
                (f.name, f.dataType.simpleString()) for f in df.schema.fields
            ]
            kept = conform(
                spark.table(full).filter(f"NOT ({replace_where})"), target_columns
            )
            df = kept.unionByName(df)
            replace_where = None
            print(
                f"SCHEMA MIGRATION  {full}: pre-existing rows carried forward onto the new schema"
            )

    writer = df.write.format("delta").mode("overwrite")
    if replace_where and exists:
        writer = writer.option("replaceWhere", replace_where)
    else:
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(full)
    n = _delta_rows_written(full)
    watermark(component, source, n, "COMPLETE", rid, started)
    audit(component, source, "rows_written", n, status="PASS", rid=rid)
    if unclassified:
        audit(
            component,
            source,
            "unclassified_columns",
            float(len(unclassified)),
            status="WARN",
            error=",".join(unclassified),
            rid=rid,
        )
    print(f"OK  {full}: {n if n is not None else '?'} rows written")
    return n


# COMMAND ----------

# DBTITLE 1,Configuration -- inspection additions
# Row identifiers and native strings: a distinct count is not diagnostic.
SEMANTIC_HIGH_CARDINALITY = {
    "observation_key",
    "daily_key",
    "location_key",
    "source_location_id",
    "observation_ts_native",
    "date_native",
    "event_key",
    "sample_key",
    "device_key",
    "source_device_id",
    "device_name",
    "ip_address",
}
FINDINGS_SOURCE = "weather"

# COMMAND ----------

# DBTITLE 1,Helper -- structure-specific checks for the findings export


def structure_extra_checks(df) -> dict:
    """`extra_checks` for `inspect_table`: row counts by the semantic columns the
    frame carries, plus the time span. One grouped pass and one aggregate."""
    HIGH_CARDINALITY_SKIP.update(SEMANTIC_HIGH_CARDINALITY)
    group_cols = [
        c
        for c in (
            "source_system",
            "location_role",
            "continent",
            "country_code",
            "source_dataset",
            "statistic",
            "market_area_code",
            "time_basis",
            "interval_reference",
            "measurement_basis",
        )
        if c in df.columns
    ]
    counts = df.groupBy(*group_cols).count().collect()
    out = {}
    for c in group_cols:
        agg = {}
        for r in counts:
            agg[r[c]] = agg.get(r[c], 0) + r["count"]
        out[f"rows_by_{c}"] = dict(sorted(agg.items(), key=lambda kv: str(kv[0])))
    span_col = next(
        (c for c in ("observation_ts_utc", "local_date") if c in df.columns), None
    )
    if span_col:
        span = df.agg(F.min(span_col).alias("lo"), F.max(span_col).alias("hi")).first()
        out[f"{span_col}_span"] = f"{span['lo']} .. {span['hi']}"
    return out


# COMMAND ----------

# DBTITLE 1,Helper -- DataFrame to markdown table


def rows_to_markdown(df, limit: int = 200) -> str:
    rows = df.limit(limit).collect()
    header = "| " + " | ".join(df.columns) + " |"
    rule = "|" + "|".join("---" for _ in df.columns) + "|"
    body = [
        "| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows
    ]
    return "\n".join([header, rule, *body])


def dict_to_markdown_row(stats: dict) -> str:
    """A single-row markdown table from a plain dict -- for a small numeric
    summary that doesn't need a Spark round-trip."""
    header = "| " + " | ".join(stats.keys()) + " |"
    rule = "|" + "|".join("---" for _ in stats) + "|"
    row = "| " + " | ".join(str(v) for v in stats.values()) + " |"
    return f"{header}\n{rule}\n{row}"


# COMMAND ----------

# DBTITLE 1,Helper -- dedup/conflict reconciliation proof (Genomics-style rule)


def reconciliation_stats(
    source_df, kept_df, quarantine_df, *, raw_bronze_df=None
) -> dict:
    """Proof of the collapse-identical / quarantine-conflicting rule: Bronze
    -> exact duplicates collapsed -> conflicts quarantined -> kept. Pass
    `raw_bronze_df` when `source_df` (resolve_conflicts' input) is already
    filtered, so `bronze_rows` reflects the true Bronze count."""
    dedup_input_rows = source_df.count()
    kept_rows = kept_df.count()
    quarantined_rows = quarantine_df.count()
    return {
        "bronze_rows": raw_bronze_df.count()
        if raw_bronze_df is not None
        else dedup_input_rows,
        "dedup_input_rows": dedup_input_rows,
        "exact_duplicates_collapsed": dedup_input_rows - kept_rows - quarantined_rows,
        "conflicts_quarantined": quarantined_rows,
        "kept_rows": kept_rows,
    }


# COMMAND ----------

# DBTITLE 1,Helper -- validation check log (shared by the validation notebooks)
CHECK_RESULTS = []
FINDINGS_BLOCKS = []


def report(name: str, ok: bool, detail: str = "", status: str | None = None) -> None:
    status = status or ("PASS" if ok else "FAIL")
    CHECK_RESULTS.append((name, status, detail))
    print(f"{status}  {name}  {detail}")


def keep(heading: str, df) -> None:
    """Show a result table and queue it for the findings export."""
    display(df)
    FINDINGS_BLOCKS.append((heading, rows_to_markdown(df)))


def checks_blocks() -> list:
    """The check table followed by every kept result table."""
    table = "\n".join(
        [
            "| check | status | detail |",
            "|---|---|---|",
            *[f"| {n} | {s} | {d} |" for n, s, d in CHECK_RESULTS],
        ]
    )
    return [("checks", table), *FINDINGS_BLOCKS]
