# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER SEMANTIC STRUCTURES SHARED LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the flat energy and commerce Silver structures, the helpers
# MAGIC of the source notebooks (time columns, place keys, column conformance,
# MAGIC cleaning of Bronze, wide joins on a grain, per-source writes) and the
# MAGIC constants of the weather, MaStR and Honda sources. A notebook pulls it in
# MAGIC with one `%run ../_semantic_common`; the library runs `_silver_common`
# MAGIC itself. No Silver column is an array, struct or map.

# COMMAND ----------

# MAGIC %run ./_silver_common

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

# COMMAND ----------

# DBTITLE 1,Source constants (weather, Honda, DWD products)
# DWD: per-table read metadata (quality column, native time window, own value
# columns). The tables share one merged Bronze table, so a part carries the empty
# value columns of the others; `values` names its own.
_CLOUD_LAYERS = [
    f"V_S{n}_{kind}" for n in range(1, 5) for kind in ("CS", "CSA", "HHS", "NS")
]
_MOISTURE = ["ABSF_STD", "VP_STD", "TF_STD", "P_STD", "TT_STD", "RF_STD", "TD_STD"]
_SOIL = ["V_TE002", "V_TE005", "V_TE010", "V_TE020", "V_TE050", "V_TE100"]
DWD_TABLES = {
    "dwd_air_temperature": {
        "qn": "QN_9",
        "time": "hourly",
        "values": ["TT_TU", "RF_TU"],
    },
    "dwd_moisture": {"qn": "QN_8", "time": "hourly", "values": _MOISTURE},
    "dwd_dew_point": {"qn": "QN_8", "time": "hourly", "values": ["TT", "TD"]},
    "dwd_pressure": {"qn": "QN_8", "time": "hourly", "values": ["P", "P0"]},
    "dwd_precipitation": {
        "qn": "QN_8",
        "time": "hourly",
        "values": ["R1", "RS_IND", "WRTR"],
    },
    "dwd_sun": {"qn": "QN_7", "time": "hourly", "values": ["SD_SO"]},
    "dwd_wind": {"qn": "QN_3", "time": "hourly", "values": ["F", "D"]},
    "dwd_wind_synop": {"qn": "QN_8", "time": "hourly", "values": ["FF", "DD"]},
    "dwd_extreme_wind": {"qn": "QN_8", "time": "hourly", "values": ["FX_911"]},
    "dwd_visibility": {"qn": "QN_8", "time": "hourly", "values": ["V_VV_I", "V_VV"]},
    "dwd_cloudiness": {"qn": "QN_8", "time": "hourly", "values": ["V_N_I", "V_N"]},
    "dwd_cloud_type": {
        "qn": "QN_8",
        "time": "hourly",
        "values": ["V_N", "V_N_I", *_CLOUD_LAYERS],
    },
    "dwd_weather_phenomena": {
        "qn": "QN_8",
        "time": "hourly",
        "values": ["WW", "WW_Text"],
    },
    "dwd_soil_temperature": {"qn": "QN_2", "time": "hourly", "values": _SOIL},
    "dwd_solar": {
        "qn": "QN_592",
        "time": "hourly_true_solar",
        "values": [
            "ATMO_LBERG",
            "FD_LBERG",
            "FG_LBERG",
            "SD_LBERG",
            "ZENIT",
            "MESS_DATUM_WOZ",
        ],
    },
}
METHOD_LABELS = {"P": "human_observation", "I": "instrument"}
HONDA_INTERVAL_SECONDS = {"1min": 60, "15min": 900, "1h": 3600}
EIGHTHS_TO_PERCENT = 12.5  # eighths (okta) -> percent
J_CM2_TO_W_M2 = 10000.0  # J/cm2 summed over an interval -> mean W/m2, / seconds

# COMMAND ----------

# DBTITLE 1,Structure parts shared by the time-series structures
# Place/time scaffolding shared by the observation structures. Grain =
# (source_system, place, observation_timestamp_utc, interval_seconds,
# interval_reference).
_PLACE_HEAD = [
    ("observation_key", "string"),
    ("location_key", "string"),
    ("source_location_id", "string"),
]
_TIME_COLUMNS = [
    ("observation_timestamp_native", "string"),
    ("time_basis", "string"),
    ("utc_offset_hours", "double"),
    ("observation_timestamp_utc", "timestamp"),
    ("observation_timestamp_project", "timestamp"),
    ("local_date", "date"),
    ("interval_seconds", "int"),
    ("interval_reference", "string"),
]
_TAIL = [("measurement_basis", "string"), *_PROVENANCE_HEAD, *_PROVENANCE_TAIL]


def _doubles(*names):
    return [(name, "double") for name in names]


def _texts(*names):
    return [(name, "string") for name in names]


def _booleans(*names):
    return [(name, "boolean") for name in names]


# COMMAND ----------

# DBTITLE 1,Structure -- weather_observation (wide: one row per station, instant, interval)
# One column group per weather family. Flags and quality columns carry the
# family name; every list of values is columns (alternate readings by source,
# cloud layers 1 to 4, wind statistics), never an array.
_CLOUD_LAYER_FIELDS = (
    ("genus_code", "string"),
    ("genus_text", "string"),
    ("base_height_m", "double"),
    ("cover_percent", "double"),
    ("cover_native_value", "double"),
    ("cover_native_unit", "string"),
)
WEATHER_FAMILY_MEASUREMENTS = {
    "temperature": [
        *_doubles(
            "air_temperature_degc",
            "dew_point_temperature_degc",
            "wet_bulb_temperature_degc",
            "air_temperature_moisture_degc",
        ),
        *_texts("air_temperature_moisture_quality_code"),
        *_doubles("air_temperature_dew_point_degc"),
        *_texts("air_temperature_dew_point_quality_code"),
        *_doubles("dew_point_temperature_moisture_degc"),
        *_texts("dew_point_temperature_moisture_quality_code"),
        *_booleans("dew_point_above_air_temperature"),
    ],
    "humidity": [
        *_doubles(
            "relative_humidity_percent",
            "absolute_humidity_g_per_m3",
            "vapour_pressure_hpa",
            "relative_humidity_moisture_percent",
        ),
        *_texts("relative_humidity_moisture_quality_code"),
        *_booleans("relative_humidity_above_100"),
    ],
    "pressure": [
        *_doubles("pressure_station_hpa", "pressure_station_native_value"),
        *_texts("pressure_station_native_unit"),
        *_doubles("pressure_station_moisture_hpa"),
        *_texts("pressure_station_moisture_quality_code"),
        *_doubles("pressure_sea_level_hpa", "pressure_sea_level_native_value"),
        *_texts("pressure_sea_level_native_unit"),
    ],
    "wind": [
        *_doubles(
            "wind_mean_speed_m_per_s",
            "wind_mean_direction_degrees",
        ),
        *_booleans("wind_mean_direction_variable"),
        *_doubles(
            "wind_instant_speed_m_per_s",
            "wind_instant_direction_degrees",
            "wind_max_gust_m_per_s",
            "wind_unspecified_speed_m_per_s",
            "wind_unspecified_direction_degrees",
            "wind_unspecified_gust_m_per_s",
        ),
    ],
    "precipitation": [
        *_doubles("precipitation_mm"),
        *_booleans("precipitation_occurred"),
        *_texts("precipitation_form_code", "precipitation_form_text"),
    ],
    "cloud": [
        *_doubles("cloud_cover_total_percent", "cloud_cover_total_native_value"),
        *_texts("cloud_cover_total_native_unit"),
        *_doubles(
            "cloud_cover_total_cloud_type_percent",
            "cloud_cover_total_cloud_type_native_value",
        ),
        *_texts(
            "cloud_cover_total_cloud_type_native_unit",
            "cloud_cover_total_cloud_type_quality_code",
        ),
        *_doubles("cloud_base_height_m"),
        *_texts("cloud_observation_method"),
        *[
            (f"cloud_layer_{number}_{field}", kind)
            for number in (1, 2, 3, 4)
            for field, kind in _CLOUD_LAYER_FIELDS
        ],
    ],
    "visibility": [
        *_doubles("visibility_m", "visibility_native_value"),
        *_texts("visibility_native_unit", "visibility_observation_method"),
    ],
    "present_weather": [*_texts("present_weather_code", "present_weather_text")],
    "soil_temperature": [
        *_doubles(
            *[f"soil_temperature_{depth}cm_degc" for depth in (2, 5, 10, 20, 50, 100)]
        )
    ],
    "solar_radiation": [
        *_doubles("global_radiation_w_per_m2", "global_radiation_native_value"),
        *_texts("global_radiation_native_unit"),
        *_doubles("diffuse_radiation_w_per_m2", "diffuse_radiation_native_value"),
        *_texts("diffuse_radiation_native_unit"),
    ],
    "longwave_radiation": [
        *_doubles(
            "longwave_downward_radiation_w_per_m2",
            "longwave_downward_radiation_native_value",
        ),
        *_texts("longwave_downward_radiation_native_unit"),
    ],
    "sunshine_duration": [*_doubles("sunshine_duration_minutes")],
    "solar_geometry": [*_doubles("solar_zenith_angle_degrees")],
    "uv_index": [*_doubles("uv_index")],
}
WEATHER_FAMILIES = list(WEATHER_FAMILY_MEASUREMENTS)
# Data-quality flags of every family; a flag a source does not check stays NULL.
WEATHER_FLAG_NAMES = ("key_conflict_resolved", "qn_missing", "stuck_reading")


def _weather_family_block(family: str) -> list:
    return [
        *WEATHER_FAMILY_MEASUREMENTS[family],
        (f"{family}_quality_code", "string"),
        (f"{family}_quality_label", "string"),
        (f"{family}_source_dataset", "string"),
        *[(f"{family}_{flag}", "boolean") for flag in WEATHER_FLAG_NAMES],
    ]


WEATHER_OBSERVATION_CONTEXT = [
    *_PLACE_HEAD,
    *_TIME_COLUMNS,
    ("true_solar_time_native", "string"),
    ("measurement_basis", "string"),
    ("source_system", "string"),
]
WEATHER_OBSERVATION_COLUMNS = [
    *WEATHER_OBSERVATION_CONTEXT,
    *[
        column
        for family in WEATHER_FAMILIES
        for column in _weather_family_block(family)
    ],
    *_PROVENANCE_TAIL,
]

WEATHER_GRAIN = [
    "source_system",
    "location_key",
    "observation_timestamp_utc",
    "interval_seconds",
    "interval_reference",
]

# COMMAND ----------

# DBTITLE 1,Structures -- weather daily, location, station periods, gaps
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
    ("observation_count", "int"),
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
    ("official_municipality_key", "string"),
    ("geography_basis", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

# One row per station period. `period_kind`: location (coordinates), name (name
# history), instrument, parameter (parameter period). Several rows can share a
# start date (relocations), so record_ordinal keeps them apart; every row keeps
# its own validity window.
WEATHER_STATION_PERIOD_COLUMNS = [
    ("period_kind", "string"),
    ("location_key", "string"),
    ("source_location_id", "string"),
    ("valid_from", "date"),
    ("valid_to", "date"),
    ("record_ordinal", "int"),
    ("name", "string"),
    ("latitude", "double"),
    ("longitude", "double"),
    ("elevation_m", "double"),
    ("coordinate_outside_germany_bounding_box", "boolean"),
    *_texts("parameter_category"),
    *_doubles("sensor_height_m"),
    *_texts(
        "device_type",
        "measurement_method",
        "parameter_source_code",
        "parameter_description_de",
        "parameter_unit",
        "parameter_data_source",
        "parameter_extra_info",
        "parameter_special_notes",
        "parameter_reference",
    ),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]
WEATHER_MISSING_VALUE_PERIOD_COLUMNS = [
    ("location_key", "string"),
    ("source_location_id", "string"),
    ("name", "string"),
    ("parameter_source_code", "string"),
    ("gap_start_timestamp", "timestamp"),
    ("gap_end_timestamp", "timestamp"),
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
    ("observed_count", "bigint"),
    ("time_span_start_utc", "timestamp"),
    ("time_span_end_utc", "timestamp"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

# COMMAND ----------

# DBTITLE 1,Structures -- code lists and register links
# MaStR code lists and the DWD parameter catalog in one structure, keyed by
# (catalog_kind, code_id); katalog values point at their category through
# parent_id. The unit, description and vintage columns are filled for the DWD
# parameter rows only.
REFERENCE_CODE_LIST_COLUMNS = [
    ("catalog_kind", "string"),
    ("code_id", "string"),
    ("parent_id", "string"),
    ("label", "string"),
    ("unit", "string"),
    ("description_de", "string"),
    ("catalog_vintage", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

# Every MaStR link as one typed relationship structure.
REGISTER_LINK_COLUMNS = [
    ("relationship_type", "string"),
    ("parent_type", "string"),
    ("parent_id", "string"),
    ("linked_type", "string"),
    ("linked_id", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

# COMMAND ----------

# DBTITLE 1,Structure -- commerce events (REES46 and GA4 in one grain: one row per event)
_EVENT_TIME = [
    ("event_timestamp_native", "string"),
    ("event_timestamp_utc", "timestamp"),
    ("event_timestamp_project", "timestamp"),
    ("local_date", "date"),
]
COMMERCE_EVENT_COLUMNS = [
    ("event_key", "string"),
    ("event_date_native", "string"),
    *_EVENT_TIME,
    *_texts("event_type", "event_name", "user_id", "user_session", "product_id"),
    *_texts("category_id", "category_code", "category_l1", "category_l2"),
    *_texts("category_l3", "brand"),
    *_doubles("price"),
    *_booleans("currency_unknown"),
    *_texts("user_pseudo_id"),
    ("session_id", "bigint"),
    ("session_number", "bigint"),
    *_texts(
        "page_location",
        "page_title",
        "search_term",
        "unique_search_term",
        "geo_country",
        "transaction_id",
    ),
    *_doubles("purchase_revenue"),
    ("unique_items", "bigint"),
    ("total_item_quantity", "bigint"),
    *_booleans(
        "key_conflict_resolved", "bot_burst_suspected", "category_code_ambiguous"
    ),
    ("measurement_basis", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]
# item_id is a campaign id on promotion events, a product id otherwise.
GA4_EVENT_ITEM_COLUMNS = [
    ("event_key", "string"),
    ("item_ordinal", "int"),
    ("event_name", "string"),
    ("event_timestamp_utc", "timestamp"),
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

# COMMAND ----------

# DBTITLE 1,Structures -- energy observation (wide) and energy component (rows)
# Energy: a place is a site (location_key) or a market area (market_area_code,
# never a geography). Sign is always native.
_ENERGY_HEAD = [*_PLACE_HEAD, ("market_area_code", "string")]
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
MARKET_AREA_VALUES = tuple(dict.fromkeys(MARKET_AREA_CODES.values()))

# Cumulative register state (kWh) plus its derived forward increment.
HONDA_METER_CHANNELS = {
    "electricity_total": ("electricity", "total"),
    "electricity_solar_photovoltaic": ("electricity", "PV"),
    "electricity_combined_heat_and_power": ("electricity", "CHP"),
    "heating_total": ("heating", "total"),
    "heating_combined_heat_and_power_heat": ("heating", "CHP_heat"),
    "heating_combined_heat_and_power_electricity": ("heating", "CHP_elec"),
    "cooling_total": ("cooling", "total"),
    "cooling_electricity": ("cooling", "cool_elec"),
}
THERMAL_COLUMNS = (
    "heating_total_w",
    "heating_combined_heat_and_power_heat_w",
    "cooling_total_w",
)
PRICE_COLUMN = "price_eur_per_mwh"
METER_COLUMNS = [f"{channel}_kwh" for channel in HONDA_METER_CHANNELS]
# Flag columns of energy_observation: five-sigma outlier and stuck reading per
# thermal channel, outlier for the price, four checks per meter register.
ENERGY_FLAG_COLUMNS = [
    f"{PRICE_COLUMN}_outlier_five_sigma",
    *[
        f"{column}_{check}"
        for column in THERMAL_COLUMNS
        for check in ("outlier_five_sigma", "stuck_reading")
    ],
    *[
        name
        for channel in HONDA_METER_CHANNELS
        for name in (
            f"{channel}_kwh_outlier_five_sigma",
            f"{channel}_kwh_stuck_reading",
            f"{channel}_kwh_magnitude_decrease",
            f"{channel}_increment_kwh_spans_gap",
        )
    ],
]
ENERGY_OBSERVATION_COLUMNS = [
    *_ENERGY_HEAD,
    *_TIME_COLUMNS,
    *_doubles(PRICE_COLUMN, *THERMAL_COLUMNS),
    # the SMARD balancing energy price of the quarter-hour
    ("balancing_price_eur_per_mwh", "double"),
    *[
        column
        for channel in HONDA_METER_CHANNELS
        for column in (
            (f"{channel}_kwh", "double"),
            (f"{channel}_increment_kwh", "double"),
        )
    ],
    ("increment_derivation_rule", "string"),
    ("sign_convention", "string"),
    *_booleans(*ENERGY_FLAG_COLUMNS),
    *_TAIL,
]

# One row per component of an energy balance or forecast at a place and window.
ENERGY_COMPONENT_COLUMNS = [
    *_ENERGY_HEAD,
    *_TIME_COLUMNS,
    *_texts("component_group", "component_kind", "forecast_scope", "carrier_code"),
    *_texts("native_label"),
    *_doubles("energy_mwh", "power_w"),
    *_texts("semantic_status", "semantic_issue_ref", "source_series"),
    *_texts("source_filter_id"),
    # the neighbour of a cross-border exchange, and the licence text a source gives
    *_texts("counterpart_area_code", "source_license"),
    *_booleans("is_outlier_five_sigma", "is_stuck_reading"),
    ("sign_convention", "string"),
    *_TAIL,
]

# COMMAND ----------

# DBTITLE 1,Structures -- grid events, plant sample, device snapshot
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
    ("instructing_transmission_system_operator_native", "string"),
    ("instructing_market_area_code", "string"),
    ("requesting_transmission_system_operator_native", "string"),
    *[(f"requested_by_{area}", "boolean") for area in MARKET_AREA_VALUES],
    ("affected_asset_text", "string"),
    ("affected_unit_match_name", "string"),
    ("affected_unit_match_confidence", "string"),
    ("primary_energy_type_native", "string"),
    ("primary_energy_type", "string"),
    *_booleans(
        "negative_duration",
        "mean_power_above_maximum_power",
        "energy_differs_from_power_times_duration",
    ),
    ("measurement_basis", "string"),
    *_PROVENANCE_HEAD,
    *_PROVENANCE_TAIL,
]

# No time, no key: one row per documented source row.
PLANT_OPERATING_SAMPLE_COLUMNS = [
    ("sample_key", "string"),
    ("ambient_temperature_degc", "double"),
    ("exhaust_vacuum_cm_of_mercury", "double"),
    ("ambient_pressure_mbar", "double"),
    ("relative_humidity_percent", "double"),
    ("net_electrical_output_mw", "double"),
    ("repeat_index", "int"),
    *_booleans("exact_repeat", "relative_humidity_above_100"),
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
    ("observation_timestamp_native", "string"),
    ("observation_timestamp_utc", "timestamp"),
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

# COMMAND ----------

# DBTITLE 1,Structure registry (the declared inventory)
# Every semantic Silver table and its columns. The field-class generator
# registers exactly these (energy_silver); notebooks conform to them.
SEMANTIC_STRUCTURES = {
    "weather_observation": WEATHER_OBSERVATION_COLUMNS,
    "weather_daily": WEATHER_DAILY_COLUMNS,
    "weather_location": WEATHER_LOCATION_COLUMNS,
    "weather_station_period": WEATHER_STATION_PERIOD_COLUMNS,
    "weather_missing_value_period": WEATHER_MISSING_VALUE_PERIOD_COLUMNS,
    "weather_missingness_reconciliation": WEATHER_MISSINGNESS_RECONCILIATION_COLUMNS,
    "reference_code_list": REFERENCE_CODE_LIST_COLUMNS,
    "register_link": REGISTER_LINK_COLUMNS,
    "energy_observation": ENERGY_OBSERVATION_COLUMNS,
    "energy_component": ENERGY_COMPONENT_COLUMNS,
    "grid_intervention_event": GRID_INTERVENTION_EVENT_COLUMNS,
    "plant_operating_sample": PLANT_OPERATING_SAMPLE_COLUMNS,
    "device_telemetry_snapshot": DEVICE_TELEMETRY_SNAPSHOT_COLUMNS,
    "commerce_event": COMMERCE_EVENT_COLUMNS,
    "ga4_event_item": GA4_EVENT_ITEM_COLUMNS,
}

# Register structures whose wide column set comes from the MaStR / plant-list
# contracts and mappings: registered as the union of their member tables'
# registry rows (minus `ecosystem`) plus the listed discriminators.
# MaStR Bronze table -> English name for its own key column (the same source
# name means a different entity per table, so it cannot sit in the flat mapping).
MASTR_KEY_NAMES = {
    "mastr_ertuechtigungen": {"Id": "repowering_id"},
    "mastr_bilanzierungsgebiete": {"Id": "balancing_area_id"},
    "mastr_marktakteure": {"MastrNummer": "market_actor_id"},
    "mastr_marktakteure_und_rollen": {"MastrNummer": "market_actor_role_id"},
    "mastr_lokationen": {"MastrNummer": "location_id"},
    "mastr_netze": {"MastrNummer": "grid_id"},
}

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
    "grid_location": (
        ["mastr_lokationen"],
        ["distinct_coordinate_count", "coordinate_conflict"],
    ),
    "grid_connection_point": (["mastr_netzanschlusspunkte"], []),
    "grid_network": (["mastr_netze"], []),
    "balancing_area": (["mastr_bilanzierungsgebiete"], []),
    "register_change_event": (
        [
            "mastr_unit_deletion_events",
            "mastr_actor_deletion_events",
            "mastr_grid_operator_change_events",
        ],
        ["event_kind"],
    ),
    "power_plant_register": (["power_plant_list"], []),
    "power_plant_capacity_plan": (["power_plant_capacity_additions"], []),
}

# Schema per structure (per-schema table quota): reference and master data
# in energy_silver_reference, Commerce in commerce_silver, the rest in
# SEMANTIC_SCHEMA.
REFERENCE_STRUCTURES = {
    "weather_location",
    "weather_station_period",
    "weather_missing_value_period",
    "weather_missingness_reconciliation",
    "reference_code_list",
    "market_actor",
    "market_actor_role",
    "grid_location",
    "grid_connection_point",
    "grid_network",
    "balancing_area",
}
COMMERCE_STRUCTURES = {"commerce_event", "ga4_event_item"}


def semantic_target_schema(table: str) -> str:
    if table in REFERENCE_STRUCTURES:
        return f"{SEMANTIC_SCHEMA}_reference"
    if table in COMMERCE_STRUCTURES:
        return "commerce_silver"
    return SEMANTIC_SCHEMA


# COMMAND ----------

# DBTITLE 1,Continent lookup
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

# DBTITLE 1,Helper -- time columns and place keys


def add_project_time(df, utc_col: str = "observation_timestamp_utc"):
    """Project-time timestamp (Europe/Berlin wall clock) and local date from a
    UTC instant. NULL where the source has no instant."""
    local = F.from_utc_timestamp(F.col(utc_col), PROJECT_TZ)
    return df.withColumn("observation_timestamp_project", local).withColumn(
        "local_date", F.to_date(local)
    )


def local_time_to_utc(local_ts_col: str, offset_hours_col: str):
    """UTC instant from a local wall-clock timestamp and its GMT offset (hours)."""
    return (
        F.col(local_ts_col).cast("timestamp").cast("long")
        - (F.col(offset_hours_col) * 3600).cast("long")
    ).cast("timestamp")


def location_key(source_system: str, source_location_id):
    """Deterministic place key; `source_location_id` is a column name or Column."""
    return sha_key(F.lit(source_system), source_location_id)


def continent_of(country_code_col: str):
    m = F.create_map([F.lit(x) for kv in CONTINENT_BY_ISO2.items() for x in kv])
    return m[F.upper(F.col(country_code_col))]


def finish_observation(df, *, source: str, grain: list):
    """The place key (when the frame has a `source_location_id`), the project
    time and local date, and the observation key and record id built from the
    `grain` columns. The source notebook has set the native and UTC time, the
    interval and the basis before."""
    if "source_location_id" in df.columns:
        df = df.withColumn("location_key", location_key(source, "source_location_id"))
    df = add_project_time(df)
    return df.withColumn("observation_key", sha_key(*grain)).withColumn(
        "source_record_id", F.col("observation_key")
    )


# COMMAND ----------

# DBTITLE 1,Helper -- columns from a spec, rows with a value, boolean flags


def lit_map(d: dict):
    """Literal dict as a Spark map column; index it with a Column."""
    return F.create_map([F.lit(x) for kv in d.items() for x in kv])


def from_spec(df, columns: dict, keep: list | None = None):
    """Add each column of `columns` (name -> source column name or Column);
    with `keep`, keep only the rows where at least one of those columns is set."""
    for name, source in columns.items():
        df = df.withColumn(name, F.col(source) if isinstance(source, str) else source)
    return any_present(df, keep) if keep else df


def any_present(df, cols: list):
    """Filter to rows where at least one of `cols` is non-NULL, so a source
    row that populates none of a family's fields doesn't produce a junk row."""
    cond = F.lit(False)
    for c in cols:
        cond = cond | F.col(c).isNotNull()
    return df.filter(cond)


def flag(condition):
    """A boolean flag column: true where the condition holds, else false."""
    return F.coalesce(condition, F.lit(False))


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


def mastr_structure(
    df, bronze_table: str, id_col: str, *, name_map: dict, coded_map: dict, rid: str
):
    """One MaStR Bronze table as a Silver structure: decoded, renamed and typed
    by `mastr_standardise`, with the record id taken from `id_col`."""
    names = {**name_map, **MASTR_KEY_NAMES.get(bronze_table, {})}
    out = mastr_standardise(df, names, coded_map)
    out = out.withColumn("_srid", F.col(names.get(id_col, id_col)).cast("string"))
    return add_semantic_provenance(out, "mastr", bronze_table, rid, "_srid")


# COMMAND ----------

# DBTITLE 1,Helper -- clean one Bronze table (collapse identical rows, quarantine conflicts)


def clean_bronze(
    df,
    key_cols: list,
    content_cols: list,
    *,
    source: str,
    bronze_table: str,
    rid: str,
    qn_col: str | None = None,
    raw_bronze_df=None,
):
    """(kept, stats): identical rows collapsed, same-key conflicts quarantined
    (written to the quarantine table), and the reconciliation numbers for the
    findings."""
    kept, quarantined = resolve_conflicts(
        df, key_cols, content_cols, qn_col=qn_col, bronze_table=bronze_table
    )
    write_quarantine(quarantined.withColumn("source_system", F.lit(source)), rid)
    stats = reconciliation_stats(df, kept, quarantined, raw_bronze_df=raw_bronze_df)
    return kept, stats


# COMMAND ----------

# DBTITLE 1,Helper -- one wide row per grain from several frames


def join_on_grain(frames: dict, grain: list, context: list):
    """One row per `grain`: the `context` columns of every frame (identical for
    a grain; one row wins) and the other columns of each frame, left-joined on
    the grain (NULL-safe). Each frame must hold one row per grain."""
    spread = None
    for frame in frames.values():
        part = frame.select(*context)
        spread = part if spread is None else spread.unionByName(part)
    wide = spread.dropDuplicates(grain)
    for frame in frames.values():
        values = [c for c in frame.columns if c not in context]
        right = frame.select(
            *[F.col(c).alias(f"_g_{c}") for c in grain],
            *values,
        )
        condition = F.lit(True)
        for c in grain:
            condition = condition & wide[c].eqNullSafe(right[f"_g_{c}"])
        wide = wide.join(right, condition, "left").drop(*[f"_g_{c}" for c in grain])
    return wide


# COMMAND ----------

# DBTITLE 1,Helper -- write (full overwrite or replace one source's rows)


def _schema_signature(schema) -> set:
    """(name, type) pairs, ignoring field order and nullability -- the things
    that make a replaceWhere genuinely incompatible with the existing table."""
    return {(f.name, f.dataType.simpleString()) for f in schema.fields}


def _migrate_schema(existing_df, columns: list):
    """Like conform(), for carrying pre-existing rows onto a changed schema."""
    return existing_df.select(
        *[
            (F.col(n) if n in existing_df.columns else F.lit(None)).cast(t).alias(n)
            for n, t in columns
        ]
    )


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
            kept = _migrate_schema(
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
    "observation_timestamp_native",
    "date_native",
    "event_key",
    "sample_key",
    "device_key",
    "source_device_id",
    "device_name",
    "ip_address",
}

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
            "period_kind",
            "component_group",
            "event_kind",
            "catalog_kind",
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
        (c for c in ("observation_timestamp_utc", "local_date") if c in df.columns),
        None,
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

# DBTITLE 1,Helper -- dedup/conflict reconciliation proof


def reconciliation_stats(
    source_df, kept_df, quarantine_df, *, raw_bronze_df=None
) -> dict:
    """Proof of the collapse-identical / quarantine-conflicting rule: Bronze
    -> duplicates collapsed -> conflicts quarantined -> kept.
    Pass `raw_bronze_df` when `source_df` (resolve_conflicts' input) is already
    filtered, so `bronze_rows` reflects the true Bronze count. `qn_only_collapsed`
    appears when kept_df carries `_qn_only_dropped`."""
    dedup_input_rows = source_df.count()
    quarantined_rows = quarantine_df.count()
    if "_qn_only_dropped" in kept_df.columns:
        agg = kept_df.agg(
            F.count(F.lit(1)).alias("n"),
            F.sum("_qn_only_dropped").alias("qn"),
        ).first()
        kept_rows, qn_only = agg["n"], int(agg["qn"] or 0)
    else:
        kept_rows, qn_only = kept_df.count(), None
    out = {
        "bronze_rows": raw_bronze_df.count()
        if raw_bronze_df is not None
        else dedup_input_rows,
        "dedup_input_rows": dedup_input_rows,
        "exact_duplicates_collapsed": dedup_input_rows
        - kept_rows
        - quarantined_rows
        - (qn_only or 0),
    }
    if qn_only is not None:
        out["qn_only_collapsed"] = qn_only
    out["conflicts_quarantined"] = quarantined_rows
    out["kept_rows"] = kept_rows
    return out


# COMMAND ----------

# DBTITLE 1,Helper -- validation check log (shared by the validation notebook)
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


# COMMAND ----------

# DBTITLE 1,Helper -- DWD reference notebooks (station filter, metadata renames)
# References `SOURCE`, `RID`, `STATION_IDS`, `TABLES` and `META_FIELDS` from the
# caller's configuration cell (resolved at call time, not at import time).


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

# DBTITLE 1,Helper -- MaStR generation units (structural constants, bounding-box quarantine)
# References `NAME_MAP`, `SOURCE`, `RID` and `STRUCTURAL_CONSTANT_COLS` from the
# caller's configuration cell (resolved at call time, not at import time).


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


def quarantine_out_of_bbox(df, bronze_table: str) -> None:
    """Record (not drop) units whose coordinate falls outside the Germany
    bounding box -- `df` keeps every row."""
    bad = df.filter(bbox_outside_de("latitude", "longitude"))
    write_quarantine(
        _q_rows(
            bad,
            "coord_outside_de_bbox",
            "unit coordinate outside the Germany bounding box",
            "latitude,longitude",
            "latitude",
            "unit_id",
            SOURCE,
            bronze_table,
        ),
        RID,
    )


# COMMAND ----------

# DBTITLE 1,Helper -- Honda data-quality flags (five-sigma outlier, stuck reading)


def add_5sigma_outlier_flags(df, value_cols):
    """Add `_{c}_5sigma_outlier` per value column, from that column's own
    mean/sd (per-source profiling data-quality section)."""
    if not value_cols:
        return df
    stat_exprs = []
    for c in value_cols:
        stat_exprs += [F.mean(c).alias(f"{c}__mean"), F.stddev(c).alias(f"{c}__sd")]
    stats = df.agg(*stat_exprs).first()
    for c in value_cols:
        mean_c, sd_c = stats[f"{c}__mean"], stats[f"{c}__sd"]
        df = df.withColumn(
            f"_{c}_5sigma_outlier",
            F.when(
                F.lit(sd_c).isNotNull() & (F.lit(sd_c) > 0) & F.col(c).isNotNull(),
                F.abs(F.col(c) - F.lit(mean_c)) > (F.lit(5.0) * F.lit(sd_c)),
            ).otherwise(F.lit(False)),
        )
    return df


def add_stuck_reading_flags(df, value_cols, lookback_by_freq, w):
    """Add `_{c}_stuck_reading_flag` per value column -- flat for the window
    `lookback_by_freq` names per frequency (matching profiling's "value ==
    value N steps back" definition, scaled to the same ~10h window per
    frequency). `w` is the caller's own `Window.partitionBy("frequency").
    orderBy("datetime_utc")` -- passed in (not built here) so a caller can
    reuse the same window for its monotonicity-violation flags."""
    for c in value_cols:
        stuck_expr = F.lit(False)
        for freq, lag_n in lookback_by_freq.items():
            lag1 = F.lag(F.col(c), 1).over(w)
            lagN = F.lag(F.col(c), lag_n).over(w)
            cond = (
                (F.col("frequency") == freq)
                & F.col(c).isNotNull()
                & (F.col(c) == lag1)
                & (F.col(c) == lagN)
            )
            stuck_expr = F.when(cond, F.lit(True)).otherwise(stuck_expr)
        df = df.withColumn(f"_{c}_stuck_reading_flag", stuck_expr)
    return df
