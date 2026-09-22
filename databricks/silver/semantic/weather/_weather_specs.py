# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # WEATHER SOURCE COLUMN SPECIFICATIONS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** per-source column -> variable -> family specifications for
# MAGIC the weather structures. `family` routes a column to the shared structure
# MAGIC (WEATHER_FAMILIES) it belongs in by meaning, not by source. Pulled in
# MAGIC with `%run ./_weather_specs` after `_semantic_common`. Definitions only.

# COMMAND ----------

# DBTITLE 1,Shared labels and constants
METHOD_LABELS = {"P": "human_observation", "I": "instrument"}

# W/m2 mean over an interval from J/cm2 summed over it: 10000 J/m2 per J/cm2.
_J_CM2_TO_W_M2 = 10000.0


def _radiation(col, variable, seconds):
    return spec(
        col,
        variable,
        "sum",
        "J_per_cm2",
        family="weather_solar_radiation",
        unit="W_per_m2",
        factor=_J_CM2_TO_W_M2 / seconds,
        interval_seconds=seconds,
        rule="J/cm2 summed over the interval -> mean W/m2 (x10000 / interval_seconds)",
    )


def _eighths_to_percent(col, variable, level=None, primary=True, method_col=None):
    return spec(
        col,
        variable,
        "instant",
        "eighths",
        family="weather_cloud",
        unit="percent",
        factor=12.5,
        level=level,
        primary=primary,
        method_col=method_col,
        interval_seconds=3600,
        rule="eighths (okta) -> percent (x12.5)",
        null_value=-1.0 if level is None else None,
        null_reason="sky_not_recognisable" if level is None else None,
    )


# COMMAND ----------

# DBTITLE 1,DWD -- cloud layer specs (four layers of the cloud-type product)
_CLOUD_LAYERS = []
for _n in (1, 2, 3, 4):
    _lv = f"layer_{_n}"
    _CLOUD_LAYERS += [
        spec(
            f"V_S{_n}_CS",
            "cloud_genus",
            "instant",
            "code",
            family="weather_cloud",
            categorical=True,
            level=_lv,
            text_col=f"V_S{_n}_CSA",
            interval_seconds=3600,
        ),
        spec(
            f"V_S{_n}_HHS",
            "cloud_base_height",
            "instant",
            "metres",
            family="weather_cloud",
            level=_lv,
            interval_seconds=3600,
        ),
        _eighths_to_percent(f"V_S{_n}_NS", "cloud_cover", level=_lv),
    ]

# COMMAND ----------

# DBTITLE 1,DWD -- table specifications
# `primary=False` marks a variable that a dedicated product also carries; the
# alternate is kept, not dropped, so agreement can be measured. `family`
# groups by meaning (temperature, humidity, ...), not by this source table.
DWD_TABLES = {
    "dwd_air_temperature": {
        "qn": "QN_9",
        "time": "hourly",
        "specs": [
            spec(
                "TT_TU",
                "air_temperature",
                "instant",
                "degC",
                family="weather_temperature",
                interval_seconds=3600,
            ),
            spec(
                "RF_TU",
                "relative_humidity",
                "instant",
                "percent",
                family="weather_humidity",
                interval_seconds=3600,
            ),
        ],
    },
    "dwd_moisture": {
        "qn": "QN_8",
        "time": "hourly",
        "specs": [
            spec(
                "ABSF_STD",
                "absolute_humidity",
                "instant",
                "g_per_m3",
                family="weather_humidity",
                interval_seconds=3600,
            ),
            spec(
                "VP_STD",
                "vapour_pressure",
                "instant",
                "hPa",
                family="weather_humidity",
                interval_seconds=3600,
            ),
            spec(
                "TF_STD",
                "wet_bulb_temperature",
                "instant",
                "degC",
                family="weather_temperature",
                interval_seconds=3600,
            ),
            spec(
                "P_STD",
                "pressure_station",
                "instant",
                "hPa",
                family="weather_pressure",
                primary=False,
                interval_seconds=3600,
            ),
            spec(
                "TT_STD",
                "air_temperature",
                "instant",
                "degC",
                family="weather_temperature",
                primary=False,
                interval_seconds=3600,
            ),
            spec(
                "RF_STD",
                "relative_humidity",
                "instant",
                "percent",
                family="weather_humidity",
                primary=False,
                interval_seconds=3600,
            ),
            spec(
                "TD_STD",
                "dew_point_temperature",
                "instant",
                "degC",
                family="weather_temperature",
                primary=False,
                interval_seconds=3600,
            ),
        ],
    },
    "dwd_dew_point": {
        "qn": "QN_8",
        "time": "hourly",
        "specs": [
            spec(
                "TT",
                "air_temperature",
                "instant",
                "degC",
                family="weather_temperature",
                primary=False,
                interval_seconds=3600,
            ),
            spec(
                "TD",
                "dew_point_temperature",
                "instant",
                "degC",
                family="weather_temperature",
                interval_seconds=3600,
            ),
        ],
    },
    "dwd_pressure": {
        "qn": "QN_8",
        "time": "hourly",
        "specs": [
            spec(
                "P",
                "pressure_sea_level",
                "instant",
                "hPa",
                family="weather_pressure",
                interval_seconds=3600,
            ),
            spec(
                "P0",
                "pressure_station",
                "instant",
                "hPa",
                family="weather_pressure",
                interval_seconds=3600,
            ),
        ],
    },
    "dwd_precipitation": {
        "qn": "QN_8",
        "time": "hourly",
        "specs": [
            spec(
                "R1",
                "precipitation",
                "sum",
                "mm",
                family="weather_precipitation",
                interval_seconds=3600,
            ),
            spec(
                "RS_IND",
                "precipitation_occurred",
                "instant",
                "flag",
                family="weather_precipitation",
                interval_seconds=3600,
            ),
            spec(
                "WRTR",
                "precipitation_form",
                "instant",
                "code",
                family="weather_precipitation",
                categorical=True,
                interval_seconds=3600,
            ),
        ],
    },
    "dwd_sun": {
        "qn": "QN_7",
        "time": "hourly",
        "specs": [
            spec(
                "SD_SO",
                "sunshine_duration",
                "sum",
                "minutes",
                family="weather_solar_radiation",
                interval_seconds=3600,
            ),
        ],
    },
    "dwd_wind": {
        "qn": "QN_3",
        "time": "hourly",
        "specs": [
            spec(
                "F",
                "wind_speed",
                "mean",
                "m_per_s",
                family="weather_wind",
                interval_seconds=3600,
            ),
            spec(
                "D",
                "wind_direction",
                "mean",
                "degrees",
                family="weather_wind",
                interval_seconds=3600,
                null_value=990.0,
                null_reason="variable_direction",
            ),
        ],
    },
    "dwd_wind_synop": {
        "qn": "QN_8",
        "time": "hourly",
        "specs": [
            spec(
                "FF",
                "wind_speed",
                "instant",
                "m_per_s",
                family="weather_wind",
                interval_seconds=3600,
            ),
            spec(
                "DD",
                "wind_direction",
                "instant",
                "degrees",
                family="weather_wind",
                interval_seconds=3600,
            ),
        ],
    },
    "dwd_extreme_wind": {
        "qn": "QN_8",
        "time": "hourly",
        "specs": [
            spec(
                "FX_911",
                "wind_gust",
                "max",
                "m_per_s",
                family="weather_wind",
                interval_seconds=3600,
            ),
        ],
    },
    "dwd_visibility": {
        "qn": "QN_8",
        "time": "hourly",
        "specs": [
            spec(
                "V_VV",
                "visibility",
                "instant",
                "metres",
                family="weather_visibility",
                interval_seconds=3600,
                method_col="V_VV_I",
            ),
        ],
    },
    "dwd_cloudiness": {
        "qn": "QN_8",
        "time": "hourly",
        "specs": [_eighths_to_percent("V_N", "cloud_cover", method_col="V_N_I")],
    },
    "dwd_cloud_type": {
        "qn": "QN_8",
        "time": "hourly",
        "specs": [
            _eighths_to_percent(
                "V_N", "cloud_cover", primary=False, method_col="V_N_I"
            ),
            *_CLOUD_LAYERS,
        ],
    },
    "dwd_weather_phenomena": {
        "qn": "QN_8",
        "time": "hourly",
        "specs": [
            spec(
                "WW",
                "present_weather",
                "instant",
                "code",
                family="weather_present_weather",
                categorical=True,
                text_col="WW_Text",
                interval_seconds=3600,
                null_value=-1.0,
                null_reason="no_phenomenon_coded",
            ),
        ],
    },
    "dwd_soil_temperature": {
        "qn": "QN_2",
        "time": "hourly",
        "specs": [
            spec(
                f"V_TE{d:03d}",
                "soil_temperature",
                "instant",
                "degC",
                family="weather_soil_temperature",
                level=f"depth_{d}cm",
                interval_seconds=3600,
            )
            for d in (2, 5, 10, 20, 50, 100)
        ],
    },
    "dwd_solar": {
        "qn": "QN_592",
        "time": "10min",
        "specs": [
            _radiation("ATMO_LBERG", "longwave_downward_radiation", 3600),
            _radiation("FD_LBERG", "diffuse_radiation", 600),
            _radiation("FG_LBERG", "global_radiation", 600),
            spec(
                "SD_LBERG",
                "sunshine_duration",
                "sum",
                "minutes",
                family="weather_solar_radiation",
                interval_seconds=600,
            ),
            spec(
                "ZENIT",
                "solar_zenith_angle",
                "instant",
                "degrees",
                family="weather_solar_radiation",
                interval_seconds=600,
            ),
        ],
    },
}

# COMMAND ----------

# DBTITLE 1,Honda site weather specifications
# Sampling interval comes from the row's `frequency`; the sensor statistic is
# not documented.
HONDA_INTERVAL_SECONDS = {"1min": 60, "15min": 900, "1h": 3600}
HONDA_SPECS = [
    spec(
        "WeatherStation_Weather_Ta",
        "air_temperature",
        "unspecified",
        "degC",
        family="weather_temperature",
    ),
    # Same underlying meaning as DWD's global_radiation (surface global solar
    # irradiance); labelled the same so all sources land in one comparable row.
    spec(
        "WeatherStation_Weather_Igm",
        "global_radiation",
        "unspecified",
        "W_per_m2",
        family="weather_solar_radiation",
    ),
]

# COMMAND ----------

# DBTITLE 1,AccuWeather historical hourly (metric) specifications
# Statistic is not documented for the provider's hourly values.
AW_SPECS = [
    spec(
        "temperature",
        "air_temperature",
        "unspecified",
        "degC",
        family="weather_temperature",
        interval_seconds=3600,
    ),
    spec(
        "temperature_dew_point",
        "dew_point_temperature",
        "unspecified",
        "degC",
        family="weather_temperature",
        interval_seconds=3600,
    ),
    spec(
        "humidity_relative",
        "relative_humidity",
        "unspecified",
        "percent",
        family="weather_humidity",
        interval_seconds=3600,
    ),
    spec(
        "pressure",
        "pressure_station",
        "unspecified",
        "Pa",
        family="weather_pressure",
        unit="hPa",
        factor=0.01,
        interval_seconds=3600,
        rule="Pa -> hPa (x0.01)",
    ),
    spec(
        "pressure_msl",
        "pressure_sea_level",
        "unspecified",
        "Pa",
        family="weather_pressure",
        unit="hPa",
        factor=0.01,
        interval_seconds=3600,
        rule="Pa -> hPa (x0.01)",
    ),
    spec(
        "wind_speed",
        "wind_speed",
        "unspecified",
        "m_per_s",
        family="weather_wind",
        interval_seconds=3600,
    ),
    spec(
        "wind_gust",
        "wind_gust",
        "unspecified",
        "m_per_s",
        family="weather_wind",
        interval_seconds=3600,
    ),
    spec(
        "wind_direction",
        "wind_direction",
        "unspecified",
        "degrees",
        family="weather_wind",
        interval_seconds=3600,
    ),
    spec(
        "precipitation_lwe",
        "precipitation",
        "unspecified",
        "mm",
        family="weather_precipitation",
        interval_seconds=3600,
    ),
    # Same underlying meaning as DWD's global_radiation; labelled the same.
    spec(
        "solar_irradiance",
        "global_radiation",
        "unspecified",
        "W_per_m2",
        family="weather_solar_radiation",
        interval_seconds=3600,
    ),
    spec(
        "cloud_cover_total",
        "cloud_cover",
        "unspecified",
        "fraction",
        family="weather_cloud",
        unit="percent",
        factor=100.0,
        interval_seconds=3600,
        rule="fraction -> percent (x100)",
    ),
    spec(
        "cloud_base_height",
        "cloud_base_height",
        "unspecified",
        "metres",
        family="weather_cloud",
        interval_seconds=3600,
    ),
    spec(
        "visibility",
        "visibility",
        "unspecified",
        "km",
        family="weather_visibility",
        unit="metres",
        factor=1000.0,
        interval_seconds=3600,
        rule="km -> m (x1000)",
    ),
    spec(
        "minutes_of_sun",
        "sunshine_duration",
        "unspecified",
        "minutes",
        family="weather_solar_radiation",
        interval_seconds=3600,
    ),
    spec(
        "index_uv",
        "uv_index",
        "unspecified",
        "index",
        family="weather_solar_radiation",
        interval_seconds=3600,
    ),
]
