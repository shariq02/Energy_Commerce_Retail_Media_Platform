# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- WEATHER OBSERVATION AND DAILY (ENERGY)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** every weather source (DWD, the Honda site station, AccuWeather) in one
# MAGIC wide table: one row per station, instant and interval, with one column group per
# MAGIC weather family. Same-instant products of one family are columns (alternate
# MAGIC readings, wind statistics, cloud layers 1 to 4), never an array. The daily
# MAGIC temperature rows (Seattle files and the DWD day minimum, maximum and mean) go in
# MAGIC `weather_daily`.

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "weather"
COMPONENT = "silver/energy/02_weather_observation"
FINDINGS = "energy"
RID = run_id()

STATION_IDS = [
    str(s) for s in load_contract("dwd")["conventions"]["station_set"]["ids"]
]
_KEY_COLS = ("STATIONS_ID", "city", "MESS_DATUM", "eor")

# Cloud cover eighths sentinels: -1 = sky not recognisable, 9 = sky obscured.
_CLOUD_COVER_SENTINELS = (-1.0, 9.0)
_SOIL_DEPTHS_CM = (2, 5, 10, 20, 50, 100)

# Context columns every family frame carries; the grain is a part of them.
CONTEXT = [
    "source_system",
    "location_key",
    "source_location_id",
    "observation_timestamp_native",
    "time_basis",
    "utc_offset_hours",
    "observation_timestamp_utc",
    "interval_seconds",
    "interval_reference",
    "true_solar_time_native",
    "measurement_basis",
]
NO_TEXT = F.lit(None).cast("string")
NO_FLAG = F.lit(None).cast("boolean")
NO_NUMBER = F.lit(None).cast("double")
METHODS = lit_map(METHOD_LABELS)
QN_LABELS = lit_map(DWD_QN_LABELS)

# source table -> reconciliation numbers, filled while the sources are read.
RECONCILIATION = {}

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Helpers -- one family frame, read and clean one DWD table, join products


def family_frame(
    df,
    family,
    values,
    keep,
    *,
    dataset,
    source,
    quality_code=None,
    quality_label=None,
    conflict=None,
    qn_missing=None,
    stuck=None,
):
    """The context, the `values` (name -> Column) and the quality columns of one
    family; only rows where one of the `keep` columns is set stay."""
    quality = {
        f"{family}_quality_code": quality_code,
        f"{family}_quality_label": quality_label,
        f"{family}_source_dataset": F.lit(dataset)
        if isinstance(dataset, str)
        else dataset,
        f"{family}_key_conflict_resolved": conflict,
        f"{family}_qn_missing": qn_missing,
        f"{family}_stuck_reading": stuck,
    }
    kinds = {
        name: (
            NO_FLAG if name.endswith(("resolved", "missing", "reading")) else NO_TEXT
        )
        for name in quality
    }
    frame = df.withColumn("location_key", location_key(source, "source_location_id"))
    return any_present(
        frame.select(
            *CONTEXT,
            *[value.alias(name) for name, value in values.items()],
            *[
                (value if value is not None else kinds[name]).alias(name)
                for name, value in quality.items()
            ],
        ),
        keep,
    )


def _prep(table):
    """One DWD Bronze table: sentinels stripped, then identical rows collapsed and
    conflicting rows quarantined on (STATIONS_ID, MESS_DATUM). Once per table."""
    if table in _PREPPED:
        return _PREPPED[table]
    qn = DWD_TABLES[table]["qn"]
    bronze_df = read_bronze(table)
    data_cols = [c for c in bronze_df.columns if c not in _KEY_COLS and c != qn]
    df = (
        bronze_df.withColumn("STATIONS_ID", strip_float_suffix("STATIONS_ID"))
        .withColumn("MESS_DATUM", strip_float_suffix("MESS_DATUM"))
        .filter(F.col("STATIONS_ID").isin(STATION_IDS))
    )
    df = strip_sentinels(df, [*data_cols, qn])
    kept, q = resolve_conflicts(
        df, ["STATIONS_ID", "MESS_DATUM"], data_cols, qn_col=qn, bronze_table=table
    )
    write_quarantine(q.withColumn("source_system", F.lit("dwd")), RID)
    RECONCILIATION[table] = reconciliation_stats(df, kept, q, raw_bronze_df=bronze_df)
    _PREPPED[table] = (kept, qn)
    return _PREPPED[table]


_PREPPED = {}


def _join_on_instant(prepped):
    """Full outer join on (STATIONS_ID, MESS_DATUM); the other columns are prefixed
    with the table name."""
    joined = None
    for table, df in prepped.items():
        prefixed = df.select(
            "STATIONS_ID",
            "MESS_DATUM",
            *[
                F.col(c).alias(f"{table}__{c}")
                for c in df.columns
                if c not in ("STATIONS_ID", "MESS_DATUM")
            ],
        )
        joined = (
            prefixed
            if joined is None
            else joined.join(prefixed, ["STATIONS_ID", "MESS_DATUM"], "full_outer")
        )
    return joined


def dwd_frame(df, family, values, keep, dataset, *, qn_col, conflict_col, solar=False):
    """A DWD family frame: the DWD context, the quality of the family's own
    product and the key-conflict flag."""
    code = strip_float_suffix(qn_col)
    parse = parse_mess_datum_10min if solar else parse_mess_datum
    timestamp = parse("MESS_DATUM")
    register_lenient(f"{dataset}.MESS_DATUM", df, F.col("MESS_DATUM"), timestamp)
    base = (
        df.withColumn("source_system", F.lit("dwd"))
        .withColumn("source_location_id", F.col("STATIONS_ID"))
        .withColumn("observation_timestamp_native", F.trim(F.col("MESS_DATUM")))
        .withColumn("time_basis", F.lit("utc"))
        .withColumn("utc_offset_hours", NO_NUMBER)
        .withColumn("observation_timestamp_utc", timestamp)
        .withColumn("interval_seconds", F.lit(3600))
        .withColumn(
            "interval_reference", F.lit("true_solar_time" if solar else "clock")
        )
        .withColumn(
            "true_solar_time_native",
            F.trim(F.col("MESS_DATUM_WOZ")) if solar else NO_TEXT,
        )
        .withColumn("measurement_basis", F.lit("station_observation"))
    )
    return family_frame(
        base,
        family,
        values,
        keep,
        dataset=dataset,
        source="dwd",
        quality_code=code,
        quality_label=QN_LABELS[code],
        conflict=F.col(conflict_col),
        qn_missing=code.isNull(),
    )


def number(column):
    return F.col(column).cast("double")


# COMMAND ----------

# DBTITLE 1,Transform -- DWD temperature, humidity, pressure and wind
frames = {}

at_df, at_qn = _prep("dwd_air_temperature")
mo_df, mo_qn = _prep("dwd_moisture")
dp_df, dp_qn = _prep("dwd_dew_point")
pr_df, pr_qn = _prep("dwd_pressure")
w_df, w_qn = _prep("dwd_wind")
s_df, _ = _prep("dwd_wind_synop")
x_df, _ = _prep("dwd_extreme_wind")

mo_q = F.col(f"dwd_moisture__{mo_qn}")
dp_q = F.col(f"dwd_dew_point__{dp_qn}")
temperature_join = _join_on_instant(
    {"dwd_air_temperature": at_df, "dwd_moisture": mo_df, "dwd_dew_point": dp_df}
)
frames["temperature"] = dwd_frame(
    temperature_join,
    "temperature",
    {
        "air_temperature_degc": number("dwd_air_temperature__TT_TU"),
        "dew_point_temperature_degc": number("dwd_dew_point__TD"),
        "wet_bulb_temperature_degc": number("dwd_moisture__TF_STD"),
        "air_temperature_moisture_degc": number("dwd_moisture__TT_STD"),
        "air_temperature_moisture_quality_code": mo_q,
        "air_temperature_dew_point_degc": number("dwd_dew_point__TT"),
        "air_temperature_dew_point_quality_code": dp_q,
        "dew_point_temperature_moisture_degc": number("dwd_moisture__TD_STD"),
        "dew_point_temperature_moisture_quality_code": mo_q,
        "dew_point_above_air_temperature": number("dwd_dew_point__TD")
        > number("dwd_air_temperature__TT_TU"),
    },
    [
        "air_temperature_degc",
        "dew_point_temperature_degc",
        "wet_bulb_temperature_degc",
        "air_temperature_moisture_degc",
        "air_temperature_dew_point_degc",
        "dew_point_temperature_moisture_degc",
    ],
    "dwd_temperature",
    qn_col=f"dwd_air_temperature__{at_qn}",
    conflict_col="dwd_air_temperature___had_key_conflict",
)
humidity_join = _join_on_instant({"dwd_air_temperature": at_df, "dwd_moisture": mo_df})
frames["humidity"] = dwd_frame(
    humidity_join,
    "humidity",
    {
        "relative_humidity_percent": number("dwd_air_temperature__RF_TU"),
        "absolute_humidity_g_per_m3": number("dwd_moisture__ABSF_STD"),
        "vapour_pressure_hpa": number("dwd_moisture__VP_STD"),
        "relative_humidity_moisture_percent": number("dwd_moisture__RF_STD"),
        "relative_humidity_moisture_quality_code": mo_q,
        "relative_humidity_above_100": number("dwd_air_temperature__RF_TU") > 100,
    },
    [
        "relative_humidity_percent",
        "absolute_humidity_g_per_m3",
        "vapour_pressure_hpa",
        "relative_humidity_moisture_percent",
    ],
    "dwd_humidity",
    qn_col=f"dwd_air_temperature__{at_qn}",
    conflict_col="dwd_air_temperature___had_key_conflict",
)
pressure_join = _join_on_instant({"dwd_pressure": pr_df, "dwd_moisture": mo_df})
frames["pressure"] = dwd_frame(
    pressure_join,
    "pressure",
    {
        "pressure_station_hpa": number("dwd_pressure__P0"),
        "pressure_station_moisture_hpa": number("dwd_moisture__P_STD"),
        "pressure_station_moisture_quality_code": mo_q,
        "pressure_sea_level_hpa": number("dwd_pressure__P"),
    },
    ["pressure_station_hpa", "pressure_station_moisture_hpa", "pressure_sea_level_hpa"],
    "dwd_pressure",
    qn_col=f"dwd_pressure__{pr_qn}",
    conflict_col="dwd_pressure___had_key_conflict",
)
wind_join = _join_on_instant(
    {"dwd_wind": w_df, "dwd_wind_synop": s_df, "dwd_extreme_wind": x_df}
)
mean_direction = number("dwd_wind__D")
frames["wind"] = dwd_frame(
    wind_join,
    "wind",
    {
        "wind_mean_speed_m_per_s": number("dwd_wind__F"),
        "wind_mean_direction_degrees": F.when(
            mean_direction == 990.0, NO_NUMBER
        ).otherwise(mean_direction),
        "wind_mean_direction_variable": mean_direction == 990.0,
        "wind_instant_speed_m_per_s": number("dwd_wind_synop__FF"),
        "wind_instant_direction_degrees": number("dwd_wind_synop__DD"),
        "wind_max_gust_m_per_s": number("dwd_extreme_wind__FX_911"),
    },
    [
        "wind_mean_speed_m_per_s",
        "wind_mean_direction_degrees",
        "wind_mean_direction_variable",
        "wind_instant_speed_m_per_s",
        "wind_instant_direction_degrees",
        "wind_max_gust_m_per_s",
    ],
    "dwd_wind",
    qn_col=f"dwd_wind__{w_qn}",
    conflict_col="dwd_wind___had_key_conflict",
)

# COMMAND ----------

# DBTITLE 1,Transform -- DWD precipitation, visibility, cloud, present weather and soil
pc_df, pc_qn = _prep("dwd_precipitation")
frames["precipitation"] = dwd_frame(
    pc_df,
    "precipitation",
    {
        "precipitation_mm": number("R1"),
        "precipitation_occurred": F.col("RS_IND").cast("int") == 1,
        "precipitation_form_code": F.col("WRTR"),
    },
    ["precipitation_mm", "precipitation_occurred", "precipitation_form_code"],
    "dwd_precipitation",
    qn_col=pc_qn,
    conflict_col="_had_key_conflict",
)
vi_df, vi_qn = _prep("dwd_visibility")
frames["visibility"] = dwd_frame(
    vi_df,
    "visibility",
    {
        "visibility_m": number("V_VV"),
        "visibility_observation_method": F.coalesce(
            METHODS[F.col("V_VV_I")], F.col("V_VV_I")
        ),
    },
    ["visibility_m"],
    "dwd_visibility",
    qn_col=vi_qn,
    conflict_col="_had_key_conflict",
)

ci_df, ci_qn = _prep("dwd_cloudiness")
ct_df, ct_qn = _prep("dwd_cloud_type")
cloud_join = _join_on_instant({"dwd_cloudiness": ci_df, "dwd_cloud_type": ct_df})
total = number("dwd_cloudiness__V_N")
total_special = total.isin(*_CLOUD_COVER_SENTINELS)
by_type = number("dwd_cloud_type__V_N")
by_type_special = by_type.isin(*_CLOUD_COVER_SENTINELS)
cloud_values = {
    "cloud_cover_total_percent": F.when(total_special, NO_NUMBER).otherwise(
        total * EIGHTHS_TO_PERCENT
    ),
    "cloud_cover_total_native_value": F.when(total_special, NO_NUMBER).otherwise(total),
    "cloud_cover_total_native_unit": F.when(total.isNotNull(), F.lit("eighths")),
    "cloud_cover_total_cloud_type_percent": F.when(
        by_type_special, NO_NUMBER
    ).otherwise(by_type * EIGHTHS_TO_PERCENT),
    "cloud_cover_total_cloud_type_native_value": F.when(
        by_type_special, NO_NUMBER
    ).otherwise(by_type),
    "cloud_cover_total_cloud_type_native_unit": F.when(
        by_type.isNotNull(), F.lit("eighths")
    ),
    "cloud_cover_total_cloud_type_quality_code": F.col(f"dwd_cloud_type__{ct_qn}"),
    "cloud_observation_method": F.coalesce(
        METHODS[F.col("dwd_cloudiness__V_N_I")], F.col("dwd_cloudiness__V_N_I")
    ),
}
layer_keep = []
for n in (1, 2, 3, 4):
    cover_raw = number(f"dwd_cloud_type__V_S{n}_NS")
    cover_special = cover_raw.isin(*_CLOUD_COVER_SENTINELS)
    cloud_values.update(
        {
            f"cloud_layer_{n}_genus_code": F.col(f"dwd_cloud_type__V_S{n}_CS"),
            f"cloud_layer_{n}_genus_text": F.col(f"dwd_cloud_type__V_S{n}_CSA"),
            f"cloud_layer_{n}_base_height_m": number(f"dwd_cloud_type__V_S{n}_HHS"),
            f"cloud_layer_{n}_cover_percent": F.when(
                cover_special, NO_NUMBER
            ).otherwise(cover_raw * EIGHTHS_TO_PERCENT),
            f"cloud_layer_{n}_cover_native_value": F.when(
                cover_special, NO_NUMBER
            ).otherwise(cover_raw),
            f"cloud_layer_{n}_cover_native_unit": F.when(
                cover_raw.isNotNull(), F.lit("eighths")
            ),
        }
    )
    layer_keep += [
        f"cloud_layer_{n}_genus_code",
        f"cloud_layer_{n}_base_height_m",
        f"cloud_layer_{n}_cover_native_value",
    ]
frames["cloud"] = dwd_frame(
    cloud_join,
    "cloud",
    cloud_values,
    [
        "cloud_cover_total_percent",
        "cloud_cover_total_cloud_type_percent",
        *layer_keep,
    ],
    "dwd_cloud",
    qn_col=f"dwd_cloudiness__{ci_qn}",
    conflict_col="dwd_cloudiness___had_key_conflict",
)

pw_df, pw_qn = _prep("dwd_weather_phenomena")
present_special = number("WW") == -1.0
frames["present_weather"] = dwd_frame(
    pw_df,
    "present_weather",
    {
        "present_weather_code": F.when(present_special, NO_TEXT).otherwise(F.col("WW")),
        "present_weather_text": F.when(present_special, NO_TEXT).otherwise(
            F.col("WW_Text")
        ),
    },
    ["present_weather_code"],
    "dwd_weather_phenomena",
    qn_col=pw_qn,
    conflict_col="_had_key_conflict",
)
so_df, so_qn = _prep("dwd_soil_temperature")
soil_values = {
    f"soil_temperature_{d}cm_degc": number(f"V_TE{d:03d}") for d in _SOIL_DEPTHS_CM
}
frames["soil_temperature"] = dwd_frame(
    so_df,
    "soil_temperature",
    soil_values,
    list(soil_values),
    "dwd_soil_temperature",
    qn_col=so_qn,
    conflict_col="_had_key_conflict",
)

# COMMAND ----------

# DBTITLE 1,Transform -- DWD sunshine and solar (the solar product has its own time)
su_df, su_qn = _prep("dwd_sun")
sun_hourly = dwd_frame(
    su_df,
    "sunshine_duration",
    {"sunshine_duration_minutes": number("SD_SO")},
    ["sunshine_duration_minutes"],
    "dwd_sun",
    qn_col=su_qn,
    conflict_col="_had_key_conflict",
)

sol_df, sol_qn = _prep("dwd_solar")


def radiation(native_column, field):
    """J/cm2 hourly sum -> mean W/m2 (x10000 / 3600), the native sum kept."""
    native = number(native_column)
    return {
        f"{field}_w_per_m2": F.when(
            native.isNotNull(), native * J_CM2_TO_W_M2 / 3600.0
        ),
        f"{field}_native_value": native,
        f"{field}_native_unit": F.when(native.isNotNull(), F.lit("J_per_cm2")),
    }


def solar_frame(family, values, keep):
    return dwd_frame(
        sol_df,
        family,
        values,
        keep,
        "dwd_solar",
        qn_col=sol_qn,
        conflict_col="_had_key_conflict",
        solar=True,
    )


frames["solar_radiation"] = solar_frame(
    "solar_radiation",
    {
        **radiation("FG_LBERG", "global_radiation"),
        **radiation("FD_LBERG", "diffuse_radiation"),
    },
    ["global_radiation_w_per_m2", "diffuse_radiation_w_per_m2"],
)
frames["longwave_radiation"] = solar_frame(
    "longwave_radiation",
    radiation("ATMO_LBERG", "longwave_downward_radiation"),
    ["longwave_downward_radiation_w_per_m2"],
)
frames["solar_geometry"] = solar_frame(
    "solar_geometry",
    {"solar_zenith_angle_degrees": number("ZENIT")},
    ["solar_zenith_angle_degrees"],
)
sun_solar = solar_frame(
    "sunshine_duration",
    {"sunshine_duration_minutes": number("SD_LBERG")},
    ["sunshine_duration_minutes"],
)
# Sunshine comes from two products on two grains (clock hour and true solar time):
# one frame, so a column holds one frame's values only.
frames["sunshine_duration"] = sun_hourly.unionByName(sun_solar)

# COMMAND ----------

# DBTITLE 1,Transform -- DWD wide rows (one join per grain)
dwd_wide = join_on_grain(frames, WEATHER_GRAIN, CONTEXT)
dwd_wide = finish_observation(dwd_wide, source="dwd", grain=WEATHER_GRAIN)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- Honda site weather and dedupe
HONDA_TABLE = "honda_iot_weather"
SITE_ID = "honda_site"
STUCK_LOOKBACK_STEPS_BY_FREQ = {"1h": 9, "15min": 39, "1min": 599}
FLAGGED = {"WeatherStation_Weather_Ta": "ta", "WeatherStation_Weather_Igm": "igm"}
honda_bronze = read_bronze(HONDA_TABLE)
honda_kept, honda_q = resolve_conflicts(
    honda_bronze,
    ["frequency", "datetime_utc"],
    ["WeatherStation_Weather_Ta", "WeatherStation_Weather_Igm"],
    bronze_table=HONDA_TABLE,
)
write_quarantine(honda_q.withColumn("source_system", F.lit("honda_iot")), RID)
RECONCILIATION[HONDA_TABLE] = reconciliation_stats(honda_bronze, honda_kept, honda_q)

# COMMAND ----------

# DBTITLE 1,Transform -- Honda wide rows (temperature and global radiation)
interval = lit_map({k: str(v) for k, v in HONDA_INTERVAL_SECONDS.items()})
honda_base = (
    honda_kept.withColumn("source_system", F.lit("honda_iot"))
    .withColumn("interval_seconds", interval[F.col("frequency")].cast("int"))
    .withColumn("interval_reference", F.lit("clock"))
    .withColumn("source_location_id", F.lit(SITE_ID))
    .withColumn("observation_timestamp_native", F.col("datetime_utc").cast("string"))
    .withColumn("time_basis", F.lit("utc"))
    .withColumn("utc_offset_hours", NO_NUMBER)
    .withColumn("observation_timestamp_utc", F.col("datetime_utc").cast("timestamp"))
    .withColumn("true_solar_time_native", NO_TEXT)
    .withColumn("measurement_basis", F.lit("site_sensor"))
)
_w = Window.partitionBy("frequency").orderBy(F.col("datetime_utc").cast("timestamp"))
for _src, _name in FLAGGED.items():
    honda_base = honda_base.withColumn(_name, F.col(_src).cast("double"))
honda_base = add_stuck_reading_flags(
    honda_base, list(FLAGGED.values()), STUCK_LOOKBACK_STEPS_BY_FREQ, _w
)
honda_frames = {
    "temperature": family_frame(
        honda_base,
        "temperature",
        {"air_temperature_degc": F.col("ta")},
        ["air_temperature_degc"],
        dataset=HONDA_TABLE,
        source="honda_iot",
        stuck=F.col("_ta_stuck_reading_flag"),
    ),
    "solar_radiation": family_frame(
        honda_base,
        "solar_radiation",
        {"global_radiation_w_per_m2": F.col("igm")},
        ["global_radiation_w_per_m2"],
        dataset=HONDA_TABLE,
        source="honda_iot",
        stuck=F.col("_igm_stuck_reading_flag"),
    ),
}
honda_wide = join_on_grain(honda_frames, WEATHER_GRAIN, CONTEXT)
honda_wide = finish_observation(honda_wide, source="honda_iot", grain=WEATHER_GRAIN)

# COMMAND ----------

# DBTITLE 1,Read Samples -- AccuWeather metric and imperial hourly tables
ACCU = "accuweather"
METRIC_DATASET = "historical_hourly_metric"
IMPERIAL_DATASET = "historical_hourly_imperial"
AW_KEY = ["city_name", "datetime_valid_local"]
AW_FIELDS = [
    "temperature",
    "temperature_dew_point",
    "humidity_relative",
    "pressure",
    "pressure_msl",
    "wind_speed",
    "wind_gust",
    "wind_direction",
    "precipitation_lwe",
    "solar_irradiance",
    "cloud_cover_total",
    "cloud_base_height",
    "visibility",
    "minutes_of_sun",
    "index_uv",
]
# field -> (metric factor, metric offset, metric native unit or None,
#           imperial factor, imperial offset, imperial native unit or None);
# standard value = raw * factor + offset.
AW_CONVERSION = {
    "temperature": (1.0, 0.0, None, 5 / 9, -160 / 9, "degF"),
    "temperature_dew_point": (1.0, 0.0, None, 5 / 9, -160 / 9, "degF"),
    "humidity_relative": (1.0, 0.0, None, 1.0, 0.0, None),
    "pressure": (0.01, 0.0, "Pa", 33.8639, 0.0, "inHg"),
    "pressure_msl": (0.01, 0.0, "Pa", 33.8639, 0.0, "inHg"),
    "wind_speed": (1.0, 0.0, None, 0.44704, 0.0, "mph"),
    "wind_gust": (1.0, 0.0, None, 0.44704, 0.0, "mph"),
    "wind_direction": (1.0, 0.0, None, 1.0, 0.0, None),
    "precipitation_lwe": (1.0, 0.0, None, 25.4, 0.0, "in"),
    "solar_irradiance": (1.0, 0.0, None, 3.15459, 0.0, "BTU_per_hr_ft2"),
    "cloud_cover_total": (100.0, 0.0, "fraction", 100.0, 0.0, "fraction"),
    "cloud_base_height": (1.0, 0.0, None, 0.3048, 0.0, "ft"),
    "visibility": (1000.0, 0.0, "km", 1609.344, 0.0, "mi"),
    "minutes_of_sun": (1.0, 0.0, None, 1.0, 0.0, None),
    "index_uv": (1.0, 0.0, None, 1.0, 0.0, None),
}
metric_src = spark.table(f"samples.accuweather.{METRIC_DATASET}")
imperial_src = spark.table(f"samples.accuweather.{IMPERIAL_DATASET}").withColumnRenamed(
    "date", "datetime_valid_local"
)

# Collapse identical rows, quarantine same-key value conflicts (per table).
metric_kept, metric_q = resolve_conflicts(
    metric_src,
    AW_KEY,
    [c for c in metric_src.columns if c not in AW_KEY],
    bronze_table=METRIC_DATASET,
)
imperial_kept, imperial_q = resolve_conflicts(
    imperial_src,
    AW_KEY,
    [c for c in imperial_src.columns if c not in AW_KEY],
    bronze_table=IMPERIAL_DATASET,
)
write_quarantine(metric_q.withColumn("source_system", F.lit(ACCU)), RID)
write_quarantine(imperial_q.withColumn("source_system", F.lit(ACCU)), RID)
RECONCILIATION[METRIC_DATASET] = reconciliation_stats(metric_src, metric_kept, metric_q)
RECONCILIATION[IMPERIAL_DATASET] = reconciliation_stats(
    imperial_src, imperial_kept, imperial_q
)

# COMMAND ----------

# DBTITLE 1,Transform -- AccuWeather standard value and native value per field
# Imperial rows with no matching metric (city, instant) survive.
gmt_offset_by_city = metric_kept.select("city_name", "gmt_offset").distinct()
imperial_only = (
    imperial_kept.join(metric_kept.select(*AW_KEY), AW_KEY, "left_anti")
    .join(gmt_offset_by_city, "city_name", "left")
    .withColumn("source_dataset", F.lit(IMPERIAL_DATASET))
    .withColumn("_is_imperial", F.lit(True))
)
metric_tagged = metric_kept.withColumn(
    "source_dataset", F.lit(METRIC_DATASET)
).withColumn("_is_imperial", F.lit(False))
aw_source = metric_tagged.unionByName(imperial_only, allowMissingColumns=True)
for _field in AW_FIELDS:
    m_factor, m_offset, m_unit, i_factor, i_offset, i_unit = AW_CONVERSION[_field]
    _raw = F.col(_field).cast("double")
    _factor = F.when(F.col("_is_imperial"), F.lit(i_factor)).otherwise(F.lit(m_factor))
    _offset = F.when(F.col("_is_imperial"), F.lit(i_offset)).otherwise(F.lit(m_offset))
    _native_unit = F.when(F.col("_is_imperial"), F.lit(i_unit)).otherwise(F.lit(m_unit))
    aw_source = (
        aw_source.withColumn(
            f"{_field}__value", F.when(_raw.isNotNull(), _raw * _factor + _offset)
        )
        .withColumn(f"{_field}__native_value", F.when(_native_unit.isNotNull(), _raw))
        .withColumn(f"{_field}__native_unit", _native_unit)
    )
aw_base = (
    aw_source.withColumn("source_system", F.lit(ACCU))
    .withColumn("source_location_id", F.col("city_name"))
    .withColumn(
        "observation_timestamp_native", F.col("datetime_valid_local").cast("string")
    )
    .withColumn("time_basis", F.lit("local_with_offset"))
    .withColumn("utc_offset_hours", F.col("gmt_offset").cast("double"))
    .withColumn("interval_seconds", F.lit(3600))
    .withColumn("interval_reference", F.lit("clock"))
    .withColumn(
        "observation_timestamp_utc",
        local_time_to_utc("datetime_valid_local", "utc_offset_hours"),
    )
    .withColumn("true_solar_time_native", NO_TEXT)
    .withColumn("measurement_basis", F.lit("provider_historical"))
)

# COMMAND ----------

# DBTITLE 1,Transform -- AccuWeather wide rows (one frame per family)
aw_dataset = F.col("source_dataset")


def aw_family(family, values, keep):
    return family_frame(
        aw_base,
        family,
        values,
        keep,
        dataset=aw_dataset,
        source=ACCU,
    )


def aw(field, suffix="value"):
    return F.col(f"{field}__{suffix}")


aw_frames = {
    "temperature": aw_family(
        "temperature",
        {
            "air_temperature_degc": aw("temperature"),
            "dew_point_temperature_degc": aw("temperature_dew_point"),
        },
        ["air_temperature_degc", "dew_point_temperature_degc"],
    ),
    "humidity": aw_family(
        "humidity",
        {"relative_humidity_percent": aw("humidity_relative")},
        ["relative_humidity_percent"],
    ),
    "pressure": aw_family(
        "pressure",
        {
            "pressure_station_hpa": aw("pressure"),
            "pressure_station_native_value": aw("pressure", "native_value"),
            "pressure_station_native_unit": aw("pressure", "native_unit"),
            "pressure_sea_level_hpa": aw("pressure_msl"),
            "pressure_sea_level_native_value": aw("pressure_msl", "native_value"),
            "pressure_sea_level_native_unit": aw("pressure_msl", "native_unit"),
        },
        ["pressure_station_hpa", "pressure_sea_level_hpa"],
    ),
    "wind": aw_family(
        "wind",
        {
            "wind_unspecified_speed_m_per_s": aw("wind_speed"),
            "wind_unspecified_direction_degrees": aw("wind_direction"),
            "wind_unspecified_gust_m_per_s": aw("wind_gust"),
        },
        [
            "wind_unspecified_speed_m_per_s",
            "wind_unspecified_direction_degrees",
            "wind_unspecified_gust_m_per_s",
        ],
    ),
    "precipitation": aw_family(
        "precipitation",
        {"precipitation_mm": aw("precipitation_lwe")},
        ["precipitation_mm"],
    ),
    "cloud": aw_family(
        "cloud",
        {
            "cloud_cover_total_percent": aw("cloud_cover_total"),
            "cloud_cover_total_native_value": aw("cloud_cover_total", "native_value"),
            "cloud_cover_total_native_unit": aw("cloud_cover_total", "native_unit"),
            "cloud_base_height_m": aw("cloud_base_height"),
        },
        ["cloud_cover_total_percent", "cloud_base_height_m"],
    ),
    "visibility": aw_family(
        "visibility",
        {
            "visibility_m": aw("visibility"),
            "visibility_native_value": aw("visibility", "native_value"),
            "visibility_native_unit": aw("visibility", "native_unit"),
        },
        ["visibility_m"],
    ),
    "solar_radiation": aw_family(
        "solar_radiation",
        {"global_radiation_w_per_m2": aw("solar_irradiance")},
        ["global_radiation_w_per_m2"],
    ),
    "sunshine_duration": aw_family(
        "sunshine_duration",
        {"sunshine_duration_minutes": aw("minutes_of_sun")},
        ["sunshine_duration_minutes"],
    ),
    "uv_index": aw_family("uv_index", {"uv_index": aw("index_uv")}, ["uv_index"]),
}
aw_wide = join_on_grain(aw_frames, WEATHER_GRAIN, CONTEXT)
aw_wide = finish_observation(aw_wide, source=ACCU, grain=WEATHER_GRAIN)

# COMMAND ----------

# DBTITLE 1,Transform -- the three sources in one table
observation = None
for wide, source in ((dwd_wide, "dwd"), (honda_wide, "honda_iot"), (aw_wide, ACCU)):
    part = add_semantic_provenance(wide, source, None, RID)
    part = conform(part, WEATHER_OBSERVATION_COLUMNS)
    observation = part if observation is None else observation.unionByName(part)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- weather_observation
reconciliation_blocks = [
    (
        f"dedup/conflict reconciliation -- {table}",
        dict_to_markdown_row(stats),
    )
    for table, stats in RECONCILIATION.items()
]
weather_observation = write_inspect_export(
    observation,
    "weather_observation",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["observation_key"],
    writer=write_semantic,
    extra_checks=structure_extra_checks,
    extra_blocks=reconciliation_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- daily temperature rows (Seattle files and the DWD day statistics)
ROOT = "/Volumes/samples/databricks/datasets/weather"
FILE_STATISTIC = {"high_temps": "max", "low_temps": "min"}
SEATTLE = "weather_seattle"
seattle_src = (
    spark.read.format("csv")
    .option("header", True)
    .load([f"{ROOT}/{name}" for name in FILE_STATISTIC])
    .select("date", "temp", F.col("_metadata.file_name").alias("_file"))
)
seattle_kept, seattle_q = resolve_conflicts(
    seattle_src, ["_file", "date"], ["temp"], bronze_table=SEATTLE
)
write_quarantine(seattle_q.withColumn("source_system", F.lit(SEATTLE)), RID)
seattle_stats = reconciliation_stats(seattle_src, seattle_kept, seattle_q)
statistic = lit_map(FILE_STATISTIC)
seattle_daily = (
    seattle_kept.withColumn("statistic", statistic[F.col("_file")])
    .withColumn("value_native", F.col("temp").cast("double"))
    .withColumn("date_native", F.trim(F.col("date")))
    .withColumn("local_date", F.to_date(F.col("date_native"), "yyyy-MM-dd"))
    .withColumn("variable", F.lit("air_temperature"))
    .withColumn("is_primary", F.lit(True))
    .withColumn("unit_native", F.lit("degF"))
    .withColumn("unit", F.lit("degC"))
    .withColumn("value", (F.col("value_native") - 32.0) * 5.0 / 9.0)
    .withColumn("value_origin", F.lit("converted"))
    .withColumn("derivation_rule", F.lit("degF -> degC ((F - 32) x 5/9)"))
    .withColumn("time_basis", F.lit("date_only"))
    .withColumn("measurement_basis", F.lit("observed_daily_summary"))
    .withColumn("source_location_id", F.lit("seattle"))
    .withColumn("location_key", location_key(SEATTLE, "source_location_id"))
    .withColumn("source_column", F.col("_file"))
    .withColumn("source_record_id", sha_key("_file", "date_native"))
    .withColumn("daily_key", sha_key("_file", "date_native", "variable", "statistic"))
)
seattle_daily = conform(
    add_semantic_provenance(seattle_daily, SEATTLE, "weather", RID),
    WEATHER_DAILY_COLUMNS,
)

# The DWD day statistics come from the wide observation table just written.
STATISTICS = {"min": F.min, "max": F.max, "mean": F.avg}
daily_agg = (
    weather_observation.filter(
        (F.col("source_system") == "dwd")
        & F.col("air_temperature_degc").isNotNull()
        & F.col("local_date").isNotNull()
    )
    .groupBy("location_key", "source_location_id", "local_date")
    .agg(
        *[
            fn("air_temperature_degc").alias(f"agg_{name}")
            for name, fn in STATISTICS.items()
        ],
        F.count("*").alias("observation_count"),
    )
)
stack = ", ".join(f"'{name}', agg_{name}" for name in STATISTICS)
derived_daily = (
    daily_agg.select(
        "location_key",
        "source_location_id",
        "local_date",
        "observation_count",
        F.expr(f"stack({len(STATISTICS)}, {stack}) as (statistic, value)"),
    )
    .withColumn("variable", F.lit("air_temperature"))
    .withColumn("is_primary", F.lit(True))
    .withColumn("unit", F.lit("degC"))
    .withColumn("value_origin", F.lit("derived"))
    .withColumn(
        "derivation_rule",
        F.lit(
            "min/max/mean of hourly air_temperature over the Europe/Berlin local day"
        ),
    )
    .withColumn("time_basis", F.lit("utc_project_day"))
    .withColumn("date_native", F.col("local_date").cast("string"))
    .withColumn("measurement_basis", F.lit("derived_from_station_observation"))
    .withColumn("source_column", NO_TEXT)
    .withColumn(
        "source_record_id",
        sha_key("location_key", "local_date", "variable", "statistic"),
    )
    .withColumn(
        "daily_key", sha_key("location_key", "local_date", "variable", "statistic")
    )
)
derived_daily = conform(
    add_semantic_provenance(derived_daily, "dwd", "weather_observation", RID),
    WEATHER_DAILY_COLUMNS,
)
daily = seattle_daily.unionByName(derived_daily)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- weather_daily
write_inspect_export(
    daily,
    "weather_daily",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["daily_key"],
    writer=write_semantic,
    extra_checks=structure_extra_checks,
    extra_blocks=[
        (
            f"dedup/conflict reconciliation -- {SEATTLE}",
            dict_to_markdown_row(seattle_stats),
        )
    ],
)

# COMMAND ----------

# DBTITLE 1,Check and export -- rejected values of the lenient timestamp parser
export_rejected_values(FINDINGS, COMPONENT)
