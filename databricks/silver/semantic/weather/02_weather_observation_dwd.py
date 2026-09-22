# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- WEATHER OBSERVATION (DWD)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** each DWD measurement table built into the family-specific
# MAGIC Silver structures its columns actually belong to (a table may feed more
# MAGIC than one family, e.g. `dwd_moisture` feeds temperature, humidity and
# MAGIC pressure). Each family builder produces that family's own designed
# MAGIC record shape directly -- no shared generic row model. Loads the tables
# MAGIC listed in `LOAD_TABLES`.
# MAGIC
# MAGIC Folds in two corrections identified during the first run: the DWD solar
# MAGIC product's radiation components are hourly sums (3600 s, not 600 s -- the
# MAGIC 600 s conversion put peak global radiation at 8167 W/m2, implausible for
# MAGIC this latitude; 3600 s gives 1361 W/m2, at the solar constant); and cloud
# MAGIC cover codes -1 and 9 are both non-measurement sentinels (sky obscured /
# MAGIC not observable), for the total and every layer.

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

# DBTITLE 1,Weather specifications
# MAGIC %run ./_weather_specs

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "silver/semantic/weather/02_weather_observation_dwd"
RID = run_id()

# All DWD measurement tables; narrow the list to load a subset.
LOAD_TABLES = list(DWD_TABLES)

STATION_IDS = [
    str(s) for s in load_contract(SOURCE)["conventions"]["station_set"]["ids"]
]
_KEY_COLS = ("STATIONS_ID", "city", "MESS_DATUM", "eor")

# Cloud cover eighths sentinels: -1 = sky not recognisable, 9 = sky obscured
# (fog etc, no cover reading possible). Neither is a real eighths value.
_CLOUD_COVER_SENTINELS = (-1.0, 9.0)

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Helper -- read and clean one DWD table (shared by every family builder)


def _prep(table: str):
    meta = DWD_TABLES[table]
    qn = meta["qn"]
    bronze_df = read_bronze(table)
    data_cols = [c for c in bronze_df.columns if c not in _KEY_COLS and c != qn]

    df = bronze_df.withColumn(
        "STATIONS_ID", F.regexp_replace(F.trim(F.col("STATIONS_ID")), r"\.0$", "")
    ).filter(F.col("STATIONS_ID").isin(STATION_IDS))
    df = strip_sentinels(df, [*data_cols, qn])
    df, _conflicts = resolve_conflicts(
        df, ["STATIONS_ID", "MESS_DATUM"], data_cols, qn_col=qn, bronze_table=table
    )
    return df, meta


# COMMAND ----------

# DBTITLE 1,Helper -- place, time, quality and provenance (shared by every family builder)


def _scaffold(df, table: str, meta: dict):
    qn = meta["qn"]
    parse = parse_mess_datum if meta["time"] == "hourly" else parse_mess_datum_10min
    qn_code = F.regexp_replace(F.trim(F.col(qn)), r"\.0$", "")
    qn_labels = F.create_map([F.lit(x) for kv in DWD_QN_LABELS.items() for x in kv])

    out = (
        df.withColumn("source_location_id", F.col("STATIONS_ID"))
        .withColumn("location_key", location_key(SOURCE, "source_location_id"))
        .withColumn("observation_ts_native", F.trim(F.col("MESS_DATUM")))
        .withColumn("time_basis", F.lit("utc"))
        .withColumn("observation_ts_utc", parse("MESS_DATUM"))
        .withColumn("quality_code", qn_code)
        .withColumn("quality_label", qn_labels[qn_code])
        .withColumn(
            "quality_flag",
            F.when(F.col("_had_key_conflict"), "key_conflict_resolved").when(
                qn_code.isNull(), "qn_missing"
            ),
        )
        .withColumn("measurement_basis", F.lit("station_observation"))
        .withColumn(
            "source_record_id", sha_key(F.lit(table), "STATIONS_ID", "MESS_DATUM")
        )
        .withColumn(
            "observation_key", sha_key(F.lit(table), "STATIONS_ID", "MESS_DATUM")
        )
    )
    out = add_project_time(out)
    return add_semantic_provenance(out, SOURCE, table, RID)


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_air_temperature (TT_TU, RF_TU: both primary)


def build_air_temperature():
    df, meta = _prep("dwd_air_temperature")
    df = _scaffold(df, "dwd_air_temperature", meta)
    temperature = any_present(
        df.withColumn("air_temperature_degc", F.col("TT_TU").cast("double")).withColumn(
            "air_temperature_is_primary", F.lit(True)
        ),
        ["air_temperature_degc"],
    )
    humidity = any_present(
        df.withColumn(
            "relative_humidity_percent", F.col("RF_TU").cast("double")
        ).withColumn("relative_humidity_is_primary", F.lit(True)),
        ["relative_humidity_percent"],
    )
    return {"weather_temperature": temperature, "weather_humidity": humidity}


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_moisture (temperature/humidity alts + pressure alt)


def build_moisture():
    df, meta = _prep("dwd_moisture")
    df = _scaffold(df, "dwd_moisture", meta)
    temperature = any_present(
        df.withColumn("air_temperature_degc", F.col("TT_STD").cast("double"))
        .withColumn("air_temperature_is_primary", F.lit(False))
        .withColumn("dew_point_temperature_degc", F.col("TD_STD").cast("double"))
        .withColumn("dew_point_temperature_is_primary", F.lit(False))
        .withColumn("wet_bulb_temperature_degc", F.col("TF_STD").cast("double")),
        [
            "air_temperature_degc",
            "dew_point_temperature_degc",
            "wet_bulb_temperature_degc",
        ],
    )
    humidity = any_present(
        df.withColumn("absolute_humidity_g_per_m3", F.col("ABSF_STD").cast("double"))
        .withColumn("vapour_pressure_hpa", F.col("VP_STD").cast("double"))
        .withColumn("relative_humidity_percent", F.col("RF_STD").cast("double"))
        .withColumn("relative_humidity_is_primary", F.lit(False)),
        [
            "absolute_humidity_g_per_m3",
            "vapour_pressure_hpa",
            "relative_humidity_percent",
        ],
    )
    pressure = any_present(
        df.withColumn("pressure_station_hpa", F.col("P_STD").cast("double")).withColumn(
            "pressure_station_is_primary", F.lit(False)
        ),
        ["pressure_station_hpa"],
    )
    return {
        "weather_temperature": temperature,
        "weather_humidity": humidity,
        "weather_pressure": pressure,
    }


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_dew_point (TT alt, TD primary)


def build_dew_point():
    df, meta = _prep("dwd_dew_point")
    df = _scaffold(df, "dwd_dew_point", meta)
    temperature = any_present(
        df.withColumn("air_temperature_degc", F.col("TT").cast("double"))
        .withColumn("air_temperature_is_primary", F.lit(False))
        .withColumn("dew_point_temperature_degc", F.col("TD").cast("double"))
        .withColumn("dew_point_temperature_is_primary", F.lit(True)),
        ["air_temperature_degc", "dew_point_temperature_degc"],
    )
    return {"weather_temperature": temperature}


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_pressure (P0 station primary, P sea level)


def build_pressure():
    df, meta = _prep("dwd_pressure")
    df = _scaffold(df, "dwd_pressure", meta)
    pressure = any_present(
        df.withColumn("pressure_station_hpa", F.col("P0").cast("double"))
        .withColumn("pressure_station_is_primary", F.lit(True))
        .withColumn("pressure_sea_level_hpa", F.col("P").cast("double")),
        ["pressure_station_hpa", "pressure_sea_level_hpa"],
    )
    return {"weather_pressure": pressure}


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_precipitation (amount + occurred + form, one event)


def build_precipitation():
    df, meta = _prep("dwd_precipitation")
    df = _scaffold(df, "dwd_precipitation", meta)
    precipitation = any_present(
        df.withColumn("precipitation_mm", F.col("R1").cast("double"))
        .withColumn("precipitation_occurred", F.col("RS_IND").cast("int") == 1)
        .withColumn("precipitation_form_code", F.col("WRTR")),
        ["precipitation_mm", "precipitation_occurred", "precipitation_form_code"],
    )
    return {"weather_precipitation": precipitation}


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_sun (sunshine duration, already minutes)


def build_sun():
    df, meta = _prep("dwd_sun")
    df = _scaffold(df, "dwd_sun", meta)
    solar = any_present(
        df.withColumn("sunshine_duration_minutes", F.col("SD_SO").cast("double")),
        ["sunshine_duration_minutes"],
    )
    return {"weather_solar_radiation": solar}


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_wind (hourly mean; statistic is a real distinguishing fact)


def build_wind():
    df, meta = _prep("dwd_wind")
    df = _scaffold(df, "dwd_wind", meta)
    d_raw = F.col("D").cast("double")
    variable = d_raw == 990.0
    wind = any_present(
        df.withColumn("statistic", F.lit("mean"))
        .withColumn("wind_speed_m_per_s", F.col("F").cast("double"))
        .withColumn("wind_direction_variable", variable)
        .withColumn(
            "wind_direction_degrees", F.when(variable, F.lit(None)).otherwise(d_raw)
        ),
        ["wind_speed_m_per_s", "wind_direction_degrees"],
    )
    return {"weather_wind": wind}


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_wind_synop (instantaneous synoptic reading)


def build_wind_synop():
    df, meta = _prep("dwd_wind_synop")
    df = _scaffold(df, "dwd_wind_synop", meta)
    wind = any_present(
        df.withColumn("statistic", F.lit("instant"))
        .withColumn("wind_speed_m_per_s", F.col("FF").cast("double"))
        .withColumn("wind_direction_degrees", F.col("DD").cast("double")),
        ["wind_speed_m_per_s", "wind_direction_degrees"],
    )
    return {"weather_wind": wind}


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_extreme_wind (hourly max gust)


def build_extreme_wind():
    df, meta = _prep("dwd_extreme_wind")
    df = _scaffold(df, "dwd_extreme_wind", meta)
    wind = any_present(
        df.withColumn("statistic", F.lit("max")).withColumn(
            "wind_gust_m_per_s", F.col("FX_911").cast("double")
        ),
        ["wind_gust_m_per_s"],
    )
    return {"weather_wind": wind}


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_visibility (already metres)


def build_visibility():
    df, meta = _prep("dwd_visibility")
    df = _scaffold(df, "dwd_visibility", meta)
    methods = F.create_map([F.lit(x) for kv in METHOD_LABELS.items() for x in kv])
    visibility = any_present(
        df.withColumn("visibility_m", F.col("V_VV").cast("double")).withColumn(
            "observation_method",
            F.coalesce(methods[F.col("V_VV_I")], F.col("V_VV_I")),
        ),
        ["visibility_m"],
    )
    return {"weather_visibility": visibility}


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_cloudiness (total cover, primary)


def build_cloudiness():
    df, meta = _prep("dwd_cloudiness")
    df = _scaffold(df, "dwd_cloudiness", meta)
    methods = F.create_map([F.lit(x) for kv in METHOD_LABELS.items() for x in kv])
    v_n = F.col("V_N").cast("double")
    special = v_n.isin(*_CLOUD_COVER_SENTINELS)
    cloud = any_present(
        df.withColumn(
            "cloud_cover_total_percent",
            F.when(special, F.lit(None)).otherwise(v_n * EIGHTHS_TO_PERCENT),
        )
        .withColumn("cloud_cover_total_is_primary", F.lit(True))
        .withColumn(
            "cloud_cover_total_native_value",
            F.when(special, F.lit(None)).otherwise(v_n),
        )
        .withColumn(
            "cloud_cover_total_native_unit",
            F.when(v_n.isNotNull(), F.lit("eighths")),
        )
        .withColumn(
            "observation_method",
            F.coalesce(methods[F.col("V_N_I")], F.col("V_N_I")),
        ),
        ["cloud_cover_total_percent"],
    )
    return {"weather_cloud": cloud}


# COMMAND ----------

# DBTITLE 1,Helper -- one cloud layer's struct, NULL when the layer has no reading


def _cloud_layer(n: int):
    genus = F.col(f"V_S{n}_CS")
    genus_text = F.col(f"V_S{n}_CSA")
    height = F.col(f"V_S{n}_HHS").cast("double")
    cover_raw = F.col(f"V_S{n}_NS").cast("double")
    cover_special = cover_raw.isin(*_CLOUD_COVER_SENTINELS)
    populated = genus.isNotNull() | height.isNotNull() | cover_raw.isNotNull()
    layer = F.struct(
        F.lit(n).alias("layer_number"),
        genus.alias("genus_code"),
        genus_text.alias("genus_text"),
        height.alias("base_height_m"),
        F.when(cover_special, F.lit(None))
        .otherwise(cover_raw * EIGHTHS_TO_PERCENT)
        .alias("cover_percent"),
        F.when(cover_special, F.lit(None))
        .otherwise(cover_raw)
        .alias("cover_native_value"),
        F.when(cover_raw.isNotNull(), F.lit("eighths")).alias("cover_native_unit"),
    )
    return F.when(populated, layer)


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_cloud_type (total cover alt + the 4 layers)


def build_cloud_type():
    df, meta = _prep("dwd_cloud_type")
    df = _scaffold(df, "dwd_cloud_type", meta)
    methods = F.create_map([F.lit(x) for kv in METHOD_LABELS.items() for x in kv])
    v_n = F.col("V_N").cast("double")
    special = v_n.isin(*_CLOUD_COVER_SENTINELS)
    cloud = (
        df.withColumn(
            "cloud_cover_total_percent",
            F.when(special, F.lit(None)).otherwise(v_n * EIGHTHS_TO_PERCENT),
        )
        .withColumn("cloud_cover_total_is_primary", F.lit(False))
        .withColumn(
            "cloud_cover_total_native_value",
            F.when(special, F.lit(None)).otherwise(v_n),
        )
        .withColumn(
            "cloud_cover_total_native_unit",
            F.when(v_n.isNotNull(), F.lit("eighths")),
        )
        .withColumn(
            "observation_method",
            F.coalesce(methods[F.col("V_N_I")], F.col("V_N_I")),
        )
        .withColumn(
            "layers",
            F.filter(
                F.array(*[_cloud_layer(n) for n in (1, 2, 3, 4)]),
                lambda x: x.isNotNull(),
            ),
        )
    )
    cloud = cloud.filter(
        F.col("cloud_cover_total_percent").isNotNull() | (F.size("layers") > 0)
    )
    return {"weather_cloud": cloud}


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_weather_phenomena (categorical code + text only)


def build_weather_phenomena():
    df, meta = _prep("dwd_weather_phenomena")
    df = _scaffold(df, "dwd_weather_phenomena", meta)
    ww = F.col("WW").cast("double")
    special = ww == -1.0
    present_weather = any_present(
        df.withColumn(
            "present_weather_code", F.when(special, F.lit(None)).otherwise(F.col("WW"))
        ).withColumn(
            "present_weather_text",
            F.when(special, F.lit(None)).otherwise(F.col("WW_Text")),
        ),
        ["present_weather_code"],
    )
    return {"weather_present_weather": present_weather}


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_soil_temperature (6 depths, always reported together)

_SOIL_DEPTHS_CM = (2, 5, 10, 20, 50, 100)


def build_soil_temperature():
    df, meta = _prep("dwd_soil_temperature")
    df = _scaffold(df, "dwd_soil_temperature", meta)
    for d in _SOIL_DEPTHS_CM:
        df = df.withColumn(
            f"soil_temperature_{d}cm_degc", F.col(f"V_TE{d:03d}").cast("double")
        )
    soil = any_present(df, [f"soil_temperature_{d}cm_degc" for d in _SOIL_DEPTHS_CM])
    return {"weather_soil_temperature": soil}


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_solar (longwave/diffuse/global/sunshine/zenith, one row)


def _add_radiation_columns(df, native_col: str, field: str):
    """J/cm2 hourly sum -> mean W/m2 (x10000 / 3600), native sum kept alongside.
    `field` is the variable's base name, e.g. "longwave_downward_radiation"."""
    native = F.col(native_col).cast("double")
    return (
        df.withColumn(
            f"{field}_w_per_m2",
            F.when(native.isNotNull(), native * J_CM2_TO_W_M2 / 3600.0),
        )
        .withColumn(f"{field}_native_value", native)
        .withColumn(
            f"{field}_native_unit", F.when(native.isNotNull(), F.lit("J_per_cm2"))
        )
    )


def build_solar():
    df, meta = _prep("dwd_solar")
    df = _scaffold(df, "dwd_solar", meta)
    df = _add_radiation_columns(df, "ATMO_LBERG", "longwave_downward_radiation")
    df = _add_radiation_columns(df, "FD_LBERG", "diffuse_radiation")
    df = _add_radiation_columns(df, "FG_LBERG", "global_radiation")
    solar = any_present(
        df.withColumn(
            "sunshine_duration_minutes", F.col("SD_LBERG").cast("double")
        ).withColumn("solar_zenith_angle_degrees", F.col("ZENIT").cast("double")),
        [
            "longwave_downward_radiation_w_per_m2",
            "diffuse_radiation_w_per_m2",
            "global_radiation_w_per_m2",
            "sunshine_duration_minutes",
            "solar_zenith_angle_degrees",
        ],
    )
    return {"weather_solar_radiation": solar}


# COMMAND ----------

# DBTITLE 1,Configuration -- table -> builder function
BUILDERS = {
    "dwd_air_temperature": build_air_temperature,
    "dwd_moisture": build_moisture,
    "dwd_dew_point": build_dew_point,
    "dwd_pressure": build_pressure,
    "dwd_precipitation": build_precipitation,
    "dwd_sun": build_sun,
    "dwd_wind": build_wind,
    "dwd_wind_synop": build_wind_synop,
    "dwd_extreme_wind": build_extreme_wind,
    "dwd_visibility": build_visibility,
    "dwd_cloudiness": build_cloudiness,
    "dwd_cloud_type": build_cloud_type,
    "dwd_weather_phenomena": build_weather_phenomena,
    "dwd_soil_temperature": build_soil_temperature,
    "dwd_solar": build_solar,
}

# COMMAND ----------

# DBTITLE 1,Transform -- each loaded table's contribution to its families
table_family_frames = {table: BUILDERS[table]() for table in LOAD_TABLES}

# COMMAND ----------

# DBTITLE 1,Transform -- group contributions by target family
per_family: dict[str, list[tuple[str, object]]] = {}
for table, fam_dict in table_family_frames.items():
    for fam, fdf in fam_dict.items():
        per_family.setdefault(fam, []).append((table, fdf))

# COMMAND ----------

# DBTITLE 1,Write Silver -- one family table at a time, unioning its contributing tables
for fam, contributions in per_family.items():
    frames = [
        conform(fdf, WEATHER_FAMILY_COLUMNS[fam]) for _table, fdf in contributions
    ]
    combined = frames[0]
    for extra in frames[1:]:
        combined = combined.unionByName(extra)
    tables_in = sorted({table for table, _fdf in contributions})
    in_list = ", ".join(f"'{t}'" for t in tables_in)
    write_semantic(
        combined,
        fam,
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
        replace_where=f"source_system = '{SOURCE}' AND source_dataset IN ({in_list})",
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- each family structure per loaded table
findings_blocks = {}
for table, fam_dict in table_family_frames.items():
    for fam in fam_dict:
        written = spark.table(semantic_table(fam)).filter(
            F.col("source_dataset") == table
        )
        findings_blocks[(table, fam)] = inspect_table(
            written,
            fam,
            source=FINDINGS_SOURCE,
            component=COMPONENT,
            rid=RID,
            key_cols=["observation_key"],
            extra_checks=structure_extra_checks(written),
        )

# COMMAND ----------

# DBTITLE 1,Export findings -- each family structure per loaded table
for (table, fam), blocks in findings_blocks.items():
    write_silver_findings(
        FINDINGS_SOURCE,
        f"{COMPONENT.split('/')[-1]}__{table}__{fam}",
        f"{fam} -- {table}",
        blocks,
    )