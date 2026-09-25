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
# MAGIC **Purpose:** each DWD product built into its family-specific Silver
# MAGIC structures, one row per (station, instant) per family. Same-instant
# MAGIC products become an `_alt` array (competing) or a `readings` array
# MAGIC (non-competing), never extra rows. `dwd_solar` splits by meaning across
# MAGIC four families.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../../_semantic_common

# COMMAND ----------

# DBTITLE 1,Weather specifications
# MAGIC %run ./_weather_specs

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "dwd"
COMPONENT = "silver/energy/weather/02_weather_observation_dwd"
RID = run_id()

STATION_IDS = [
    str(s) for s in load_contract(SOURCE)["conventions"]["station_set"]["ids"]
]
_KEY_COLS = ("STATIONS_ID", "city", "MESS_DATUM", "eor")

# Cloud cover eighths sentinels: -1 = sky not recognisable, 9 = sky obscured
# (fog etc, no cover reading possible). Neither is a real eighths value.
_CLOUD_COVER_SENTINELS = (-1.0, 9.0)

# table -> reconciliation_stats(), filled in by _prep(); exported as findings.
RECONCILIATION = {}
# (family, source_dataset) -> rows before the all-null filter; counted at export.
PREFILTER = {}

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Helper -- read, dedupe and conflict-resolve one DWD table (per-source rule)


_PREPPED = {}


def _prep(table: str):
    """Bronze -> cleaned rows: sentinels stripped, then collapse-identical /
    quarantine-conflicting on (STATIONS_ID, MESS_DATUM), per product. Once
    per table per run (several families read the same product)."""
    if table in _PREPPED:
        return _PREPPED[table]
    meta = DWD_TABLES[table]
    qn = meta["qn"]
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
    write_quarantine(q.withColumn("source_system", F.lit(SOURCE)), RID)
    RECONCILIATION[table] = reconciliation_stats(df, kept, q, raw_bronze_df=bronze_df)
    _PREPPED[table] = (kept, meta)
    return kept, meta


# COMMAND ----------

# DBTITLE 1,Helper -- DWD time


def _dwd_time(native_ts_col):
    """MESS_DATUM is UTC for the whole hourly-historical product (dwd.yml's
    own convention, no era exception). Project time is the IANA-correct
    Europe/Berlin civil clock for that instant (CET/CEST, pre-1893 LMT,
    1945/1947 Hochsommerzeit) -- display only, never fed back into utc.
    Returns (observation_ts_utc, observation_ts_project, time_basis)."""
    utc = native_ts_col
    project = F.from_utc_timestamp(utc, PROJECT_TZ)
    return utc, project, F.lit("utc")


# COMMAND ----------

# DBTITLE 1,Helper -- place, time, quality (shared shape; source_dataset/provenance is per family)


def _scaffold(
    df,
    *,
    family: str,
    parse,
    qn_col: str,
    had_conflict_col: str,
    interval_reference: str = "clock",
    extra_flags: dict | None = None,
):
    qn_code = strip_float_suffix(qn_col)
    qn_labels = F.create_map([F.lit(x) for kv in DWD_QN_LABELS.items() for x in kv])
    utc, project, time_basis = _dwd_time(parse("MESS_DATUM"))

    return (
        df.withColumn("source_location_id", F.col("STATIONS_ID"))
        .withColumn("location_key", location_key(SOURCE, "source_location_id"))
        .withColumn("observation_ts_native", F.trim(F.col("MESS_DATUM")))
        .withColumn("time_basis", time_basis)
        .withColumn("observation_ts_utc", utc)
        .withColumn("observation_ts_project", project)
        .withColumn("local_date", F.to_date(project))
        .withColumn("interval_seconds", F.lit(3600))
        .withColumn("interval_reference", F.lit(interval_reference))
        .withColumn("quality_code", qn_code)
        .withColumn("quality_label", qn_labels[qn_code])
        .withColumn(
            "quality_flags",
            flag_array(
                {
                    "key_conflict_resolved": F.col(had_conflict_col),
                    "qn_missing": qn_code.isNull(),
                    **(extra_flags or {}),
                }
            ),
        )
        .withColumn("measurement_basis", F.lit("station_observation"))
        .withColumn(
            "source_record_id", sha_key(F.lit(family), "STATIONS_ID", "MESS_DATUM")
        )
        .withColumn(
            "observation_key", sha_key(F.lit(family), "STATIONS_ID", "MESS_DATUM")
        )
    )


# COMMAND ----------

# DBTITLE 1,Helper -- full outer join several DWD products onto one instant


def _join_on_instant(prepped: dict):
    """Full outer join on (STATIONS_ID, MESS_DATUM); non-key columns
    prefixed by table name to avoid collisions."""
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


# COMMAND ----------

# DBTITLE 1,Helper -- alternate-reading array (primary/alt is a deliberate relationship, not a row)


def _alt_array(*entries):
    """One array entry per populated alternate: (value, source_dataset,
    quality_code[, native_value, native_unit])."""
    structs = []
    for e in entries:
        value, source_dataset, quality_code = e[0], e[1], e[2]
        native_value = e[3] if len(e) > 3 else F.lit(None).cast("double")
        native_unit = e[4] if len(e) > 4 else F.lit(None).cast("string")
        s = F.struct(
            value.alias("value"),
            native_value.alias("native_value"),
            native_unit.alias("native_unit"),
            source_dataset.alias("source_dataset"),
            quality_code.alias("quality_code"),
        )
        structs.append(F.when(value.isNotNull(), s))
    return F.filter(F.array(*structs), lambda x: x.isNotNull())


# COMMAND ----------

# DBTITLE 1,Helper -- keep a row if any of its scalar or array fields is populated


def _keep_if_populated(df, fields: list, track: tuple):
    PREFILTER[track] = df
    cond = F.lit(False)
    for name in fields:
        col = F.col(name)
        cond = cond | (
            (F.size(col) > 0)
            if name.endswith(("_alt", "layers", "readings"))
            else col.isNotNull()
        )
    return df.filter(cond)


# COMMAND ----------

# DBTITLE 1,Family builder -- weather_temperature (join: air_temperature, moisture, dew_point)


def build_temperature():
    at_df, at_meta = _prep("dwd_air_temperature")
    mo_df, mo_meta = _prep("dwd_moisture")
    dp_df, dp_meta = _prep("dwd_dew_point")
    joined = _join_on_instant(
        {"dwd_air_temperature": at_df, "dwd_moisture": mo_df, "dwd_dew_point": dp_df}
    )
    row = _scaffold(
        joined,
        family="weather_temperature",
        parse=parse_mess_datum,
        qn_col=f"dwd_air_temperature__{at_meta['qn']}",
        had_conflict_col="dwd_air_temperature___had_key_conflict",
        extra_flags={
            "dew_point_above_air_temperature": F.col("dwd_dew_point__TD").cast("double")
            > F.col("dwd_air_temperature__TT_TU").cast("double")
        },
    )

    mo_q = F.col(f"dwd_moisture__{mo_meta['qn']}")
    dp_q = F.col(f"dwd_dew_point__{dp_meta['qn']}")
    row = (
        row.withColumn(
            "air_temperature_degc", F.col("dwd_air_temperature__TT_TU").cast("double")
        )
        .withColumn(
            "air_temperature_alt",
            _alt_array(
                (
                    F.col("dwd_moisture__TT_STD").cast("double"),
                    F.lit("dwd_moisture"),
                    mo_q,
                ),
                (
                    F.col("dwd_dew_point__TT").cast("double"),
                    F.lit("dwd_dew_point"),
                    dp_q,
                ),
            ),
        )
        .withColumn(
            "dew_point_temperature_degc", F.col("dwd_dew_point__TD").cast("double")
        )
        .withColumn(
            "dew_point_temperature_alt",
            _alt_array(
                (
                    F.col("dwd_moisture__TD_STD").cast("double"),
                    F.lit("dwd_moisture"),
                    mo_q,
                ),
            ),
        )
        .withColumn(
            "wet_bulb_temperature_degc", F.col("dwd_moisture__TF_STD").cast("double")
        )
    )
    row = _keep_if_populated(
        row,
        [
            "air_temperature_degc",
            "air_temperature_alt",
            "dew_point_temperature_degc",
            "dew_point_temperature_alt",
            "wet_bulb_temperature_degc",
        ],
        ("weather_temperature", "dwd_temperature"),
    )
    row = add_semantic_provenance(row, SOURCE, "dwd_temperature", RID)
    return {"weather_temperature": row}


# COMMAND ----------

# DBTITLE 1,Family builder -- weather_humidity (join: air_temperature, moisture)


def build_humidity():
    at_df, at_meta = _prep("dwd_air_temperature")
    mo_df, mo_meta = _prep("dwd_moisture")
    joined = _join_on_instant({"dwd_air_temperature": at_df, "dwd_moisture": mo_df})
    row = _scaffold(
        joined,
        family="weather_humidity",
        parse=parse_mess_datum,
        qn_col=f"dwd_air_temperature__{at_meta['qn']}",
        had_conflict_col="dwd_air_temperature___had_key_conflict",
        extra_flags={
            "relative_humidity_above_100": F.col("dwd_air_temperature__RF_TU").cast(
                "double"
            )
            > 100
        },
    )
    mo_q = F.col(f"dwd_moisture__{mo_meta['qn']}")
    row = (
        row.withColumn(
            "relative_humidity_percent",
            F.col("dwd_air_temperature__RF_TU").cast("double"),
        )
        .withColumn(
            "relative_humidity_alt",
            _alt_array(
                (
                    F.col("dwd_moisture__RF_STD").cast("double"),
                    F.lit("dwd_moisture"),
                    mo_q,
                ),
            ),
        )
        .withColumn(
            "absolute_humidity_g_per_m3", F.col("dwd_moisture__ABSF_STD").cast("double")
        )
        .withColumn("vapour_pressure_hpa", F.col("dwd_moisture__VP_STD").cast("double"))
    )
    row = _keep_if_populated(
        row,
        [
            "relative_humidity_percent",
            "relative_humidity_alt",
            "absolute_humidity_g_per_m3",
            "vapour_pressure_hpa",
        ],
        ("weather_humidity", "dwd_humidity"),
    )
    row = add_semantic_provenance(row, SOURCE, "dwd_humidity", RID)
    return {"weather_humidity": row}


# COMMAND ----------

# DBTITLE 1,Family builder -- weather_pressure (join: pressure, moisture)


def build_pressure():
    pr_df, pr_meta = _prep("dwd_pressure")
    mo_df, mo_meta = _prep("dwd_moisture")
    joined = _join_on_instant({"dwd_pressure": pr_df, "dwd_moisture": mo_df})
    row = _scaffold(
        joined,
        family="weather_pressure",
        parse=parse_mess_datum,
        qn_col=f"dwd_pressure__{pr_meta['qn']}",
        had_conflict_col="dwd_pressure___had_key_conflict",
    )
    mo_q = F.col(f"dwd_moisture__{mo_meta['qn']}")
    row = (
        row.withColumn("pressure_station_hpa", F.col("dwd_pressure__P0").cast("double"))
        .withColumn(
            "pressure_station_alt",
            _alt_array(
                (
                    F.col("dwd_moisture__P_STD").cast("double"),
                    F.lit("dwd_moisture"),
                    mo_q,
                ),
            ),
        )
        .withColumn("pressure_sea_level_hpa", F.col("dwd_pressure__P").cast("double"))
    )
    row = _keep_if_populated(
        row,
        ["pressure_station_hpa", "pressure_station_alt", "pressure_sea_level_hpa"],
        ("weather_pressure", "dwd_pressure"),
    )
    row = add_semantic_provenance(row, SOURCE, "dwd_pressure", RID)
    return {"weather_pressure": row}


# COMMAND ----------

# DBTITLE 1,Family builder -- weather_wind (join: wind, wind_synop, extreme_wind -> readings array)


def build_wind():
    w_df, w_meta = _prep("dwd_wind")
    s_df, _s_meta = _prep("dwd_wind_synop")
    x_df, _x_meta = _prep("dwd_extreme_wind")
    joined = _join_on_instant(
        {"dwd_wind": w_df, "dwd_wind_synop": s_df, "dwd_extreme_wind": x_df}
    )
    row = _scaffold(
        joined,
        family="weather_wind",
        parse=parse_mess_datum,
        qn_col=f"dwd_wind__{w_meta['qn']}",
        had_conflict_col="dwd_wind___had_key_conflict",
    )

    d_mean = F.col("dwd_wind__D").cast("double")
    mean_variable = d_mean == 990.0
    mean_reading = F.struct(
        F.lit("mean").alias("statistic"),
        F.col("dwd_wind__F").cast("double").alias("wind_speed_m_per_s"),
        F.when(mean_variable, F.lit(None))
        .otherwise(d_mean)
        .alias("wind_direction_degrees"),
        mean_variable.alias("wind_direction_variable"),
        F.lit(None).cast("double").alias("wind_gust_m_per_s"),
        F.lit("dwd_wind").alias("source_dataset"),
    )
    instant_reading = F.struct(
        F.lit("instant").alias("statistic"),
        F.col("dwd_wind_synop__FF").cast("double").alias("wind_speed_m_per_s"),
        F.col("dwd_wind_synop__DD").cast("double").alias("wind_direction_degrees"),
        F.lit(None).cast("boolean").alias("wind_direction_variable"),
        F.lit(None).cast("double").alias("wind_gust_m_per_s"),
        F.lit("dwd_wind_synop").alias("source_dataset"),
    )
    max_reading = F.struct(
        F.lit("max").alias("statistic"),
        F.lit(None).cast("double").alias("wind_speed_m_per_s"),
        F.lit(None).cast("double").alias("wind_direction_degrees"),
        F.lit(None).cast("boolean").alias("wind_direction_variable"),
        F.col("dwd_extreme_wind__FX_911").cast("double").alias("wind_gust_m_per_s"),
        F.lit("dwd_extreme_wind").alias("source_dataset"),
    )
    row = row.withColumn(
        "readings",
        F.filter(
            F.array(
                F.when(
                    F.col("dwd_wind__F").isNotNull() | d_mean.isNotNull(), mean_reading
                ),
                F.when(
                    F.col("dwd_wind_synop__FF").isNotNull()
                    | F.col("dwd_wind_synop__DD").isNotNull(),
                    instant_reading,
                ),
                F.when(F.col("dwd_extreme_wind__FX_911").isNotNull(), max_reading),
            ),
            lambda x: x.isNotNull(),
        ),
    )
    PREFILTER[("weather_wind", "dwd_wind")] = row
    row = row.filter(F.size("readings") > 0)
    row = add_semantic_provenance(row, SOURCE, "dwd_wind", RID)
    return {"weather_wind": row}


# COMMAND ----------

# DBTITLE 1,Family builder -- weather_precipitation (single product)


def build_precipitation():
    df, meta = _prep("dwd_precipitation")
    row = _scaffold(
        df,
        family="weather_precipitation",
        parse=parse_mess_datum,
        qn_col=meta["qn"],
        had_conflict_col="_had_key_conflict",
    )
    row = _keep_if_populated(
        row.withColumn("precipitation_mm", F.col("R1").cast("double"))
        .withColumn("precipitation_occurred", F.col("RS_IND").cast("int") == 1)
        .withColumn("precipitation_form_code", F.col("WRTR")),
        ["precipitation_mm", "precipitation_occurred", "precipitation_form_code"],
        ("weather_precipitation", "dwd_precipitation"),
    )
    row = add_semantic_provenance(row, SOURCE, "dwd_precipitation", RID)
    return {"weather_precipitation": row}


# COMMAND ----------

# DBTITLE 1,Family builder -- weather_visibility (single product)


def build_visibility():
    df, meta = _prep("dwd_visibility")
    row = _scaffold(
        df,
        family="weather_visibility",
        parse=parse_mess_datum,
        qn_col=meta["qn"],
        had_conflict_col="_had_key_conflict",
    )
    methods = F.create_map([F.lit(x) for kv in METHOD_LABELS.items() for x in kv])
    row = _keep_if_populated(
        row.withColumn("visibility_m", F.col("V_VV").cast("double")).withColumn(
            "observation_method", F.coalesce(methods[F.col("V_VV_I")], F.col("V_VV_I"))
        ),
        ["visibility_m"],
        ("weather_visibility", "dwd_visibility"),
    )
    row = add_semantic_provenance(row, SOURCE, "dwd_visibility", RID)
    return {"weather_visibility": row}


# COMMAND ----------

# DBTITLE 1,Family builder -- weather_cloud (join: cloudiness, cloud_type -> alt + layers)


def build_cloud():
    ci_df, ci_meta = _prep("dwd_cloudiness")
    ct_df, ct_meta = _prep("dwd_cloud_type")
    joined = _join_on_instant({"dwd_cloudiness": ci_df, "dwd_cloud_type": ct_df})
    row = _scaffold(
        joined,
        family="weather_cloud",
        parse=parse_mess_datum,
        qn_col=f"dwd_cloudiness__{ci_meta['qn']}",
        had_conflict_col="dwd_cloudiness___had_key_conflict",
    )
    methods = F.create_map([F.lit(x) for kv in METHOD_LABELS.items() for x in kv])

    v_n_primary = F.col("dwd_cloudiness__V_N").cast("double")
    primary_special = v_n_primary.isin(*_CLOUD_COVER_SENTINELS)
    v_n_alt = F.col("dwd_cloud_type__V_N").cast("double")
    alt_special = v_n_alt.isin(*_CLOUD_COVER_SENTINELS)
    ct_q = F.col(f"dwd_cloud_type__{ct_meta['qn']}")

    def layer(n: int):
        genus = F.col(f"dwd_cloud_type__V_S{n}_CS")
        genus_text = F.col(f"dwd_cloud_type__V_S{n}_CSA")
        height = F.col(f"dwd_cloud_type__V_S{n}_HHS").cast("double")
        cover_raw = F.col(f"dwd_cloud_type__V_S{n}_NS").cast("double")
        cover_special = cover_raw.isin(*_CLOUD_COVER_SENTINELS)
        populated = genus.isNotNull() | height.isNotNull() | cover_raw.isNotNull()
        entry = F.struct(
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
        return F.when(populated, entry)

    row = (
        row.withColumn(
            "cloud_cover_total_percent",
            F.when(primary_special, F.lit(None)).otherwise(
                v_n_primary * EIGHTHS_TO_PERCENT
            ),
        )
        .withColumn(
            "cloud_cover_total_native_value",
            F.when(primary_special, F.lit(None)).otherwise(v_n_primary),
        )
        .withColumn(
            "cloud_cover_total_native_unit",
            F.when(v_n_primary.isNotNull(), F.lit("eighths")),
        )
        .withColumn(
            "cloud_cover_total_alt",
            _alt_array(
                (
                    F.when(alt_special, F.lit(None)).otherwise(
                        v_n_alt * EIGHTHS_TO_PERCENT
                    ),
                    F.lit("dwd_cloud_type"),
                    ct_q,
                    F.when(alt_special, F.lit(None)).otherwise(v_n_alt),
                    F.when(v_n_alt.isNotNull(), F.lit("eighths")),
                ),
            ),
        )
        .withColumn(
            "observation_method",
            F.coalesce(
                methods[F.col("dwd_cloudiness__V_N_I")], F.col("dwd_cloudiness__V_N_I")
            ),
        )
        .withColumn(
            "layers",
            F.filter(
                F.array(*[layer(n) for n in (1, 2, 3, 4)]), lambda x: x.isNotNull()
            ),
        )
    )
    row = _keep_if_populated(
        row,
        ["cloud_cover_total_percent", "cloud_cover_total_alt", "layers"],
        ("weather_cloud", "dwd_cloud"),
    )
    row = add_semantic_provenance(row, SOURCE, "dwd_cloud", RID)
    return {"weather_cloud": row}


# COMMAND ----------

# DBTITLE 1,Family builder -- weather_present_weather (single product, categorical only)


def build_present_weather():
    df, meta = _prep("dwd_weather_phenomena")
    row = _scaffold(
        df,
        family="weather_present_weather",
        parse=parse_mess_datum,
        qn_col=meta["qn"],
        had_conflict_col="_had_key_conflict",
    )
    ww = F.col("WW").cast("double")
    special = ww == -1.0
    row = _keep_if_populated(
        row.withColumn(
            "present_weather_code", F.when(special, F.lit(None)).otherwise(F.col("WW"))
        ).withColumn(
            "present_weather_text",
            F.when(special, F.lit(None)).otherwise(F.col("WW_Text")),
        ),
        ["present_weather_code"],
        ("weather_present_weather", "dwd_weather_phenomena"),
    )
    row = add_semantic_provenance(row, SOURCE, "dwd_weather_phenomena", RID)
    return {"weather_present_weather": row}


# COMMAND ----------

# DBTITLE 1,Family builder -- weather_soil_temperature (single product, 6 depths together)

_SOIL_DEPTHS_CM = (2, 5, 10, 20, 50, 100)


def build_soil_temperature():
    df, meta = _prep("dwd_soil_temperature")
    row = _scaffold(
        df,
        family="weather_soil_temperature",
        parse=parse_mess_datum,
        qn_col=meta["qn"],
        had_conflict_col="_had_key_conflict",
    )
    for d in _SOIL_DEPTHS_CM:
        row = row.withColumn(
            f"soil_temperature_{d}cm_degc", F.col(f"V_TE{d:03d}").cast("double")
        )
    row = _keep_if_populated(
        row,
        [f"soil_temperature_{d}cm_degc" for d in _SOIL_DEPTHS_CM],
        ("weather_soil_temperature", "dwd_soil_temperature"),
    )
    row = add_semantic_provenance(row, SOURCE, "dwd_soil_temperature", RID)
    return {"weather_soil_temperature": row}


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_sun (sunshine duration only, hourly grid)


def build_sun():
    df, meta = _prep("dwd_sun")
    row = _scaffold(
        df,
        family="weather_sunshine_duration",
        parse=parse_mess_datum,
        qn_col=meta["qn"],
        had_conflict_col="_had_key_conflict",
    )
    row = _keep_if_populated(
        row.withColumn("sunshine_duration_minutes", F.col("SD_SO").cast("double")),
        ["sunshine_duration_minutes"],
        ("weather_sunshine_duration", "dwd_sun"),
    )
    row = add_semantic_provenance(row, SOURCE, "dwd_sun", RID)
    return {"weather_sunshine_duration": row}


# COMMAND ----------

# DBTITLE 1,Family builder -- dwd_solar (true-solar-time hours; split by meaning)


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
    row = _scaffold(
        df,
        family="dwd_solar",
        parse=parse_mess_datum_10min,
        qn_col=meta["qn"],
        had_conflict_col="_had_key_conflict",
        interval_reference="true_solar_time",
    ).withColumn("true_solar_time_native", F.trim(F.col("MESS_DATUM_WOZ")))
    row = _add_radiation_columns(row, "ATMO_LBERG", "longwave_downward_radiation")
    row = _add_radiation_columns(row, "FD_LBERG", "diffuse_radiation")
    row = _add_radiation_columns(row, "FG_LBERG", "global_radiation")
    row = row.withColumn(
        "sunshine_duration_minutes", F.col("SD_LBERG").cast("double")
    ).withColumn("solar_zenith_angle_degrees", F.col("ZENIT").cast("double"))

    radiation = _keep_if_populated(
        row,
        ["global_radiation_w_per_m2", "diffuse_radiation_w_per_m2"],
        ("weather_solar_radiation", "dwd_solar"),
    )
    radiation = add_semantic_provenance(radiation, SOURCE, "dwd_solar", RID)

    longwave = _keep_if_populated(
        row,
        ["longwave_downward_radiation_w_per_m2"],
        ("weather_longwave_radiation", "dwd_solar"),
    )
    longwave = add_semantic_provenance(longwave, SOURCE, "dwd_solar", RID)

    geometry = _keep_if_populated(
        row,
        ["solar_zenith_angle_degrees"],
        ("weather_solar_geometry", "dwd_solar"),
    )
    geometry = add_semantic_provenance(geometry, SOURCE, "dwd_solar", RID)

    sunshine = _keep_if_populated(
        row,
        ["sunshine_duration_minutes"],
        ("weather_sunshine_duration", "dwd_solar"),
    )
    sunshine = add_semantic_provenance(sunshine, SOURCE, "dwd_solar", RID)

    return {
        "weather_solar_radiation": radiation,
        "weather_longwave_radiation": longwave,
        "weather_solar_geometry": geometry,
        "weather_sunshine_duration": sunshine,
    }


# COMMAND ----------

# DBTITLE 1,Configuration -- builders producing this run's families
BUILDERS = [
    build_temperature,
    build_humidity,
    build_pressure,
    build_wind,
    build_precipitation,
    build_visibility,
    build_cloud,
    build_present_weather,
    build_soil_temperature,
    build_sun,
    build_solar,
]

# COMMAND ----------

# DBTITLE 1,Transform -- run every builder
built = [b() for b in BUILDERS]

# COMMAND ----------

# DBTITLE 1,Transform -- group contributions by family (more than one builder can feed a family)
per_family: dict[str, list] = {}
for fam_dict in built:
    for fam, df in fam_dict.items():
        per_family.setdefault(fam, []).append(df)

# COMMAND ----------

# DBTITLE 1,Write Silver -- one write per family, unioning every contributing builder
# Conform each contribution to the family schema before unioning -- each
# still carries its own raw source columns (e.g. QN_7 vs QN_592).
for fam, frames in per_family.items():
    conformed = [conform(f, WEATHER_FAMILY_COLUMNS[fam]) for f in frames]
    combined = conformed[0]
    for extra in conformed[1:]:
        combined = combined.unionByName(extra)
    write_semantic(
        combined,
        fam,
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
        replace_where=f"source_system = '{SOURCE}'",
    )

# COMMAND ----------

# DBTITLE 1,Inspect -- each family structure written this run
findings_blocks = {}
for fam in per_family:
    written = spark.table(semantic_table(fam)).filter(F.col("source_system") == SOURCE)
    findings_blocks[fam] = inspect_table(
        written,
        fam,
        source=FINDINGS_SOURCE,
        component=COMPONENT,
        rid=RID,
        key_cols=["observation_key"],
        extra_checks=structure_extra_checks(written),
    )

# COMMAND ----------

# DBTITLE 1,Export findings -- each family structure written this run
for fam, blocks in findings_blocks.items():
    write_silver_findings(
        FINDINGS_SOURCE,
        f"{COMPONENT.split('/')[-1]}__{fam}",
        fam,
        blocks,
    )

# COMMAND ----------

# DBTITLE 1,Export findings -- dedup/conflict reconciliation proof, per DWD table
for table, stats in RECONCILIATION.items():
    write_silver_findings(
        FINDINGS_SOURCE,
        f"{COMPONENT.split('/')[-1]}__{table}__reconciliation",
        f"dedup/conflict reconciliation -- {table}",
        [
            (
                "Bronze -> duplicates collapsed (exact / QN-only) -> conflicts quarantined -> kept",
                dict_to_markdown_row(stats),
            )
        ],
    )

# COMMAND ----------

# DBTITLE 1,Export findings -- all-null rows dropped, per family (kept-side rows -> written)
for (fam, dataset), pre in PREFILTER.items():
    pre_rows = pre.count()
    written_rows = (
        spark.table(semantic_table(fam))
        .filter(
            (F.col("source_system") == SOURCE) & (F.col("source_dataset") == dataset)
        )
        .count()
    )
    write_silver_findings(
        FINDINGS_SOURCE,
        f"{COMPONENT.split('/')[-1]}__{fam}__{dataset}__allnull",
        f"all-null rows dropped -- {fam} ({dataset})",
        [
            (
                "Rows before the no-measurement filter -> dropped -> written",
                dict_to_markdown_row(
                    {
                        "rows_before_filter": pre_rows,
                        "all_null_rows_dropped": pre_rows - written_rows,
                        "written_rows": written_rows,
                    }
                ),
            )
        ],
    )
