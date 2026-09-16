# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- DWD HOURLY MEASUREMENTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the 14 DWD hourly-historical measurement Bronze tables into
# MAGIC source-scoped Silver at their (station, hour) grain, one config-driven
# MAGIC block per table (read / transform / write / inspect each their own cell).
# MAGIC `dwd_solar` (10-minute grid) is a separate notebook.

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
SOURCE = "dwd"
COMPONENT = "silver/energy/weather/01_dwd_hourly_measurements"
RID = run_id()

CONTRACT = load_contract(SOURCE)
MAPPING = load_mapping(SOURCE)
TABLES = contract_tables(CONTRACT)
NAME_MAP = flatten_business_names(MAPPING, SOURCE)
CODED = coded_columns(MAPPING, SOURCE)

# (station, parameter_source_code, has_observed_missing) accumulated across
# tables, captured pre-rename so codes match parameter_source_code.
_observed_rows: list[tuple] = []

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_air_temperature
_bronze_air_temperature = read_bronze("dwd_air_temperature")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_cloudiness
_bronze_cloudiness = read_bronze("dwd_cloudiness")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_moisture
_bronze_moisture = read_bronze("dwd_moisture")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_precipitation
_bronze_precipitation = read_bronze("dwd_precipitation")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_pressure
_bronze_pressure = read_bronze("dwd_pressure")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_sun
_bronze_sun = read_bronze("dwd_sun")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_wind
_bronze_wind = read_bronze("dwd_wind")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_dew_point
_bronze_dew_point = read_bronze("dwd_dew_point")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_soil_temperature
_bronze_soil_temperature = read_bronze("dwd_soil_temperature")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_visibility
_bronze_visibility = read_bronze("dwd_visibility")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_cloud_type
_bronze_cloud_type = read_bronze("dwd_cloud_type")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_wind_synop
_bronze_wind_synop = read_bronze("dwd_wind_synop")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_extreme_wind
_bronze_extreme_wind = read_bronze("dwd_extreme_wind")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- dwd_weather_phenomena
_bronze_weather_phenomena = read_bronze("dwd_weather_phenomena")

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_air_temperature
_cols_air_temperature = [c["name"] for c in TABLES["dwd_air_temperature"]["columns"]]
_qn_col_air_temperature = next(c for c in _cols_air_temperature if c.startswith("QN_"))
_value_cols_air_temperature = [
    c
    for c in _cols_air_temperature
    if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor") and not c.startswith("QN_")
]

_df_air_temperature = strip_sentinels(
    _bronze_air_temperature, [*_value_cols_air_temperature, _qn_col_air_temperature]
)

for _r in (
    _df_air_temperature.groupBy("STATIONS_ID")
    .agg(
        *[
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in _value_cols_air_temperature
        ]
    )
    .collect()
):
    _sid = str(_r["STATIONS_ID"]).strip()
    for _c in _value_cols_air_temperature:
        _observed_rows.append((_sid, _c, (_r[_c] or 0) > 0))

_df_air_temperature, _q_air_temperature = resolve_conflicts(
    _df_air_temperature,
    ["STATIONS_ID", "MESS_DATUM"],
    _value_cols_air_temperature,
    qn_col=_qn_col_air_temperature,
    bronze_table="dwd_air_temperature",
)
_q_air_temperature = _q_air_temperature.withColumn("source_system", F.lit(SOURCE))
write_quarantine(_q_air_temperature, RID)

_df_air_temperature = cast_logical(
    _df_air_temperature, TABLES["dwd_air_temperature"]["columns"]
)
_df_air_temperature = decode_qn(_df_air_temperature, _qn_col_air_temperature)

for _raw, _en_map in CODED.items():
    if _raw in _df_air_temperature.columns:
        _pref = NAME_MAP.get(_raw, _raw)
        _df_air_temperature = decode_via_labeled_map(
            _df_air_temperature, _raw, _pref, _en_map
        ).drop(_raw)

_df_air_temperature = apply_renames(_df_air_temperature, NAME_MAP)
_df_air_temperature = _df_air_temperature.withColumn(
    "_srid", sha_key(F.lit("dwd_air_temperature"), "STATIONS_ID", "MESS_DATUM")
)
_df_air_temperature = _df_air_temperature.withColumn(
    "observation_ts", parse_mess_datum("MESS_DATUM")
).drop("MESS_DATUM")
_df_air_temperature = attach_city_ags(_df_air_temperature, "city")
_df_air_temperature = add_provenance(_df_air_temperature, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_air_temperature
write_silver(
    _df_air_temperature,
    "dwd_air_temperature",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_air_temperature
_findings_blocks = inspect_table(
    _df_air_temperature,
    "dwd_air_temperature",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_bronze_air_temperature,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_air_temperature",
    "dwd_air_temperature",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_cloudiness
_cols_cloudiness = [c["name"] for c in TABLES["dwd_cloudiness"]["columns"]]
_qn_col_cloudiness = next(c for c in _cols_cloudiness if c.startswith("QN_"))
_value_cols_cloudiness = [
    c
    for c in _cols_cloudiness
    if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor") and not c.startswith("QN_")
]

_df_cloudiness = strip_sentinels(
    _bronze_cloudiness, [*_value_cols_cloudiness, _qn_col_cloudiness]
)

for _r in (
    _df_cloudiness.groupBy("STATIONS_ID")
    .agg(
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in _value_cols_cloudiness]
    )
    .collect()
):
    _sid = str(_r["STATIONS_ID"]).strip()
    for _c in _value_cols_cloudiness:
        _observed_rows.append((_sid, _c, (_r[_c] or 0) > 0))

_df_cloudiness, _q_cloudiness = resolve_conflicts(
    _df_cloudiness,
    ["STATIONS_ID", "MESS_DATUM"],
    _value_cols_cloudiness,
    qn_col=_qn_col_cloudiness,
    bronze_table="dwd_cloudiness",
)
_q_cloudiness = _q_cloudiness.withColumn("source_system", F.lit(SOURCE))
write_quarantine(_q_cloudiness, RID)

_df_cloudiness = cast_logical(_df_cloudiness, TABLES["dwd_cloudiness"]["columns"])
_df_cloudiness = decode_qn(_df_cloudiness, _qn_col_cloudiness)

for _raw, _en_map in CODED.items():
    if _raw in _df_cloudiness.columns:
        _pref = NAME_MAP.get(_raw, _raw)
        _df_cloudiness = decode_via_labeled_map(
            _df_cloudiness, _raw, _pref, _en_map
        ).drop(_raw)

_df_cloudiness = apply_renames(_df_cloudiness, NAME_MAP)
_df_cloudiness = _df_cloudiness.withColumn(
    "_srid", sha_key(F.lit("dwd_cloudiness"), "STATIONS_ID", "MESS_DATUM")
)
_df_cloudiness = _df_cloudiness.withColumn(
    "observation_ts", parse_mess_datum("MESS_DATUM")
).drop("MESS_DATUM")
_df_cloudiness = attach_city_ags(_df_cloudiness, "city")
_df_cloudiness = add_provenance(_df_cloudiness, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_cloudiness
write_silver(
    _df_cloudiness, "dwd_cloudiness", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_cloudiness
_findings_blocks = inspect_table(
    _df_cloudiness,
    "dwd_cloudiness",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_bronze_cloudiness,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_cloudiness",
    "dwd_cloudiness",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_moisture
_cols_moisture = [c["name"] for c in TABLES["dwd_moisture"]["columns"]]
_qn_col_moisture = next(c for c in _cols_moisture if c.startswith("QN_"))
_value_cols_moisture = [
    c
    for c in _cols_moisture
    if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor") and not c.startswith("QN_")
]

_df_moisture = strip_sentinels(
    _bronze_moisture, [*_value_cols_moisture, _qn_col_moisture]
)

for _r in (
    _df_moisture.groupBy("STATIONS_ID")
    .agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in _value_cols_moisture])
    .collect()
):
    _sid = str(_r["STATIONS_ID"]).strip()
    for _c in _value_cols_moisture:
        _observed_rows.append((_sid, _c, (_r[_c] or 0) > 0))

_df_moisture, _q_moisture = resolve_conflicts(
    _df_moisture,
    ["STATIONS_ID", "MESS_DATUM"],
    _value_cols_moisture,
    qn_col=_qn_col_moisture,
    bronze_table="dwd_moisture",
)
_q_moisture = _q_moisture.withColumn("source_system", F.lit(SOURCE))
write_quarantine(_q_moisture, RID)

_df_moisture = cast_logical(_df_moisture, TABLES["dwd_moisture"]["columns"])
_df_moisture = decode_qn(_df_moisture, _qn_col_moisture)

for _raw, _en_map in CODED.items():
    if _raw in _df_moisture.columns:
        _pref = NAME_MAP.get(_raw, _raw)
        _df_moisture = decode_via_labeled_map(_df_moisture, _raw, _pref, _en_map).drop(
            _raw
        )

_df_moisture = apply_renames(_df_moisture, NAME_MAP)
_df_moisture = _df_moisture.withColumn(
    "_srid", sha_key(F.lit("dwd_moisture"), "STATIONS_ID", "MESS_DATUM")
)
_df_moisture = _df_moisture.withColumn(
    "observation_ts", parse_mess_datum("MESS_DATUM")
).drop("MESS_DATUM")
_df_moisture = attach_city_ags(_df_moisture, "city")
_df_moisture = add_provenance(_df_moisture, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_moisture
write_silver(_df_moisture, "dwd_moisture", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_moisture
_findings_blocks = inspect_table(
    _df_moisture,
    "dwd_moisture",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_bronze_moisture,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_moisture",
    "dwd_moisture",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_precipitation
_cols_precipitation = [c["name"] for c in TABLES["dwd_precipitation"]["columns"]]
_qn_col_precipitation = next(c for c in _cols_precipitation if c.startswith("QN_"))
_value_cols_precipitation = [
    c
    for c in _cols_precipitation
    if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor") and not c.startswith("QN_")
]

_df_precipitation = strip_sentinels(
    _bronze_precipitation, [*_value_cols_precipitation, _qn_col_precipitation]
)

for _r in (
    _df_precipitation.groupBy("STATIONS_ID")
    .agg(
        *[
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in _value_cols_precipitation
        ]
    )
    .collect()
):
    _sid = str(_r["STATIONS_ID"]).strip()
    for _c in _value_cols_precipitation:
        _observed_rows.append((_sid, _c, (_r[_c] or 0) > 0))

_df_precipitation, _q_precipitation = resolve_conflicts(
    _df_precipitation,
    ["STATIONS_ID", "MESS_DATUM"],
    _value_cols_precipitation,
    qn_col=_qn_col_precipitation,
    bronze_table="dwd_precipitation",
)
_q_precipitation = _q_precipitation.withColumn("source_system", F.lit(SOURCE))
write_quarantine(_q_precipitation, RID)

_df_precipitation = cast_logical(
    _df_precipitation, TABLES["dwd_precipitation"]["columns"]
)
_df_precipitation = decode_qn(_df_precipitation, _qn_col_precipitation)

for _raw, _en_map in CODED.items():
    if _raw in _df_precipitation.columns:
        _pref = NAME_MAP.get(_raw, _raw)
        _df_precipitation = decode_via_labeled_map(
            _df_precipitation, _raw, _pref, _en_map
        ).drop(_raw)

_df_precipitation = apply_renames(_df_precipitation, NAME_MAP)
_df_precipitation = _df_precipitation.withColumn(
    "_srid", sha_key(F.lit("dwd_precipitation"), "STATIONS_ID", "MESS_DATUM")
)
_df_precipitation = _df_precipitation.withColumn(
    "observation_ts", parse_mess_datum("MESS_DATUM")
).drop("MESS_DATUM")
_df_precipitation = attach_city_ags(_df_precipitation, "city")
_df_precipitation = add_provenance(_df_precipitation, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_precipitation
write_silver(
    _df_precipitation, "dwd_precipitation", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_precipitation
_findings_blocks = inspect_table(
    _df_precipitation,
    "dwd_precipitation",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_bronze_precipitation,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_precipitation",
    "dwd_precipitation",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_pressure
_cols_pressure = [c["name"] for c in TABLES["dwd_pressure"]["columns"]]
_qn_col_pressure = next(c for c in _cols_pressure if c.startswith("QN_"))
_value_cols_pressure = [
    c
    for c in _cols_pressure
    if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor") and not c.startswith("QN_")
]

_df_pressure = strip_sentinels(
    _bronze_pressure, [*_value_cols_pressure, _qn_col_pressure]
)

for _r in (
    _df_pressure.groupBy("STATIONS_ID")
    .agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in _value_cols_pressure])
    .collect()
):
    _sid = str(_r["STATIONS_ID"]).strip()
    for _c in _value_cols_pressure:
        _observed_rows.append((_sid, _c, (_r[_c] or 0) > 0))

_df_pressure, _q_pressure = resolve_conflicts(
    _df_pressure,
    ["STATIONS_ID", "MESS_DATUM"],
    _value_cols_pressure,
    qn_col=_qn_col_pressure,
    bronze_table="dwd_pressure",
)
_q_pressure = _q_pressure.withColumn("source_system", F.lit(SOURCE))
write_quarantine(_q_pressure, RID)

_df_pressure = cast_logical(_df_pressure, TABLES["dwd_pressure"]["columns"])
_df_pressure = decode_qn(_df_pressure, _qn_col_pressure)

for _raw, _en_map in CODED.items():
    if _raw in _df_pressure.columns:
        _pref = NAME_MAP.get(_raw, _raw)
        _df_pressure = decode_via_labeled_map(_df_pressure, _raw, _pref, _en_map).drop(
            _raw
        )

_df_pressure = apply_renames(_df_pressure, NAME_MAP)
_df_pressure = _df_pressure.withColumn(
    "_srid", sha_key(F.lit("dwd_pressure"), "STATIONS_ID", "MESS_DATUM")
)
_df_pressure = _df_pressure.withColumn(
    "observation_ts", parse_mess_datum("MESS_DATUM")
).drop("MESS_DATUM")
_df_pressure = attach_city_ags(_df_pressure, "city")
_df_pressure = add_provenance(_df_pressure, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_pressure
write_silver(_df_pressure, "dwd_pressure", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_pressure
_findings_blocks = inspect_table(
    _df_pressure,
    "dwd_pressure",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_bronze_pressure,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_pressure",
    "dwd_pressure",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_sun
_cols_sun = [c["name"] for c in TABLES["dwd_sun"]["columns"]]
_qn_col_sun = next(c for c in _cols_sun if c.startswith("QN_"))
_value_cols_sun = [
    c
    for c in _cols_sun
    if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor") and not c.startswith("QN_")
]

_df_sun = strip_sentinels(_bronze_sun, [*_value_cols_sun, _qn_col_sun])

for _r in (
    _df_sun.groupBy("STATIONS_ID")
    .agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in _value_cols_sun])
    .collect()
):
    _sid = str(_r["STATIONS_ID"]).strip()
    for _c in _value_cols_sun:
        _observed_rows.append((_sid, _c, (_r[_c] or 0) > 0))

_df_sun, _q_sun = resolve_conflicts(
    _df_sun,
    ["STATIONS_ID", "MESS_DATUM"],
    _value_cols_sun,
    qn_col=_qn_col_sun,
    bronze_table="dwd_sun",
)
_q_sun = _q_sun.withColumn("source_system", F.lit(SOURCE))
write_quarantine(_q_sun, RID)

_df_sun = cast_logical(_df_sun, TABLES["dwd_sun"]["columns"])
_df_sun = decode_qn(_df_sun, _qn_col_sun)

for _raw, _en_map in CODED.items():
    if _raw in _df_sun.columns:
        _pref = NAME_MAP.get(_raw, _raw)
        _df_sun = decode_via_labeled_map(_df_sun, _raw, _pref, _en_map).drop(_raw)

_df_sun = apply_renames(_df_sun, NAME_MAP)
_df_sun = _df_sun.withColumn(
    "_srid", sha_key(F.lit("dwd_sun"), "STATIONS_ID", "MESS_DATUM")
)
_df_sun = _df_sun.withColumn("observation_ts", parse_mess_datum("MESS_DATUM")).drop(
    "MESS_DATUM"
)
_df_sun = attach_city_ags(_df_sun, "city")
_df_sun = add_provenance(_df_sun, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_sun
write_silver(_df_sun, "dwd_sun", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_sun
_findings_blocks = inspect_table(
    _df_sun,
    "dwd_sun",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_bronze_sun,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_sun",
    "dwd_sun",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_wind
_cols_wind = [c["name"] for c in TABLES["dwd_wind"]["columns"]]
_qn_col_wind = next(c for c in _cols_wind if c.startswith("QN_"))
_value_cols_wind = [
    c
    for c in _cols_wind
    if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor") and not c.startswith("QN_")
]

_df_wind = strip_sentinels(_bronze_wind, [*_value_cols_wind, _qn_col_wind])

for _r in (
    _df_wind.groupBy("STATIONS_ID")
    .agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in _value_cols_wind])
    .collect()
):
    _sid = str(_r["STATIONS_ID"]).strip()
    for _c in _value_cols_wind:
        _observed_rows.append((_sid, _c, (_r[_c] or 0) > 0))

_df_wind, _q_wind = resolve_conflicts(
    _df_wind,
    ["STATIONS_ID", "MESS_DATUM"],
    _value_cols_wind,
    qn_col=_qn_col_wind,
    bronze_table="dwd_wind",
)
_q_wind = _q_wind.withColumn("source_system", F.lit(SOURCE))
write_quarantine(_q_wind, RID)

_df_wind = cast_logical(_df_wind, TABLES["dwd_wind"]["columns"])
_df_wind = decode_qn(_df_wind, _qn_col_wind)

for _raw, _en_map in CODED.items():
    if _raw in _df_wind.columns:
        _pref = NAME_MAP.get(_raw, _raw)
        _df_wind = decode_via_labeled_map(_df_wind, _raw, _pref, _en_map).drop(_raw)

_df_wind = apply_renames(_df_wind, NAME_MAP)
_df_wind = _df_wind.withColumn(
    "_srid", sha_key(F.lit("dwd_wind"), "STATIONS_ID", "MESS_DATUM")
)
_df_wind = _df_wind.withColumn("observation_ts", parse_mess_datum("MESS_DATUM")).drop(
    "MESS_DATUM"
)
_df_wind = attach_city_ags(_df_wind, "city")
_df_wind = add_provenance(_df_wind, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_wind
write_silver(_df_wind, "dwd_wind", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_wind
_findings_blocks = inspect_table(
    _df_wind,
    "dwd_wind",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_bronze_wind,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_wind",
    "dwd_wind",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_dew_point
_cols_dew_point = [c["name"] for c in TABLES["dwd_dew_point"]["columns"]]
_qn_col_dew_point = next(c for c in _cols_dew_point if c.startswith("QN_"))
_value_cols_dew_point = [
    c
    for c in _cols_dew_point
    if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor") and not c.startswith("QN_")
]

_df_dew_point = strip_sentinels(
    _bronze_dew_point, [*_value_cols_dew_point, _qn_col_dew_point]
)

for _r in (
    _df_dew_point.groupBy("STATIONS_ID")
    .agg(
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in _value_cols_dew_point]
    )
    .collect()
):
    _sid = str(_r["STATIONS_ID"]).strip()
    for _c in _value_cols_dew_point:
        _observed_rows.append((_sid, _c, (_r[_c] or 0) > 0))

_df_dew_point, _q_dew_point = resolve_conflicts(
    _df_dew_point,
    ["STATIONS_ID", "MESS_DATUM"],
    _value_cols_dew_point,
    qn_col=_qn_col_dew_point,
    bronze_table="dwd_dew_point",
)
_q_dew_point = _q_dew_point.withColumn("source_system", F.lit(SOURCE))
write_quarantine(_q_dew_point, RID)

_df_dew_point = cast_logical(_df_dew_point, TABLES["dwd_dew_point"]["columns"])
_df_dew_point = decode_qn(_df_dew_point, _qn_col_dew_point)

for _raw, _en_map in CODED.items():
    if _raw in _df_dew_point.columns:
        _pref = NAME_MAP.get(_raw, _raw)
        _df_dew_point = decode_via_labeled_map(
            _df_dew_point, _raw, _pref, _en_map
        ).drop(_raw)

_df_dew_point = apply_renames(_df_dew_point, NAME_MAP)
_df_dew_point = _df_dew_point.withColumn(
    "_srid", sha_key(F.lit("dwd_dew_point"), "STATIONS_ID", "MESS_DATUM")
)
_df_dew_point = _df_dew_point.withColumn(
    "observation_ts", parse_mess_datum("MESS_DATUM")
).drop("MESS_DATUM")
_df_dew_point = attach_city_ags(_df_dew_point, "city")
_df_dew_point = add_provenance(_df_dew_point, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_dew_point
write_silver(
    _df_dew_point, "dwd_dew_point", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_dew_point
_findings_blocks = inspect_table(
    _df_dew_point,
    "dwd_dew_point",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_bronze_dew_point,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_dew_point",
    "dwd_dew_point",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_soil_temperature
_cols_soil_temperature = [c["name"] for c in TABLES["dwd_soil_temperature"]["columns"]]
_qn_col_soil_temperature = next(
    c for c in _cols_soil_temperature if c.startswith("QN_")
)
_value_cols_soil_temperature = [
    c
    for c in _cols_soil_temperature
    if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor") and not c.startswith("QN_")
]

_df_soil_temperature = strip_sentinels(
    _bronze_soil_temperature, [*_value_cols_soil_temperature, _qn_col_soil_temperature]
)

for _r in (
    _df_soil_temperature.groupBy("STATIONS_ID")
    .agg(
        *[
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in _value_cols_soil_temperature
        ]
    )
    .collect()
):
    _sid = str(_r["STATIONS_ID"]).strip()
    for _c in _value_cols_soil_temperature:
        _observed_rows.append((_sid, _c, (_r[_c] or 0) > 0))

_df_soil_temperature, _q_soil_temperature = resolve_conflicts(
    _df_soil_temperature,
    ["STATIONS_ID", "MESS_DATUM"],
    _value_cols_soil_temperature,
    qn_col=_qn_col_soil_temperature,
    bronze_table="dwd_soil_temperature",
)
_q_soil_temperature = _q_soil_temperature.withColumn("source_system", F.lit(SOURCE))
write_quarantine(_q_soil_temperature, RID)

_df_soil_temperature = cast_logical(
    _df_soil_temperature, TABLES["dwd_soil_temperature"]["columns"]
)
_df_soil_temperature = decode_qn(_df_soil_temperature, _qn_col_soil_temperature)

for _raw, _en_map in CODED.items():
    if _raw in _df_soil_temperature.columns:
        _pref = NAME_MAP.get(_raw, _raw)
        _df_soil_temperature = decode_via_labeled_map(
            _df_soil_temperature, _raw, _pref, _en_map
        ).drop(_raw)

_df_soil_temperature = apply_renames(_df_soil_temperature, NAME_MAP)
_df_soil_temperature = _df_soil_temperature.withColumn(
    "_srid", sha_key(F.lit("dwd_soil_temperature"), "STATIONS_ID", "MESS_DATUM")
)
_df_soil_temperature = _df_soil_temperature.withColumn(
    "observation_ts", parse_mess_datum("MESS_DATUM")
).drop("MESS_DATUM")
_df_soil_temperature = attach_city_ags(_df_soil_temperature, "city")
_df_soil_temperature = add_provenance(_df_soil_temperature, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_soil_temperature
write_silver(
    _df_soil_temperature,
    "dwd_soil_temperature",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_soil_temperature
_findings_blocks = inspect_table(
    _df_soil_temperature,
    "dwd_soil_temperature",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_bronze_soil_temperature,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_soil_temperature",
    "dwd_soil_temperature",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_visibility
_cols_visibility = [c["name"] for c in TABLES["dwd_visibility"]["columns"]]
_qn_col_visibility = next(c for c in _cols_visibility if c.startswith("QN_"))
_value_cols_visibility = [
    c
    for c in _cols_visibility
    if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor") and not c.startswith("QN_")
]

_df_visibility = strip_sentinels(
    _bronze_visibility, [*_value_cols_visibility, _qn_col_visibility]
)

for _r in (
    _df_visibility.groupBy("STATIONS_ID")
    .agg(
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in _value_cols_visibility]
    )
    .collect()
):
    _sid = str(_r["STATIONS_ID"]).strip()
    for _c in _value_cols_visibility:
        _observed_rows.append((_sid, _c, (_r[_c] or 0) > 0))

_df_visibility, _q_visibility = resolve_conflicts(
    _df_visibility,
    ["STATIONS_ID", "MESS_DATUM"],
    _value_cols_visibility,
    qn_col=_qn_col_visibility,
    bronze_table="dwd_visibility",
)
_q_visibility = _q_visibility.withColumn("source_system", F.lit(SOURCE))
write_quarantine(_q_visibility, RID)

_df_visibility = cast_logical(_df_visibility, TABLES["dwd_visibility"]["columns"])
_df_visibility = decode_qn(_df_visibility, _qn_col_visibility)

for _raw, _en_map in CODED.items():
    if _raw in _df_visibility.columns:
        _pref = NAME_MAP.get(_raw, _raw)
        _df_visibility = decode_via_labeled_map(
            _df_visibility, _raw, _pref, _en_map
        ).drop(_raw)

_df_visibility = apply_renames(_df_visibility, NAME_MAP)
_df_visibility = _df_visibility.withColumn(
    "_srid", sha_key(F.lit("dwd_visibility"), "STATIONS_ID", "MESS_DATUM")
)
_df_visibility = _df_visibility.withColumn(
    "observation_ts", parse_mess_datum("MESS_DATUM")
).drop("MESS_DATUM")
_df_visibility = attach_city_ags(_df_visibility, "city")
_df_visibility = add_provenance(_df_visibility, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_visibility
write_silver(
    _df_visibility, "dwd_visibility", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_visibility
_findings_blocks = inspect_table(
    _df_visibility,
    "dwd_visibility",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_bronze_visibility,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_visibility",
    "dwd_visibility",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_cloud_type
_cols_cloud_type = [c["name"] for c in TABLES["dwd_cloud_type"]["columns"]]
_qn_col_cloud_type = next(c for c in _cols_cloud_type if c.startswith("QN_"))
_value_cols_cloud_type = [
    c
    for c in _cols_cloud_type
    if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor") and not c.startswith("QN_")
]

_df_cloud_type = strip_sentinels(
    _bronze_cloud_type, [*_value_cols_cloud_type, _qn_col_cloud_type]
)

for _r in (
    _df_cloud_type.groupBy("STATIONS_ID")
    .agg(
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in _value_cols_cloud_type]
    )
    .collect()
):
    _sid = str(_r["STATIONS_ID"]).strip()
    for _c in _value_cols_cloud_type:
        _observed_rows.append((_sid, _c, (_r[_c] or 0) > 0))

_df_cloud_type, _q_cloud_type = resolve_conflicts(
    _df_cloud_type,
    ["STATIONS_ID", "MESS_DATUM"],
    _value_cols_cloud_type,
    qn_col=_qn_col_cloud_type,
    bronze_table="dwd_cloud_type",
)
_q_cloud_type = _q_cloud_type.withColumn("source_system", F.lit(SOURCE))
write_quarantine(_q_cloud_type, RID)

_df_cloud_type = cast_logical(_df_cloud_type, TABLES["dwd_cloud_type"]["columns"])
_df_cloud_type = decode_qn(_df_cloud_type, _qn_col_cloud_type)

for _raw, _en_map in CODED.items():
    if _raw in _df_cloud_type.columns:
        _pref = NAME_MAP.get(_raw, _raw)
        _df_cloud_type = decode_via_labeled_map(
            _df_cloud_type, _raw, _pref, _en_map
        ).drop(_raw)

_df_cloud_type = apply_renames(_df_cloud_type, NAME_MAP)
_df_cloud_type = _df_cloud_type.withColumn(
    "_srid", sha_key(F.lit("dwd_cloud_type"), "STATIONS_ID", "MESS_DATUM")
)
_df_cloud_type = _df_cloud_type.withColumn(
    "observation_ts", parse_mess_datum("MESS_DATUM")
).drop("MESS_DATUM")
_df_cloud_type = attach_city_ags(_df_cloud_type, "city")
_df_cloud_type = add_provenance(_df_cloud_type, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_cloud_type
write_silver(
    _df_cloud_type, "dwd_cloud_type", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_cloud_type
_findings_blocks = inspect_table(
    _df_cloud_type,
    "dwd_cloud_type",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_bronze_cloud_type,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_cloud_type",
    "dwd_cloud_type",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_wind_synop
_cols_wind_synop = [c["name"] for c in TABLES["dwd_wind_synop"]["columns"]]
_qn_col_wind_synop = next(c for c in _cols_wind_synop if c.startswith("QN_"))
_value_cols_wind_synop = [
    c
    for c in _cols_wind_synop
    if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor") and not c.startswith("QN_")
]

_df_wind_synop = strip_sentinels(
    _bronze_wind_synop, [*_value_cols_wind_synop, _qn_col_wind_synop]
)

for _r in (
    _df_wind_synop.groupBy("STATIONS_ID")
    .agg(
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in _value_cols_wind_synop]
    )
    .collect()
):
    _sid = str(_r["STATIONS_ID"]).strip()
    for _c in _value_cols_wind_synop:
        _observed_rows.append((_sid, _c, (_r[_c] or 0) > 0))

_df_wind_synop, _q_wind_synop = resolve_conflicts(
    _df_wind_synop,
    ["STATIONS_ID", "MESS_DATUM"],
    _value_cols_wind_synop,
    qn_col=_qn_col_wind_synop,
    bronze_table="dwd_wind_synop",
)
_q_wind_synop = _q_wind_synop.withColumn("source_system", F.lit(SOURCE))
write_quarantine(_q_wind_synop, RID)

_df_wind_synop = cast_logical(_df_wind_synop, TABLES["dwd_wind_synop"]["columns"])
_df_wind_synop = decode_qn(_df_wind_synop, _qn_col_wind_synop)

for _raw, _en_map in CODED.items():
    if _raw in _df_wind_synop.columns:
        _pref = NAME_MAP.get(_raw, _raw)
        _df_wind_synop = decode_via_labeled_map(
            _df_wind_synop, _raw, _pref, _en_map
        ).drop(_raw)

_df_wind_synop = apply_renames(_df_wind_synop, NAME_MAP)
_df_wind_synop = _df_wind_synop.withColumn(
    "_srid", sha_key(F.lit("dwd_wind_synop"), "STATIONS_ID", "MESS_DATUM")
)
_df_wind_synop = _df_wind_synop.withColumn(
    "observation_ts", parse_mess_datum("MESS_DATUM")
).drop("MESS_DATUM")
_df_wind_synop = attach_city_ags(_df_wind_synop, "city")
_df_wind_synop = add_provenance(_df_wind_synop, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_wind_synop
write_silver(
    _df_wind_synop, "dwd_wind_synop", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_wind_synop
_findings_blocks = inspect_table(
    _df_wind_synop,
    "dwd_wind_synop",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_bronze_wind_synop,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_wind_synop",
    "dwd_wind_synop",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_extreme_wind
_cols_extreme_wind = [c["name"] for c in TABLES["dwd_extreme_wind"]["columns"]]
_qn_col_extreme_wind = next(c for c in _cols_extreme_wind if c.startswith("QN_"))
_value_cols_extreme_wind = [
    c
    for c in _cols_extreme_wind
    if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor") and not c.startswith("QN_")
]

_df_extreme_wind = strip_sentinels(
    _bronze_extreme_wind, [*_value_cols_extreme_wind, _qn_col_extreme_wind]
)

for _r in (
    _df_extreme_wind.groupBy("STATIONS_ID")
    .agg(
        *[
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in _value_cols_extreme_wind
        ]
    )
    .collect()
):
    _sid = str(_r["STATIONS_ID"]).strip()
    for _c in _value_cols_extreme_wind:
        _observed_rows.append((_sid, _c, (_r[_c] or 0) > 0))

_df_extreme_wind, _q_extreme_wind = resolve_conflicts(
    _df_extreme_wind,
    ["STATIONS_ID", "MESS_DATUM"],
    _value_cols_extreme_wind,
    qn_col=_qn_col_extreme_wind,
    bronze_table="dwd_extreme_wind",
)
_q_extreme_wind = _q_extreme_wind.withColumn("source_system", F.lit(SOURCE))
write_quarantine(_q_extreme_wind, RID)

_df_extreme_wind = cast_logical(_df_extreme_wind, TABLES["dwd_extreme_wind"]["columns"])
_df_extreme_wind = decode_qn(_df_extreme_wind, _qn_col_extreme_wind)

for _raw, _en_map in CODED.items():
    if _raw in _df_extreme_wind.columns:
        _pref = NAME_MAP.get(_raw, _raw)
        _df_extreme_wind = decode_via_labeled_map(
            _df_extreme_wind, _raw, _pref, _en_map
        ).drop(_raw)

_df_extreme_wind = apply_renames(_df_extreme_wind, NAME_MAP)
_df_extreme_wind = _df_extreme_wind.withColumn(
    "_srid", sha_key(F.lit("dwd_extreme_wind"), "STATIONS_ID", "MESS_DATUM")
)
_df_extreme_wind = _df_extreme_wind.withColumn(
    "observation_ts", parse_mess_datum("MESS_DATUM")
).drop("MESS_DATUM")
_df_extreme_wind = attach_city_ags(_df_extreme_wind, "city")
_df_extreme_wind = add_provenance(_df_extreme_wind, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_extreme_wind
write_silver(
    _df_extreme_wind, "dwd_extreme_wind", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_extreme_wind
_findings_blocks = inspect_table(
    _df_extreme_wind,
    "dwd_extreme_wind",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_bronze_extreme_wind,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_extreme_wind",
    "dwd_extreme_wind",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- dwd_weather_phenomena
_cols_weather_phenomena = [
    c["name"] for c in TABLES["dwd_weather_phenomena"]["columns"]
]
_qn_col_weather_phenomena = next(
    c for c in _cols_weather_phenomena if c.startswith("QN_")
)
_value_cols_weather_phenomena = [
    c
    for c in _cols_weather_phenomena
    if c not in ("STATIONS_ID", "city", "MESS_DATUM", "eor") and not c.startswith("QN_")
]

_df_weather_phenomena = strip_sentinels(
    _bronze_weather_phenomena,
    [*_value_cols_weather_phenomena, _qn_col_weather_phenomena],
)

for _r in (
    _df_weather_phenomena.groupBy("STATIONS_ID")
    .agg(
        *[
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in _value_cols_weather_phenomena
        ]
    )
    .collect()
):
    _sid = str(_r["STATIONS_ID"]).strip()
    for _c in _value_cols_weather_phenomena:
        _observed_rows.append((_sid, _c, (_r[_c] or 0) > 0))

_df_weather_phenomena, _q_weather_phenomena = resolve_conflicts(
    _df_weather_phenomena,
    ["STATIONS_ID", "MESS_DATUM"],
    _value_cols_weather_phenomena,
    qn_col=_qn_col_weather_phenomena,
    bronze_table="dwd_weather_phenomena",
)
_q_weather_phenomena = _q_weather_phenomena.withColumn("source_system", F.lit(SOURCE))
write_quarantine(_q_weather_phenomena, RID)

_df_weather_phenomena = cast_logical(
    _df_weather_phenomena, TABLES["dwd_weather_phenomena"]["columns"]
)
_df_weather_phenomena = decode_qn(_df_weather_phenomena, _qn_col_weather_phenomena)

for _raw, _en_map in CODED.items():
    if _raw in _df_weather_phenomena.columns:
        _pref = NAME_MAP.get(_raw, _raw)
        _df_weather_phenomena = decode_via_labeled_map(
            _df_weather_phenomena, _raw, _pref, _en_map
        ).drop(_raw)

_df_weather_phenomena = apply_renames(_df_weather_phenomena, NAME_MAP)
_df_weather_phenomena = _df_weather_phenomena.withColumn(
    "_srid", sha_key(F.lit("dwd_weather_phenomena"), "STATIONS_ID", "MESS_DATUM")
)
_df_weather_phenomena = _df_weather_phenomena.withColumn(
    "observation_ts", parse_mess_datum("MESS_DATUM")
).drop("MESS_DATUM")
_df_weather_phenomena = attach_city_ags(_df_weather_phenomena, "city")
_df_weather_phenomena = add_provenance(_df_weather_phenomena, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_weather_phenomena
write_silver(
    _df_weather_phenomena,
    "dwd_weather_phenomena",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_weather_phenomena
_findings_blocks = inspect_table(
    _df_weather_phenomena,
    "dwd_weather_phenomena",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["STATIONS_ID", "observation_ts"],
    df_before=_bronze_weather_phenomena,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_weather_phenomena",
    "dwd_weather_phenomena",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Missingness reconciliation -- build OBSERVED (from the writes above)
# REPORTED = dwd_missing_value_periods; OBSERVED = captured during the writes
# above. Flags disagreement only -- never deletes or corrects either signal.
_observed = (
    spark.createDataFrame(
        _observed_rows,
        "station_id string, parameter_source_code string, has_observed_missing boolean",
    )
    .filter("has_observed_missing")
    .select("station_id", "parameter_source_code")
    .distinct()
    .withColumn("observed", F.lit(True))
)

# COMMAND ----------

# DBTITLE 1,Missingness reconciliation -- read REPORTED
_reported = (
    read_silver("dwd_missing_value_periods")
    .select("station_id", "parameter_source_code")
    .distinct()
    .withColumn("reported", F.lit(True))
)

# COMMAND ----------

# DBTITLE 1,Missingness reconciliation -- join
_recon = (
    _observed.join(_reported, ["station_id", "parameter_source_code"], "full_outer")
    .na.fill(False, subset=["observed", "reported"])
    .withColumn(
        "_missingness_reconciliation_status",
        F.when(F.col("observed") & F.col("reported"), F.lit("matched"))
        .when(F.col("observed") & ~F.col("reported"), F.lit("observed_only"))
        .otherwise(F.lit("reported_only")),
    )
    .select("station_id", "parameter_source_code", "_missingness_reconciliation_status")
)
_recon = _recon.withColumn("_srid", sha_key("station_id", "parameter_source_code"))
_recon = add_provenance(_recon, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- dwd_missingness_reconciliation
write_silver(
    _recon,
    "dwd_missingness_reconciliation",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Inspect -- dwd_missingness_reconciliation
_findings_blocks = inspect_table(
    _recon,
    "dwd_missingness_reconciliation",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["station_id", "parameter_source_code"],
    extra_checks={
        "observed_only_count": _recon.filter(
            F.col("_missingness_reconciliation_status") == "observed_only"
        ).count(),
        "reported_only_count": _recon.filter(
            F.col("_missingness_reconciliation_status") == "reported_only"
        ).count(),
    },
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__dwd_missingness_reconciliation",
    "dwd_missingness_reconciliation",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Summary
audit(
    COMPONENT,
    SOURCE,
    "measurement_tables_written",
    14.0,
    status="PASS",
    rid=RID,
)
