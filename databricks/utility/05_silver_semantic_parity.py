# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER SEMANTIC PARITY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** read-only comparison of each old source-scoped Silver table
# MAGIC with its contribution to the new semantic structures: key coverage,
# MAGIC values, and an inventory of old tables not yet covered. Output goes to
# MAGIC `src/schemas/silver_findings/parity.md`. Nothing is dropped or changed.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../silver/_silver_inspect

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../silver/_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
OLD_SCHEMA = "energy_silver"
FINDINGS = "parity"
COMPONENT = "utility/05_silver_semantic_parity"
FREQ_SECONDS = {"1min": 60, "15min": 900, "1h": 3600}


def _dwd_alt(col: str, table: str) -> str:
    return f"exists({col}, a -> a.source_dataset = '{table}')"


# Old DWD table -> [(family, SQL predicate: rows this product contributed)].
DWD_CONTRIB = {
    "dwd_air_temperature": [
        ("weather_temperature", "air_temperature_degc IS NOT NULL"),
        ("weather_humidity", "relative_humidity_percent IS NOT NULL"),
    ],
    "dwd_moisture": [
        (
            "weather_temperature",
            (
                f"{_dwd_alt('air_temperature_alt', 'dwd_moisture')} OR "
                f"{_dwd_alt('dew_point_temperature_alt', 'dwd_moisture')} OR "
                "wet_bulb_temperature_degc IS NOT NULL"
            ),
        ),
        (
            "weather_humidity",
            (
                f"{_dwd_alt('relative_humidity_alt', 'dwd_moisture')} OR "
                "absolute_humidity_g_per_m3 IS NOT NULL OR vapour_pressure_hpa IS NOT NULL"
            ),
        ),
        ("weather_pressure", _dwd_alt("pressure_station_alt", "dwd_moisture")),
    ],
    "dwd_dew_point": [
        (
            "weather_temperature",
            (
                "dew_point_temperature_degc IS NOT NULL OR "
                f"{_dwd_alt('air_temperature_alt', 'dwd_dew_point')}"
            ),
        )
    ],
    "dwd_pressure": [
        (
            "weather_pressure",
            "pressure_station_hpa IS NOT NULL OR pressure_sea_level_hpa IS NOT NULL",
        )
    ],
    "dwd_precipitation": [("weather_precipitation", "true")],
    "dwd_sun": [("weather_sunshine_duration", "source_dataset = 'dwd_sun'")],
    "dwd_wind": [("weather_wind", _dwd_alt("readings", "dwd_wind"))],
    "dwd_wind_synop": [("weather_wind", _dwd_alt("readings", "dwd_wind_synop"))],
    "dwd_extreme_wind": [("weather_wind", _dwd_alt("readings", "dwd_extreme_wind"))],
    "dwd_visibility": [("weather_visibility", "true")],
    "dwd_cloudiness": [("weather_cloud", "cloud_cover_total_native_unit IS NOT NULL")],
    "dwd_cloud_type": [
        ("weather_cloud", "size(layers) > 0 OR size(cloud_cover_total_alt) > 0")
    ],
    "dwd_weather_phenomena": [("weather_present_weather", "true")],
    "dwd_soil_temperature": [("weather_soil_temperature", "true")],
    "dwd_solar": [
        (f, "source_dataset = 'dwd_solar'")
        for f in (
            "weather_solar_radiation",
            "weather_longwave_radiation",
            "weather_solar_geometry",
            "weather_sunshine_duration",
        )
    ],
}

# New Honda field -> (old table, old column); keys (frequency, datetime_utc).
HONDA_P_COMPONENTS = {
    "electricity.total": ("honda_electricity_p", "total"),
    "electricity.PV": ("honda_electricity_p", "PV"),
    "electricity.CHP": ("honda_electricity_p", "CHP"),
    "cooling.cool_elec": ("honda_cooling_p", "cool_elec"),
}
HONDA_THERMAL = {
    "heating_total_w": ("honda_heating_p", "total"),
    "heating_chp_heat_w": ("honda_heating_p", "CHP_heat"),
    "cooling_total_w": ("honda_cooling_p", "total"),
}
HONDA_METER = {
    f"{ch}_kwh": (f"honda_{t}_w", native)
    for ch, (t, native) in HONDA_METER_CHANNELS.items()
}

# Old table -> the new structures that now hold its content.
COVERED_BY = {
    **{t: sorted({f for f, _ in c}) for t, c in DWD_CONTRIB.items()},
    "honda_weather": ["weather_temperature", "weather_solar_radiation"],
    "smard_energy_timeseries": [
        "electricity_balance",
        "electricity_price",
        "electricity_generation_forecast",
    ],
    **{
        t: ["electricity_balance", "thermal_energy"]
        for t, _ in [*HONDA_P_COMPONENTS.values(), *HONDA_THERMAL.values()]
    },
    **{t: ["energy_meter_reading"] for t, _ in HONDA_METER.values()},
    "redispatch_measures": ["grid_intervention_event"],
}


def _k(*cols):
    """Composite key column from column names or Columns."""
    return F.concat_ws(
        "|", *[(F.col(c) if isinstance(c, str) else c).cast("string") for c in cols]
    )


def _record_key(df):
    return F.col("source_record_id")


def _bridge_key(df):
    return _k("parent_id", "linked_id")


def _old_code_key(df):
    return strip_float_suffix("Id")


def _new_code_key(df):
    return F.col("code_id")


_GA4_KEY = ["event_date", "event_timestamp", "user_pseudo_id", "event_name"]

# (old table, old key, new structure, new row filter or None, new key); a key
# is a function of the frame. Keys, not values: these structures reuse the
# old transformation helpers unchanged.
KEY_PARITY = [
    *[
        (
            f"mastr_einheiten_{u}",
            lambda df: F.col("unit_id"),
            "generation_unit",
            f"unit_type = '{u}'",
            lambda df: F.col("unit_id"),
        )
        for u in (
            "wind",
            "biomasse",
            "wasser",
            "verbrennung",
            "kernkraft",
            "geothermie_gsgk",
        )
    ],
    *[
        (
            t,
            _record_key,
            "support_registration",
            f"source_dataset = '{t}'",
            lambda df: F.col("support_registration_id"),
        )
        for t in (
            "mastr_anlagen_eeg_wind",
            "mastr_anlagen_eeg_biomasse",
            "mastr_anlagen_eeg_wasser",
            "mastr_anlagen_eeg_geothermie_gsgk",
            "mastr_anlagen_kwk",
        )
    ],
    *[
        (old, _record_key, new, None, _record_key)
        for old, new in (
            ("mastr_einheiten_genehmigung", "unit_authorisation"),
            ("mastr_ertuechtigungen", "unit_repowering"),
            ("mastr_marktakteure", "market_actor"),
            ("mastr_marktakteure_und_rollen", "market_actor_role"),
            ("mastr_lokationen", "grid_location"),
            ("mastr_netzanschlusspunkte", "grid_connection_point"),
            ("mastr_netze", "grid_network"),
            ("mastr_bilanzierungsgebiete", "balancing_area"),
            ("mastr_unit_deletion_events", "unit_deletion_event"),
            ("mastr_actor_deletion_events", "actor_deletion_event"),
            ("mastr_grid_operator_change_events", "grid_operator_change_event"),
            ("power_plant_list", "power_plant_register"),
            ("power_plant_capacity_additions", "power_plant_capacity_plan"),
        )
    ],
    (
        "mastr_location_coordinate_conflict",
        lambda df: F.col("location_id"),
        "grid_location_coordinate_conflict",
        None,
        lambda df: F.col("location_id"),
    ),
    *[
        (
            f"mastr_{rel}_bridge",
            _bridge_key,
            "register_link",
            f"relationship_type = '{rel}'",
            _bridge_key,
        )
        for rel in (
            "eeg_support_unit",
            "kwk_support_unit",
            "authorisation_unit",
            "repowering_eeg",
            "location_unit",
            "location_connection",
            "actor_role",
        )
    ],
    *[
        (
            f"mastr_{kind}",
            _old_code_key,
            "mastr_code_list",
            f"catalog_kind = '{kind}'",
            _new_code_key,
        )
        for kind in (
            "katalogkategorien",
            "katalogwerte",
            "einheitentypen",
            "lokationstypen",
            "marktfunktionen",
            "marktrollen",
        )
    ],
    (
        "dwd_station_geography",
        lambda df: _k("station_id", F.to_date("valid_from"), "_src_id_ord"),
        "weather_location_validity",
        None,
        lambda df: _k("source_location_id", "valid_from", "record_ordinal"),
    ),
    (
        "dwd_station_name_history",
        lambda df: _k("station_id", F.to_date("valid_from"), "_src_id_ord"),
        "weather_station_name_history",
        None,
        lambda df: _k("source_location_id", "valid_from", "record_ordinal"),
    ),
    (
        "dwd_device_instrument",
        lambda df: _k(
            "station_id", "parameter_category", F.to_date("valid_from"), "_src_id_ord"
        ),
        "weather_station_instrument",
        None,
        lambda df: _k(
            "source_location_id", "parameter_category", "valid_from", "record_ordinal"
        ),
    ),
    (
        "dwd_parameter_unit",
        lambda df: _k(
            "station_id",
            "parameter_source_code",
            F.to_date("valid_from"),
            "_src_id_ord",
        ),
        "weather_parameter_period",
        None,
        lambda df: _k(
            "source_location_id",
            "parameter_source_code",
            "valid_from",
            "record_ordinal",
        ),
    ),
    (
        "dwd_parameter_catalog",
        lambda df: F.col("parameter_source_code"),
        "weather_parameter_catalog",
        None,
        lambda df: F.col("parameter_source_code"),
    ),
    (
        "dwd_missing_value_periods",
        lambda df: _k(
            "station_id", "parameter_source_code", "gap_start_ts", "gap_end_ts"
        ),
        "weather_missing_value_period",
        None,
        lambda df: _k(
            "source_location_id",
            "parameter_source_code",
            "gap_start_ts",
            "gap_end_ts",
        ),
    ),
    (
        "dwd_missingness_reconciliation",
        lambda df: _k("station_id", "parameter_source_code"),
        "weather_missingness_reconciliation",
        None,
        lambda df: _k("source_location_id", "parameter_source_code"),
    ),
    ("rees46_events", _record_key, "rees46_event", None, lambda df: F.col("event_key")),
    ("ga4_events", _record_key, "ga4_event", None, lambda df: F.col("event_key")),
    (
        "ga4_items",
        lambda df: _k(sha_key(*_GA4_KEY), "item_ordinal"),
        "ga4_event_item",
        None,
        lambda df: _k("event_key", "item_ordinal"),
    ),
    (
        "ga4_transactions",
        _record_key,
        "ga4_event",
        "transaction_id IS NOT NULL",
        lambda df: F.col("event_key"),
    ),
]
COVERED_BY.update(
    {old: [new] for old, _, new, _, _ in KEY_PARITY}
    | {
        "dwd_city_bundesland_xref": ["weather_location"],
        "honda_channel_catalog": ["labels in the Honda energy structures"],
    }
)

# COMMAND ----------

# DBTITLE 1,Helper -- read a table if present


def table_or_none(schema_table: str):
    full = f"{CATALOG}.{schema_table}"
    return spark.table(full) if spark.catalog.tableExists(full) else None


# COMMAND ----------

# DBTITLE 1,Helper -- compare two long (key..., value) frames


def compare(name: str, old, new, keys: list) -> None:
    """Full outer join on `keys`. PASS when no value differs; one-sided
    counts are reported for review (sentinels, quarantine, NULL values)."""
    j = old.withColumnRenamed("value", "old_v").join(
        new.withColumnRenamed("value", "new_v"), keys, "full_outer"
    )
    r = j.agg(
        F.count("*").alias("keys"),
        F.sum(F.col("old_v").isNull().cast("int")).alias("only_new_or_null_old"),
        F.sum(F.col("new_v").isNull().cast("int")).alias("only_old_or_null_new"),
        F.sum(
            (
                F.col("old_v").isNotNull()
                & F.col("new_v").isNotNull()
                & (F.abs(F.col("old_v") - F.col("new_v")) > 1e-9)
            ).cast("int")
        ).alias("value_mismatch"),
    ).first()
    report(name, r["value_mismatch"] == 0, str(r.asDict()))


# COMMAND ----------

# DBTITLE 1,Parity -- DWD key coverage per product contribution
for _t, _contrib in DWD_CONTRIB.items():
    _old = table_or_none(f"{OLD_SCHEMA}.{_t}")
    if _old is None:
        report(f"{_t} key coverage", True, "old table absent", status="SKIP")
        continue
    _new = None
    for _fam, _pred in _contrib:
        _part = (
            spark.table(semantic_table(_fam))
            .filter((F.col("source_system") == "dwd") & F.expr(_pred))
            .select("source_location_id", "observation_ts_utc")
        )
        _new = _part if _new is None else _new.unionByName(_part)
    _new = _new.distinct()
    _base = _old.select(
        F.col("STATIONS_ID").alias("source_location_id"),
        F.col("observation_ts").alias("observation_ts_utc"),
    ).distinct()
    _k = ["source_location_id", "observation_ts_utc"]
    _only_new = _new.join(_base, _k, "left_anti").count()
    _only_old = _base.join(_new, _k, "left_anti").count()
    report(
        f"{_t} key coverage",
        _only_new == 0,
        f"only_new={_only_new} only_old={_only_old} (all-null or quarantined)",
    )

# COMMAND ----------

# DBTITLE 1,Parity -- DWD primary air temperature values
_old = table_or_none(f"{OLD_SCHEMA}.dwd_air_temperature")
if _old is not None:
    compare(
        "dwd_air_temperature values",
        _old.select(
            F.col("STATIONS_ID").alias("sid"),
            F.col("observation_ts").alias("ts"),
            F.col("air_temperature_2m").alias("value"),
        ).dropna(subset=["value"]),
        spark.table(semantic_table("weather_temperature"))
        .filter(F.col("source_system") == "dwd")
        .select(
            F.col("source_location_id").alias("sid"),
            F.col("observation_ts_utc").alias("ts"),
            F.col("air_temperature_degc").alias("value"),
        )
        .dropna(subset=["value"]),
        ["sid", "ts"],
    )

# COMMAND ----------

# DBTITLE 1,Parity -- Honda weather air temperature values
_old = table_or_none(f"{OLD_SCHEMA}.honda_weather")
if _old is not None:
    _freq = lit_map({v: k for k, v in FREQ_SECONDS.items()})
    compare(
        "honda_weather air temperature values",
        _old.select(
            "frequency", "datetime_utc", F.col("air_temperature_2m").alias("value")
        ),
        spark.table(semantic_table("weather_temperature"))
        .filter(F.col("source_dataset") == "honda_iot_weather")
        .select(
            _freq[F.col("interval_seconds")].alias("frequency"),
            F.col("observation_ts_utc").alias("datetime_utc"),
            F.col("air_temperature_degc").alias("value"),
        ),
        ["frequency", "datetime_utc"],
    )

# COMMAND ----------

# DBTITLE 1,Helper -- new Honda energy values in long form (old table, column)


def honda_new_long():
    freq = lit_map({v: k for k, v in FREQ_SECONDS.items()})
    base = [
        freq[F.col("interval_seconds")].alias("frequency"),
        F.col("observation_ts_utc").alias("datetime_utc"),
    ]
    comp = lit_map({k: f"{t}.{c}" for k, (t, c) in HONDA_P_COMPONENTS.items()})
    parts = [
        spark.table(semantic_table("electricity_balance"))
        .filter(F.col("source_system") == "honda_iot")
        .select(*base, F.explode("components").alias("x"))
        .select(
            "frequency",
            "datetime_utc",
            comp[F.col("x.native_label")].alias("old_field"),
            F.col("x.power_w").alias("value"),
        )
    ]
    flat = {"thermal_energy": HONDA_THERMAL, "energy_meter_reading": HONDA_METER}
    for fam, fields in flat.items():
        df = spark.table(semantic_table(fam)).filter(
            F.col("source_system") == "honda_iot"
        )
        for new_col, (t, c) in fields.items():
            parts.append(
                df.select(
                    *base,
                    F.lit(f"{t}.{c}").alias("old_field"),
                    F.col(new_col).alias("value"),
                )
            )
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.dropna(subset=["value"])


# COMMAND ----------

# DBTITLE 1,Parity -- Honda energy values per old table and column
_fields = {**HONDA_P_COMPONENTS, **HONDA_THERMAL, **HONDA_METER}.values()
_old_parts = []
for _t, _c in _fields:
    _o = table_or_none(f"{OLD_SCHEMA}.{_t}")
    if _o is not None:
        _old_parts.append(
            _o.select(
                "frequency",
                F.col("datetime_utc").cast("timestamp").alias("datetime_utc"),
                F.lit(f"{_t}.{_c}").alias("old_field"),
                F.col(_c).cast("double").alias("value"),
            )
        )
if _old_parts:
    _old_long = _old_parts[0]
    for _p in _old_parts[1:]:
        _old_long = _old_long.unionByName(_p)
    compare(
        f"honda energy values ({', '.join(sorted({t for t, _ in _fields}))})",
        _old_long.dropna(subset=["value"]),
        honda_new_long(),
        ["frequency", "datetime_utc", "old_field"],
    )
else:
    report("honda energy values", True, "old tables absent", status="SKIP")

# COMMAND ----------

# DBTITLE 1,Parity -- SMARD values per (metric, region, resolution, timestamp)
_old = table_or_none(f"{OLD_SCHEMA}.smard_energy_timeseries")
if _old is not None:
    _is_qh = F.col("interval_reference") == "clock"
    _res = F.when(_is_qh, "quarterhour").otherwise("day")
    _k = [
        F.col("source_location_id").alias("region"),
        _res.alias("resolution"),
        F.col("observation_ts_utc").alias("ts"),
    ]
    _x = [
        "region",
        "resolution",
        "ts",
        F.col("x.native_label").alias("metric"),
        F.col("x.energy_mwh").alias("value"),
    ]
    _new = (
        spark.table(semantic_table("electricity_balance"))
        .filter(F.col("source_system") == "smard")
        .select(*_k, F.explode("components").alias("x"))
        .select(*_x)
        .unionByName(
            spark.table(semantic_table("electricity_generation_forecast"))
            .select(*_k, F.explode("components").alias("x"))
            .select(*_x)
        )
        .unionByName(
            spark.table(semantic_table("electricity_price")).select(
                *_k,
                F.lit("day_ahead_prices").alias("metric"),
                F.col("price_eur_per_mwh").alias("value"),
            )
        )
    )
    compare(
        "smard_energy_timeseries values",
        _old.select(
            F.col("market_zone").alias("region"),
            F.col("aggregation_resolution").alias("resolution"),
            F.col("observation_ts").alias("ts"),
            "metric",
            "value",
        ).dropna(subset=["value"]),
        _new,
        ["region", "resolution", "ts", "metric"],
    )

# COMMAND ----------

# DBTITLE 1,Parity -- redispatch row count and order-independent content hash
_old = table_or_none(f"{OLD_SCHEMA}.redispatch_measures")
if _old is not None:
    _old_rows = _old.select(
        F.concat_ws(" ", "measure_start_date", "measure_start_time").alias("s"),
        F.concat_ws(" ", "measure_end_date", "measure_end_time").alias("e"),
        F.trim("affected_unit_name").alias("a"),
        F.col("mean_power_mw").alias("m"),
        F.col("max_power_mw").alias("x"),
        F.col("total_energy_mwh").alias("w"),
    )
    _new_rows = spark.table(semantic_table("grid_intervention_event")).select(
        F.col("event_start_native").alias("s"),
        F.col("event_end_native").alias("e"),
        F.col("affected_asset_text").alias("a"),
        F.col("mean_power_mw").alias("m"),
        F.col("max_power_mw").alias("x"),
        F.col("energy_mwh").alias("w"),
    )

    def _fp(df):
        h = F.xxhash64(*df.columns).cast("decimal(38,0)")
        return df.agg(F.count("*").alias("rows"), F.sum(h).alias("hash_sum")).first()

    _o, _n = _fp(_old_rows), _fp(_new_rows)
    report(
        "redispatch_measures rows and content",
        (_o["rows"], _o["hash_sum"]) == (_n["rows"], _n["hash_sum"]),
        f"old {_o.asDict()} new {_n.asDict()}",
    )

# COMMAND ----------

# DBTITLE 1,Parity -- register, reference and commerce keys and row counts
for _old_name, _old_key, _new_name, _filter, _new_key in KEY_PARITY:
    _old = table_or_none(f"{target_schema_for(_old_name)}.{_old_name}")
    if _old is None:
        report(f"{_old_name} keys", True, "old table absent", status="SKIP")
        continue
    _new = spark.table(semantic_table(_new_name))
    if _filter:
        _new = _new.filter(_filter)
    _o = _old.select(_old_key(_old).alias("k"))
    _n = _new.select(_new_key(_new).alias("k"))
    _only_old = _o.join(_n, "k", "left_anti").count()
    _only_new = _n.join(_o, "k", "left_anti").count()
    report(
        f"{_old_name} -> {_new_name} keys",
        _only_old == 0 and _only_new == 0,
        (
            f"rows old={_o.count()} new={_n.count()} only_old={_only_old} "
            f"only_new={_only_new}"
        ),
    )

# COMMAND ----------

# DBTITLE 1,Inventory -- old Silver tables and what covers them now
_new_tables = {*SEMANTIC_STRUCTURES, *SEMANTIC_MEMBER_STRUCTURES}
_rows = []
for _schema in ("energy_silver", "energy_silver_reference", "commerce_silver"):
    if not spark.catalog.databaseExists(f"{CATALOG}.{_schema}"):
        continue
    for _tbl in spark.catalog.listTables(f"{CATALOG}.{_schema}"):
        if _tbl.name in _new_tables:
            continue
        _cov = ", ".join(COVERED_BY.get(_tbl.name, [])) or "not yet covered"
        _rows.append(f"| {_schema}.{_tbl.name} | {_cov} |")
FINDINGS_BLOCKS.append(
    (
        "old Silver tables -> new structures",
        "\n".join(["| old table | covered by |", "|---|---|", *sorted(_rows)]),
    )
)

# COMMAND ----------

# DBTITLE 1,Export findings -- parity
write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}",
    "silver semantic parity",
    checks_blocks(),
)
