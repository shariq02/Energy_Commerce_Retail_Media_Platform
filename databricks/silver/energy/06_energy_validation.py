# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- ENERGY VALIDATION (ENERGY)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the checks of the energy Silver tables in one notebook: weather
# MAGIC (keys, grain, places, time, value ranges, daily rows), energy (grain, places, the
# MAGIC balance identities, Honda power against meter increments) and the MaStR
# MAGIC registers (unit keys, link coverage). The result table goes to the energy
# MAGIC findings file.

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "silver/energy/06_energy_validation"
FINDINGS = "energy"
# Structure -> the column register_link ids refer to.
ID_COLUMN = {
    "generation_unit": "unit_id",
    "support_registration": "support_registration_id",
    "unit_authorisation": "source_record_id",
    "unit_repowering": "source_record_id",
    "grid_location": "source_record_id",
    "grid_connection_point": "source_record_id",
    "market_actor": "source_record_id",
    "reference_code_list": "code_id",
}

# COMMAND ----------

# DBTITLE 1,Read Silver -- the structures this notebook checks
weather = spark.table(semantic_table("weather_observation"))
daily = spark.table(semantic_table("weather_daily"))
locations = spark.table(semantic_table("weather_location")).select("location_key")
energy = spark.table(semantic_table("energy_observation"))
components = spark.table(semantic_table("energy_component"))
devices = spark.table(semantic_table("device_telemetry_snapshot"))
T = {name: spark.table(semantic_table(name)) for name in ID_COLUMN}
links = spark.table(semantic_table("register_link"))
plants = spark.table(semantic_table("power_plant_register"))

# COMMAND ----------

# DBTITLE 1,Check -- weather: rows per source, time basis and interval
keep(
    "weather_observation rows per source",
    weather.groupBy("source_system", "measurement_basis", "interval_reference")
    .agg(
        F.count("*").alias("rows"),
        F.min("observation_timestamp_utc").alias("first_timestamp"),
        F.max("observation_timestamp_utc").alias("last_timestamp"),
    )
    .orderBy("source_system", "interval_reference"),
)
keep(
    "weather_observation rows by time_basis",
    weather.groupBy("source_system", "time_basis")
    .agg(F.count("*").alias("rows"))
    .orderBy("source_system", "time_basis"),
)

# COMMAND ----------

# DBTITLE 1,Check -- weather: keys, grain and places
_dup_key = weather.groupBy("observation_key").count().filter("count > 1").count()
report(
    "weather_observation observation_key unique",
    _dup_key == 0,
    f"duplicates: {_dup_key}",
)
_dup_grain = weather.groupBy(*WEATHER_GRAIN).count().filter("count > 1").count()
report(
    "weather_observation one row per grain key",
    _dup_grain == 0,
    f"groups with >1 row: {_dup_grain}",
)
_orphans = (
    weather.select("location_key")
    .distinct()
    .join(locations, "location_key", "left_anti")
    .count()
)
report(
    "observation locations exist in weather_location",
    _orphans == 0,
    f"orphans: {_orphans}",
)

# COMMAND ----------

# DBTITLE 1,Check -- weather: project time offset from UTC (DWD, Europe/Berlin)
keep(
    "project time offset from UTC by time_basis (DWD, hours)",
    weather.filter(
        F.col("observation_timestamp_utc").isNotNull()
        & (F.col("source_system") == "dwd")
    )
    .withColumn(
        "offset_h",
        (
            F.col("observation_timestamp_project").cast("long")
            - F.col("observation_timestamp_utc").cast("long")
        )
        / 3600,
    )
    .groupBy("time_basis", "offset_h")
    .count(),
)

# COMMAND ----------

# DBTITLE 1,Check -- weather: value ranges by measurement column and source
_numeric = [
    name
    for name, dtype in weather.dtypes
    if dtype == "double" and name != "utc_offset_hours"
]
_stack = ", ".join(f"'{c}', `{c}`" for c in _numeric)
keep(
    "weather_observation value ranges",
    weather.select(
        "source_system",
        F.expr(f"stack({len(_numeric)}, {_stack}) as (field, value)"),
    )
    .filter(F.col("value").isNotNull())
    .groupBy("field", "source_system")
    .agg(
        F.count("*").alias("rows"),
        F.min("value").alias("min"),
        F.percentile_approx("value", 0.5).alias("median"),
        F.max("value").alias("max"),
    )
    .orderBy("field", "source_system"),
)

# COMMAND ----------

# DBTITLE 1,Check -- weather: DWD air temperature against its alternate readings
_dwd = weather.filter(F.col("source_system") == "dwd")
keep(
    "DWD air temperature against the moisture and dew point product readings",
    _dwd.filter(F.col("air_temperature_degc").isNotNull()).agg(
        F.count("air_temperature_moisture_degc").alias("moisture_pairs"),
        F.avg(
            (
                F.abs(
                    F.col("air_temperature_degc")
                    - F.col("air_temperature_moisture_degc")
                )
                < 1e-9
            ).cast("int")
        ).alias("moisture_share_equal"),
        F.count("air_temperature_dew_point_degc").alias("dew_point_pairs"),
        F.avg(
            (
                F.abs(
                    F.col("air_temperature_degc")
                    - F.col("air_temperature_dew_point_degc")
                )
                < 1e-9
            ).cast("int")
        ).alias("dew_point_share_equal"),
    ),
)

# COMMAND ----------

# DBTITLE 1,Check -- weather daily rows and derived-day sanity
keep(
    "weather_daily rows",
    daily.groupBy("source_system", "source_dataset", "statistic", "value_origin")
    .agg(F.count("*").alias("rows"), F.countDistinct("local_date").alias("days"))
    .orderBy("source_system", "statistic"),
)
_d = (
    daily.filter(
        (F.col("value_origin") == "derived") & (F.col("variable") == "air_temperature")
    )
    .groupBy("location_key", "local_date")
    .pivot("statistic", ["min", "mean", "max"])
    .agg(F.first("value"))
)
_bad = _d.filter(
    ~((F.col("min") <= F.col("mean")) & (F.col("mean") <= F.col("max")))
).count()
report("derived daily min <= mean <= max", _bad == 0, f"violations: {_bad}")
_s = (
    daily.filter(F.col("source_system") == "weather_seattle")
    .groupBy("local_date")
    .pivot("statistic", ["min", "max"])
    .agg(F.first("value"))
)
_bad_s = _s.filter(~(F.col("max") > F.col("min"))).count()
report("seattle max above min on every date", _bad_s == 0, f"violations: {_bad_s}")

# COMMAND ----------

# DBTITLE 1,Check -- energy: keys, grain and places
_codes = set(MARKET_AREA_CODES.values())
for name, df in (("energy_observation", energy), ("energy_component", components)):
    _dup_key = df.groupBy("source_record_id").count().filter("count > 1").count()
    report(f"{name} source_record_id unique", _dup_key == 0, f"duplicates: {_dup_key}")
    _bad_code = df.filter(
        F.col("market_area_code").isNotNull() & ~F.col("market_area_code").isin(_codes)
    ).count()
    _no_place = df.filter(
        F.col("market_area_code").isNull() & F.col("location_key").isNull()
    ).count()
    _orphan = (
        df.select("location_key")
        .dropna()
        .distinct()
        .join(locations, "location_key", "left_anti")
        .count()
    )
    report(
        f"{name} places resolve",
        _bad_code == 0 and _no_place == 0 and _orphan == 0,
        f"unknown codes {_bad_code}, no place {_no_place}, orphans {_orphan}",
    )
_dup_grain = energy.groupBy(*ENERGY_GRAIN).count().filter("count > 1").count()
report(
    "energy_observation one row per grain key",
    _dup_grain == 0,
    f"groups >1: {_dup_grain}",
)

# COMMAND ----------

# DBTITLE 1,Check -- SMARD residual load, quarter-hour sum and the forecast mirror


def smard_components(metrics):
    """SMARD balance components as one column per metric."""
    return (
        components.filter(
            (F.col("source_system") == "smard")
            & (F.col("component_group") == "balance")
        )
        .groupBy(
            "observation_key", "market_area_code", "interval_reference", "local_date"
        )
        .pivot("native_label", metrics)
        .agg(F.first("energy_mwh"))
    )


_r = smard_components(
    [
        "residual_load",
        "total_power_consumption",
        "generation_onshore_wind",
        "generation_offshore_wind",
        "generation_photovoltaic",
    ]
).dropna()
_implied = (
    F.col("total_power_consumption")
    - F.col("generation_onshore_wind")
    - F.col("generation_offshore_wind")
    - F.col("generation_photovoltaic")
)
_bad = _r.filter(
    F.abs(F.col("residual_load") - _implied)
    > F.greatest(F.lit(0.05), 0.02 * F.abs(F.col("residual_load")))
).withColumn("implied_residual", _implied)
_viol = _bad.count()
report(
    "smard residual-load identity", _viol == 0, f"violations {_viol} of {_r.count()}"
)
if _viol:
    keep("smard residual-load violations", _bad.limit(20))

_c = smard_components(["total_power_consumption"]).filter(
    F.col("market_area_code") == "de_lu"
)
_qh = (
    _c.filter(F.col("interval_reference") == "clock")
    .groupBy("local_date")
    .agg(F.sum("total_power_consumption").alias("qh_sum"), F.count("*").alias("n"))
)
_day = _c.filter(F.col("interval_reference") == "local_day_europe_berlin").select(
    "local_date", F.col("total_power_consumption").alias("day_value")
)
_ratio = (
    _qh.filter(F.col("n") >= 92)
    .join(_day, "local_date")
    .select((F.col("day_value") / F.col("qh_sum")).alias("ratio"))
    .agg(F.percentile_approx("ratio", 0.5).alias("median"), F.count("*").alias("days"))
    .first()
)
_median = _ratio["median"]
report(
    "smard quarter-hour unit is MWh per interval",
    _median is not None and abs(_median - 1) < 0.02,
    f"median day/sum(qh) = {_median} over {_ratio['days']} days (MW would give ~4)",
)

_f = (
    components.filter(
        (F.col("source_system") == "smard") & (F.col("component_group") == "forecast")
    )
    .groupBy("observation_key")
    .pivot("forecast_scope", ["photovoltaic", "wind_and_photovoltaic"])
    .agg(F.first("energy_mwh"))
    .dropna()
)
_share = _f.agg(
    F.avg(
        (F.abs(F.col("photovoltaic") + F.col("wind_and_photovoltaic")) < 1e-6).cast(
            "int"
        )
    )
).first()[0]
report(
    "smard forecast pv mirror still holds",
    True,
    f"share mirrored {_share}",
    status="INFO",
)

# COMMAND ----------

# DBTITLE 1,Check -- Honda power against the forward meter increment, registers across frequencies
_honda = energy.filter(F.col("source_system") == "honda_iot")
_hour = _honda.filter(F.col("interval_seconds") == 3600)
_p_bal = (
    components.filter(
        (F.col("source_system") == "honda_iot") & (F.col("interval_seconds") == 3600)
    )
    .groupBy("observation_key")
    .pivot("native_label")
    .agg(F.first("power_w"))
)
_p = _hour.join(_p_bal, "observation_key")
_pairs = {
    "electricity.PV": "electricity_solar_photovoltaic",
    "electricity.CHP": "electricity_combined_heat_and_power",
    "electricity.total": "electricity_total",
    "cooling.cool_elec": "cooling_electricity",
    "heating_total_w": "heating_total",
    "heating_combined_heat_and_power_heat_w": "heating_combined_heat_and_power_heat",
    "cooling_total_w": "cooling_total",
}
keep(
    "Honda P vs forward W increment x1000, correlation at 1 h",
    _p.select(
        *[
            F.corr(F.col(f"`{p}`"), F.col(f"{w}_increment_kwh") * 1000).alias(p)
            for p, w in _pairs.items()
        ]
    ),
)
_cols = [f"{ch}_kwh" for ch in HONDA_METER_CHANNELS]
_one = _honda.filter(F.col("interval_seconds") == 60).select(
    "observation_timestamp_utc", *[F.col(c).alias(f"m1_{c}") for c in _cols]
)
_hourly = _hour.select("observation_timestamp_utc", *_cols)
keep(
    "Honda W: share equal between 1 min and 1 h rows at the same instant",
    _one.join(_hourly, "observation_timestamp_utc").select(
        *[F.avg((F.col(c) == F.col(f"m1_{c}")).cast("int")).alias(c) for c in _cols]
    ),
)
keep(
    "iot lcd label by co2 range (derived by the source, ranges must not overlap)",
    devices.groupBy("lcd_label")
    .agg(F.min("co2_level").alias("min_co2"), F.max("co2_level").alias("max_co2"))
    .orderBy("min_co2"),
)

# COMMAND ----------

# DBTITLE 1,Check -- MaStR units and registrations
_dup_units = T["generation_unit"].groupBy("unit_id").count().filter("count > 1").count()
report("generation_unit unit_id unique", _dup_units == 0, f"duplicates {_dup_units}")
_shared_ids = (
    T["support_registration"]
    .groupBy("support_registration_id")
    .agg(F.countDistinct("support_scheme").alias("schemes"))
    .filter("schemes > 1")
    .count()
)
report("support id spaces disjoint", _shared_ids == 0, f"shared ids {_shared_ids}")

# COMMAND ----------


# DBTITLE 1,Check -- register link coverage and the plant register
def ids_of(name):
    df = T[name]
    if name == "reference_code_list":
        df = df.filter(
            (F.col("source_system") == "mastr")
            & (F.col("catalog_kind") == "marktrollen")
        )
    return df.select(F.col(ID_COLUMN[name]).cast("string").alias("id")).distinct()


_rows = []
for rel, parent_type, linked_type in (
    links.select("relationship_type", "parent_type", "linked_type").distinct().collect()
):
    _l = links.filter(F.col("relationship_type") == rel)
    _n = _l.count()
    _parent = _l.join(ids_of(parent_type), _l.parent_id == F.col("id"), "left_semi")
    _linked = _l.join(ids_of(linked_type), _l.linked_id == F.col("id"), "left_semi")
    _rows.append((rel, _n, _parent.count() / max(_n, 1), _linked.count() / max(_n, 1)))
keep(
    "register_link: share of parent and linked ids found in their structures",
    spark.createDataFrame(
        _rows,
        "relationship_type string, links long, parent_found double, linked_found double",
    ),
)
_with_id = plants.filter(F.col("mastr_unit_id").isNotNull())
_found = _with_id.join(
    ids_of("generation_unit"), _with_id.mastr_unit_id == F.col("id"), "left_semi"
).count()
report(
    "power_plant_register -> generation_unit",
    True,
    f"{_found} of {_with_id.count()} plant rows with a MaStR unit id match a unit",
    status="INFO",
)

# COMMAND ----------

# DBTITLE 1,Export findings -- energy validation
write_silver_findings(
    FINDINGS,
    f"{COMPONENT.split('/')[-1]}__validation",
    "energy validation",
    checks_blocks(),
)
