# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ANALYTICS ENERGY -- ASSET, SITE AND WEATHER MARTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** 9 `energy_analytics` tables: installed capacity, unit lifecycle
# MAGIC events, monthly utilisation, the yearly capacity margin, operator concentration,
# MAGIC the support scheme build-out, daily site energy with the site weather, national
# MAGIC weather against generation, and the station days beyond the percentile of their
# MAGIC station and month. Runs after `01_market_marts`.

# COMMAND ----------

# DBTITLE 1,Gold library
# MAGIC %run ../../gold/_gold_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "analytics/energy/02_asset_site_weather_marts"
RID = analytics_run_id()
RULES = load_yaml("src/schemas/mappings/analytics_rules.yml")
SCHEMA = ANALYTICS_SCHEMAS["energy"]
GATE = {"source": SOURCE, "component": COMPONENT, "rid": RID}
COMMON = {**GATE, "schema": SCHEMA}
HOME = RULES["home_market_area"]
WEATHER_SOURCE = RULES["station_weather_source"]
SITE_SOURCE = "honda_iot"
STATION_DAY = ["location_key", "local_date"]
# unit lifecycle dates -> event kind of fleet_lifecycle_event
UNIT_DATES = {
    "commissioning_date": "commissioning",
    "provisional_shutdown_start_date": "provisional_shutdown",
    "final_decommissioning_date": "final_decommissioning",
}
UNIT_ATTRIBUTES = [
    "unit_id",
    "unit_type",
    "carrier_key",
    "generation_technology",
    "federal_state",
    "operator_id",
    "capacity_net_kw",
]
STATION_MEASURES = (
    "mean_air_temperature_degc",
    "min_air_temperature_degc",
    "max_air_temperature_degc",
    "mean_wind_speed_m_per_s",
    "max_wind_gust_m_per_s",
    "precipitation_mm",
    "mean_global_radiation_w_per_m2",
    "sunshine_duration_minutes",
)
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Function -- the units in operation at the end of a period


def operating_at(units, end):
    """Units commissioned on or before `end` and not finally decommissioned by it."""
    commissioned = F.to_date("commissioning_date") <= end
    retired = F.to_date("final_decommissioning_date") <= end
    return units.filter(commissioned & (retired.isNull() | ~retired))


# COMMAND ----------

# DBTITLE 1,Read Gold
gold = {
    table: read_gold(table, source=SOURCE)
    for table in (
        "generation_unit",
        "unit_event",
        "support_registration",
        "capacity_plan",
        "grid_connection_point",
        "market_actor",
        "market_energy_component",
        "channel_reading",
        "weather_air_state",
        "weather_wind_precipitation",
        "weather_ground_sun",
        "weather_daily",
        "grid_intervention_event",
    )
}
units = gold["generation_unit"]
generation_daily = read_gold(
    "generation_by_carrier_daily", source=SOURCE, schema=SCHEMA
)
dim_place = read_shared_conformed("dim_place").select("location_key", "name", "region")
operator_names = (
    gold["market_actor"]
    .groupBy(F.col("market_actor_id").alias("operator_id"))
    .agg(F.min("company_name").alias("operator_name"))
)
consumption = gold["market_energy_component"].filter(
    (F.col("component_group") == "balance")
    & (F.col("component_kind") == "consumption")
    & (F.col("market_area_code") == HOME)
)

# COMMAND ----------

# DBTITLE 1,Transform -- capacity_registry (units and capacity by register attributes)
point_attributes = (
    gold["grid_connection_point"]
    .groupBy("location_id")
    .agg(
        sorted_values("voltage_level").alias("voltage_levels"),
        sorted_values("control_zone").alias("control_zones"),
    )
)
registry_grain = [
    "carrier_key",
    "generation_technology",
    "federal_state",
    "voltage_levels",
    "control_zones",
    "operator_id",
    "lifecycle_state",
    "commissioning_year",
]
TABLES["capacity_registry"] = (
    add_provenance(
        units.withColumn("commissioning_year", F.year("commissioning_date"))
        .join(point_attributes, "location_id", "left")
        .groupBy(*registry_grain)
        .agg(
            F.count(F.lit(1)).alias("unit_count"),
            (F.sum("capacity_net_kw") / 1000.0).alias("capacity_net_mw"),
            (F.sum("capacity_gross_kw") / 1000.0).alias("capacity_gross_mw"),
        )
        .join(operator_names, "operator_id", "left"),
        SOURCE,
        RID,
    ),
    registry_grain,
)

# COMMAND ----------

# DBTITLE 1,Transform -- fleet_lifecycle_event (register events and unit dates per unit)
by_support = units.select(
    F.col("renewable_energy_act_support_id").alias("_support_id"),
    F.col("unit_id").alias("_support_unit"),
).filter(F.col("_support_id").isNotNull())
by_authorisation = units.select(
    F.col("authorisation_id").alias("_authorisation_id"),
    F.col("unit_id").alias("_authorisation_unit"),
).filter(F.col("_authorisation_id").isNotNull())
lifecycle = (
    gold["unit_event"]
    .join(
        by_support,
        F.col("renewable_energy_act_support_id") == F.col("_support_id"),
        "left",
    )
    .join(
        by_authorisation,
        F.col("authorisation_id") == F.col("_authorisation_id"),
        "left",
    )
    .select(
        "event_kind",
        "source_record_id",
        F.coalesce("unit_id", "_support_unit", "_authorisation_unit").alias("unit_id"),
        F.to_date(
            F.coalesce(
                "decision_date",
                "recommissioning_date",
                "grid_operator_change_effective_date",
                "last_updated_at",
            )
        ).alias("event_date"),
        "market_actor_id",
        "location_id",
        "change_type",
        "previous_grid_operator_id",
        "new_grid_operator_id",
        "authorisation_type",
        "repowering_type",
        "capacity_increase",
    )
)
for column, kind in UNIT_DATES.items():
    lifecycle = lifecycle.unionByName(
        units.filter(F.col(column).isNotNull()).select(
            F.lit(kind).alias("event_kind"),
            "source_record_id",
            "unit_id",
            F.to_date(column).alias("event_date"),
        ),
        allowMissingColumns=True,
    )
TABLES["fleet_lifecycle_event"] = (
    add_provenance(
        lifecycle.join(units.select(*UNIT_ATTRIBUTES), "unit_id", "left"), SOURCE, RID
    ),
    ["event_kind", "source_record_id", "unit_id"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- capacity_utilisation_monthly (generation against capacity)
monthly_generation = (
    one_resolution(
        generation_daily,
        ["market_area_code", "carrier_key", "local_date"],
        "resolution_seconds",
        largest=False,
    )
    .groupBy(
        "market_area_code", "carrier_key", F.trunc("local_date", "month").alias("month")
    )
    .agg(
        F.sum("generation_mwh").alias("generation_mwh"),
        F.countDistinct("local_date").alias("generation_day_count"),
    )
)
months = (
    monthly_generation.select("month")
    .distinct()
    .withColumn("_month_end", F.last_day("month"))
)
monthly_capacity = (
    operating_at(units.crossJoin(F.broadcast(months)), F.col("_month_end"))
    .groupBy("carrier_key", "month")
    .agg(
        (F.sum("capacity_net_kw") / 1000.0).alias("capacity_net_mw"),
        F.count(F.lit(1)).alias("unit_count"),
    )
    .withColumn("market_area_code", F.lit(HOME))
)
utilisation_grain = ["market_area_code", "carrier_key", "month"]
hours = F.datediff(F.add_months("month", 1), F.col("month")) * 24
TABLES["capacity_utilisation_monthly"] = (
    add_provenance(
        monthly_generation.join(monthly_capacity, utilisation_grain, "full")
        .withColumn("hours_in_month", hours)
        .withColumn(
            "utilisation",
            ratio("generation_mwh", F.col("capacity_net_mw") * F.col("hours_in_month")),
        ),
        SOURCE,
        RID,
    ),
    utilisation_grain,
)

# COMMAND ----------

# DBTITLE 1,Transform -- capacity_margin_yearly (capacity, plan and peak load per year)
peak_load = consumption.groupBy(F.year("local_date").alias("year")).agg(
    F.max(F.col("energy_mwh") / (F.col("interval_seconds") / 3600.0)).alias(
        "peak_load_mw"
    )
)
section = F.regexp_replace(F.lower(F.col("capacity_section")), "[^a-z0-9]+", "_")
planned = (
    gold["capacity_plan"]
    .filter(~F.coalesce(F.col("is_total"), F.lit(False)))
    .withColumn("_column", F.concat(F.lit("planned_"), section, F.lit("_mw")))
    .groupBy("year")
    .pivot("_column")
    .agg(F.sum("capacity_mw"))
)
years = peak_load.select("year").union(planned.select("year")).distinct()
year_ends = years.withColumn("_year_end", F.make_date("year", F.lit(12), F.lit(31)))
installed = (
    operating_at(units.crossJoin(F.broadcast(year_ends)), F.col("_year_end"))
    .groupBy("year")
    .agg((F.sum("capacity_net_kw") / 1000.0).alias("installed_capacity_mw"))
)
TABLES["capacity_margin_yearly"] = (
    add_provenance(
        years.join(installed, "year", "left")
        .join(peak_load, "year", "left")
        .join(planned, "year", "left")
        .withColumn("market_area_code", F.lit(HOME))
        .withColumn(
            "capacity_margin_mw",
            F.col("installed_capacity_mw") - F.col("peak_load_mw"),
        ),
        SOURCE,
        RID,
    ),
    ["year"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- operator_concentration (capacity share and HHI per year)
first_year = RULES["operator_concentration_first_year"]
concentration_ends = spark.range(first_year, _dt.date.today().year + 1).select(
    F.col("id").cast("int").alias("year"),
    F.make_date(F.col("id").cast("int"), F.lit(12), F.lit(31)).alias("_year_end"),
)
market = Window.partitionBy("carrier_key", "federal_state", "year")
concentration_grain = ["operator_id", "carrier_key", "federal_state", "year"]
TABLES["operator_concentration"] = (
    add_provenance(
        operating_at(
            units.crossJoin(F.broadcast(concentration_ends)), F.col("_year_end")
        )
        .groupBy(*concentration_grain)
        .agg(
            (F.sum("capacity_net_kw") / 1000.0).alias("capacity_net_mw"),
            F.count(F.lit(1)).alias("unit_count"),
        )
        .withColumn("market_capacity_net_mw", F.sum("capacity_net_mw").over(market))
        .withColumn(
            "capacity_share", ratio("capacity_net_mw", "market_capacity_net_mw")
        )
        .withColumn(
            "herfindahl_hirschman_index",
            F.sum(F.pow(F.col("capacity_share") * 100.0, 2)).over(market),
        )
        .withColumn("operator_count", F.count(F.lit(1)).over(market))
        .join(operator_names, "operator_id", "left"),
        SOURCE,
        RID,
    ),
    concentration_grain,
)

# COMMAND ----------

# DBTITLE 1,Transform -- support_scheme_buildout (registrations by carrier and vintage)
unit_by_support = (
    units.select(
        F.col("renewable_energy_act_support_id").alias("_support_id"),
        "carrier_key",
        "federal_state",
    )
    .unionByName(
        units.select(
            F.col("combined_heat_and_power_support_id").alias("_support_id"),
            "carrier_key",
            "federal_state",
        )
    )
    .filter(F.col("_support_id").isNotNull())
    .groupBy("_support_id")
    .agg(
        F.min("carrier_key").alias("carrier_key"),
        F.min("federal_state").alias("federal_state"),
    )
)
buildout_grain = ["support_scheme", "carrier_key", "federal_state", "vintage_year"]
capacity_kw = F.coalesce(
    "installed_capacity_kw", "electrical_combined_heat_and_power_capacity_kw"
)
TABLES["support_scheme_buildout"] = (
    add_provenance(
        gold["support_registration"]
        .withColumn(
            "_support_id",
            F.coalesce(
                "renewable_energy_act_support_id", "combined_heat_and_power_support_id"
            ),
        )
        .withColumn(
            "vintage_year",
            F.year(
                F.coalesce(
                    "renewable_energy_act_commissioning_date",
                    "commissioning_date",
                    "registration_date",
                )
            ),
        )
        .join(unit_by_support, "_support_id", "left")
        .groupBy(*buildout_grain)
        .agg(
            F.count(F.lit(1)).alias("registration_count"),
            (F.sum(capacity_kw) / 1000.0).alias("capacity_mw"),
            F.count("auction_award_number").alias("auction_award_count"),
        ),
        SOURCE,
        RID,
    ),
    buildout_grain,
)

# COMMAND ----------

# DBTITLE 1,Transform -- station day values (DWD rows: hourly, radiation 10-minute)
is_source = F.col("source_system") == WEATHER_SOURCE
hourly = is_source & (
    F.col("interval_seconds") == RULES["station_weather_interval_seconds"]
)
ten_minute = is_source & (
    F.col("interval_seconds") == RULES["station_radiation_interval_seconds"]
)
station_air = (
    gold["weather_daily"]
    .filter(is_source & (F.col("variable") == "air_temperature"))
    .groupBy(*STATION_DAY)
    .pivot("statistic", ["mean", "min", "max"])
    .agg(F.first("value"))
    .select(
        *STATION_DAY,
        *[
            F.col(statistic).alias(f"{statistic}_air_temperature_degc")
            for statistic in ("mean", "min", "max")
        ],
    )
)
station_wind = (
    gold["weather_wind_precipitation"]
    .filter(hourly)
    .groupBy(*STATION_DAY)
    .agg(
        F.avg("wind_mean_speed_m_per_s").alias("mean_wind_speed_m_per_s"),
        F.max("wind_max_gust_m_per_s").alias("max_wind_gust_m_per_s"),
        F.sum("precipitation_mm").alias("precipitation_mm"),
    )
)
ground_sun = gold["weather_ground_sun"]
station_radiation = (
    ground_sun.filter(ten_minute)
    .groupBy(*STATION_DAY)
    .agg(F.avg("global_radiation_w_per_m2").alias("mean_global_radiation_w_per_m2"))
)
station_sunshine = (
    ground_sun.filter(hourly)
    .groupBy(*STATION_DAY)
    .agg(F.sum("sunshine_duration_minutes").alias("sunshine_duration_minutes"))
)
station_day = station_air
for part in (station_wind, station_radiation, station_sunshine):
    station_day = station_day.join(part, STATION_DAY, "full")

# COMMAND ----------

# DBTITLE 1,Transform -- site_energy_weather_daily (site energy per resolution, weather)
site_weather = (
    gold["weather_air_state"]
    .filter(
        (F.col("source_system") == SITE_SOURCE)
        & F.col("air_temperature_degc").isNotNull()
    )
    .groupBy(*STATION_DAY)
    .agg(
        F.avg("air_temperature_degc").alias("mean_air_temperature_degc"),
        F.min("air_temperature_degc").alias("min_air_temperature_degc"),
        F.max("air_temperature_degc").alias("max_air_temperature_degc"),
        F.count(F.lit(1)).alias("weather_observation_count"),
    )
)
site_grain = [*STATION_DAY, "subsystem", "channel", "resolution_seconds"]
site_energy = (
    gold["channel_reading"]
    .filter(F.col("value_kind") == "increment")
    .withColumnRenamed("interval_seconds", "resolution_seconds")
    .groupBy(*site_grain, "unit")
    .agg(
        F.sum("value").alias("daily_energy"),
        F.count(F.lit(1)).alias("interval_count"),
    )
)
TABLES["site_energy_weather_daily"] = (
    add_provenance(
        site_energy.join(site_weather, STATION_DAY, "left"), SITE_SOURCE, RID
    ),
    site_grain,
)

# COMMAND ----------

# DBTITLE 1,Check -- every site of the energy rows has weather rows
unmatched_sites = (
    site_energy.select("location_key")
    .distinct()
    .join(site_weather.select("location_key").distinct(), "location_key", "left_anti")
    .count()
)
check(
    COMPONENT,
    SITE_SOURCE,
    "site_place_keys_match_weather",
    unmatched_sites == 0,
    detail=f"unmatched_site_count={unmatched_sites}",
    metric_value=float(unmatched_sites),
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Transform -- weather_energy_daily (national weather against generation)
carriers = RULES["weather_generation_carriers"]
generation = (
    one_resolution(
        generation_daily.filter(F.col("market_area_code") == HOME),
        ["carrier_key", "local_date"],
        "resolution_seconds",
        largest=True,
    )
    .groupBy("local_date")
    .agg(
        F.sum("generation_mwh").alias("generation_mwh"),
        *[
            F.sum(
                F.when(F.col("carrier_key").isin(keys), F.col("generation_mwh"))
            ).alias(f"{name}_generation_mwh")
            for name, keys in carriers.items()
        ],
    )
)
daily_consumption = one_resolution(
    consumption.groupBy("local_date", "interval_seconds").agg(
        F.sum("energy_mwh").alias("consumption_mwh")
    ),
    ["local_date"],
    "interval_seconds",
    largest=True,
).select("local_date", "consumption_mwh")
national_weather = station_day.groupBy("local_date").agg(
    *[F.avg(name).alias(name) for name in STATION_MEASURES],
    F.countDistinct("location_key").alias("station_count"),
)
TABLES["weather_energy_daily"] = (
    add_provenance(
        national_weather.join(generation, "local_date", "full")
        .join(daily_consumption, "local_date", "left")
        .withColumn("market_area_code", F.lit(HOME)),
        WEATHER_SOURCE,
        RID,
    ),
    ["market_area_code", "local_date"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- extreme_weather_stress_event (station days beyond the percentile)
station_month = station_day.withColumn("month", F.month("local_date"))
intervention_day = (
    gold["grid_intervention_event"]
    .withColumn("local_date", F.to_date("event_start_project"))
    .groupBy("local_date")
    .agg(
        F.count(F.lit(1)).alias("grid_intervention_count"),
        F.sum("energy_mwh").alias("grid_intervention_energy_mwh"),
    )
)
stress = None
for kind, rule in RULES["stress_kinds"].items():
    value = F.col(rule["value"])
    cutoff = station_month.groupBy("location_key", "month").agg(
        F.percentile_approx(rule["value"], rule["percentile"]).alias("threshold")
    )
    beyond = value > F.col("threshold") if rule["above"] else value < F.col("threshold")
    part = (
        station_month.join(cutoff, ["location_key", "month"])
        .filter(beyond)
        .select(
            F.lit(kind).alias("stress_kind"),
            *STATION_DAY,
            value.alias("value"),
            "threshold",
            F.lit(rule["percentile"]).alias("percentile"),
        )
    )
    stress = part if stress is None else stress.unionByName(part)
TABLES["extreme_weather_stress_event"] = (
    add_provenance(
        stress.join(dim_place, "location_key", "left").join(
            intervention_day, "local_date", "left"
        ),
        WEATHER_SOURCE,
        RID,
    ),
    ["stress_kind", *STATION_DAY],
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
