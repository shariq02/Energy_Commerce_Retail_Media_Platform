# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ML ENERGY -- MARKET, REDISPATCH AND PUMPED-STORAGE DATASETS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** build seven energy datasets of the families `market_daily`,
# MAGIC `market_quarter_hour`, `redispatch` and `self_supervised_rl`: price daily and
# MAGIC quarter-hour, forecast bias, load, redispatch, redispatch trajectories and
# MAGIC pumped-storage trajectories. The calendar, the PV forecast features and the
# MAGIC load target are kept tables; every other target and feature is built in memory.
# MAGIC For each dataset: the contract, the split, the fit and the family rows.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../_ml_inspect

# COMMAND ----------

# DBTITLE 1,ML build library
# MAGIC %run ../_ml_fit

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "smard"
COMPONENT = "ml/energy/market"
DAY = "local_day_europe_berlin"
MARKET_AREA = "de_lu"
CARRIERS = [
    "onshore_wind",
    "offshore_wind",
    "photovoltaic",
    "biomass",
    "hydro",
    "lignite",
    "hard_coal",
    "natural_gas",
    "nuclear",
    "pumped_storage",
    "other_conventional",
    "other_renewable",
]
# forecast scope -> realised carriers; the last two mappings are assumed.
SCOPE_CARRIERS = {
    "onshore_wind": (["onshore_wind"], "confirmed"),
    "offshore_wind": (["offshore_wind"], "confirmed"),
    "wind_and_photovoltaic": (
        ["onshore_wind", "offshore_wind", "photovoltaic"],
        "confirmed",
    ),
    "photovoltaic": (["photovoltaic"], "derived_forecast"),
    "other": (["biomass", "hydro", "other_conventional", "other_renewable"], "assumed"),
    "total": (CARRIERS, "assumed"),
}
FORECAST_SCOPES = ["onshore_wind", "offshore_wind", "other", "total"]
FORECAST_SCOPES = [*FORECAST_SCOPES, "wind_and_photovoltaic"]
GENERATION_QH = ["onshore_wind", "offshore_wind", "photovoltaic"]
PRICE_LAGS, GENERATION_LAGS, LOAD_LAGS = [2, 7, 14, 28], [2, 7], [2, 7, 14]
PV_LAGS, QH_LAG_DAYS, EVENT_LAGS = [2, 7], [2, 7], [2, 3, 7, 14]
CALENDAR_FROM, CALENDAR_TO = "2013-01-01", "2027-12-31"
DATE_FROM = SPLIT_CALENDARS["energy_daily"]["train"][0]
QH_FROM = SPLIT_CALENDARS["quarter_hour"]["train"][0]
REGIME_START = SPLIT_CALENDARS["redispatch"]["train"][0]
REGIME_END = SPLIT_CALENDARS["redispatch"]["test"][1]
TSOS = ["fifty_hertz", "amprion", "tennet_de", "transnetbw"]
# national weather of the load dataset: a station-day needs 18 hourly values (radiation
# 20), a day needs 10 stations; degree days against a heating and a cooling base
WEATHER_MIN_HOURS, WEATHER_MIN_RADIATION_HOURS, WEATHER_MIN_STATIONS = 18, 20, 10
HEATING_BASE_DEGC, COOLING_BASE_DEGC = 15.0, 22.0
FRAMES, BLOCKS = {}, {}

# COMMAND ----------

# DBTITLE 1,Run identity and session time zone
rid = ml_run_id()
set_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Gold -- prices, balance and forecast components, events
price = read_gold("market_price", source=SOURCE)
_component = read_gold("energy_component", source=SOURCE)
balance = _component.filter(F.col("component_group") == "balance").withColumn(
    "interval_start_utc", F.col("observation_timestamp_utc")
)
forecast = _component.filter(F.col("component_group") == "forecast")
events = read_gold("grid_intervention_event", source="redispatch")
assert_values_present(
    price, "interval_reference", [DAY], component=COMPONENT, source=SOURCE, rid=rid
)
assert_values_present(
    balance, "interval_reference", [DAY], component=COMPONENT, source=SOURCE, rid=rid
)
assert_values_present(
    forecast,
    "forecast_scope",
    ["wind_and_photovoltaic", "onshore_wind", "offshore_wind"],
    component=COMPONENT,
    source=SOURCE,
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Calendar and holiday features -- a kept table (the models read it)
_dates = spark.range(1).select(
    F.explode(
        F.sequence(F.lit(CALENDAR_FROM).cast("date"), F.lit(CALENDAR_TO).cast("date"))
    ).alias("local_date")
)
_areas = spark.createDataFrame(
    list(MARKET_AREA_COUNTRY.items()), "market_area_code string, holiday_country string"
)
_holidays = spark.createDataFrame(
    [
        (country, d)
        for country in HOLIDAY_RULES
        for year in range(int(CALENDAR_FROM[:4]), int(CALENDAR_TO[:4]) + 1)
        for d in holiday_dates(country, year)
    ],
    "holiday_country string, holiday_date date",
)
_base = _dates.crossJoin(_areas).join(
    _holidays.withColumnRenamed("holiday_date", "local_date").withColumn(
        "is_holiday", F.lit(True)
    ),
    ["holiday_country", "local_date"],
    "left",
)
_base = _base.withColumn("is_holiday", F.coalesce(F.col("is_holiday"), F.lit(False)))
_w = Window.partitionBy("market_area_code").orderBy("local_date")
calendar_all = (
    _base.withColumn("day_of_week", F.dayofweek("local_date"))
    .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
    .withColumn("month", F.month("local_date"))
    .withColumn("day_of_year", F.dayofyear("local_date"))
    .withColumn("iso_week", F.weekofyear("local_date"))
    .withColumn("year", F.year("local_date"))
    .withColumn("holiday_next_day", F.lead("is_holiday", 1).over(_w))
    .withColumn("holiday_previous_day", F.lag("is_holiday", 1).over(_w))
    .withColumn(
        "is_bridge_day",
        (
            (F.col("day_of_week") == 2)
            & F.coalesce(F.col("holiday_next_day"), F.lit(False))
        )
        | (
            (F.col("day_of_week") == 6)
            & F.coalesce(F.col("holiday_previous_day"), F.lit(False))
        ),
    )
    .withColumn("local_day_hours", local_day_hours("local_date"))
    .withColumn("as_of_ts", local_day_start_utc("local_date"))
    .drop("holiday_next_day", "holiday_previous_day")
)
write_stored_table(
    calendar_all,
    "features_calendar_market_area",
    ECO,
    key_cols=["market_area_code", "local_date"],
    source=SOURCE,
    component=f"{COMPONENT}/calendar",
    rid=rid,
    section="features",
)
calendar = read_ml("features_calendar_market_area", ecosystem=ECO)

# COMMAND ----------


# DBTITLE 1,Derived PV forecast (wind and PV minus the two wind scopes)
def _scope_value(scope):
    return F.max(F.when(F.col("forecast_scope") == scope, F.col("energy_mwh")))


derived_pv = (
    forecast.groupBy(
        "market_area_code",
        "observation_timestamp_utc",
        "local_date",
        "interval_seconds",
        "interval_reference",
    )
    .agg(
        _scope_value("wind_and_photovoltaic").alias("wind_and_pv"),
        _scope_value("onshore_wind").alias("onshore"),
        _scope_value("offshore_wind").alias("offshore"),
    )
    .filter(
        F.col("wind_and_pv").isNotNull()
        & F.col("onshore").isNotNull()
        & F.col("offshore").isNotNull()
    )
    .select(
        "market_area_code",
        "observation_timestamp_utc",
        "local_date",
        "interval_seconds",
        "interval_reference",
        (F.col("wind_and_pv") - F.col("onshore") - F.col("offshore")).alias(
            "forecast_photovoltaic_mwh"
        ),
    )
)

# COMMAND ----------

# DBTITLE 1,Daily market features -- price, generation, load and forecast lags
_daily_price = (
    price.filter(F.col("interval_reference") == DAY)
    .groupBy("market_area_code", "local_date")
    .agg(F.avg("price_eur_per_mwh").alias("price_eur_per_mwh"))
)
_generation = (
    balance.filter(
        (F.col("component_kind") == "generation") & (F.col("interval_reference") == DAY)
    )
    .groupBy("market_area_code", "local_date")
    .pivot("carrier_code", CARRIERS)
    .agg(F.sum("energy_mwh"))
)
for _c in CARRIERS:
    _generation = _generation.withColumnRenamed(_c, f"generation_{_c}_mwh")
_load = (
    balance.filter(
        F.col("component_kind").isin("consumption", "residual_load")
        & (F.col("interval_reference") == DAY)
    )
    .groupBy("market_area_code", "local_date")
    .pivot("component_kind", ["consumption", "residual_load"])
    .agg(F.sum("energy_mwh"))
    .withColumnRenamed("consumption", "consumption_mwh")
    .withColumnRenamed("residual_load", "residual_load_mwh")
)
# the disputed PV scope is left out of the day-ahead forecasts
_forecast_daily = (
    forecast.filter(
        (F.col("interval_reference") == DAY)
        & (F.col("forecast_scope") != "photovoltaic")
    )
    .groupBy("market_area_code", "local_date")
    .pivot("forecast_scope", FORECAST_SCOPES)
    .agg(F.sum("energy_mwh"))
)
for _s in FORECAST_SCOPES:
    _forecast_daily = _forecast_daily.withColumnRenamed(_s, f"forecast_{_s}_mwh")
_spine = calendar.select(
    "market_area_code",
    "local_date",
    "day_of_week",
    "is_weekend",
    "month",
    "day_of_year",
    "iso_week",
    "year",
    "is_holiday",
    "is_bridge_day",
    "local_day_hours",
    "as_of_ts",
).join(_daily_price.select("market_area_code").distinct(), "market_area_code")
_spine = _spine.filter(
    F.col("local_date").between(
        DATE_FROM, str(_daily_price.agg(F.max("local_date")).first()[0])
    )
)
_raw = (
    _spine.join(_daily_price, ["market_area_code", "local_date"], "left")
    .join(_generation, ["market_area_code", "local_date"], "left")
    .join(_load, ["market_area_code", "local_date"], "left")
)
_gen_cols = [f"generation_{c}_mwh" for c in CARRIERS]
_lagged = add_date_lags(
    _raw,
    keys=["market_area_code"],
    date_col="local_date",
    cols=["price_eur_per_mwh"],
    lags=PRICE_LAGS,
)
_lagged = add_date_lags(
    _lagged,
    keys=["market_area_code"],
    date_col="local_date",
    cols=_gen_cols,
    lags=GENERATION_LAGS,
)
_lagged = add_date_lags(
    _lagged,
    keys=["market_area_code"],
    date_col="local_date",
    cols=["consumption_mwh", "residual_load_mwh"],
    lags=LOAD_LAGS,
)
market_daily = _lagged.drop(
    "price_eur_per_mwh", *_gen_cols, "consumption_mwh", "residual_load_mwh"
).join(_forecast_daily, ["market_area_code", "local_date"], "left")
_forecast_columns = [c for c in market_daily.columns if c.startswith("forecast_")]

# COMMAND ----------

# DBTITLE 1,National weather per day -- temperature, degree days and radiation (load)
# The realised weather of the day is used as if it were a forecast (perfect prognosis,
# named in the contract). The value is the mean of the DWD stations with enough hours.
_weather = read_gold("weather_observation", source="dwd").filter(
    F.col("source_system") == "dwd"
)
_station_temperature = (
    _weather.filter(F.col("air_temperature_degc").isNotNull())
    .groupBy("location_key", "local_date")
    .agg(
        F.count("*").alias("n_hours"),
        F.avg("air_temperature_degc").alias("temperature"),
    )
    .filter(F.col("n_hours") >= WEATHER_MIN_HOURS)
)
_station_radiation = (
    _weather.filter(F.col("global_radiation_w_per_m2").isNotNull())
    .groupBy("location_key", "local_date")
    .agg(
        F.count("*").alias("n_hours"),
        F.avg("global_radiation_w_per_m2").alias("radiation"),
    )
    .filter(F.col("n_hours") >= WEATHER_MIN_RADIATION_HOURS)
)
_national_radiation = _station_radiation.groupBy("local_date").agg(
    F.count("*").alias("n_stations"),
    F.avg("radiation").alias("weather_global_radiation_mean_w_per_m2"),
)
weather_national_daily = (
    _station_temperature.groupBy("local_date")
    .agg(
        F.count("*").alias("n_stations"),
        F.avg("temperature").alias("weather_temperature_mean_degc"),
    )
    .filter(F.col("n_stations") >= WEATHER_MIN_STATIONS)
    .drop("n_stations")
    .withColumn(
        "heating_degree_days",
        F.greatest(
            F.lit(HEATING_BASE_DEGC) - F.col("weather_temperature_mean_degc"),
            F.lit(0.0),
        ),
    )
    .withColumn(
        "cooling_degree_days",
        F.greatest(
            F.col("weather_temperature_mean_degc") - F.lit(COOLING_BASE_DEGC),
            F.lit(0.0),
        ),
    )
    .join(
        _national_radiation.filter(F.col("n_stations") >= WEATHER_MIN_STATIONS).drop(
            "n_stations"
        ),
        "local_date",
        "left",
    )
)

# COMMAND ----------

# DBTITLE 1,PV forecast features -- a kept table, no dataset joins it yet
_derived_daily = (
    derived_pv.filter(F.col("interval_reference") == DAY)
    .groupBy("market_area_code", "local_date")
    .agg(F.avg("forecast_photovoltaic_mwh").alias("forecast_photovoltaic_mwh"))
)
_realised_pv = (
    balance.filter(
        (F.col("component_kind") == "generation")
        & (F.col("carrier_code") == "photovoltaic")
        & (F.col("interval_reference") == DAY)
    )
    .groupBy("market_area_code", "local_date")
    .agg(F.sum("energy_mwh").alias("generation_photovoltaic_mwh"))
)
_pv_lagged = add_date_lags(
    _derived_daily.join(_realised_pv, ["market_area_code", "local_date"], "outer"),
    keys=["market_area_code"],
    date_col="local_date",
    cols=["generation_photovoltaic_mwh"],
    lags=PV_LAGS,
)
write_stored_table(
    _pv_lagged.drop("generation_photovoltaic_mwh")
    .filter(F.col("forecast_photovoltaic_mwh").isNotNull())
    .withColumn("forecast_value_origin", F.lit("derived"))
    .withColumn("forecast_vintage", F.lit("published_before_day_unknown_issue_time")),
    "features_pv_forecast_daily",
    ECO,
    key_cols=["market_area_code", "local_date"],
    source=SOURCE,
    component=f"{COMPONENT}/pv_forecast",
    rid=rid,
    section="features",
)

# COMMAND ----------

# DBTITLE 1,Quarter-hour market features
_prices_qh = price.filter(
    (F.col("interval_reference") == "clock")
    & (F.col("interval_seconds") == 900)
    & (F.col("local_date") >= QH_FROM)
).select(
    "market_area_code",
    F.col("observation_timestamp_utc").alias("interval_start_utc"),
    "local_date",
    "price_eur_per_mwh",
)
_b = balance.filter(
    (F.col("interval_reference") == "clock")
    & (F.col("interval_seconds") == 900)
    & (F.col("local_date") >= QH_FROM)
)
_load_qh = (
    _b.filter(F.col("component_kind").isin("consumption", "residual_load"))
    .groupBy("market_area_code", "interval_start_utc")
    .pivot("component_kind", ["consumption", "residual_load"])
    .agg(F.sum("energy_mwh"))
    .withColumnRenamed("consumption", "consumption_mwh")
    .withColumnRenamed("residual_load", "residual_load_mwh")
)
_generation_qh = (
    _b.filter(
        (F.col("component_kind") == "generation")
        & F.col("carrier_code").isin(*GENERATION_QH)
    )
    .groupBy("market_area_code", "interval_start_utc")
    .pivot("carrier_code", GENERATION_QH)
    .agg(F.sum("energy_mwh"))
)
for _c in GENERATION_QH:
    _generation_qh = _generation_qh.withColumnRenamed(_c, f"generation_{_c}_mwh")
_raw_qh = _prices_qh.join(
    _load_qh, ["market_area_code", "interval_start_utc"], "left"
).join(_generation_qh, ["market_area_code", "interval_start_utc"], "left")
_cols_qh = [
    "price_eur_per_mwh",
    "consumption_mwh",
    "residual_load_mwh",
    *[f"generation_{c}_mwh" for c in GENERATION_QH],
]
_lagged_qh = add_timestamp_lags(
    _raw_qh,
    keys=["market_area_code"],
    ts_col="interval_start_utc",
    cols=_cols_qh,
    lag_days=QH_LAG_DAYS,
).drop(*_cols_qh)
_local = F.from_utc_timestamp("interval_start_utc", PROJECT_TIMEZONE)
market_quarter_hour = (
    _lagged_qh.withColumn("hour_of_day", F.hour(_local))
    .withColumn("quarter_of_hour", (F.minute(_local) / 15).cast("int"))
    .withColumn("as_of_ts", F.col("interval_start_utc"))
    .join(
        calendar.select(
            "market_area_code",
            "local_date",
            "day_of_week",
            "is_weekend",
            "is_holiday",
            "is_bridge_day",
            "month",
        ),
        ["market_area_code", "local_date"],
        "left",
    )
    .join(
        market_daily.select("market_area_code", "local_date", *_forecast_columns),
        ["market_area_code", "local_date"],
        "left",
    )
)

# COMMAND ----------

# DBTITLE 1,Redispatch features -- event history ending two days before the day
_tso_events = (
    events.filter(
        F.col("instructing_market_area_code").isin(*TSOS)
        & (F.col("duration_hours") >= 0)
    )
    .withColumn("local_date", F.to_date("event_start_project"))
    .filter(F.col("local_date").between(REGIME_START, REGIME_END))
    .groupBy(
        F.col("instructing_market_area_code").alias("market_area_code"), "local_date"
    )
    .agg(
        F.count("*").alias("event_count"), F.sum("energy_mwh").alias("event_energy_mwh")
    )
)
_grid = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(F.lit(REGIME_START).cast("date"), F.lit(REGIME_END).cast("date"))
        ).alias("local_date")
    )
    .crossJoin(spark.createDataFrame([(t,) for t in TSOS], "market_area_code string"))
)
_hist = (
    _grid.join(_tso_events, ["market_area_code", "local_date"], "left")
    .withColumn(
        "any_event", (F.coalesce(F.col("event_count"), F.lit(0)) > 0).cast("int")
    )
    .withColumn("event_energy_mwh", F.coalesce(F.col("event_energy_mwh"), F.lit(0.0)))
)
_w7 = Window.partitionBy("market_area_code").orderBy("local_date").rowsBetween(-8, -2)
_w30 = Window.partitionBy("market_area_code").orderBy("local_date").rowsBetween(-31, -2)
_event_lagged = add_date_lags(
    _hist,
    keys=["market_area_code"],
    date_col="local_date",
    cols=["any_event", "event_energy_mwh"],
    lags=EVENT_LAGS,
)
_event_lagged = (
    _event_lagged.withColumn("event_share_7d", F.avg("any_event").over(_w7))
    .withColumn("event_share_30d", F.avg("any_event").over(_w30))
    .withColumn("event_energy_mean_30d", F.avg("event_energy_mwh").over(_w30))
    .drop("event_count", "any_event", "event_energy_mwh")
)
redispatch_features = _event_lagged.join(
    calendar.select(
        "market_area_code",
        "local_date",
        "day_of_week",
        "is_weekend",
        "month",
        "is_holiday",
        "is_bridge_day",
        "as_of_ts",
    ),
    ["market_area_code", "local_date"],
    "left",
).join(
    market_daily.select("market_area_code", "local_date", *_forecast_columns),
    ["market_area_code", "local_date"],
    "left",
)

# COMMAND ----------

# DBTITLE 1,Targets -- price daily, price quarter-hour, load (kept) and redispatch
_candidates = (
    price.filter(
        (F.col("interval_reference") == DAY)
        & (F.col("market_area_code") == MARKET_AREA)
    )
    .groupBy("market_area_code", "local_date")
    .agg(
        F.avg("price_eur_per_mwh").alias("target_price_eur_per_mwh"),
        F.first("interval_seconds").alias("interval_seconds"),
    )
)
target_price_daily = (
    _candidates.filter(F.col("target_price_eur_per_mwh").isNotNull())
    .withColumn("regime_year", F.year("local_date"))
    .withColumn("provenance_tier", F.lit("sourced"))
)
BLOCKS["price_daily"] = [
    (
        "dropped_rows",
        markdown_table(
            ["rows_dropped"], [(_candidates.count() - target_price_daily.count(),)]
        ),
    )
]
_qh_candidates = price.filter(
    (F.col("interval_reference") == "clock")
    & (F.col("interval_seconds") == 900)
    & (F.col("market_area_code") == MARKET_AREA)
    & (F.col("local_date") >= QH_FROM)
)
target_price_qh = (
    _qh_candidates.filter(F.col("price_eur_per_mwh").isNotNull())
    .select(
        "market_area_code",
        F.col("observation_timestamp_utc").alias("interval_start_utc"),
        "local_date",
        F.col("price_eur_per_mwh").alias("target_price_eur_per_mwh"),
        (F.col("price_eur_per_mwh") < 0).alias("target_is_negative_price"),
    )
    .withColumn("provenance_tier", F.lit("sourced"))
)
BLOCKS["price_quarter_hour"] = [
    (
        "dropped_rows",
        markdown_table(
            ["rows_dropped"], [(_qh_candidates.count() - target_price_qh.count(),)]
        ),
    )
]
_load_candidates = balance.filter(
    F.col("component_kind").isin("consumption", "residual_load")
    & (F.col("interval_reference") == DAY)
)
target_load = (
    _load_candidates.groupBy(
        "market_area_code", "local_date", F.col("component_kind").alias("load_kind")
    )
    .agg(F.sum("energy_mwh").alias("target_value_mwh"))
    .filter(F.col("target_value_mwh").isNotNull())
    .withColumn("provenance_tier", F.lit("sourced"))
)
write_stored_table(
    target_load,
    "target_load",
    ECO,
    key_cols=["market_area_code", "local_date", "load_kind"],
    source=SOURCE,
    component=f"{COMPONENT}/target_load",
    rid=rid,
    section="targets",
    target_col="target_value_mwh",
)
_regime_events = events.filter(
    F.to_date("event_start_project").between(REGIME_START, REGIME_END)
)
_valid = (
    _regime_events.filter(
        F.col("instructing_market_area_code").isin(*TSOS)
        & (F.col("duration_hours") >= 0)
        & F.col("energy_mwh").isNotNull()
    )
    .withColumn("local_date", F.to_date("event_start_project"))
    .groupBy(
        F.col("instructing_market_area_code").alias("market_area_code"), "local_date"
    )
    .agg(
        F.count("*").alias("event_count"), F.sum("energy_mwh").alias("event_energy_mwh")
    )
)
target_redispatch = (
    _grid.join(_valid, ["market_area_code", "local_date"], "left")
    .withColumn("event_count", F.coalesce(F.col("event_count"), F.lit(0)))
    .withColumn("target_any_event", F.col("event_count") > 0)
    .withColumn(
        "target_event_energy_mwh",
        F.when(F.col("target_any_event"), F.col("event_energy_mwh")),
    )
    .withColumn(
        "target_log_event_energy_mwh",
        F.when(
            F.col("target_any_event") & (F.col("event_energy_mwh") > 0),
            F.log1p(F.col("event_energy_mwh")),
        ),
    )
    .drop("event_energy_mwh")
    .withColumn("provenance_tier", F.lit("sourced"))
)
BLOCKS["redispatch"] = [
    (
        "excluded_events",
        markdown_table(
            ["reason", "events"],
            [
                (
                    "negative_duration",
                    _regime_events.filter(F.col("duration_hours") < 0).count(),
                ),
                (
                    "other_or_missing_operator",
                    _regime_events.filter(
                        ~F.col("instructing_market_area_code").isin(*TSOS)
                        | F.col("instructing_market_area_code").isNull()
                    ).count(),
                ),
            ],
        ),
    )
]

# COMMAND ----------

# DBTITLE 1,Targets -- forecast bias per scope and day
_realised_carrier = (
    balance.filter(
        (F.col("component_kind") == "generation") & (F.col("interval_reference") == DAY)
    )
    .groupBy("market_area_code", "local_date", "carrier_code")
    .agg(F.sum("energy_mwh").alias("mwh"))
)
_parts = []
for _scope, (_carriers, _status) in SCOPE_CARRIERS.items():
    _parts.append(
        _realised_carrier.filter(F.col("carrier_code").isin(*_carriers))
        .groupBy("market_area_code", "local_date")
        .agg(
            F.sum("mwh").alias("realised_mwh"),
            F.countDistinct("carrier_code").alias("carriers_present"),
        )
        .filter(F.col("carriers_present") == len(_carriers))
        .select(
            "market_area_code",
            "local_date",
            F.lit(_scope).alias("forecast_scope"),
            "realised_mwh",
            F.lit(_status).alias("scope_mapping_status"),
        )
    )
_realised = _parts[0]
for _p in _parts[1:]:
    _realised = _realised.unionByName(_p)
_forecast_scope = (
    forecast.filter(
        (F.col("interval_reference") == DAY)
        & (F.col("forecast_scope") != "photovoltaic")
    )
    .groupBy("market_area_code", "local_date", "forecast_scope")
    .agg(F.sum("energy_mwh").alias("forecast_mwh"))
)
_pv_scope = (
    derived_pv.filter(F.col("interval_reference") == DAY)
    .groupBy("market_area_code", "local_date")
    .agg(F.sum("forecast_photovoltaic_mwh").alias("forecast_mwh"))
    .withColumn("forecast_scope", F.lit("photovoltaic"))
    .select("market_area_code", "local_date", "forecast_scope", "forecast_mwh")
)
_bias_candidates = _realised.join(
    _forecast_scope.unionByName(_pv_scope),
    ["market_area_code", "local_date", "forecast_scope"],
    "left",
)
target_bias = (
    _bias_candidates.filter(
        F.col("forecast_mwh").isNotNull() & F.col("realised_mwh").isNotNull()
    )
    .withColumn("target_bias_mwh", F.col("realised_mwh") - F.col("forecast_mwh"))
    .withColumn("provenance_tier", F.lit("constructed"))
)
BLOCKS["bias"] = [
    (
        "dropped_rows",
        markdown_table(
            ["rows_dropped"], [(_bias_candidates.count() - target_bias.count(),)]
        ),
    )
]

# COMMAND ----------

# DBTITLE 1,Targets -- redispatch event steps and pumped-storage steps
_valid_events = _regime_events.filter(
    F.col("instructing_market_area_code").isin(*TSOS)
    & (F.col("duration_hours") >= 0)
    & F.col("energy_mwh").isNotNull()
)
_step_window = Window.partitionBy(
    "instructing_market_area_code", F.to_date("event_start_project")
).orderBy("event_start_utc", "event_key")
target_rl_redispatch = (
    _valid_events.select(
        "event_key",
        F.col("instructing_market_area_code").alias("market_area_code"),
        F.to_date("event_start_project").alias("local_date"),
        "event_start_utc",
        F.row_number().over(_step_window).alias("step"),
        F.col("direction").alias("action_direction"),
        F.col("reason").alias("action_reason"),
        F.col("energy_mwh").alias("action_energy_mwh"),
        (-F.col("energy_mwh")).alias("reward"),
    )
    .withColumn(
        "episode_id",
        F.concat_ws("|", "market_area_code", F.col("local_date").cast("string")),
    )
    .withColumn("provenance_tier", F.lit("rule_derived"))
)
BLOCKS["rl_redispatch"] = [
    (
        "dropped_rows",
        markdown_table(
            ["rows_dropped"],
            [(_regime_events.count() - target_rl_redispatch.count(),)],
        ),
    )
]
_qh_balance = balance.filter(
    (F.col("market_area_code") == MARKET_AREA)
    & (F.col("interval_reference") == "clock")
    & (F.col("interval_seconds") == 900)
    & (F.col("local_date") >= QH_FROM)
)
_pumped_generation = (
    _qh_balance.filter(
        (F.col("component_kind") == "generation")
        & (F.col("carrier_code") == "pumped_storage")
    )
    .groupBy("interval_start_utc", "local_date")
    .agg(F.sum("energy_mwh").alias("action_generation_mwh"))
)
_pumped_consumption = (
    _qh_balance.filter(F.col("component_kind") == "pumped_storage_consumption")
    .groupBy("interval_start_utc")
    .agg(F.sum("energy_mwh").alias("action_consumption_mwh"))
)
_residual = (
    _qh_balance.filter(F.col("component_kind") == "residual_load")
    .groupBy("interval_start_utc")
    .agg(F.sum("energy_mwh").alias("state_residual_load_mwh"))
)
_price_qh = price.filter(
    (F.col("market_area_code") == MARKET_AREA)
    & (F.col("interval_reference") == "clock")
    & (F.col("interval_seconds") == 900)
).select(
    F.col("observation_timestamp_utc").alias("interval_start_utc"),
    F.col("price_eur_per_mwh").alias("state_price_eur_per_mwh"),
)
_steps = (
    _pumped_generation.join(_pumped_consumption, "interval_start_utc", "full")
    .join(_residual, "interval_start_utc", "left")
    .join(_price_qh, "interval_start_utc", "left")
    .withColumn(
        "local_date",
        F.coalesce(
            F.col("local_date"),
            F.to_date(F.from_utc_timestamp("interval_start_utc", PROJECT_TIMEZONE)),
        ),
    )
)
_day_start = local_day_start_utc("local_date")
target_rl_pumped = (
    _steps.filter(
        F.col("state_price_eur_per_mwh").isNotNull()
        & (
            F.col("action_generation_mwh").isNotNull()
            | F.col("action_consumption_mwh").isNotNull()
        )
    )
    .withColumn("episode_id", F.col("local_date").cast("string"))
    .withColumn(
        "step",
        (
            (F.col("interval_start_utc").cast("long") - _day_start.cast("long")) / 900
        ).cast("int"),
    )
    .withColumn(
        "action_net_mwh",
        F.coalesce(F.col("action_generation_mwh"), F.lit(0.0))
        - F.coalesce(F.col("action_consumption_mwh"), F.lit(0.0)),
    )
    .withColumn(
        "reward_eur", F.col("state_price_eur_per_mwh") * F.col("action_net_mwh")
    )
    .withColumn("provenance_tier", F.lit("constructed"))
)
BLOCKS["rl_pumped_storage"] = [
    (
        "dropped_rows",
        markdown_table(
            ["rows_dropped"], [(_steps.count() - target_rl_pumped.count(),)]
        ),
    )
]

# COMMAND ----------

# DBTITLE 1,Assemble the seven datasets (the join is in memory, no view)
_day = ["market_area_code", "local_date"]
FRAMES["price_daily"] = assembled_frame(target_price_daily, [(market_daily, _day)])
FRAMES["price_quarter_hour"] = assembled_frame(
    target_price_qh,
    [(market_quarter_hour, ["market_area_code", "interval_start_utc"], ["local_date"])],
)
FRAMES["bias"] = assembled_frame(target_bias, [(market_daily, _day)])
FRAMES["load"] = assembled_frame(
    target_load, [(market_daily, _day), (weather_national_daily, ["local_date"])]
)
FRAMES["redispatch"] = assembled_frame(target_redispatch, [(redispatch_features, _day)])
FRAMES["rl_redispatch"] = assembled_frame(
    target_rl_redispatch, [(redispatch_features, _day)]
)
FRAMES["rl_pumped_storage"] = assembled_frame(
    target_rl_pumped,
    [],
    extra={
        "hour_of_day": F.hour(
            F.from_utc_timestamp("interval_start_utc", PROJECT_TIMEZONE)
        ),
        "day_of_week": F.dayofweek("local_date"),
        "month": F.month("local_date"),
    },
)

# COMMAND ----------

# DBTITLE 1,Build the datasets -- contract, split, fit and the family rows
build_datasets(FRAMES, rid=rid, source=SOURCE, blocks=BLOCKS)

# COMMAND ----------

# DBTITLE 1,Summary
for _dataset in FRAMES:
    print(
        _dataset,
        read_ml("dataset_manifest", ecosystem=ECO)
        .filter(F.col("dataset_id") == _dataset)
        .first()["status"],
    )
