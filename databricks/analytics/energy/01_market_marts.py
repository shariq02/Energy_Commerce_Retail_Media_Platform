# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ANALYTICS ENERGY -- MARKET, GRID AND FORECAST MARTS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** 9 `energy_analytics` tables from Gold: generation by carrier and
# MAGIC price statistics per day, one row per market window, price formation per day,
# MAGIC negative price events, cross-border trade, grid interventions per operator and
# MAGIC day, forecast errors per day, and `metric_definition`. Values come from
# MAGIC `analytics_rules.yml`.

# COMMAND ----------

# DBTITLE 1,Gold library
# MAGIC %run ../../gold/_gold_common

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "smard"
COMPONENT = "analytics/energy/01_market_marts"
RID = analytics_run_id()
RULES = load_yaml("src/schemas/mappings/analytics_rules.yml")
GATE = {"source": SOURCE, "component": COMPONENT, "rid": RID}
COMMON = {**GATE, "schema": ANALYTICS_SCHEMAS["energy"]}
HOME = RULES["home_market_area"]
PRICE = "price_eur_per_mwh"
WINDOW = [
    "market_area_code",
    "observation_timestamp_utc",
    "interval_seconds",
    "interval_reference",
]
DAY = ["market_area_code", "local_date", "interval_seconds"]
KIND = F.col("component_kind")
ENERGY = F.col("energy_mwh")
METRICS = [
    (
        "generation_by_carrier",
        "Electricity generation, by resolved energy carrier, per market area and day.",
        "sum(generation_mwh) grouped by market_area_code, local_date, carrier_key",
        "generation_by_carrier_daily",
        "market_area_code x local_date x carrier_key",
    ),
    (
        "average_market_price",
        "Mean day-ahead electricity price per market area and day.",
        "avg(price_eur_per_mwh) grouped by market_area_code, local_date",
        "market_price_daily",
        "market_area_code x local_date",
    ),
    (
        "price_volatility",
        "Within-day dispersion of the day-ahead price.",
        "stddev_pop(price_eur_per_mwh) grouped by market_area_code, local_date",
        "market_price_daily",
        "market_area_code x local_date",
    ),
    (
        "negative_price_intervals",
        "Number of day-ahead price windows below zero per market area and day.",
        "count(price_eur_per_mwh < 0) grouped by market_area_code, local_date",
        "market_price_daily",
        "market_area_code x local_date",
    ),
    (
        "site_energy_consumption",
        "Daily site energy flow per subsystem/channel, additive readings only.",
        (
            "sum(value) where value_kind = 'increment', grouped by location_key, "
            "local_date, subsystem, channel, resolution_seconds"
        ),
        "site_energy_weather_daily",
        "location_key x local_date x subsystem x channel x resolution_seconds",
    ),
    (
        "site_energy_weather_context",
        "Site daily energy paired with that site's own mean air temperature.",
        "site daily energy joined to the site daily air temperature",
        "site_energy_weather_daily",
        "location_key x local_date x subsystem x channel x resolution_seconds",
    ),
    (
        "renewable_share",
        "Share of renewable carriers in the generation of the priced windows of a day.",
        (
            "sum(renewable_generation_mwh) / sum(generation_mwh) grouped by "
            "market_area_code, local_date, interval_seconds"
        ),
        "price_formation_daily",
        "market_area_code x local_date x interval_seconds",
    ),
    (
        "forecast_mean_absolute_error",
        "Mean absolute difference between a forecast and its actual per day.",
        (
            "avg(abs(energy_mwh - actual_energy_mwh)) grouped by source_system, "
            "market_area_code, local_date, forecast_scope, interval_seconds"
        ),
        "forecast_accuracy",
        (
            "source_system x market_area_code x local_date x forecast_scope x "
            "interval_seconds"
        ),
    ),
    (
        "grid_intervention_energy",
        "Energy of the grid interventions per instructing operator and day.",
        (
            "sum(energy_mwh) grouped by instructing_market_area_code, local_date, "
            "direction, reason"
        ),
        "grid_intervention_daily",
        "instructing_market_area_code x local_date x direction x reason",
    ),
    (
        "national_mean_air_temperature",
        "Mean over the weather stations of the daily mean air temperature.",
        "avg(mean_air_temperature_degc) of the station days grouped by local_date",
        "weather_energy_daily",
        "market_area_code x local_date",
    ),
]
TABLES = {}

# COMMAND ----------

# DBTITLE 1,Function -- left join on the market window


def window_join(left, right):
    """Left join on the market window; a null part matches a null part."""
    right = right.select(
        *[F.col(c).alias(f"_r_{c}") if c in WINDOW else F.col(c) for c in right.columns]
    )
    match = F.lit(True)
    for c in WINDOW:
        match = match & left[c].eqNullSafe(right[f"_r_{c}"])
    return left.join(right, match, "left").drop(*[f"_r_{c}" for c in WINDOW])


# COMMAND ----------

# DBTITLE 1,Read Gold
market_price = read_gold("market_price", source=SOURCE)
component = read_gold("market_energy_component", source=SOURCE)
forecast_actual = read_gold("forecast_actual", source=SOURCE)
grid_intervention = read_gold("grid_intervention_event", source=SOURCE)
balance = component.filter(F.col("component_group") == "balance")
exchange = component.filter(F.col("component_group") == "exchange")

# COMMAND ----------

# DBTITLE 1,Transform -- generation_by_carrier_daily (one row per day and resolution)
TABLES["generation_by_carrier_daily"] = (
    add_provenance(
        balance.filter(KIND == "generation")
        .groupBy(
            "market_area_code",
            "local_date",
            "carrier_key",
            F.col("interval_seconds").alias("resolution_seconds"),
        )
        .agg(
            F.sum(ENERGY).alias("generation_mwh"),
            F.countDistinct("source_system").alias("source_count"),
            F.countDistinct("source_filter_id").alias("filter_count"),
            F.count(F.lit(1)).alias("interval_count"),
        ),
        SOURCE,
        RID,
    ),
    ["market_area_code", "local_date", "carrier_key", "resolution_seconds"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- market_price_daily (price statistics per market area and day)
percentiles = RULES["price_percentiles"]
TABLES["market_price_daily"] = (
    add_provenance(
        market_price.groupBy("market_area_code", "local_date")
        .agg(
            F.avg(PRICE).alias("mean_price_eur_per_mwh"),
            F.percentile_approx(PRICE, 0.5).alias("median_price_eur_per_mwh"),
            F.percentile_approx(PRICE, percentiles["low"]).alias(
                "low_percentile_price_eur_per_mwh"
            ),
            F.percentile_approx(PRICE, percentiles["high"]).alias(
                "high_percentile_price_eur_per_mwh"
            ),
            F.min(PRICE).alias("min_price_eur_per_mwh"),
            F.max(PRICE).alias("max_price_eur_per_mwh"),
            F.stddev_pop(PRICE).alias("price_volatility_eur_per_mwh"),
            F.count(F.when(F.col(PRICE) < RULES["negative_price_below"], 1)).alias(
                "negative_interval_count"
            ),
            F.count(PRICE).alias("interval_count"),
            F.avg("balancing_price_eur_per_mwh").alias(
                "mean_balancing_price_eur_per_mwh"
            ),
            F.count("balancing_price_eur_per_mwh").alias("balancing_interval_count"),
        )
        .withColumn(
            "price_spread_eur_per_mwh",
            F.col("max_price_eur_per_mwh") - F.col("min_price_eur_per_mwh"),
        ),
        SOURCE,
        RID,
    ),
    ["market_area_code", "local_date"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- market_timeseries (price window with balance and exchange)
renewable = F.col("carrier_key").isin(RULES["renewable_carriers"])
balance_window = balance.groupBy(*WINDOW).agg(
    F.sum(F.when(KIND == "generation", ENERGY)).alias("generation_mwh"),
    F.sum(F.when((KIND == "generation") & renewable, ENERGY)).alias(
        "renewable_generation_mwh"
    ),
    F.sum(F.when(KIND == "consumption", ENERGY)).alias("consumption_mwh"),
    F.sum(F.when(KIND == "residual_load", ENERGY)).alias("residual_load_mwh"),
    F.sum(F.when(KIND == "pumped_storage_consumption", ENERGY)).alias(
        "pumped_storage_consumption_mwh"
    ),
)
exchange_window = (
    exchange.filter(F.col("counterpart_area_code") == RULES["all_neighbours"])
    .groupBy(*WINDOW)
    .agg(
        F.sum(F.when(KIND == "commercial_net_export", ENERGY)).alias(
            "commercial_net_export_mwh"
        ),
        F.sum(F.when(KIND == "commercial_net_import", ENERGY)).alias(
            "commercial_net_import_mwh"
        ),
        F.sum(F.when(KIND == "physical_flow", ENERGY)).alias("physical_net_import_mwh"),
    )
)
priced = market_price.select(
    *WINDOW,
    "local_date",
    PRICE,
    "balancing_price_eur_per_mwh",
    f"{PRICE}_outlier_five_sigma",
)
market_timeseries = window_join(
    window_join(priced, balance_window), exchange_window
).withColumn("renewable_share", ratio("renewable_generation_mwh", "generation_mwh"))
TABLES["market_timeseries"] = (add_provenance(market_timeseries, SOURCE, RID), WINDOW)

# COMMAND ----------

# DBTITLE 1,Transform -- price_formation_daily (price against load and renewables)
TABLES["price_formation_daily"] = (
    add_provenance(
        market_timeseries.groupBy(*DAY)
        .agg(
            F.avg(PRICE).alias("mean_price_eur_per_mwh"),
            F.sum("generation_mwh").alias("generation_mwh"),
            F.sum("renewable_generation_mwh").alias("renewable_generation_mwh"),
            F.sum("consumption_mwh").alias("consumption_mwh"),
            F.sum("residual_load_mwh").alias("residual_load_mwh"),
            F.sum("physical_net_import_mwh").alias("physical_net_import_mwh"),
            F.corr(PRICE, "residual_load_mwh").alias("price_residual_load_correlation"),
            F.corr(PRICE, "renewable_share").alias("price_renewable_share_correlation"),
            F.count(PRICE).alias("interval_count"),
        )
        .withColumn(
            "renewable_share", ratio("renewable_generation_mwh", "generation_mwh")
        ),
        SOURCE,
        RID,
    ),
    DAY,
)

# COMMAND ----------

# DBTITLE 1,Transform -- negative_price_event (one event per run of negative windows)
area_step = Window.partitionBy("market_area_code", "interval_seconds").orderBy(
    "observation_timestamp_utc"
)
epoch = F.col("observation_timestamp_utc").cast("long")
negative = (
    market_timeseries.filter(F.col(PRICE) < RULES["negative_price_below"])
    .withColumn(
        "_run",
        F.floor(epoch / F.col("interval_seconds")) - F.row_number().over(area_step),
    )
    .withColumn("_end", epoch + F.col("interval_seconds"))
)
TABLES["negative_price_event"] = (
    add_provenance(
        negative.groupBy("market_area_code", "interval_seconds", "_run")
        .agg(
            F.min("observation_timestamp_utc").alias("event_start_utc"),
            F.timestamp_seconds(F.max("_end")).alias("event_end_utc"),
            F.min("local_date").alias("local_date"),
            F.count(F.lit(1)).alias("interval_count"),
            F.min(PRICE).alias("min_price_eur_per_mwh"),
            F.avg(PRICE).alias("mean_price_eur_per_mwh"),
            F.sum("renewable_generation_mwh").alias("renewable_generation_mwh"),
            F.sum("generation_mwh").alias("generation_mwh"),
            F.sum("residual_load_mwh").alias("residual_load_mwh"),
        )
        .withColumn(
            "duration_hours",
            F.col("interval_count") * F.col("interval_seconds") / 3600.0,
        )
        .withColumn(
            "renewable_share", ratio("renewable_generation_mwh", "generation_mwh")
        )
        .drop("_run"),
        SOURCE,
        RID,
    ),
    ["market_area_code", "interval_seconds", "event_start_utc"],
)

# COMMAND ----------

# DBTITLE 1,Transform -- cross_border_trade (one row per border and window)
price_key = WINDOW[:3]
border = ["market_area_code", "counterpart_area_code", *WINDOW[1:]]
area_price = market_price.select(
    *price_key, F.col(PRICE).alias("home_price_eur_per_mwh")
)
counterpart_price = market_price.select(
    F.col("market_area_code").alias("counterpart_area_code"),
    *price_key[1:],
    F.col(PRICE).alias("counterpart_price_eur_per_mwh"),
)
TABLES["cross_border_trade"] = (
    add_provenance(
        exchange.filter(F.col("counterpart_area_code") != RULES["all_neighbours"])
        .groupBy(*border, "local_date")
        .agg(
            F.sum(F.when(KIND == "commercial_export", ENERGY)).alias(
                "commercial_export_mwh"
            ),
            F.sum(F.when(KIND == "commercial_import", ENERGY)).alias(
                "commercial_import_mwh"
            ),
            F.sum(F.when(KIND == "physical_flow", ENERGY)).alias("physical_import_mwh"),
        )
        .join(area_price, price_key, "left")
        .join(counterpart_price, ["counterpart_area_code", *price_key[1:]], "left")
        .withColumn(
            "price_spread_eur_per_mwh",
            F.col("counterpart_price_eur_per_mwh") - F.col("home_price_eur_per_mwh"),
        ),
        SOURCE,
        RID,
    ),
    border,
)

# COMMAND ----------

# DBTITLE 1,Transform -- grid_intervention_daily (interventions per operator and day)
balancing_day = (
    market_price.filter(F.col("market_area_code") == HOME)
    .groupBy("local_date")
    .agg(F.avg("balancing_price_eur_per_mwh").alias("mean_balancing_price_eur_per_mwh"))
)
intervention_grain = [
    "instructing_market_area_code",
    "local_date",
    "direction",
    "reason",
]
TABLES["grid_intervention_daily"] = (
    add_provenance(
        grid_intervention.withColumn("local_date", F.to_date("event_start_project"))
        .groupBy(*intervention_grain)
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.sum("energy_mwh").alias("energy_mwh"),
            F.max("max_power_mw").alias("max_power_mw"),
            F.sum("duration_hours").alias("duration_hours"),
            sorted_values("carrier_key").alias("carrier_keys"),
        )
        .join(balancing_day, "local_date", "left"),
        "redispatch",
        RID,
    ),
    intervention_grain,
)

# COMMAND ----------

# DBTITLE 1,Transform -- forecast_accuracy (errors of the matched windows per day)
error = F.col("energy_mwh") - F.col("actual_energy_mwh")
forecast_grain = ["source_system", *DAY[:2], "forecast_scope", "interval_seconds"]
TABLES["forecast_accuracy"] = (
    add_provenance(
        forecast_actual.groupBy(*forecast_grain).agg(
            F.avg(F.abs(error)).alias("mean_absolute_error_mwh"),
            F.sqrt(F.avg(F.pow(error, 2))).alias("root_mean_squared_error_mwh"),
            F.avg(error).alias("mean_error_mwh"),
            F.avg(ratio(F.abs(error), F.abs(F.col("actual_energy_mwh")))).alias(
                "mean_absolute_percentage_error"
            ),
            F.sum("energy_mwh").alias("forecast_mwh"),
            F.sum("actual_energy_mwh").alias("actual_mwh"),
            F.count(F.lit(1)).alias("interval_count"),
            F.count("actual_energy_mwh").alias("matched_interval_count"),
            F.count(F.when(F.col("semantic_status") == "disputed", 1)).alias(
                "disputed_interval_count"
            ),
        ),
        SOURCE,
        RID,
    ),
    forecast_grain,
)

# COMMAND ----------

# DBTITLE 1,Transform -- metric_definition (one row per energy metric)
TABLES["metric_definition"] = (
    add_provenance(
        spark.createDataFrame(
            METRICS,
            "metric_key string, definition string, formula string, "
            "source_mart string, grain string",
        ),
        SOURCE,
        RID,
    ),
    ["metric_key"],
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
