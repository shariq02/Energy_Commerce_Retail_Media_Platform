# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ML ENERGY -- SITE, UNIT AND SENSOR DATASETS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** build the Honda forecast and anomaly datasets (family `honda`) and
# MAGIC the unit datasets (family `unit`: survival, capacity additions, redispatch
# MAGIC matching, combined-cycle plant, weak supervision). Kept tables: the Honda
# MAGIC increment target, the filled hub heights and the gap limits (rows of the
# MAGIC imputer registry). For each dataset: the contract, the split, the fit and the
# MAGIC family rows. Run after the market notebook (the calendar table is read).

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
SOURCE = "mastr"
COMPONENT = "ml/energy/unit_and_site"
RESOLUTIONS = (3600, 900)
HONDA_LAG_DAYS, HONDA_WEATHER_LAG_DAYS = [2, 3, 7], [2]
MIGRATION_DATE = "2019-06-01"
TRAIN_END = SPLIT_CALENDARS["energy_daily"]["train"][1]
HUB_FEATURES = [
    "capacity_net_kw",
    "rotor_diameter_m",
    "commissioning_year",
    "is_offshore_int",
    "manufacturer_code",
]
HUB_GROUP = ["is_offshore_int", "commissioning_year"]
# gap-limit evaluation: variable -> (Gold column expression, circular)
_WIND = "coalesce(wind_mean_speed_m_per_s, wind_instant_speed_m_per_s, "
_WIND += "wind_unspecified_speed_m_per_s)"
_DIRECTION = "coalesce(wind_mean_direction_degrees, wind_instant_direction_degrees, "
_DIRECTION += "wind_unspecified_direction_degrees)"
HOURLY_VARIABLES = {
    "air_temperature": ("air_temperature_degc", False),
    "dew_point_temperature": ("air_temperature_dew_point_degc", False),
    "relative_humidity": ("relative_humidity_percent", False),
    "pressure_station": ("pressure_station_hpa", False),
    "pressure_sea_level": ("pressure_sea_level_hpa", False),
    "wind_speed": (_WIND, False),
    "wind_direction": (_DIRECTION, True),
    "cloud_cover": ("cloud_cover_total_percent", False),
    "visibility": ("visibility_m", False),
    "soil_temperature": ("soil_temperature_5cm_degc", False),
}
HOURLY = {"step": 3600, "block": 24, "period": 72, "candidates": [1, 2, 3, 6, 12, 24]}
QUARTER_HOUR = {"step": 900, "block": 16, "period": 48, "candidates": [1, 2, 4, 8, 16]}
METHODS = ("locf", "locf_diurnal", "linear_extrapolation")
MASK_SHARE = 10  # one block in ten is masked
FRAMES, BLOCKS = {}, {}

# COMMAND ----------

# DBTITLE 1,Run identity and session time zone
rid = ml_run_id()
set_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Gold -- units, connection points, readings, events, sensors
units_all = read_gold("generation_unit", source=SOURCE)
connection_points = read_gold("grid_connection_point", source=SOURCE)
readings = read_gold("channel_reading", source="honda_iot").withColumn(
    "interval_start_utc", F.col("observation_timestamp_utc")
)
weather = read_gold("weather_observation", source="dwd")
events = read_gold("grid_intervention_event", source="redispatch")
sensors = read_gold("plant_sensor_observation", source="power_plant_ccpp")
coverage = read_shared_conformed("series_coverage")
wind_units_operating = wind_units(units_all).filter(
    F.col("lifecycle_state").isin(*OPERATING_STATES)
    & F.col("commissioning_date").isNotNull()
)

# COMMAND ----------

# DBTITLE 1,Hub height fill -- a kept table (small wind unit table, three methods)
import numpy as np
import pandas as pd

pdf = wind_units_operating.select(
    "unit_id",
    "hub_height_m",
    "capacity_net_kw",
    "rotor_diameter_m",
    "manufacturer",
    F.year("commissioning_date").alias("commissioning_year"),
    F.to_date("commissioning_date").alias("commissioning_day"),
    F.col("is_offshore").cast("int").alias("is_offshore_int"),
).toPandas()
assert len(pdf) < 200_000, "hub height fill is for the small wind unit table only"
pdf["manufacturer_code"] = pdf["manufacturer"].astype("category").cat.codes
_cut = pd.Timestamp(TRAIN_END).date()
fit_known = pdf[
    pdf["hub_height_m"].notna() & (pdf["commissioning_day"] <= _cut)
].reset_index(drop=True)
to_fill = pdf[pdf["hub_height_m"].isna()].reset_index(drop=True)
assert len(fit_known) > 0, "no units with a known hub height in the fit population"
hub_scores = static_impute_scores(fit_known, "hub_height_m", HUB_FEATURES, HUB_GROUP)
best_method = min(hub_scores, key=hub_scores.get)
print(f"{len(fit_known)} known, {len(to_fill)} to fill:", hub_scores, "->", best_method)
filled = pdf.copy()
if len(to_fill):
    pred = static_impute_predict(
        fit_known, to_fill, "hub_height_m", HUB_FEATURES, best_method, HUB_GROUP
    )
    filled.loc[filled["hub_height_m"].isna(), "hub_height_m"] = np.asarray(pred, float)
filled["hub_height_was_imputed"] = pdf["hub_height_m"].isna()
filled["hub_height_method"] = np.where(
    filled["hub_height_was_imputed"], best_method, "observed"
)
hub_height = spark.createDataFrame(
    filled[["unit_id", "hub_height_m", "hub_height_was_imputed", "hub_height_method"]]
    .rename(columns={"hub_height_m": "hub_height_m_filled"})
    .astype({"unit_id": str})
)
write_stored_table(
    hub_height,
    "features_hub_height_fill",
    ECO,
    key_cols=["unit_id"],
    source=SOURCE,
    component=f"{COMPONENT}/hub_height_fill",
    rid=rid,
    section="features",
)
record_imputer(
    "zone_weather",
    ECO,
    fold_id=-1,
    rows=[
        (
            "hub_height_m",
            best_method,
            {
                "scores": hub_scores,
                "features": HUB_FEATURES,
                "group": HUB_GROUP,
                "fit_end": TRAIN_END,
            },
        )
    ],
    fit_rows=len(fit_known),
)

# COMMAND ----------

# DBTITLE 1,Honda -- the increment target (a kept table) and the site features
_candidates = readings.filter(
    (F.col("subsystem") == "electricity")
    & (F.col("value_kind") == "increment")
    & F.col("interval_seconds").isin(*RESOLUTIONS)
)
target_honda = (
    _candidates.filter(F.col("value").isNotNull())
    .select(
        "location_key",
        "channel",
        "interval_seconds",
        "interval_start_utc",
        "local_date",
        F.col("value").alias("target_increment"),
        "unit",
        "sign_convention",
    )
    .withColumn("provenance_tier", F.lit("sourced"))
)
write_stored_table(
    target_honda,
    "target_honda_increment",
    ECO,
    key_cols=["location_key", "channel", "interval_seconds", "interval_start_utc"],
    source="honda_iot",
    component=f"{COMPONENT}/target_honda_increment",
    rid=rid,
    section="targets",
    target_col="target_increment",
    blocks=[
        (
            "dropped_rows",
            markdown_table(
                ["rows_dropped"],
                [(_candidates.count() - target_honda.count(),)],
            ),
        )
    ],
)
_increments = _candidates.select(
    "location_key",
    "channel",
    "interval_seconds",
    "interval_start_utc",
    "local_date",
    F.col("value").alias("increment"),
)
_site_weather = (
    weather.filter(F.col("source_system") == "honda_iot")
    .select(
        "location_key",
        F.date_trunc("hour", "observation_timestamp_utc").alias("hour_start"),
        "air_temperature_degc",
        "relative_humidity_percent",
    )
    .groupBy("location_key", "hour_start")
    .agg(
        F.avg("air_temperature_degc").alias("air_temperature_degc"),
        F.avg("relative_humidity_percent").alias("relative_humidity_percent"),
    )
)
_lagged = add_timestamp_lags(
    _increments,
    keys=["location_key", "channel", "interval_seconds"],
    ts_col="interval_start_utc",
    cols=["increment"],
    lag_days=HONDA_LAG_DAYS,
).drop("increment")
_weather_lag = add_timestamp_lags(
    _site_weather,
    keys=["location_key"],
    ts_col="hour_start",
    cols=["air_temperature_degc", "relative_humidity_percent"],
    lag_days=HONDA_WEATHER_LAG_DAYS,
)
_weather_lag = _weather_lag.select(
    "location_key", "hour_start", *[c for c in _weather_lag.columns if "_lag" in c]
)
_local = F.from_utc_timestamp("interval_start_utc", PROJECT_TIMEZONE)
honda_site = (
    _lagged.withColumn("hour_start", F.date_trunc("hour", "interval_start_utc"))
    .join(_weather_lag, ["location_key", "hour_start"], "left")
    .drop("hour_start")
    .withColumn("hour_of_day", F.hour(_local))
    .withColumn("day_of_week", F.dayofweek(_local))
    .withColumn("month", F.month(_local))
    .withColumn("as_of_ts", F.col("interval_start_utc"))
    .join(
        read_ml("features_calendar_market_area", ecosystem=ECO)
        .filter(F.col("market_area_code") == "de_lu")
        .select("local_date", "is_holiday", "is_bridge_day"),
        "local_date",
        "left",
    )
)

# COMMAND ----------

# DBTITLE 1,Survival -- wind units at risk in the window, and their static features
_w0 = F.lit(SURVIVAL_WINDOW_START).cast("date")
_censor = F.lit(SURVIVAL_CENSOR_DATE).cast("date")
_comm = F.to_date("commissioning_date")
_dec = F.to_date("final_decommissioning_date")
_at_risk = wind_units_operating.filter(
    (_comm <= _censor) & (_dec.isNull() | (_dec >= _w0))
)
_event = _dec.isNotNull() & (_dec <= _censor)
_stop = F.when(_event, _dec).otherwise(_censor)
target_survival = _at_risk.select(
    "unit_id",
    _comm.alias("commissioning_date"),
    F.greatest(_comm, _w0).alias("entry_date"),
    _stop.alias("stop_date"),
    _event.alias("target_event"),
    (F.datediff(F.greatest(_comm, _w0), _comm) / 365.25).alias("entry_years"),
    (F.datediff(_stop, _comm) / 365.25).alias("target_duration_years"),
    (_comm < _w0).alias("left_truncated"),
).withColumn("provenance_tier", F.lit("rule_derived"))
_before = wind_units_operating.filter(_dec.isNotNull() & (_dec < _w0))
BLOCKS["survival"] = [
    (
        "excluded_before_window",
        markdown_table(
            ["decommissioning_year", "units"],
            [
                (r["y"], r["n"])
                for r in _before.groupBy(F.year(_dec).alias("y"))
                .agg(F.count("*").alias("n"))
                .orderBy("y")
                .collect()
            ],
        ),
    )
]
_zones = unit_zone_map(wind_units_operating, connection_points, zone_code_frame())
unit_static = (
    wind_units_operating.select(
        "unit_id",
        "capacity_net_kw",
        "capacity_gross_kw",
        "rotor_diameter_m",
        "manufacturer",
        "model_designation",
        "latitude",
        "longitude",
        "federal_state",
        "operator_id",
        "is_offshore",
        F.year("commissioning_date").alias("commissioning_year"),
        F.to_date("commissioning_date").alias("commissioning_date"),
    )
    .join(
        _zones.select(
            "unit_id",
            F.col("market_area_code").alias("control_zone_code"),
            "zone_status",
        ),
        "unit_id",
        "left",
    )
    .join(
        hub_height.select("unit_id", "hub_height_m_filled", "hub_height_was_imputed"),
        "unit_id",
        "left",
    )
)

# COMMAND ----------

# DBTITLE 1,Capacity additions and redispatch matching
_operating = units_all.filter(
    F.col("lifecycle_state").isin(*OPERATING_STATES)
    & F.col("commissioning_date").isNotNull()
    & F.col("capacity_net_kw").isNotNull()
    & F.col("carrier_key").isNotNull()
)
target_capacity = (
    _operating.groupBy(
        "carrier_key",
        F.trunc("commissioning_date", "month").alias("commissioning_month"),
    )
    .agg(
        (F.sum("capacity_net_kw") / 1000).alias("target_capacity_added_mw"),
        F.count("*").alias("units_commissioned"),
    )
    .withColumn("provenance_tier", F.lit("constructed"))
)
_asset_events = events.filter(F.col("affected_asset_text").isNotNull())
target_match = (
    _asset_events.groupBy("affected_asset_text")
    .agg(
        F.first("affected_unit_match_name", ignorenulls=True).alias(
            "matched_unit_name"
        ),
        F.first("affected_unit_match_confidence", ignorenulls=True).alias(
            "match_confidence"
        ),
    )
    .withColumn(
        "target_match_tier", F.coalesce(F.col("match_confidence"), F.lit("unmatched"))
    )
    .withColumn("provenance_tier", F.lit("rule_derived"))
)
match_features = (
    _asset_events.groupBy("affected_asset_text")
    .agg(
        F.count("*").alias("event_count"),
        F.min(F.to_date("event_start_project")).alias("first_event_date"),
        F.max(F.to_date("event_start_project")).alias("last_event_date"),
        F.max("max_power_mw").alias("max_power_mw"),
        F.first("primary_energy_type", ignorenulls=True).alias("primary_energy_type"),
        F.countDistinct("direction").alias("direction_count"),
    )
    .withColumn("text_length", F.length("affected_asset_text"))
    .withColumn("token_count", F.size(F.split(F.trim("affected_asset_text"), r"\s+")))
    .withColumn("has_digit", F.col("affected_asset_text").rlike(r"[0-9]"))
)
BLOCKS["redispatch_matching"] = [
    (
        "dropped_rows",
        markdown_table(
            ["rows_dropped"],
            [
                (
                    _asset_events.select("affected_asset_text").distinct().count()
                    - target_match.count(),
                )
            ],
        ),
    )
]

# COMMAND ----------

# DBTITLE 1,Combined-cycle plant and weak supervision labels
_five = [
    "ambient_temperature_degc",
    "exhaust_vacuum_cm_of_mercury",
    "ambient_pressure_mbar",
    "relative_humidity_percent",
    "net_electrical_output_mw",
]
_ccpp_rows = sensors.filter(
    F.col("net_electrical_output_mw").isNotNull()
).dropDuplicates(_five)
# target and features come from one plan: the repeat removal picks the same rows
ccpp_frame = _ccpp_rows.select(
    "sample_key",
    F.col("net_electrical_output_mw").alias("target_net_output_mw"),
    (F.col("relative_humidity_percent") > 100).alias("humidity_above_100_flag"),
    F.lit("sourced").alias("provenance_tier"),
    "ambient_temperature_degc",
    "exhaust_vacuum_cm_of_mercury",
    "ambient_pressure_mbar",
    "relative_humidity_percent",
)
BLOCKS["ccpp"] = [
    (
        "dropped_rows",
        markdown_table(["rows_dropped"], [(sensors.count() - ccpp_frame.count(),)]),
    )
]
_reconciliation = coverage.filter(F.col("record_kind") == "reconciliation").select(
    "location_key", "parameter_source_code", "reconciliation_status"
)
_has_gap = (
    coverage.filter(F.col("record_kind") == "missing_value_period")
    .select("location_key", "parameter_source_code")
    .distinct()
    .withColumn("_g", F.lit(True))
)
_series = _reconciliation.join(
    _has_gap, ["location_key", "parameter_source_code"], "left"
)
_entity = F.concat_ws("|", "location_key", "parameter_source_code")
_label_functions = [
    _series.select(
        F.lit("series").alias("entity_type"),
        _entity.alias("entity_id"),
        F.lit("reconciliation_status").alias("lf_name"),
        F.when(F.col("reconciliation_status").isin("matched", "reported_only"), "gap")
        .otherwise("no_gap")
        .alias("lf_label"),
    ),
    _series.select(
        F.lit("series").alias("entity_type"),
        _entity.alias("entity_id"),
        F.lit("declared_gap_present").alias("lf_name"),
        F.when(F.col("_g"), "gap").otherwise("no_gap").alias("lf_label"),
    ),
    # a unit id repeats across unit types: the Gold grain is (unit_type, unit_id)
    units_all.filter(F.col("registration_date").isNotNull()).select(
        F.lit("unit").alias("entity_type"),
        F.concat_ws("|", "unit_type", "unit_id").alias("entity_id"),
        F.lit("registration_era").alias("lf_name"),
        F.when(
            F.col("registration_date") >= F.lit(MIGRATION_DATE).cast("date"),
            "post_migration",
        )
        .otherwise("pre_migration")
        .alias("lf_label"),
    ),
]
target_weak = _label_functions[0]
for _part in _label_functions[1:]:
    target_weak = target_weak.unionByName(_part)
target_weak = target_weak.withColumn("provenance_tier", F.lit("weak"))
_wide = (
    target_weak.filter(F.col("entity_type") == "series")
    .groupBy("entity_id")
    .pivot("lf_name", ["reconciliation_status", "declared_gap_present"])
    .agg(F.first("lf_label"))
)
_both = _wide.filter(
    F.col("reconciliation_status").isNotNull()
    & F.col("declared_gap_present").isNotNull()
)
BLOCKS["weak_supervision"] = [
    (
        "labelling_function_report",
        markdown_table(
            ["lf_name", "labelled_entities"],
            [
                (r["lf_name"], r["n"])
                for r in target_weak.groupBy("lf_name")
                .agg(F.count("*").alias("n"))
                .collect()
            ]
            + [
                (
                    "conflict_rate_series_functions",
                    f"{_both.filter(F.col('reconciliation_status') != F.col('declared_gap_present')).count()}/{_both.count()}",
                )
            ],
        ),
    )
]

# COMMAND ----------

# DBTITLE 1,Solar and storage buildout, and physical unit decommissioning
BUILDOUT_TYPES = ["solar", "electricity_storage"]
PHYSICAL_TYPES = ["wind", "verbrennung", "biomasse", "wasser", "geothermie_gsgk"]
BUILDOUT_FROM, UNIT_YEARS = "2018-01-01", (2010, 2025)
REGISTRATION_LAG_MONTHS = 3
PLACEHOLDER_DATE, PLACEHOLDER_YEAR = "1900-01-01", 2100
_commissioned = F.to_date("commissioning_date")
_decommissioned = F.to_date("final_decommissioning_date")
# a placeholder date (1899 or the year 9999) is null: its unit has no age
_commissioning_year = F.year(
    F.when(_commissioned > F.lit(PLACEHOLDER_DATE), _commissioned)
)
_decommissioning_year = F.year(
    F.when(F.year(_decommissioned) < PLACEHOLDER_YEAR, _decommissioned)
)
# buildout: the months after the data end minus the registration lag are left out
_register_end = (
    units_all.filter(F.col("unit_type").isin(*BUILDOUT_TYPES))
    .agg(F.max(F.to_date("registration_date")))
    .first()[0]
)
_last_month = (
    spark.range(1)
    .select(
        F.add_months(
            F.trunc(F.lit(str(_register_end)).cast("date"), "month"),
            -REGISTRATION_LAG_MONTHS,
        )
    )
    .first()[0]
)
_months = spark.range(1).select(
    F.explode(
        F.sequence(
            F.lit(BUILDOUT_FROM).cast("date"),
            F.lit(str(_last_month)).cast("date"),
            F.expr("interval 1 month"),
        )
    ).alias("month_start")
)
_buildout_units = units_all.filter(F.col("unit_type").isin(*BUILDOUT_TYPES))
_cells = (
    _buildout_units.filter(F.col("federal_state").isNotNull())
    .select("federal_state", "unit_type")
    .distinct()
    .crossJoin(_months)
)
_added = (
    _buildout_units.filter(_commissioned >= F.lit(BUILDOUT_FROM).cast("date"))
    .groupBy(
        "federal_state",
        "unit_type",
        F.trunc("commissioning_date", "month").alias("month_start"),
    )
    .agg(F.count("*").alias("target_units_added"))
)
_cell_window = Window.partitionBy("federal_state", "unit_type").orderBy("month_start")
_filled = _cells.join(_added, ["federal_state", "unit_type", "month_start"], "left")
_filled = _filled.withColumn(
    "target_units_added", F.coalesce(F.col("target_units_added"), F.lit(0))
)
# lags of three, six and twelve months, named by their days; the registration lag is 31
# days on average and 86 days at the 90th percentile
target_buildout = _filled.select(
    "federal_state",
    "unit_type",
    "month_start",
    "target_units_added",
    F.lag("target_units_added", 3).over(_cell_window).alias("units_added_lag90d"),
    F.lag("target_units_added", 6).over(_cell_window).alias("units_added_lag180d"),
    F.lag("target_units_added", 12).over(_cell_window).alias("units_added_lag365d"),
    F.avg("target_units_added")
    .over(_cell_window.rowsBetween(-14, -3))
    .alias("units_added_mean_12m_lag90d"),
    F.month("month_start").alias("month_of_year"),
    F.year("month_start").alias("year"),
    (F.year("month_start") >= 2023).alias("after_registration_wave"),
).withColumn("provenance_tier", F.lit("constructed"))
BLOCKS["solar_storage_buildout"] = [
    ("last_complete_month", markdown_table(["month"], [(str(_last_month),)]))
]
# decommissioning: one row per unit and calendar year until the year it ends
_spans = (
    units_all.filter(F.col("unit_type").isin(*PHYSICAL_TYPES))
    .select(
        "unit_type",
        "unit_id",
        "capacity_net_kw",
        "federal_state",
        "generation_technology",
        _commissioning_year.alias("commissioning_year"),
        _decommissioning_year.alias("decommissioning_year"),
    )
    .filter(
        F.col("commissioning_year").isNotNull()
        & (
            F.col("decommissioning_year").isNull()
            | (F.col("decommissioning_year") >= F.col("commissioning_year"))
        )
    )
    .withColumn(
        "first_year", F.greatest(F.col("commissioning_year"), F.lit(UNIT_YEARS[0]))
    )
    .withColumn(
        "last_year",
        F.least(
            F.coalesce(F.col("decommissioning_year"), F.lit(UNIT_YEARS[1])),
            F.lit(UNIT_YEARS[1]),
        ),
    )
    .filter(F.col("first_year") <= F.col("last_year"))
)
unit_years = (
    _spans.select(
        "unit_type",
        "unit_id",
        "capacity_net_kw",
        "federal_state",
        "generation_technology",
        "commissioning_year",
        "decommissioning_year",
        F.explode(F.sequence("first_year", "last_year")).alias("calendar_year"),
    )
    .withColumn("year_start", F.make_date("calendar_year", F.lit(1), F.lit(1)))
    .withColumn("age_years", F.col("calendar_year") - F.col("commissioning_year"))
    .withColumn("target_ended", F.col("calendar_year") == F.col("decommissioning_year"))
    .drop("decommissioning_year")
)
_physical = units_all.filter(F.col("unit_type").isin(*PHYSICAL_TYPES))
BLOCKS["unit_decommissioning"] = [
    (
        "excluded_units",
        markdown_table(
            ["reason", "units"],
            [
                (
                    "placeholder_or_missing_commissioning_date",
                    _physical.filter(_commissioning_year.isNull()).count(),
                ),
            ],
        ),
    )
]

# COMMAND ----------

# DBTITLE 1,Assemble the datasets (the join is in memory, no view)
_honda_keys = ["location_key", "channel", "interval_seconds", "interval_start_utc"]
FRAMES["honda_forecast"] = assembled_frame(
    target_honda, [(honda_site, _honda_keys, ["local_date"])]
)
FRAMES["honda_anomaly"] = assembled_frame(
    target_honda.filter(F.col("interval_seconds") == 3600),
    [(honda_site, _honda_keys, ["local_date"])],
)
FRAMES["survival"] = assembled_frame(
    target_survival, [(unit_static, ["unit_id"], ["commissioning_date"])]
)
FRAMES["capacity_additions"] = assembled_frame(
    target_capacity,
    [],
    extra={
        "year": F.year("commissioning_month"),
        "month": F.month("commissioning_month"),
    },
)
FRAMES["redispatch_matching"] = assembled_frame(
    target_match, [(match_features, ["affected_asset_text"])]
)
FRAMES["ccpp"] = ccpp_frame
FRAMES["weak_supervision"] = target_weak
FRAMES["solar_storage_buildout"] = assembled_frame(target_buildout, [])
FRAMES["unit_decommissioning"] = unit_years

# COMMAND ----------

# DBTITLE 1,Build the datasets -- contract, split, fit and the family rows
build_datasets(FRAMES, rid=rid, source=SOURCE, blocks=BLOCKS)

# COMMAND ----------


# DBTITLE 1,Gap limits -- helpers (one series, the final limit)
def evaluate_series(df, *, ts, keys, spec, circular):
    """Per-offset error of each method and of the climatological baseline.
    df has keys, ts and a column `value`."""
    step, block, period = spec["step"], spec["block"], spec["period"]
    grid = regular_grid(
        df.filter(F.col("value").isNotNull()), keys=keys, ts_col=ts, step_seconds=step
    )
    idx = (F.col(ts).cast("long") / step).cast("long")
    blk = F.floor(idx / period)
    pos = F.pmod(idx, F.lit(period))
    masked = (pos < block) & (
        F.pmod(F.xxhash64(*[F.col(k) for k in keys], blk), F.lit(MASK_SHARE)) == 0
    )
    base = (
        grid.withColumn("truth", F.col("value"))
        .withColumn("_masked", masked)
        .withColumn("offset", (pos + 1).cast("int"))
        .withColumn(
            "v", F.when(F.col("_masked"), F.lit(None)).otherwise(F.col("value"))
        )
    )
    clim = (
        base.filter(F.col("v").isNotNull())
        .groupBy(*keys, F.hour(ts).alias("hour_of_day"), F.month(ts).alias("month"))
        .agg(F.avg("v").alias("climatology_value"))
    )
    frames = []
    for m in METHODS:
        if circular and m != "locf":
            continue
        filled = causal_fill(
            base.select(*keys, ts, "v", "truth", "_masked", "offset"),
            keys=keys,
            ts_col=ts,
            value_col="v",
            cap_hours=block * step / 3600,
            method=m,
            climatology=clim if m == "locf_diurnal" else None,
        ).filter(F.col("_masked") & F.col("truth").isNotNull() & F.col("v").isNotNull())
        diff = F.col("v") - F.col("truth")
        err = F.abs(F.pmod(diff + 180, F.lit(360.0)) - 180) if circular else F.abs(diff)
        frames.append(
            filled.groupBy("offset")
            .agg(F.avg(err).alias("mae"), F.count("*").alias("n"))
            .withColumn("method", F.lit(m))
        )
    baseline = (
        base.filter(F.col("_masked") & F.col("truth").isNotNull())
        .withColumn("hour_of_day", F.hour(ts))
        .withColumn("month", F.month(ts))
        .join(clim, [*keys, "hour_of_day", "month"], "inner")
    )
    diff = F.col("climatology_value") - F.col("truth")
    err = F.abs(F.pmod(diff + 180, F.lit(360.0)) - 180) if circular else F.abs(diff)
    frames.append(
        baseline.groupBy("offset")
        .agg(F.avg(err).alias("mae"), F.count("*").alias("n"))
        .withColumn("method", F.lit("climatology"))
    )
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out.toPandas()


def choose_limit(pdf, *, spec, cap_steps):
    """Largest candidate <= cap whose cumulative error beats the baseline."""
    best = {"limit_steps": 0, "method": "never_filled", "score": None, "baseline": None}
    for limit in spec["candidates"]:
        if limit > cap_steps:
            continue
        sub = pdf[pdf["offset"] <= limit]
        base = sub[sub["method"] == "climatology"]
        if base.empty:
            continue
        base_mae = float((base["mae"] * base["n"]).sum() / base["n"].sum())
        scored = []
        for m, g in sub[sub["method"] != "climatology"].groupby("method"):
            scored.append((float((g["mae"] * g["n"]).sum() / g["n"].sum()), m))
        if not scored:
            continue
        mae, method = min(scored)
        if mae <= base_mae:
            best = {
                "limit_steps": limit,
                "method": method,
                "score": mae,
                "baseline": base_mae,
            }
    return best


# COMMAND ----------

# DBTITLE 1,Gap limits -- evaluate every hourly variable and the quarter-hour series
_train_start, _train_end = SPLIT_CALENDARS["energy_daily"]["train"]
_hourly = weather.filter(
    (F.col("source_system") == "dwd")
    & (F.col("interval_seconds") == 3600)
    & (F.col("interval_reference") == "clock")
    & F.col("local_date").between(_train_start, _train_end)
)
_results = []
for _variable, (_expr, _circular) in HOURLY_VARIABLES.items():
    _series_frame = _hourly.select(
        "location_key",
        F.col("observation_timestamp_utc").alias("ts"),
        F.expr(_expr).alias("value"),
    )
    _pdf = evaluate_series(
        _series_frame, ts="ts", keys=["location_key"], spec=HOURLY, circular=_circular
    )
    _cap_steps = GAP_LIMIT_CAPS_HOURS[_variable]
    _best = choose_limit(_pdf, spec=HOURLY, cap_steps=_cap_steps)
    _results.append((_variable, _cap_steps, _best["limit_steps"], _best))
    print(f"{_variable}: cap {_cap_steps} h -> limit {_best['limit_steps']} h")
_q0, _q1 = SPLIT_CALENDARS["quarter_hour"]["train"]
_qh = (
    read_gold("energy_component", source="smard")
    .filter(
        (F.col("component_group") == "balance")
        & (F.col("component_kind") == "consumption")
        & (F.col("market_area_code") == "de_lu")
        & (F.col("interval_seconds") == 900)
        & F.col("local_date").between(_q0, _q1)
    )
    .select(
        "market_area_code",
        F.col("observation_timestamp_utc").alias("ts"),
        F.col("energy_mwh").alias("value"),
    )
)
_pdf_qh = evaluate_series(
    _qh, ts="ts", keys=["market_area_code"], spec=QUARTER_HOUR, circular=False
)
_cap_qh = int(GAP_LIMIT_CAPS_HOURS["smard_quarter_hour"] * 3600 / QUARTER_HOUR["step"])
_best_qh = choose_limit(_pdf_qh, spec=QUARTER_HOUR, cap_steps=_cap_qh)
_results.append(
    (
        "smard_quarter_hour",
        GAP_LIMIT_CAPS_HOURS["smard_quarter_hour"],
        int(_best_qh["limit_steps"] * QUARTER_HOUR["step"] / 3600),
        _best_qh,
    )
)
record_imputer(
    GAP_LIMIT_DATASET,
    ECO,
    fold_id=-1,
    rows=[
        (
            variable,
            best["method"],
            {
                "cap_hours": cap,
                "final_limit_hours": limit,
                "score": best["score"],
                "baseline_score": best["baseline"],
            },
        )
        for variable, cap, limit, best in _results
    ],
)
for variable, cap, limit, best in _results:
    print(f"{variable:28s} cap={cap:>3} final={limit:>3} method={best['method']}")
