# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SILVER -- ENERGY OBSERVATION AND COMPONENTS (ENERGY)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** the Honda site meters and SMARD market series. `energy_observation` is
# MAGIC wide: one row per place and window with the thermal power, the meter registers
# MAGIC (with their forward increment) and the day-ahead price in columns.
# MAGIC `energy_component` has one row per component of an energy balance or forecast
# MAGIC (generation by carrier, consumption, residual load, forecast scope). Sign is
# MAGIC always native.

# COMMAND ----------

# DBTITLE 1,Semantic structures library
# MAGIC %run ../_semantic_common

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "energy"
COMPONENT = "silver/energy/03_energy_observation"
FINDINGS = "energy"
RID = run_id()

HONDA = "honda_iot"
SITE_ID = "honda_site"
KEY = ["frequency", "datetime_utc"]
CHANNELS = {
    "electricity": ["total", "PV", "CHP"],
    "heating": ["total", "CHP_heat", "CHP_elec"],
    "cooling": ["total", "cool_elec"],
}
STUCK_LOOKBACK_STEPS_BY_FREQ = {"1h": 9, "15min": 39, "1min": 599}
DUPLICATE_AGREEMENT_THRESHOLD = 0.99
_W = Window.partitionBy("frequency").orderBy(F.col("datetime_utc").cast("timestamp"))

# P channel -> (component_kind, carrier_code) of the electricity balance.
HONDA_BALANCE = {
    "electricity__total": ("net_total", None),
    "electricity__PV": ("generation", "photovoltaic"),
    "electricity__CHP": ("generation", "combined_heat_and_power"),
    "cooling__cool_elec": ("subsystem_consumption", None),
}
THERMAL = {
    "heating_total_w": "heating__total",
    "heating_combined_heat_and_power_heat_w": "heating__CHP_heat",
    "cooling_total_w": "cooling__total",
}

SMARD = "smard"
SMARD_TABLE = "smard_energy_timeseries"
SMARD_KEY = ["metric", "filter_id", "region", "resolution", "timestamp_utc"]
PRICE_METRIC = "day_ahead_prices"
FORECAST_PREFIX = "forecast_generation_"
DISPUTED_METRIC = "forecast_generation_photovoltaic"
SEMANTIC_ISSUE_REF = "smard.quality_rules.forecast_pv_sign_mirror"
# metric -> (component_kind, carrier_code); values are MWh per window.
SMARD_BALANCE = {
    **{
        f"generation_{c}": ("generation", c)
        for c in (
            "biomass",
            "hard_coal",
            "hydro",
            "lignite",
            "natural_gas",
            "nuclear",
            "offshore_wind",
            "onshore_wind",
            "other_conventional",
            "other_renewable",
            "photovoltaic",
            "pumped_storage",
        )
    },
    "total_power_consumption": ("consumption", None),
    "pumped_storage_consumption": ("pumped_storage_consumption", "pumped_storage"),
    "residual_load": ("residual_load", None),
}
NO_TEXT = F.lit(None).cast("string")
NO_FLAG = F.lit(None).cast("boolean")
NO_NUMBER = F.lit(None).cast("double")
RECONCILIATION = {}

# COMMAND ----------

# DBTITLE 1,Session time zone
ensure_utc_session()

# COMMAND ----------

# DBTITLE 1,Read Bronze -- Honda electricity, heating and cooling, deduplicated
kept = {}
for t, cols in CHANNELS.items():
    bt = f"honda_iot_{t}"
    bronze = read_bronze(bt)
    kept[t], _q = resolve_conflicts(
        bronze, ["measurement_type", *KEY], cols, bronze_table=bt
    )
    write_quarantine(_q.withColumn("source_system", F.lit(HONDA)), RID)
    RECONCILIATION[bt] = reconciliation_stats(bronze, kept[t], _q)


def _wide(measurement_type):
    """(frequency, datetime_utc) rows with `<table>__<channel>` doubles."""
    joined = None
    for t, cols in CHANNELS.items():
        part = (
            kept[t]
            .filter(F.col("measurement_type") == measurement_type)
            .select(*KEY, *[F.col(c).cast("double").alias(f"{t}__{c}") for c in cols])
        )
        joined = part if joined is None else joined.join(part, KEY, "full_outer")
    return joined


p_wide = _wide("p")
w_wide = _wide("w")
W_COLS = [c for c in w_wide.columns if c not in KEY]

# COMMAND ----------

# DBTITLE 1,Check -- heating P CHP_elec still mirrors electricity P CHP (else halt)
_pair = p_wide.filter(
    F.col("electricity__CHP").isNotNull() & F.col("heating__CHP_elec").isNotNull()
)
_agreement = _pair.filter(
    F.col("electricity__CHP") == F.col("heating__CHP_elec")
).count() / max(_pair.count(), 1)
print(f"CHP_elec mirror agreement: {_agreement:.6f}")
if _agreement < DUPLICATE_AGREEMENT_THRESHOLD:
    raise ValueError("heating P CHP_elec no longer mirrors electricity P CHP")

# COMMAND ----------

# DBTITLE 1,Helper -- place and window columns of a Honda row


def honda_window(df):
    """The site place and the window columns of a Honda row."""
    df = (
        df.withColumn("source_location_id", F.lit(SITE_ID))
        .withColumn("location_key", location_key(HONDA, "source_location_id"))
        .withColumn("market_area_code", NO_TEXT)
        .withColumn("observation_timestamp_native", F.col("datetime_utc"))
        .withColumn("time_basis", F.lit("utc"))
        .withColumn("utc_offset_hours", NO_NUMBER)
        .withColumn(
            "observation_timestamp_utc", F.col("datetime_utc").cast("timestamp")
        )
        .withColumn(
            "interval_seconds",
            lit_map(HONDA_INTERVAL_SECONDS)[F.col("frequency")].cast("int"),
        )
        .withColumn("interval_reference", F.lit("clock"))
        .withColumn("measurement_basis", F.lit("site_meter"))
        .withColumn("sign_convention", F.lit("native_generation_negative"))
        .withColumn("source_system", F.lit(HONDA))
        .withColumn("source_dataset", F.lit(HONDA))
    )
    return add_project_time(df)


# COMMAND ----------

# DBTITLE 1,Transform -- Honda thermal power and meter registers (wide, with flags)
# Thermal power (P): five-sigma and stuck-reading flags per column.
thermal = any_present(
    p_wide.select(*KEY, *[F.col(src).alias(dst) for dst, src in THERMAL.items()]),
    list(THERMAL),
)
thermal = add_5sigma_outlier_flags(thermal, list(THERMAL))
thermal = add_stuck_reading_flags(
    thermal, list(THERMAL), STUCK_LOOKBACK_STEPS_BY_FREQ, _W
)
for c in THERMAL:
    thermal = thermal.withColumnRenamed(
        f"_{c}_5sigma_outlier", f"{c}_outlier_five_sigma"
    )
    thermal = thermal.withColumnRenamed(
        f"_{c}_stuck_reading_flag", f"{c}_stuck_reading"
    )

# Meter registers (W) are monotone in magnitude (generation counts down); a
# violation is a drop in |reading|. Increment = next reading minus this one.
meter = w_wide
_ts = F.col("datetime_utc").cast("timestamp")
_next_gap_s = F.unix_timestamp(F.lead(_ts).over(_W)) - F.unix_timestamp(_ts)
_interval_s = lit_map(HONDA_INTERVAL_SECONDS)[F.col("frequency")]
for ch, (t, native) in HONDA_METER_CHANNELS.items():
    reading = F.col(f"{t}__{native}")
    meter = (
        meter.withColumn(f"{ch}_kwh", reading)
        .withColumn(f"{ch}_increment_kwh", F.lead(reading).over(_W) - reading)
        .withColumn(
            f"{ch}_increment_kwh_spans_gap",
            F.coalesce(
                F.col(f"{ch}_increment_kwh").isNotNull() & (_next_gap_s > _interval_s),
                F.lit(False),
            ),
        )
    )
meter_cols = [f"{ch}_kwh" for ch in HONDA_METER_CHANNELS]
for c in meter_cols:
    meter = meter.withColumn(
        f"{c}_magnitude_decrease",
        F.coalesce(F.abs(F.col(c)) < F.abs(F.lag(F.col(c)).over(_W)), F.lit(False)),
    )
meter = any_present(meter.drop(*W_COLS), meter_cols)
meter = add_5sigma_outlier_flags(meter, meter_cols)
meter = add_stuck_reading_flags(meter, meter_cols, STUCK_LOOKBACK_STEPS_BY_FREQ, _W)
for c in meter_cols:
    meter = meter.withColumnRenamed(f"_{c}_5sigma_outlier", f"{c}_outlier_five_sigma")
    meter = meter.withColumnRenamed(f"_{c}_stuck_reading_flag", f"{c}_stuck_reading")

# One row per (frequency, instant): thermal power and meters side by side.
honda_observation = thermal.join(meter, KEY, "full_outer").withColumn(
    "increment_derivation_rule",
    F.lit("next reading minus this reading, same frequency"),
)
honda_observation = honda_window(honda_observation)

# COMMAND ----------

# DBTITLE 1,Transform -- Honda electricity balance components (one row per channel)
p_flagged = add_5sigma_outlier_flags(p_wide, list(HONDA_BALANCE))
p_flagged = add_stuck_reading_flags(
    p_flagged, list(HONDA_BALANCE), STUCK_LOOKBACK_STEPS_BY_FREQ, _W
)
honda_components = None
for c, (kind, carrier) in HONDA_BALANCE.items():
    part = p_flagged.filter(F.col(c).isNotNull()).select(
        *KEY,
        F.lit("balance").alias("component_group"),
        F.lit(kind).alias("component_kind"),
        F.lit(None).cast("string").alias("forecast_scope"),
        F.lit(carrier).cast("string").alias("carrier_code"),
        F.lit(c.replace("__", ".")).alias("native_label"),
        NO_NUMBER.alias("energy_mwh"),
        F.col(c).alias("power_w"),
        F.lit("honda_iot_" + c.split("__")[0] + ".p").alias("source_series"),
        NO_TEXT.alias("source_filter_id"),
        NO_TEXT.alias("semantic_status"),
        NO_TEXT.alias("semantic_issue_ref"),
        F.col(f"_{c}_5sigma_outlier").alias("is_outlier_five_sigma"),
        F.col(f"_{c}_stuck_reading_flag").alias("is_stuck_reading"),
    )
    honda_components = (
        part if honda_components is None else honda_components.unionByName(part)
    )
honda_components = honda_window(honda_components)

# COMMAND ----------

# DBTITLE 1,Read Bronze -- SMARD time series, deduplicated, with the five-sigma flag
bronze_smard = read_bronze(SMARD_TABLE)
_kept, _q = resolve_conflicts(
    bronze_smard, SMARD_KEY, ["value"], bronze_table=SMARD_TABLE
)
write_quarantine(_q.withColumn("source_system", F.lit(SMARD)), RID)
RECONCILIATION[SMARD_TABLE] = reconciliation_stats(bronze_smard, _kept, _q)

# Five-sigma per series: day and quarter-hour differ ~96x in scale.
_series = F.concat_ws("|", "metric", "filter_id", "region", "resolution")
long_df = (
    _kept.withColumn("value", F.col("value").cast("double"))
    .filter(F.col("value").isNotNull())
    .withColumn("source_series", _series)
)
_stats = long_df.groupBy("source_series").agg(
    F.mean("value").alias("_mean"), F.stddev("value").alias("_sd")
)
long_df = (
    long_df.join(F.broadcast(_stats), "source_series")
    .withColumn(
        "_outlier",
        F.coalesce(
            F.abs(F.col("value") - F.col("_mean")) > 5 * F.col("_sd"), F.lit(False)
        ),
    )
    .drop("_mean", "_sd")
)


def smard_window(df):
    """The market-area place and the window columns of a SMARD row. A day window
    is the Europe/Berlin local day (23, 24 or 25 hours)."""
    utc = F.col("timestamp_utc").cast("timestamp")
    is_day = F.col("resolution") == "day"
    project = F.from_utc_timestamp(utc, PROJECT_TZ)
    next_day_utc = F.to_utc_timestamp(
        F.date_add(F.to_date(project), 1).cast("timestamp"), PROJECT_TZ
    )
    return (
        df.withColumn("source_system", F.lit(SMARD))
        .withColumn("source_dataset", F.lit(SMARD_TABLE))
        .withColumn("source_location_id", F.col("region"))
        .withColumn("location_key", NO_TEXT)
        .withColumn("market_area_code", lit_map(MARKET_AREA_CODES)[F.col("region")])
        .withColumn("observation_timestamp_native", F.col("timestamp_utc"))
        .withColumn("time_basis", F.lit("utc"))
        .withColumn("utc_offset_hours", NO_NUMBER)
        .withColumn("observation_timestamp_utc", utc)
        .withColumn("observation_timestamp_project", project)
        .withColumn("local_date", F.to_date(project))
        .withColumn(
            "interval_seconds",
            F.when(is_day, next_day_utc.cast("long") - utc.cast("long"))
            .otherwise(900)
            .cast("int"),
        )
        .withColumn(
            "interval_reference",
            F.when(is_day, F.lit("local_day_europe_berlin")).otherwise(F.lit("clock")),
        )
        .withColumn("measurement_basis", F.lit("market_statistic"))
        .withColumn("sign_convention", F.lit("native_as_published"))
    )


# COMMAND ----------

# DBTITLE 1,Transform -- SMARD price rows and balance and forecast component rows
price = smard_window(
    long_df.filter(F.col("metric") == PRICE_METRIC).select(
        "region",
        "resolution",
        "timestamp_utc",
        F.col("value").alias("price_eur_per_mwh"),
        F.col("_outlier").alias("price_eur_per_mwh_outlier_five_sigma"),
    )
)

_kind = lit_map({m: k for m, (k, _) in SMARD_BALANCE.items()})
_carrier = lit_map({m: c for m, (_, c) in SMARD_BALANCE.items() if c})
balance = long_df.filter(F.col("metric").isin(list(SMARD_BALANCE))).select(
    "region",
    "resolution",
    "timestamp_utc",
    F.lit("balance").alias("component_group"),
    _kind[F.col("metric")].alias("component_kind"),
    NO_TEXT.alias("forecast_scope"),
    _carrier[F.col("metric")].alias("carrier_code"),
    F.col("metric").alias("native_label"),
    F.col("value").alias("energy_mwh"),
    NO_NUMBER.alias("power_w"),
    NO_TEXT.alias("semantic_status"),
    NO_TEXT.alias("semantic_issue_ref"),
    "source_series",
    # A metric can legitimately publish under more than one filter id for the
    # same window; the id keeps two such rows two distinct components.
    F.col("filter_id").alias("source_filter_id"),
    F.col("_outlier").alias("is_outlier_five_sigma"),
)
_disputed = F.col("metric") == DISPUTED_METRIC
forecast = long_df.filter(F.col("metric").startswith(FORECAST_PREFIX)).select(
    "region",
    "resolution",
    "timestamp_utc",
    F.lit("forecast").alias("component_group"),
    NO_TEXT.alias("component_kind"),
    F.regexp_replace("metric", f"^{FORECAST_PREFIX}", "").alias("forecast_scope"),
    NO_TEXT.alias("carrier_code"),
    F.col("metric").alias("native_label"),
    F.col("value").alias("energy_mwh"),
    NO_NUMBER.alias("power_w"),
    F.when(_disputed, F.lit("disputed"))
    .otherwise(F.lit("confirmed"))
    .alias("semantic_status"),
    F.when(_disputed, F.lit(SEMANTIC_ISSUE_REF)).alias("semantic_issue_ref"),
    "source_series",
    F.col("filter_id").alias("source_filter_id"),
    F.col("_outlier").alias("is_outlier_five_sigma"),
)
smard_components = smard_window(balance.unionByName(forecast))

# COMMAND ----------

# DBTITLE 1,Transform -- the two sources in one observation table and one component table
KEYED = ["observation_timestamp_utc", "interval_seconds", "interval_reference"]


def finish(df, source, components):
    """Observation key from the grain, record id, provenance and conformance."""
    key = sha_key("source_system", "location_key", "market_area_code", *KEYED)
    df = df.withColumn("observation_key", key)
    if components:
        df = df.withColumn(
            "source_record_id",
            sha_key(
                "observation_key",
                "component_group",
                "component_kind",
                "forecast_scope",
                "carrier_code",
                "native_label",
                "source_series",
                "source_filter_id",
            ),
        )
    else:
        df = df.withColumn("source_record_id", F.col("observation_key"))
    df = add_semantic_provenance(df, source, None, RID)
    return conform(
        df, ENERGY_COMPONENT_COLUMNS if components else ENERGY_OBSERVATION_COLUMNS
    )


energy_observation = finish(honda_observation, HONDA, False).unionByName(
    finish(price, SMARD, False)
)
energy_component = finish(honda_components, HONDA, True).unionByName(
    finish(smard_components, SMARD, True)
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- energy_observation
reconciliation_blocks = [
    (f"dedup/conflict reconciliation -- {table}", dict_to_markdown_row(stats))
    for table, stats in RECONCILIATION.items()
]
write_inspect_export(
    energy_observation,
    "energy_observation",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=ENERGY_GRAIN,
    writer=write_semantic,
    extra_checks=lambda t: {
        **structure_extra_checks(t),
        "chp_elec_mirror_agreement": _agreement,
    },
    extra_blocks=reconciliation_blocks,
)

# COMMAND ----------

# DBTITLE 1,Write, inspect and export -- energy_component
write_inspect_export(
    energy_component,
    "energy_component",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    findings=FINDINGS,
    key_cols=["source_record_id"],
    writer=write_semantic,
    extra_checks=structure_extra_checks,
)
