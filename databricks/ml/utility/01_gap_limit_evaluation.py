# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GAP LIMIT EVALUATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** mask known stretches of each series, refill them causally with each method
# MAGIC and compare against a climatological baseline, then write the final gap
# MAGIC limits. Run by the owner.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../_ml_common

# COMMAND ----------

# DBTITLE 1,ML inspection library
# MAGIC %run ../_ml_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
ECO = "energy"
SOURCE = "dwd"
COMPONENT = "ml/utility/gap_limit_evaluation"
TRAIN_START, TRAIN_END = SPLIT_CALENDARS["energy_daily"]["train"]
MASK_SHARE = 10  # one block in ten is masked

# variable -> (Gold column expression, circular)
_WIND = (
    "coalesce(filter(wind__readings, r -> r.statistic = 'mean')[0], wind__readings[0])"
)
HOURLY_VARIABLES = {
    "air_temperature": ("temperature__air_temperature_degc", False),
    "dew_point_temperature": ("temperature__dew_point_temperature_degc", False),
    "relative_humidity": ("humidity__relative_humidity_percent", False),
    "pressure_station": ("pressure__pressure_station_hpa", False),
    "pressure_sea_level": ("pressure__pressure_sea_level_hpa", False),
    "wind_speed": (f"{_WIND}.wind_speed_m_per_s", False),
    "wind_direction": (f"{_WIND}.wind_direction_degrees", True),
    "cloud_cover": ("cloud__cloud_cover_total_percent", False),
    "visibility": ("visibility__visibility_m", False),
    "soil_temperature": ("soil_temperature__soil_temperature_5cm_degc", False),
}
HOURLY = {"step": 3600, "block": 24, "period": 72, "candidates": [1, 2, 3, 6, 12, 24]}
QUARTER_HOUR = {"step": 900, "block": 16, "period": 48, "candidates": [1, 2, 4, 8, 16]}
METHODS = ("locf", "locf_diurnal", "linear_extrapolation")

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Read Gold weather observations for the training calendar
weather = read_gold("weather_observation", source=SOURCE).filter(
    (F.col("origin_source_system") == "dwd")
    & (F.col("interval_seconds") == 3600)
    & F.col("local_date").between(TRAIN_START, TRAIN_END)
)

# COMMAND ----------


# DBTITLE 1,Helper -- evaluate one series
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


# COMMAND ----------


# DBTITLE 1,Helper -- choose the final limit from the per-offset errors
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

# DBTITLE 1,Evaluate every hourly variable
_results = []
for variable, (expr, circular) in HOURLY_VARIABLES.items():
    series = weather.select(
        "location_key",
        F.col("observation_timestamp_utc").alias("ts"),
        F.expr(expr).alias("value"),
    )
    pdf = evaluate_series(
        series, ts="ts", keys=["location_key"], spec=HOURLY, circular=circular
    )
    cap_steps = GAP_LIMIT_CAPS_HOURS[variable]
    best = choose_limit(pdf, spec=HOURLY, cap_steps=cap_steps)
    _results.append(
        (
            variable,
            cap_steps,
            best["limit_steps"],
            best["method"],
            best["score"],
            best["baseline"],
        )
    )
    print(
        f"{variable}: cap {cap_steps} h -> limit {best['limit_steps']} h ({best['method']})"
    )

# COMMAND ----------

# DBTITLE 1,Evaluate the SMARD quarter-hour series
_q0, _q1 = (
    SPLIT_CALENDARS["quarter_hour"]["train"][0],
    SPLIT_CALENDARS["quarter_hour"]["train"][1],
)
_qh = (
    read_gold("energy_balance_component", source="smard")
    .filter(
        (F.col("component_kind") == "consumption")
        & (F.col("market_area_code") == "de_lu")
        & (F.col("interval_seconds") == 900)
        & F.col("local_date").between(_q0, _q1)
    )
    .select(
        "market_area_code",
        F.col("interval_start_utc").alias("ts"),
        F.col("energy_mwh").alias("value"),
    )
)
_pdf = evaluate_series(
    _qh, ts="ts", keys=["market_area_code"], spec=QUARTER_HOUR, circular=False
)
_cap_steps = int(
    GAP_LIMIT_CAPS_HOURS["smard_quarter_hour"] * 3600 / QUARTER_HOUR["step"]
)
_best = choose_limit(_pdf, spec=QUARTER_HOUR, cap_steps=_cap_steps)
_results.append(
    (
        "smard_quarter_hour",
        GAP_LIMIT_CAPS_HOURS["smard_quarter_hour"],
        int(_best["limit_steps"] * QUARTER_HOUR["step"] / 3600),
        _best["method"],
        _best["score"],
        _best["baseline"],
    )
)
print(
    f"smard_quarter_hour: cap {_cap_steps} steps -> limit {_best['limit_steps']} steps ({_best['method']})"
)

# COMMAND ----------

# DBTITLE 1,Add the variables that are never filled
for variable in NEVER_FILLED:
    _results.append((variable, 0, 0, "never_filled", None, None))

# COMMAND ----------

# DBTITLE 1,Write the gap limits table
_rows = [
    (
        v,
        int(cap),
        int(lim),
        m,
        None if s is None else float(s),
        None if b is None else float(b),
        now_utc(),
    )
    for v, cap, lim, m, s, b in _results
]
_df = spark.createDataFrame(_rows, REGISTRY_DDL["gap_limits"])
replace_rows(_df, "gap_limits", ecosystem=ECO, predicate="variable IS NOT NULL")

# COMMAND ----------

# DBTITLE 1,Summary
for v, cap, lim, m, s, b in _results:
    print(f"{v:28s} cap={cap:>3} final={lim:>3} method={m:22s} mae={s} baseline={b}")
