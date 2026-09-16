# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SILVER -- HONDA ENERGY (ELECTRICITY / HEATING / COOLING)
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the six Honda IoT power (`_p`) and energy (`_w`) Bronze
# MAGIC tables into source-scoped Silver at their (frequency, datetime_utc) grain.
# MAGIC The channel columns keep their source names and their sign -- a negative
# MAGIC value is on-site generation / export, not an error. `electricity_p` runs
# MAGIC before `heating_p`: the CHP_elec pre-drop safeguard below reads
# MAGIC `honda_electricity_p` from Silver.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "honda_iot"
COMPONENT = "silver/energy/iot/01_honda_energy"
RID = run_id()

# Owner decision: drop heating_p.CHP_elec, no alias -- electricity_p.CHP is
# canonical. Pre-drop safeguard below re-checks the duplication first.
_DUPLICATE_AGREEMENT_THRESHOLD = 0.99

# Stuck-reading lookback per frequency, scaled to the same ~10-hour window
# profiling measured at 1h (9 steps back = 10 readings = 10h).
STUCK_LOOKBACK_STEPS_BY_FREQ = {"1h": 9, "15min": 39, "1min": 599}

# COMMAND ----------

# DBTITLE 1,Read Bronze -- honda_iot_electricity_p
_electricity_p_bronze = read_bronze("honda_iot_electricity_p")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- honda_iot_electricity_w
_electricity_w_bronze = read_bronze("honda_iot_electricity_w")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- honda_iot_heating_p
_heating_p_bronze = read_bronze("honda_iot_heating_p")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- honda_iot_heating_w
_heating_w_bronze = read_bronze("honda_iot_heating_w")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- honda_iot_cooling_p
_cooling_p_bronze = read_bronze("honda_iot_cooling_p")

# COMMAND ----------

# DBTITLE 1,Read Bronze -- honda_iot_cooling_w
_cooling_w_bronze = read_bronze("honda_iot_cooling_w")

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_p (cast)
electricity_p = _electricity_p_bronze.withColumn(
    "datetime_utc", F.col("datetime_utc").cast("timestamp")
)
for _c in electricity_p.columns:
    if _c not in ("frequency", "datetime_utc"):
        electricity_p = electricity_p.withColumn(_c, F.col(_c).cast("double"))

electricity_p_value_cols = [
    c for c in electricity_p.columns if c not in ("frequency", "datetime_utc")
]

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_p (no CHP_elec safeguard for this table)
_electricity_p_dup_agreement = None

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_p (5-sigma outlier flags)
_electricity_p_stat_exprs = []
for c in electricity_p_value_cols:
    _electricity_p_stat_exprs += [
        F.mean(c).alias(f"{c}__mean"),
        F.stddev(c).alias(f"{c}__sd"),
    ]
if electricity_p_value_cols:
    _electricity_p_stats = electricity_p.agg(*_electricity_p_stat_exprs).first()
    for c in electricity_p_value_cols:
        _mean_c, _sd_c = (
            _electricity_p_stats[f"{c}__mean"],
            _electricity_p_stats[f"{c}__sd"],
        )
        electricity_p = electricity_p.withColumn(
            f"_{c}_5sigma_outlier",
            F.when(
                F.lit(_sd_c).isNotNull() & (F.lit(_sd_c) > 0) & F.col(c).isNotNull(),
                F.abs(F.col(c) - F.lit(_mean_c)) > (F.lit(5.0) * F.lit(_sd_c)),
            ).otherwise(F.lit(False)),
        )

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_p (stuck-reading flags)
_electricity_p_w = Window.partitionBy("frequency").orderBy("datetime_utc")
for c in electricity_p_value_cols:
    _stuck_expr = F.lit(False)
    for _freq, _lag_n in STUCK_LOOKBACK_STEPS_BY_FREQ.items():
        _lag1 = F.lag(F.col(c), 1).over(_electricity_p_w)
        _lagN = F.lag(F.col(c), _lag_n).over(_electricity_p_w)
        _cond = (
            (F.col("frequency") == _freq)
            & F.col(c).isNotNull()
            & (F.col(c) == _lag1)
            & (F.col(c) == _lagN)
        )
        _stuck_expr = F.when(_cond, F.lit(True)).otherwise(_stuck_expr)
    electricity_p = electricity_p.withColumn(f"_{c}_stuck_reading_flag", _stuck_expr)

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_p (provenance)
electricity_p = electricity_p.withColumn(
    "_srid", sha_key(F.lit("honda_iot_electricity_p"), "frequency", "datetime_utc")
)
electricity_p = add_provenance(electricity_p, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- honda_electricity_p
write_silver(
    electricity_p, "honda_electricity_p", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect honda_electricity_p + export findings
_findings_blocks = inspect_table(
    electricity_p,
    "honda_electricity_p",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    extra_checks={"chp_elec_duplicate_agreement": _electricity_p_dup_agreement}
    if _electricity_p_dup_agreement is not None
    else None,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__honda_electricity_p",
    "honda_electricity_p",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_w (cast)
electricity_w = _electricity_w_bronze.withColumn(
    "datetime_utc", F.col("datetime_utc").cast("timestamp")
)
for _c in electricity_w.columns:
    if _c not in ("frequency", "datetime_utc"):
        electricity_w = electricity_w.withColumn(_c, F.col(_c).cast("double"))

electricity_w_value_cols = [
    c for c in electricity_w.columns if c not in ("frequency", "datetime_utc")
]

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_w (no CHP_elec safeguard for this table)
_electricity_w_dup_agreement = None

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_w (5-sigma outlier flags)
_electricity_w_stat_exprs = []
for c in electricity_w_value_cols:
    _electricity_w_stat_exprs += [
        F.mean(c).alias(f"{c}__mean"),
        F.stddev(c).alias(f"{c}__sd"),
    ]
if electricity_w_value_cols:
    _electricity_w_stats = electricity_w.agg(*_electricity_w_stat_exprs).first()
    for c in electricity_w_value_cols:
        _mean_c, _sd_c = (
            _electricity_w_stats[f"{c}__mean"],
            _electricity_w_stats[f"{c}__sd"],
        )
        electricity_w = electricity_w.withColumn(
            f"_{c}_5sigma_outlier",
            F.when(
                F.lit(_sd_c).isNotNull() & (F.lit(_sd_c) > 0) & F.col(c).isNotNull(),
                F.abs(F.col(c) - F.lit(_mean_c)) > (F.lit(5.0) * F.lit(_sd_c)),
            ).otherwise(F.lit(False)),
        )

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_w (stuck-reading flags)
_electricity_w_w = Window.partitionBy("frequency").orderBy("datetime_utc")
for c in electricity_w_value_cols:
    _stuck_expr = F.lit(False)
    for _freq, _lag_n in STUCK_LOOKBACK_STEPS_BY_FREQ.items():
        _lag1 = F.lag(F.col(c), 1).over(_electricity_w_w)
        _lagN = F.lag(F.col(c), _lag_n).over(_electricity_w_w)
        _cond = (
            (F.col("frequency") == _freq)
            & F.col(c).isNotNull()
            & (F.col(c) == _lag1)
            & (F.col(c) == _lagN)
        )
        _stuck_expr = F.when(_cond, F.lit(True)).otherwise(_stuck_expr)
    electricity_w = electricity_w.withColumn(f"_{c}_stuck_reading_flag", _stuck_expr)

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_w (meter monotonicity-violation flags)
for c in electricity_w_value_cols:
    _prev = F.lag(F.col(c), 1).over(_electricity_w_w)
    electricity_w = electricity_w.withColumn(
        f"_{c}_meter_monotonicity_violated",
        F.col(c).isNotNull() & _prev.isNotNull() & (F.col(c) < _prev),
    )

# COMMAND ----------

# DBTITLE 1,Transform -- honda_electricity_w (provenance)
electricity_w = electricity_w.withColumn(
    "_srid", sha_key(F.lit("honda_iot_electricity_w"), "frequency", "datetime_utc")
)
electricity_w = add_provenance(electricity_w, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- honda_electricity_w
write_silver(
    electricity_w, "honda_electricity_w", source=SOURCE, component=COMPONENT, rid=RID
)

# COMMAND ----------

# DBTITLE 1,Inspect honda_electricity_w + export findings
_findings_blocks = inspect_table(
    electricity_w,
    "honda_electricity_w",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    extra_checks={"chp_elec_duplicate_agreement": _electricity_w_dup_agreement}
    if _electricity_w_dup_agreement is not None
    else None,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__honda_electricity_w",
    "honda_electricity_w",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- honda_heating_p (cast)
heating_p = _heating_p_bronze.withColumn(
    "datetime_utc", F.col("datetime_utc").cast("timestamp")
)
for _c in heating_p.columns:
    if _c not in ("frequency", "datetime_utc"):
        heating_p = heating_p.withColumn(_c, F.col(_c).cast("double"))

heating_p_value_cols = [
    c for c in heating_p.columns if c not in ("frequency", "datetime_utc")
]

# COMMAND ----------

# DBTITLE 1,Transform -- honda_heating_p (CHP_elec pre-drop safeguard)
# Pre-drop duplication safeguard. electricity_p is written first (above), so
# its Silver output already exists by the time this cell runs.
_heating_p_dup_agreement = None
if "CHP_elec" in heating_p.columns:
    try:
        _heating_p_elec = read_silver("honda_electricity_p").select(
            "frequency", "datetime_utc", F.col("CHP").alias("_elec_chp")
        )
        _heating_p_joined = heating_p.join(
            _heating_p_elec, ["frequency", "datetime_utc"], "inner"
        )
        _heating_p_total = _heating_p_joined.count()
        _heating_p_agree = _heating_p_joined.filter(
            F.col("CHP_elec") == F.col("_elec_chp")
        ).count()
        _heating_p_dup_agreement = (
            (_heating_p_agree / _heating_p_total) if _heating_p_total else None
        )
    except Exception as exc:
        print(f"SKIP pre-drop safeguard (electricity_p not yet written?): {exc}")
    if (
        _heating_p_dup_agreement is not None
        and _heating_p_dup_agreement >= _DUPLICATE_AGREEMENT_THRESHOLD
    ):
        heating_p = heating_p.drop("CHP_elec")
        heating_p_value_cols = [c for c in heating_p_value_cols if c != "CHP_elec"]
        print(
            f"dropped heating_p.CHP_elec -- agreement with "
            f"electricity_p.CHP = {_heating_p_dup_agreement:.4f} (>= "
            f"{_DUPLICATE_AGREEMENT_THRESHOLD})"
        )
    elif _heating_p_dup_agreement is not None:
        print(
            f"REOPENED: heating_p.CHP_elec vs electricity_p.CHP agreement = "
            f"{_heating_p_dup_agreement:.4f} (< {_DUPLICATE_AGREEMENT_THRESHOLD}) -- "
            "keeping the column, not dropping. Investigate before this "
            "decision is reapplied."
        )

# COMMAND ----------

# DBTITLE 1,Transform -- honda_heating_p (5-sigma outlier flags)
_heating_p_stat_exprs = []
for c in heating_p_value_cols:
    _heating_p_stat_exprs += [
        F.mean(c).alias(f"{c}__mean"),
        F.stddev(c).alias(f"{c}__sd"),
    ]
if heating_p_value_cols:
    _heating_p_stats = heating_p.agg(*_heating_p_stat_exprs).first()
    for c in heating_p_value_cols:
        _mean_c, _sd_c = _heating_p_stats[f"{c}__mean"], _heating_p_stats[f"{c}__sd"]
        heating_p = heating_p.withColumn(
            f"_{c}_5sigma_outlier",
            F.when(
                F.lit(_sd_c).isNotNull() & (F.lit(_sd_c) > 0) & F.col(c).isNotNull(),
                F.abs(F.col(c) - F.lit(_mean_c)) > (F.lit(5.0) * F.lit(_sd_c)),
            ).otherwise(F.lit(False)),
        )

# COMMAND ----------

# DBTITLE 1,Transform -- honda_heating_p (stuck-reading flags)
_heating_p_w = Window.partitionBy("frequency").orderBy("datetime_utc")
for c in heating_p_value_cols:
    _stuck_expr = F.lit(False)
    for _freq, _lag_n in STUCK_LOOKBACK_STEPS_BY_FREQ.items():
        _lag1 = F.lag(F.col(c), 1).over(_heating_p_w)
        _lagN = F.lag(F.col(c), _lag_n).over(_heating_p_w)
        _cond = (
            (F.col("frequency") == _freq)
            & F.col(c).isNotNull()
            & (F.col(c) == _lag1)
            & (F.col(c) == _lagN)
        )
        _stuck_expr = F.when(_cond, F.lit(True)).otherwise(_stuck_expr)
    heating_p = heating_p.withColumn(f"_{c}_stuck_reading_flag", _stuck_expr)

# COMMAND ----------

# DBTITLE 1,Transform -- honda_heating_p (provenance)
heating_p = heating_p.withColumn(
    "_srid", sha_key(F.lit("honda_iot_heating_p"), "frequency", "datetime_utc")
)
heating_p = add_provenance(heating_p, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- honda_heating_p
write_silver(heating_p, "honda_heating_p", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect honda_heating_p + export findings
_findings_blocks = inspect_table(
    heating_p,
    "honda_heating_p",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    extra_checks={"chp_elec_duplicate_agreement": _heating_p_dup_agreement}
    if _heating_p_dup_agreement is not None
    else None,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__honda_heating_p",
    "honda_heating_p",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- honda_heating_w (cast)
heating_w = _heating_w_bronze.withColumn(
    "datetime_utc", F.col("datetime_utc").cast("timestamp")
)
for _c in heating_w.columns:
    if _c not in ("frequency", "datetime_utc"):
        heating_w = heating_w.withColumn(_c, F.col(_c).cast("double"))

heating_w_value_cols = [
    c for c in heating_w.columns if c not in ("frequency", "datetime_utc")
]

# COMMAND ----------

# DBTITLE 1,Transform -- honda_heating_w (no CHP_elec safeguard for this table)
_heating_w_dup_agreement = None

# COMMAND ----------

# DBTITLE 1,Transform -- honda_heating_w (5-sigma outlier flags)
_heating_w_stat_exprs = []
for c in heating_w_value_cols:
    _heating_w_stat_exprs += [
        F.mean(c).alias(f"{c}__mean"),
        F.stddev(c).alias(f"{c}__sd"),
    ]
if heating_w_value_cols:
    _heating_w_stats = heating_w.agg(*_heating_w_stat_exprs).first()
    for c in heating_w_value_cols:
        _mean_c, _sd_c = _heating_w_stats[f"{c}__mean"], _heating_w_stats[f"{c}__sd"]
        heating_w = heating_w.withColumn(
            f"_{c}_5sigma_outlier",
            F.when(
                F.lit(_sd_c).isNotNull() & (F.lit(_sd_c) > 0) & F.col(c).isNotNull(),
                F.abs(F.col(c) - F.lit(_mean_c)) > (F.lit(5.0) * F.lit(_sd_c)),
            ).otherwise(F.lit(False)),
        )

# COMMAND ----------

# DBTITLE 1,Transform -- honda_heating_w (stuck-reading flags)
_heating_w_w = Window.partitionBy("frequency").orderBy("datetime_utc")
for c in heating_w_value_cols:
    _stuck_expr = F.lit(False)
    for _freq, _lag_n in STUCK_LOOKBACK_STEPS_BY_FREQ.items():
        _lag1 = F.lag(F.col(c), 1).over(_heating_w_w)
        _lagN = F.lag(F.col(c), _lag_n).over(_heating_w_w)
        _cond = (
            (F.col("frequency") == _freq)
            & F.col(c).isNotNull()
            & (F.col(c) == _lag1)
            & (F.col(c) == _lagN)
        )
        _stuck_expr = F.when(_cond, F.lit(True)).otherwise(_stuck_expr)
    heating_w = heating_w.withColumn(f"_{c}_stuck_reading_flag", _stuck_expr)

# COMMAND ----------

# DBTITLE 1,Transform -- honda_heating_w (meter monotonicity-violation flags)
for c in heating_w_value_cols:
    _prev = F.lag(F.col(c), 1).over(_heating_w_w)
    heating_w = heating_w.withColumn(
        f"_{c}_meter_monotonicity_violated",
        F.col(c).isNotNull() & _prev.isNotNull() & (F.col(c) < _prev),
    )

# COMMAND ----------

# DBTITLE 1,Transform -- honda_heating_w (provenance)
heating_w = heating_w.withColumn(
    "_srid", sha_key(F.lit("honda_iot_heating_w"), "frequency", "datetime_utc")
)
heating_w = add_provenance(heating_w, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- honda_heating_w
write_silver(heating_w, "honda_heating_w", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect honda_heating_w + export findings
_findings_blocks = inspect_table(
    heating_w,
    "honda_heating_w",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    extra_checks={"chp_elec_duplicate_agreement": _heating_w_dup_agreement}
    if _heating_w_dup_agreement is not None
    else None,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__honda_heating_w",
    "honda_heating_w",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- honda_cooling_p (cast)
cooling_p = _cooling_p_bronze.withColumn(
    "datetime_utc", F.col("datetime_utc").cast("timestamp")
)
for _c in cooling_p.columns:
    if _c not in ("frequency", "datetime_utc"):
        cooling_p = cooling_p.withColumn(_c, F.col(_c).cast("double"))

cooling_p_value_cols = [
    c for c in cooling_p.columns if c not in ("frequency", "datetime_utc")
]

# COMMAND ----------

# DBTITLE 1,Transform -- honda_cooling_p (no CHP_elec safeguard for this table)
_cooling_p_dup_agreement = None

# COMMAND ----------

# DBTITLE 1,Transform -- honda_cooling_p (5-sigma outlier flags)
_cooling_p_stat_exprs = []
for c in cooling_p_value_cols:
    _cooling_p_stat_exprs += [
        F.mean(c).alias(f"{c}__mean"),
        F.stddev(c).alias(f"{c}__sd"),
    ]
if cooling_p_value_cols:
    _cooling_p_stats = cooling_p.agg(*_cooling_p_stat_exprs).first()
    for c in cooling_p_value_cols:
        _mean_c, _sd_c = _cooling_p_stats[f"{c}__mean"], _cooling_p_stats[f"{c}__sd"]
        cooling_p = cooling_p.withColumn(
            f"_{c}_5sigma_outlier",
            F.when(
                F.lit(_sd_c).isNotNull() & (F.lit(_sd_c) > 0) & F.col(c).isNotNull(),
                F.abs(F.col(c) - F.lit(_mean_c)) > (F.lit(5.0) * F.lit(_sd_c)),
            ).otherwise(F.lit(False)),
        )

# COMMAND ----------

# DBTITLE 1,Transform -- honda_cooling_p (stuck-reading flags)
_cooling_p_w = Window.partitionBy("frequency").orderBy("datetime_utc")
for c in cooling_p_value_cols:
    _stuck_expr = F.lit(False)
    for _freq, _lag_n in STUCK_LOOKBACK_STEPS_BY_FREQ.items():
        _lag1 = F.lag(F.col(c), 1).over(_cooling_p_w)
        _lagN = F.lag(F.col(c), _lag_n).over(_cooling_p_w)
        _cond = (
            (F.col("frequency") == _freq)
            & F.col(c).isNotNull()
            & (F.col(c) == _lag1)
            & (F.col(c) == _lagN)
        )
        _stuck_expr = F.when(_cond, F.lit(True)).otherwise(_stuck_expr)
    cooling_p = cooling_p.withColumn(f"_{c}_stuck_reading_flag", _stuck_expr)

# COMMAND ----------

# DBTITLE 1,Transform -- honda_cooling_p (provenance)
cooling_p = cooling_p.withColumn(
    "_srid", sha_key(F.lit("honda_iot_cooling_p"), "frequency", "datetime_utc")
)
cooling_p = add_provenance(cooling_p, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- honda_cooling_p
write_silver(cooling_p, "honda_cooling_p", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect honda_cooling_p + export findings
_findings_blocks = inspect_table(
    cooling_p,
    "honda_cooling_p",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    extra_checks={"chp_elec_duplicate_agreement": _cooling_p_dup_agreement}
    if _cooling_p_dup_agreement is not None
    else None,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__honda_cooling_p",
    "honda_cooling_p",
    _findings_blocks,
)

# COMMAND ----------

# DBTITLE 1,Transform -- honda_cooling_w (cast)
cooling_w = _cooling_w_bronze.withColumn(
    "datetime_utc", F.col("datetime_utc").cast("timestamp")
)
for _c in cooling_w.columns:
    if _c not in ("frequency", "datetime_utc"):
        cooling_w = cooling_w.withColumn(_c, F.col(_c).cast("double"))

cooling_w_value_cols = [
    c for c in cooling_w.columns if c not in ("frequency", "datetime_utc")
]

# COMMAND ----------

# DBTITLE 1,Transform -- honda_cooling_w (no CHP_elec safeguard for this table)
_cooling_w_dup_agreement = None

# COMMAND ----------

# DBTITLE 1,Transform -- honda_cooling_w (5-sigma outlier flags)
_cooling_w_stat_exprs = []
for c in cooling_w_value_cols:
    _cooling_w_stat_exprs += [
        F.mean(c).alias(f"{c}__mean"),
        F.stddev(c).alias(f"{c}__sd"),
    ]
if cooling_w_value_cols:
    _cooling_w_stats = cooling_w.agg(*_cooling_w_stat_exprs).first()
    for c in cooling_w_value_cols:
        _mean_c, _sd_c = _cooling_w_stats[f"{c}__mean"], _cooling_w_stats[f"{c}__sd"]
        cooling_w = cooling_w.withColumn(
            f"_{c}_5sigma_outlier",
            F.when(
                F.lit(_sd_c).isNotNull() & (F.lit(_sd_c) > 0) & F.col(c).isNotNull(),
                F.abs(F.col(c) - F.lit(_mean_c)) > (F.lit(5.0) * F.lit(_sd_c)),
            ).otherwise(F.lit(False)),
        )

# COMMAND ----------

# DBTITLE 1,Transform -- honda_cooling_w (stuck-reading flags)
_cooling_w_w = Window.partitionBy("frequency").orderBy("datetime_utc")
for c in cooling_w_value_cols:
    _stuck_expr = F.lit(False)
    for _freq, _lag_n in STUCK_LOOKBACK_STEPS_BY_FREQ.items():
        _lag1 = F.lag(F.col(c), 1).over(_cooling_w_w)
        _lagN = F.lag(F.col(c), _lag_n).over(_cooling_w_w)
        _cond = (
            (F.col("frequency") == _freq)
            & F.col(c).isNotNull()
            & (F.col(c) == _lag1)
            & (F.col(c) == _lagN)
        )
        _stuck_expr = F.when(_cond, F.lit(True)).otherwise(_stuck_expr)
    cooling_w = cooling_w.withColumn(f"_{c}_stuck_reading_flag", _stuck_expr)

# COMMAND ----------

# DBTITLE 1,Transform -- honda_cooling_w (meter monotonicity-violation flags)
for c in cooling_w_value_cols:
    _prev = F.lag(F.col(c), 1).over(_cooling_w_w)
    cooling_w = cooling_w.withColumn(
        f"_{c}_meter_monotonicity_violated",
        F.col(c).isNotNull() & _prev.isNotNull() & (F.col(c) < _prev),
    )

# COMMAND ----------

# DBTITLE 1,Transform -- honda_cooling_w (provenance)
cooling_w = cooling_w.withColumn(
    "_srid", sha_key(F.lit("honda_iot_cooling_w"), "frequency", "datetime_utc")
)
cooling_w = add_provenance(cooling_w, SOURCE, "_srid", RID)

# COMMAND ----------

# DBTITLE 1,Write Silver -- honda_cooling_w
write_silver(cooling_w, "honda_cooling_w", source=SOURCE, component=COMPONENT, rid=RID)

# COMMAND ----------

# DBTITLE 1,Inspect honda_cooling_w + export findings
_findings_blocks = inspect_table(
    cooling_w,
    "honda_cooling_w",
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["frequency", "datetime_utc"],
    extra_checks={"chp_elec_duplicate_agreement": _cooling_w_dup_agreement}
    if _cooling_w_dup_agreement is not None
    else None,
)
write_silver_findings(
    SOURCE,
    f"{COMPONENT.split('/')[-1]}__honda_cooling_w",
    "honda_cooling_w",
    _findings_blocks,
)
