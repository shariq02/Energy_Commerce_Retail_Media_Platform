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
# MAGIC value is on-site generation / export, not an error.

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ../../_silver_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ../../_silver_inspect

# COMMAND ----------

# DBTITLE 1,Imports + config
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE = "honda_iot"
COMPONENT = "silver/energy/iot/01_honda_energy"
RID = run_id()

# Bronze table -> Silver table (the `_iot` infix is dropped).
TABLE_MAP = {
    "honda_iot_electricity_p": "honda_electricity_p",
    "honda_iot_electricity_w": "honda_electricity_w",
    "honda_iot_heating_p": "honda_heating_p",
    "honda_iot_heating_w": "honda_heating_w",
    "honda_iot_cooling_p": "honda_cooling_p",
    "honda_iot_cooling_w": "honda_cooling_w",
}

# H1 (owner decision): drop heating_p.CHP_elec, no alias -- electricity_p.CHP
# is canonical. Pre-drop safeguard below re-checks the duplication first.
_DUPLICATE_AGREEMENT_THRESHOLD = 0.99

# COMMAND ----------

# DBTITLE 1,One Honda energy table -> Silver


def process(bt: str, silver_table: str) -> None:
    df = read_bronze(bt).withColumn(
        "datetime_utc", F.col("datetime_utc").cast("timestamp")
    )
    for c in df.columns:
        if c not in ("frequency", "datetime_utc"):
            df = df.withColumn(c, F.col(c).cast("double"))

    value_cols = [c for c in df.columns if c not in ("frequency", "datetime_utc")]

    # H1 -- pre-drop duplication safeguard (heating_p only). electricity_p is
    # processed first (TABLE_MAP order), so its Silver output already exists
    # by the time heating_p runs.
    _h1_agreement = None
    if bt == "honda_iot_heating_p" and "CHP_elec" in df.columns:
        try:
            _elec = read_silver("honda_electricity_p").select(
                "frequency", "datetime_utc", F.col("CHP").alias("_elec_chp")
            )
            _joined = df.join(_elec, ["frequency", "datetime_utc"], "inner")
            _total = _joined.count()
            _agree = _joined.filter(F.col("CHP_elec") == F.col("_elec_chp")).count()
            _h1_agreement = (_agree / _total) if _total else None
        except Exception as exc:
            print(f"SKIP H1 pre-drop safeguard (electricity_p not yet written?): {exc}")
        if (
            _h1_agreement is not None
            and _h1_agreement >= _DUPLICATE_AGREEMENT_THRESHOLD
        ):
            df = df.drop("CHP_elec")
            value_cols = [c for c in value_cols if c != "CHP_elec"]
            print(
                f"H1: dropped heating_p.CHP_elec -- agreement with "
                f"electricity_p.CHP = {_h1_agreement:.4f} (>= "
                f"{_DUPLICATE_AGREEMENT_THRESHOLD})"
            )
        elif _h1_agreement is not None:
            print(
                f"H1 REOPENED: heating_p.CHP_elec vs electricity_p.CHP agreement = "
                f"{_h1_agreement:.4f} (< {_DUPLICATE_AGREEMENT_THRESHOLD}) -- "
                "keeping the column, not dropping. Investigate before H1 is "
                "reapplied."
            )

    # D5/H4 -- 5-sigma outlier flag per value column, computed from that
    # column's own mean/sd (honda_iot.md S01/S02 Data Quality).
    if value_cols:
        stat_exprs = []
        for c in value_cols:
            stat_exprs += [F.mean(c).alias(f"{c}__mean"), F.stddev(c).alias(f"{c}__sd")]
        _stats = df.agg(*stat_exprs).first()
        for c in value_cols:
            mean_c, sd_c = _stats[f"{c}__mean"], _stats[f"{c}__sd"]
            df = df.withColumn(
                f"_{c}_5sigma_outlier",
                F.when(
                    F.lit(sd_c).isNotNull() & (F.lit(sd_c) > 0) & F.col(c).isNotNull(),
                    F.abs(F.col(c) - F.lit(mean_c)) > (F.lit(5.0) * F.lit(sd_c)),
                ).otherwise(F.lit(False)),
            )

    # D5/H4 -- stuck-reading flag (10 consecutive identical non-null readings,
    # matching profiling's own "value == value 1 and 9 steps back" definition,
    # honda_iot.md). Quantified by profiling only at 1h resolution -- computed
    # here across every frequency using the same step definition, but treat
    # the 1min/15min result as unvalidated against a known baseline until
    # inspected (the 1h result has a direct profiling baseline to compare to).
    w = Window.partitionBy("frequency").orderBy("datetime_utc")
    for c in value_cols:
        lag1 = F.lag(F.col(c), 1).over(w)
        lag9 = F.lag(F.col(c), 9).over(w)
        df = df.withColumn(
            f"_{c}_stuck_reading_flag",
            F.col(c).isNotNull() & (F.col(c) == lag1) & (F.col(c) == lag9),
        )

    # H2 -- meter monotonicity-violation flag, _w (cumulative-meter) tables
    # only (honda_iot.md S01 Temporal Consistency). Row-level: value decreased
    # vs the immediately preceding reading in the same frequency partition.
    if silver_table.endswith("_w"):
        for c in value_cols:
            prev = F.lag(F.col(c), 1).over(w)
            df = df.withColumn(
                f"_{c}_meter_monotonicity_violated",
                F.col(c).isNotNull() & prev.isNotNull() & (F.col(c) < prev),
            )

    df = df.withColumn("_srid", sha_key(F.lit(bt), "frequency", "datetime_utc"))
    df = add_provenance(df, SOURCE, "_srid", RID)
    write_silver(df, silver_table, source=SOURCE, component=COMPONENT, rid=RID)
    _findings_blocks = inspect_table(
        df,
        silver_table,
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
        key_cols=["frequency", "datetime_utc"],
        extra_checks={"h1_chp_elec_agreement": _h1_agreement}
        if _h1_agreement is not None
        else None,
    )
    write_silver_findings(
        SOURCE,
        f"{COMPONENT.split('/')[-1]}__{silver_table}",
        silver_table,
        _findings_blocks,
    )


for _bt, _st in TABLE_MAP.items():
    process(_bt, _st)
