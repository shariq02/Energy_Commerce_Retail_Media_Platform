# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # HONDA ENERGY SHARED HELPERS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** the two data-quality flag builders shared by this folder's
# MAGIC 6 channel notebooks, pulled in with `%run ./_honda_energy_common` after
# MAGIC `_silver_common`. Definitions only -- no read/write inside. Every caller
# MAGIC still does its own Read Bronze / Write Silver / Inspect in its own cells;
# MAGIC these are pure DataFrame-in, DataFrame-out transform steps.
# MAGIC
# MAGIC References `F` from the caller's own Imports cell (resolved at call
# MAGIC time, not at import time). The stuck-reading window itself is built by
# MAGIC the caller (not here) and passed in, so a `_w`-suffixed table can reuse
# MAGIC it for its monotonicity-violation flags.

# COMMAND ----------

# DBTITLE 1,Helper -- 5-sigma outlier flags (definition only)


def add_5sigma_outlier_flags(df, value_cols):
    """Add `_{c}_5sigma_outlier` per value column, from that column's own
    mean/sd (per-source profiling data-quality section)."""
    if not value_cols:
        return df
    stat_exprs = []
    for c in value_cols:
        stat_exprs += [F.mean(c).alias(f"{c}__mean"), F.stddev(c).alias(f"{c}__sd")]
    stats = df.agg(*stat_exprs).first()
    for c in value_cols:
        mean_c, sd_c = stats[f"{c}__mean"], stats[f"{c}__sd"]
        df = df.withColumn(
            f"_{c}_5sigma_outlier",
            F.when(
                F.lit(sd_c).isNotNull() & (F.lit(sd_c) > 0) & F.col(c).isNotNull(),
                F.abs(F.col(c) - F.lit(mean_c)) > (F.lit(5.0) * F.lit(sd_c)),
            ).otherwise(F.lit(False)),
        )
    return df


# COMMAND ----------

# DBTITLE 1,Helper -- stuck-reading flags (definition only)


def add_stuck_reading_flags(df, value_cols, lookback_by_freq, w):
    """Add `_{c}_stuck_reading_flag` per value column -- flat for the window
    `lookback_by_freq` names per frequency (matching profiling's "value ==
    value N steps back" definition, scaled to the same ~10h window per
    frequency). `w` is the caller's own `Window.partitionBy("frequency").
    orderBy("datetime_utc")` -- passed in (not built here) so a `_w`-suffixed
    caller can reuse the same window for its monotonicity-violation flags."""
    for c in value_cols:
        stuck_expr = F.lit(False)
        for freq, lag_n in lookback_by_freq.items():
            lag1 = F.lag(F.col(c), 1).over(w)
            lagN = F.lag(F.col(c), lag_n).over(w)
            cond = (
                (F.col("frequency") == freq)
                & F.col(c).isNotNull()
                & (F.col(c) == lag1)
                & (F.col(c) == lagN)
            )
            stuck_expr = F.when(cond, F.lit(True)).otherwise(stuck_expr)
        df = df.withColumn(f"_{c}_stuck_reading_flag", stuck_expr)
    return df
