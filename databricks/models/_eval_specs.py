# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EVALUATION TASK SPECIFICATIONS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** per task, how the evaluated partition is read and prepared (as the
# MAGIC candidates were), which evaluator scores it and how its results are grouped for
# MAGIC resampling. Pulled in with `%run ../_eval_specs` after `_eval_common` and the
# MAGIC paradigm evaluation library.

# COMMAND ----------

# DBTITLE 1,Resampling rules
BY_MONTH_LOCAL = {"by": "month", "col": "local_date"}
BY_GROUP = {"by": "group", "col": "group_key"}
BY_ROW = {"by": "row"}

# COMMAND ----------

# DBTITLE 1,Calendar and realised lags for the load task
LOAD_KEYS = ["market_area_code", "load_kind"]
CALENDAR_COLUMNS = [
    "day_of_week",
    "is_weekend",
    "month",
    "day_of_year",
    "iso_week",
    "year",
    "is_holiday",
    "is_bridge_day",
    "local_day_hours",
]
REALISED_LAGS = [2, 7, 14]


def complete_load_features(frame, ctx):
    """Calendar columns for every market area and realised load at least two days
    back, taken from the rows read (earlier partitions included)."""
    keys = ["market_area_code", "local_date"]
    have = [c for c in CALENDAR_COLUMNS if c in frame.columns]
    calendar = read_ml("features_calendar_market_area", ecosystem=ctx.ecosystem)
    out = frame.drop(*have).join(calendar.select(*keys, *have), keys, "left")
    realised = frame.select(
        *LOAD_KEYS, "local_date", F.col("target_value_mwh").alias("_realised")
    )
    for n in REALISED_LAGS:
        shifted = realised.select(
            *LOAD_KEYS,
            F.date_add("local_date", n).alias("local_date"),
            F.col("_realised").alias(f"realised_lag{n}"),
        )
        out = out.join(shifted, [*LOAD_KEYS, "local_date"], "left")
    return out


# COMMAND ----------

# DBTITLE 1,Tabular task specifications (energy)
TABULAR_ENERGY = {
    "price_daily.price": {
        "kind": "tabular",
        "spec": {
            "target": "target_price_eur_per_mwh",
            "key_cols": ["market_area_code", "local_date"],
            "id_features": ["market_area_code"],
            "baseline": {"kind": "column", "column": "price_eur_per_mwh_lag7"},
            "quantile_cols": ["month"],
            "price": True,
            "date_col": "local_date",
        },
        "blocks": BY_MONTH_LOCAL,
        "time_col": "local_date",
        "segments": ["market_area_code"],
    },
    "price_quarter_hour.price": {
        "kind": "tabular",
        "spec": {
            "target": "target_price_eur_per_mwh",
            "key_cols": ["market_area_code", "interval_start_utc"],
            "id_features": ["market_area_code"],
            "baseline": {"kind": "column", "column": "price_eur_per_mwh_lag2d"},
            "quantile_cols": ["month"],
            "price": True,
            "date_col": "local_date",
        },
        "blocks": BY_MONTH_LOCAL,
        "time_col": "local_date",
        "segments": ["market_area_code"],
    },
    "price_quarter_hour.negative_price": {
        "kind": "tabular",
        "spec": {
            "target": "target_is_negative_price",
            "key_cols": ["market_area_code", "interval_start_utc"],
            "id_features": ["market_area_code"],
            "date_col": "local_date",
        },
        "blocks": BY_MONTH_LOCAL,
        "time_col": "local_date",
        "segments": ["market_area_code"],
    },
    "load.load": {
        "kind": "tabular",
        "history": True,
        "spec": {
            "target": "target_value_mwh",
            "key_cols": ["market_area_code", "local_date", "load_kind"],
            "id_features": ["market_area_code", "load_kind"],
            "extra_features": ["realised_lag2", "realised_lag7", "realised_lag14"],
            "baseline": {
                "kind": "group_mean",
                "cols": ["market_area_code", "load_kind", "day_of_week", "month"],
            },
            "date_col": "local_date",
        },
        "blocks": BY_MONTH_LOCAL,
        "time_col": "local_date",
        "segments": ["market_area_code", "load_kind"],
    },
    "bias.bias": {
        "kind": "tabular",
        "spec": {
            "target": "target_bias_mwh",
            "key_cols": ["market_area_code", "local_date", "forecast_scope"],
            "id_features": ["market_area_code", "forecast_scope"],
            "baseline": {"kind": "zero"},
            "date_col": "local_date",
        },
        "blocks": BY_MONTH_LOCAL,
        "time_col": "local_date",
        "segments": ["forecast_scope", "market_area_code"],
    },
    "zone_generation.generation": {
        "kind": "tabular",
        "spec": {
            "target": "target_generation_mwh",
            "key_cols": ["market_area_code", "local_date", "carrier"],
            "id_features": ["market_area_code", "carrier"],
            "baseline": {
                "kind": "group_ratio",
                "cols": ["market_area_code", "carrier", "month"],
                "denominator": "capacity_net_mw",
            },
            "date_col": "local_date",
        },
        "blocks": BY_MONTH_LOCAL,
        "time_col": "local_date",
        "segments": ["carrier", "market_area_code"],
    },
    "honda_forecast.increment": {
        "kind": "tabular",
        "spec": {
            "target": "target_increment",
            "key_cols": [
                "location_key",
                "channel",
                "interval_seconds",
                "interval_start_utc",
            ],
            "id_features": ["location_key", "channel", "interval_seconds"],
            "baseline": {"kind": "column", "column": "increment_lag2d"},
            "date_col": "local_date",
        },
        "blocks": BY_MONTH_LOCAL,
        "time_col": "local_date",
        "segments": ["channel"],
    },
    "ccpp.output": {
        "kind": "tabular",
        "spec": {
            "target": "target_net_output_mw",
            "key_cols": ["sample_key"],
            "id_features": [],
            "baseline": {"kind": "train_mean"},
        },
        "blocks": BY_GROUP,
    },
    "capacity_additions.additions": {
        "kind": "tabular",
        "spec": {
            "target": "target_capacity_added_mw",
            "key_cols": ["carrier_key", "commissioning_month"],
            "id_features": ["carrier_key"],
            "baseline": {"kind": "group_mean", "cols": ["carrier_key", "month"]},
            "date_col": "commissioning_month",
        },
        "blocks": {"by": "month", "col": "commissioning_month"},
        "time_col": "commissioning_month",
        "segments": ["carrier_key"],
    },
    "redispatch.any_event": {
        "kind": "tabular",
        "spec": {
            "target": "target_any_event",
            "key_cols": ["market_area_code", "local_date"],
            "id_features": ["market_area_code"],
            "date_col": "local_date",
        },
        "blocks": BY_MONTH_LOCAL,
        "time_col": "local_date",
        "segments": ["market_area_code"],
    },
    "redispatch.event_energy": {
        "kind": "tabular",
        "events_only": True,
        "spec": {
            "target": "target_log_event_energy_mwh",
            "key_cols": ["market_area_code", "local_date"],
            "id_features": ["market_area_code"],
            "baseline": {"kind": "group_mean", "cols": ["market_area_code"]},
            "date_col": "local_date",
        },
        "blocks": BY_MONTH_LOCAL,
        "time_col": "local_date",
        "segments": ["market_area_code"],
    },
}

# COMMAND ----------

# DBTITLE 1,Specifications of the other energy paradigms
OTHER_ENERGY = {
    "survival.unit_lifetime": {
        "kind": "survival",
        "spec": {
            "duration_col": "target_duration_years",
            "event_col": "target_event",
            "key_cols": ["unit_id"],
            "group_col": "is_offshore",
            "drop": ["entry_years", "left_truncated", "entry_date"],
        },
        "blocks": BY_GROUP,
        "segments": ["is_offshore"],
        "bootstrap_max_rows": 20_000,
    },
    "honda_anomaly.anomaly": {
        "kind": "anomaly",
        "spec": {
            "series_cols": ["location_key", "channel"],
            "time_col": "interval_start_utc",
            "target": "target_increment",
            "key_cols": [
                "location_key",
                "channel",
                "interval_seconds",
                "interval_start_utc",
            ],
            "drop": [],
        },
        "blocks": {"by": "month", "col": "interval_start_utc"},
        "time_col": "interval_start_utc",
        "segments": ["channel"],
    },
    "redispatch_matching.tier": {
        "kind": "matching",
        "manifest": ["affected_asset_text"],
        "spec": {
            "text_col": "affected_asset_text",
            "target": "target_match_tier",
            "key_cols": ["affected_asset_text"],
        },
        "blocks": BY_GROUP,
    },
    "weak_supervision.labels": {
        "kind": "weak",
        "manifest": ["entity_type", "entity_id", "lf_name"],
        "spec": {"key_cols": ["entity_type", "entity_id", "lf_name"]},
        "blocks": BY_GROUP,
    },
    "rl_pumped_storage.policy": {
        "kind": "pumped",
        "spec": {
            "state_cols": [
                "state_residual_load_mwh",
                "state_price_eur_per_mwh",
                "hour_of_day",
                "day_of_week",
                "month",
            ],
            "action_col": "action_net_mwh",
            "reward_col": "reward_eur",
            "price_col": "state_price_eur_per_mwh",
            "key_cols": ["episode_id", "step"],
        },
        "blocks": BY_GROUP,
    },
    "rl_redispatch.policy": {
        "kind": "action",
        "spec": {
            "action_col": "action_direction",
            "reward_col": "reward",
            "key_cols": ["episode_id", "step"],
            "drop": [
                "action_direction",
                "action_reason",
                "action_energy_mwh",
                "reward",
            ],
            "id_features": ["market_area_code"],
        },
        "blocks": BY_GROUP,
        "segments": ["market_area_code"],
    },
}

# COMMAND ----------

# DBTITLE 1,Reconstruction task specifications
RECONSTRUCTION_ENERGY = {
    "weather_imputation.reconstruction": {
        "kind": "reconstruction",
        "spec": {
            "sets": [
                {
                    "name": "weather",
                    "id_col": "location_key",
                    "ts_col": "observation_timestamp_utc",
                    "variables": [
                        "air_temperature",
                        "dew_point_temperature",
                        "relative_humidity",
                        "pressure_station",
                        "pressure_sea_level",
                        "wind_speed",
                        "wind_direction",
                        "cloud_cover",
                        "visibility",
                        "soil_temperature",
                    ],
                    "freq": "h",
                    "period": 24,
                    "step_hours": 1,
                    "baseline": "carry_forward",
                    "circular": ["wind_direction"],
                }
            ]
        },
        "blocks": BY_ROW,
    },
    "self_supervised_other.reconstruction": {
        "kind": "reconstruction",
        "spec": {
            "sets": [
                {
                    "name": "daily_load",
                    "filter_col": "series_family",
                    "filter_value": "daily_load",
                    "id_col": "series_id",
                    "ts_col": "ts",
                    "variables": ["series_value"],
                    "freq": "D",
                    "period": 7,
                    "step_hours": 24,
                    "baseline": "seasonal_carry",
                },
                {
                    "name": "honda_hourly",
                    "filter_col": "series_family",
                    "filter_value": "honda_hourly",
                    "id_col": "series_id",
                    "ts_col": "ts",
                    "variables": ["series_value"],
                    "freq": "h",
                    "period": 24,
                    "step_hours": 1,
                    "baseline": "seasonal_carry",
                },
            ]
        },
        "blocks": BY_ROW,
    },
}

# COMMAND ----------

# DBTITLE 1,Commerce task specifications
COMMERCE_TASKS = {
    "session_purchase_ga4.purchase": {
        "kind": "tabular",
        "spec": {"target": "target_purchase_after_prefix", "key_cols": ["session_key"]},
        "blocks": BY_GROUP,
    },
    "session_purchase_rees46.purchase": {
        "kind": "tabular",
        "spec": {"target": "target_purchase_after_prefix", "key_cols": ["session_key"]},
        "blocks": BY_GROUP,
    },
    "lapse_ga4.return": {
        "kind": "tabular",
        "spec": {
            "target": "target_returned",
            "key_cols": ["user_id", "cutoff_date"],
        },
        "blocks": BY_GROUP,
    },
    "lapse_rees46.return": {
        "kind": "tabular",
        "spec": {
            "target": "target_returned",
            "key_cols": ["user_id", "cutoff_date"],
        },
        "blocks": BY_GROUP,
    },
    "next_item_ga4.next_item": {
        "kind": "ranking",
        "spec": {
            "session_col": "session_key",
            "step_col": "step",
            "src_col": "seq_product_id",
            "truth_col": "target_next_product_id",
            "key_cols": ["session_key", "step"],
        },
        "blocks": BY_GROUP,
    },
    "next_item_rees46.next_item": {
        "kind": "ranking",
        "sample_users": True,
        "spec": {
            "session_col": "session_key",
            "step_col": "step",
            "src_col": "seq_product_id",
            "truth_col": "target_next_product_id",
            "key_cols": ["session_key", "step"],
        },
        "blocks": BY_GROUP,
    },
    "rl_session_sequences.policy": {
        "kind": "action",
        "sample_users": True,
        "spec": {
            "action_col": "target_action_event_type",
            "reward_col": "reward",
            "session_col": "session_key",
            "step_col": "step",
            "prev_col": "seq_previous_event_type",
            "key_cols": ["episode_id", "step"],
            "drop": [
                "seq_event_type",
                "seq_product_id",
                "seq_category",
                "seq_brand",
                "seq_price",
                "seq_price_imputed",
                "seq_price_is_missing",
                "reward",
                "target_action_event_type",
                "target_action_product_id",
            ],
            "id_features": ["step"],
        },
        "blocks": BY_GROUP,
    },
}
EVAL_TASKS = {
    **TABULAR_ENERGY,
    **OTHER_ENERGY,
    **RECONSTRUCTION_ENERGY,
    **COMMERCE_TASKS,
}

# COMMAND ----------

# DBTITLE 1,Evaluator classes by kind
EVALUATOR_NAMES = {
    "tabular": "TabularEvaluator",
    "survival": "SurvivalEvaluator",
    "ranking": "RankingEvaluator",
    "anomaly": "AnomalyEvaluator",
    "reconstruction": "ReconstructionEvaluator",
    "weak": "WeakEvaluator",
    "matching": "MatchingEvaluator",
    "pumped": "PumpedEvaluator",
    "action": "ActionEvaluator",
}

# COMMAND ----------

# DBTITLE 1,The frame of one partition, prepared as the candidates were


def task_frame(ctx, partition: str) -> DataFrame:
    """Spark frame of one partition of the task. Tasks with their own split manifest
    return every row with the manifest partition attached; the evaluator picks."""
    cfg = EVAL_TASKS[ctx.task_id]
    if cfg.get("manifest"):
        df = attach_manifest_partition(read_partitions(ctx, None), ctx, cfg["manifest"])
        return df.filter(F.col("partition") != TEST_PARTITION) if ctx.smoke else df
    if cfg.get("history"):
        kept = tuple(dict.fromkeys((*ALLOWED_PARTITIONS, ctx.partition)))
        df = complete_load_features(read_partitions(ctx, kept), ctx)
        df = df.filter(F.col("partition") == partition)
    else:
        df = read_partitions(ctx, (partition,))
    if cfg.get("events_only"):
        df = df.filter(
            F.col("target_any_event") & F.col("target_log_event_energy_mwh").isNotNull()
        )
    if cfg.get("sample_users"):
        df = sample_users(df)
    if ctx.smoke and cfg["kind"] in ("tabular", "survival", "action", "pumped"):
        df = df.limit(SMOKE_ROWS)
    return df


# COMMAND ----------

# DBTITLE 1,Evaluate one task by id


def evaluate_by_id(task_id: str, rid: str, smoke: bool) -> None:
    """Read the frames of the task, build its evaluator and score the forwarded
    models."""
    ctx = EvalContext(task_id, rid, smoke)
    cfg = EVAL_TASKS[task_id]
    evaluator = globals()[EVALUATOR_NAMES[cfg["kind"]]](ctx, cfg)
    models = forwarded_models(ctx)
    try:
        evaluator.load(
            task_frame(ctx, ctx.partition),
            task_frame(ctx, "validation"),
            row_fraction_of(models),
        )
        evaluate_task(ctx, evaluator)
    finally:
        evaluator.frames = {"eval": None, "validation": None}
        evaluator.sampler = None
        del evaluator
        _gc.collect()
