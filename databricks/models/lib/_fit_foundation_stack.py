# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION STACKING LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** boosted trees that take the zero-shot Chronos-2 forecast of a row as
# MAGIC one more feature, for the time-series tasks. Pulled in with
# MAGIC `%run ../lib/_fit_foundation_stack` after `_fit_foundation_ts`. The forecast of a
# MAGIC row is made from a context that ends at the time of the row minus the gap, so it
# MAGIC is out of sample for train rows too. A tuned adapter would not be (it saw the
# MAGIC train rows), so only the zero-shot model is used. The frozen dataset is not
# MAGIC changed: the feature is made again whenever the bundle predicts.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Candidates and the feature name
STACK_FAMILY = "foundation_stack"
STACK_FEATURE = "fm_forecast"
STACK_MODEL = "chronos2"
# name, library, name of the tree builder in the tabular library
STACK_CANDIDATES = (
    ("lgbm_stack_chronos2_zero_shot", "lightgbm", "_make_lgbm_regressor"),
    ("hgb_stack_chronos2_zero_shot", "sklearn", "_make_hgb_regressor"),
)

# COMMAND ----------

# DBTITLE 1,Bundle -- the forecast reference and the tree


class StackedForecastBundle:
    """Forecasts every row with the stored reference, adds the forecast as a feature
    and predicts with the tree."""

    task = "regression"

    def __init__(self, reference, encoder, model):
        self.reference = reference
        self.encoder = encoder
        self.model = model

    def predict(self, frame):
        frame = frame.reset_index(drop=True)
        forecast = np.asarray(self.reference.predict(frame), dtype="float64")
        x = self.encoder.transform(frame.assign(**{STACK_FEATURE: forecast}))
        return self.model.predict(x)


# COMMAND ----------

# DBTITLE 1,Runner -- stacked candidates of one time-series task


def run_foundation_stacking(ctx, df, models=None) -> None:
    """Trees with the Chronos-2 forecast as a feature, on the validation rows of one
    time-series task, with the features, baseline and rows of the accepted trees."""
    cfg = TS_TASKS[ctx.task_id]
    spec = cfg["spec"]
    if cfg.get("complete_load_features"):
        df = complete_load_features(df, ctx)
    pdf, feats, fraction = _prepare(ctx, df, spec)
    tr_m, va_m = _masks(pdf)
    y = pd.to_numeric(pdf[spec["target"]], errors="coerce").to_numpy(dtype="float64")
    note_target(y, tr_m, va_m, "regression")
    start_task(ctx)
    base_pred = fit_baseline(pdf[tr_m], spec["target"], spec["baseline"]).predict(
        pdf[va_m]
    )
    price = bool(spec.get("price"))
    frame_va = pdf[va_m].reset_index(drop=True)
    entry = model_entry(STACK_MODEL)
    ref_cfg = {
        **reference_config(ctx, cfg, entry, STACK_MODEL, "zero_shot"),
        "has_quantiles": False,
        "quantiles": [0.5],
    }
    made = {}

    def train_features():
        if not made:
            reference = ForecastReference(ref_cfg)
            made["forecast"] = np.asarray(reference.predict(pdf), dtype="float64")
            made["report"] = dict(reference.last_report)
            if made["report"]["rule_violations"]:
                raise RuntimeError("a forecast context reaches past the gap")
        return made

    chosen = [c for c in STACK_CANDIDATES if models is None or c[0] in models]
    for name, library, builder in chosen:
        params = {"row_fraction": round(fraction, 4)}

        def fit(builder=builder, params=params):
            got = train_features()
            report_train = got["report"]
            stacked = pdf.assign(**{STACK_FEATURE: got["forecast"]})
            enc = FeatureEncoder([*feats, STACK_FEATURE]).fit(stacked[tr_m])
            x = enc.transform(stacked)
            model = globals()[builder](GBT_GRID[0]).fit(x[tr_m], y[tr_m])
            bundle = StackedForecastBundle(ForecastReference(ref_cfg), enc, model)
            pred = bundle.predict(frame_va)
            metrics = _point_metrics(y[va_m], pred, base_pred, price)
            report = bundle.reference.last_report
            metrics["forecast_items"] = float(report["items"])
            metrics["rows_without_context"] = float(report["rows_without_context"])
            metrics["context_rule_violations"] = float(report["rule_violations"])
            params.update(
                {
                    "model_key": entry["model_key"],
                    "repo_id": entry["repo_id"],
                    "revision": entry["revision"],
                    "licence": entry["licence"],
                    "mode": "zero_shot",
                    "stack_feature": STACK_FEATURE,
                    "train_rows_without_context": report_train["rows_without_context"],
                    "seed": MODEL_SEED,
                    **GBT_GRID[0],
                }
            )
            return bundle, metrics, int(va_m.sum())

        run_candidate(
            ctx,
            name,
            STACK_FAMILY,
            fit,
            requires=("chronos", library),
            params=params,
        )
    finish_foundation_task(ctx)
