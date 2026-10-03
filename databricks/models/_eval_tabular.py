# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # TABULAR EVALUATION LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** scoring of the stored regression, quantile and classification
# MAGIC models and their baselines on the evaluated partition, pulled in with
# MAGIC `%run ../_eval_tabular` after `_fit_tabular` and `_eval_common`.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Tabular evaluator


class TabularEvaluator(Evaluator):
    """Regression (point and quantile) and binary classification."""

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.classification = TASK_BY_ID[ctx.task_id]["task_type"] == "classification"

    def _columns(self, df, feats) -> list:
        spec = self.spec
        base = spec.get("baseline", {})
        extra = [base.get("column"), base.get("denominator"), *base.get("cols", [])]
        extra += [*spec.get("quantile_cols", []), spec.get("date_col")]
        needed = [spec["target"], "partition", *spec["key_cols"], *extra]
        needed += self.extra_columns(df)
        have = set(df.columns)
        cols: list = []
        for c in [*feats, *needed]:
            for cand in (c, f"{c}_imputed" if c else None):
                if cand and cand in have and cand not in cols:
                    cols.append(cand)
        return cols

    def _frame(self, df, fraction: float = 1.0):
        spec = self.spec
        feats = resolve_features(
            self.ctx,
            df.columns,
            id_features=[*spec.get("id_features", ()), *spec.get("extra_features", ())],
            drop=spec.get("drop", ()),
        )
        if fraction < 1.0:
            bucket = F.pmod(
                F.xxhash64(*[F.col(c).cast("string") for c in spec["key_cols"]]),
                F.lit(10000),
            )
            df = df.filter(bucket < int(fraction * 10000))
        pdf, _ = to_training_frame(
            df, key_cols=spec["key_cols"], columns=self._columns(df, feats)
        )
        y = pd.to_numeric(pdf[spec["target"]], errors="coerce")
        return pdf[np.isfinite(y.to_numpy(dtype="float64"))].reset_index(drop=True)

    def load(self, df_eval, df_validation, validation_fraction: float = 1.0):
        self.frames["eval"] = self._frame(df_eval)
        note_read(
            self.ctx,
            self.frames["eval"],
            self.cfg.get("time_col"),
            self.cfg["blocks"].get("col"),
        )
        if df_validation is not None:
            self.frames["validation"] = self._frame(df_validation, validation_fraction)

    def _target(self, frame) -> np.ndarray:
        col = pd.to_numeric(frame[self.spec["target"]], errors="coerce")
        return col.to_numpy(dtype="float64")

    def score(self, bundle, part, bundles):
        frame = self.frame(part)
        y = self._target(frame)
        price = bool(self.spec.get("price"))
        is_baseline = bundle is bundles.get(self.base_name)
        if self.classification:
            return self._score_class(bundle, frame, y)
        if hasattr(bundle, "taus"):
            return Scored(quantile_metrics(y, bundle.predict(frame)), len(y))
        base_pred = bundles[self.base_name].predict(frame)
        if getattr(bundle, "task", None) == "quantile":
            preds = bundle.predict(frame)
            qbase = bundles.get("empirical_quantiles")
            baseline = qbase.predict(frame) if qbase is not None else None
            metrics = quantile_metrics(y, preds, baseline)
            metrics.update(regression_metrics(y, preds[0.5], base_pred))
            pred = preds[0.5]
        else:
            pred = bundle.predict(frame)
            skill_base = None if is_baseline else base_pred
            metrics = {**regression_metrics(y, pred, skill_base)}
            if price:
                metrics["pinball_q50"] = pinball(y, pred, 0.5)

        def row_stat(idx, y=y, pred=pred):
            return mae(y[idx], pred[idx])

        return Scored(metrics, len(y), row_stat, pred, y)

    def _score_class(self, bundle, frame, y):
        p = np.asarray(bundle.predict(frame), dtype="float64")
        metrics = classification_metrics(y, p)
        base = metrics["base_rate"]
        metrics["pr_auc_lift"] = (
            metrics["pr_auc"] / base if base and np.isfinite(base) else float("nan")
        )

        def row_stat(idx, y=y, p=p):
            return average_precision(y[idx], p[idx])

        return Scored(metrics, len(y), row_stat, p, None)
