# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SURVIVAL EVALUATION LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** scoring of the stored survival models and the Kaplan-Meier baseline
# MAGIC on the evaluated partition, pulled in with `%run ../lib/_eval_survival` after
# MAGIC `_fit_survival` and `_eval_common`.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Survival evaluator


class SurvivalEvaluator(Evaluator):
    """Concordance, Brier score and calibration at each horizon."""

    def _frame(self, df):
        spec = self.spec
        dcol, ecol = spec["duration_col"], spec["event_col"]
        feats = resolve_features(self.ctx, df.columns, drop=spec.get("drop", ()))
        have = set(df.columns)
        wanted = [*feats, dcol, ecol, spec["group_col"], "partition"]
        wanted += [*spec["key_cols"], *self.extra_columns(df)]
        cols = [c for c in dict.fromkeys(wanted) if c in have]
        pdf, _ = to_training_frame(df, key_cols=spec["key_cols"], columns=cols)
        return pdf[pd.to_numeric(pdf[dcol], errors="coerce") > 0].reset_index(drop=True)

    def load(self, df_eval, df_validation, validation_fraction: float = 1.0):
        self.frames["eval"] = self._frame(df_eval)
        note_read(
            self.ctx,
            self.frames["eval"],
            self.cfg.get("time_col"),
            self.cfg["blocks"].get("col"),
        )
        if df_validation is not None:
            self.frames["validation"] = self._frame(df_validation)

    def score(self, bundle, part, bundles):
        frame = self.frame(part)
        dur = pd.to_numeric(frame[self.spec["duration_col"]]).to_numpy(dtype="float64")
        ev = pd.to_numeric(frame[self.spec["event_col"]]).to_numpy(dtype="float64")
        risk = np.asarray(bundle.risk(frame), dtype="float64")
        metrics = _score(dur, ev, risk, bundle.survival(frame))

        def row_stat(idx, dur=dur, ev=ev, risk=risk):
            return concordance_index(dur[idx], ev[idx], risk[idx])

        return Scored(metrics, len(frame), row_stat, risk, None)
