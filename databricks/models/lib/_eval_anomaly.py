# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ANOMALY EVALUATION LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** scoring of the stored anomaly detectors on a copy of the evaluated
# MAGIC partition with injected anomalies (new fixed seed), at the flag threshold
# MAGIC recorded for each detector, pulled in with `%run ../lib/_eval_anomaly` after
# MAGIC `_fit_anomaly` and `_eval_common`.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Anomaly evaluator


class AnomalyEvaluator(Evaluator):
    """Precision, recall and flag rate on injected anomalies. The injection is drawn
    again, so the validation result cannot be reproduced."""

    can_reproduce = False

    def _frame(self, df):
        spec = self.spec
        sc, tc, yc = spec["series_cols"], spec["time_col"], spec["target"]
        feats = resolve_features(self.ctx, df.columns, drop=spec.get("drop", ()))
        seasonal = [*sc, "hour_of_day", "day_of_week"]
        wanted = [*feats, *seasonal, tc, yc, "partition", *spec["key_cols"]]
        wanted += self.extra_columns(df)
        cols = [c for c in dict.fromkeys(wanted) if c in set(df.columns)]
        pdf, _ = to_training_frame(df, key_cols=spec["key_cols"], columns=cols)
        return pdf.dropna(subset=[yc]).sort_values([*sc, tc]).reset_index(drop=True)

    def _train_spread(self):
        """Per-series and overall spread of the target on the training rows."""
        sc, yc = self.spec["series_cols"], self.spec["target"]
        train = read_partitions(self.ctx, ("train",)).filter(F.col(yc).isNotNull())
        per = train.groupBy(*sc).agg(F.stddev(yc).alias("sd")).collect()
        overall = train.agg(F.stddev(yc).alias("sd")).first()["sd"]
        return pd.DataFrame([r.asDict() for r in per]), float(overall)

    def load(self, df_eval, df_validation, validation_fraction: float = 1.0):
        spec = self.spec
        sc, yc = spec["series_cols"], spec["target"]
        frame = self._frame(df_eval)
        self.frames["eval"] = frame
        note_read(self.ctx, frame, spec["time_col"], self.cfg["blocks"].get("col"))
        lengths, weights, kinds, magnitudes = read_injection_spec(
            self.ctx.ecosystem, self.ctx.dataset_id
        )
        sd_train, overall = self._train_spread()
        group_index = frame.groupby(sc).ngroup().to_numpy()
        keys = frame.assign(_g=group_index).drop_duplicates("_g").sort_values("_g")[sc]
        sd_by_group = (
            keys.merge(sd_train, on=sc, how="left")["sd"]
            .fillna(overall)
            .to_numpy(dtype="float64")
        )
        values, self.injected = inject_anomalies(
            frame[yc].to_numpy(dtype="float64"),
            group_index,
            sd_by_group,
            lengths,
            weights,
            kinds,
            magnitudes,
            INJECT_RATE,
            self.ctx.thresholds["injection_seed"],
        )
        self.injected_frame = frame.copy()
        self.injected_frame[yc] = values

    def score(self, bundle, part, bundles):
        name = self.name_of(bundle, bundles)
        threshold = float(self.recorded[name]["threshold"])
        s_inj = np.asarray(bundle.score(self.injected_frame), dtype="float64")
        s_orig = np.asarray(bundle.score(self.frames["eval"]), dtype="float64")
        injected = self.injected
        metrics = detection_metrics(
            injected, s_inj > threshold, float(np.mean(s_orig > threshold))
        )
        metrics["pr_auc"] = average_precision(injected, s_inj)
        metrics["threshold"] = threshold
        metrics["injected_rows"] = float(injected.sum())

        def row_stat(idx, injected=injected, s_inj=s_inj):
            return average_precision(injected[idx], s_inj[idx])

        return Scored(metrics, len(s_orig), row_stat, s_inj, None)
