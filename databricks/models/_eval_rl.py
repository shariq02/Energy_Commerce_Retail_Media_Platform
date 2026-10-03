# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # OFFLINE POLICY EVALUATION LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** scoring of the stored offline policies and their baselines on the
# MAGIC evaluated partition (reward and action agreement against the logged data),
# MAGIC pulled in with `%run ../_eval_rl` after `_fit_rl` and `_eval_common`.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np

# COMMAND ----------

# DBTITLE 1,Pumped storage policy evaluator


class PumpedEvaluator(Evaluator):
    """Numeric action policy: reward under a price-taker assumption, with actions
    clipped to the range logged on the training rows."""

    def _frame(self, df):
        spec = self.spec
        wanted = [*spec["state_cols"], spec["action_col"], spec["reward_col"]]
        wanted += [spec["price_col"], "partition", *spec["key_cols"]]
        wanted += self.extra_columns(df)
        cols = [c for c in dict.fromkeys(wanted) if c in set(df.columns)]
        pdf, _ = to_training_frame(df, key_cols=spec["key_cols"], columns=cols)
        needed = [spec["action_col"], spec["reward_col"], spec["price_col"]]
        return pdf.dropna(subset=needed).reset_index(drop=True)

    def _train_bounds(self):
        spec = self.spec
        keep = F.col(spec["action_col"]).isNotNull()
        keep &= F.col(spec["reward_col"]).isNotNull()
        keep &= F.col(spec["price_col"]).isNotNull()
        row = (
            read_partitions(self.ctx, ("train",))
            .filter(keep)
            .agg(
                F.min(spec["action_col"]).alias("lo"),
                F.max(spec["action_col"]).alias("hi"),
            )
            .first()
        )
        return float(row["lo"]), float(row["hi"])

    def load(self, df_eval, df_validation, validation_fraction: float = 1.0):
        self.frames["eval"] = self._frame(df_eval)
        note_read(self.ctx, self.frames["eval"], None, self.cfg["blocks"].get("col"))
        if df_validation is not None:
            self.frames["validation"] = self._frame(df_validation)
        self.spec = {**self.spec, "action_bounds": self._train_bounds()}

    def score(self, bundle, part, bundles):
        frame = self.frame(part)
        spec = self.spec
        action = np.asarray(bundle.action(frame), dtype="float64")
        metrics = pumped_metrics(frame, action, spec)
        lo, hi = spec["action_bounds"]
        clipped = np.clip(action, lo, hi)
        logged = frame[spec["action_col"]].to_numpy(dtype="float64")
        price = frame[spec["price_col"]].to_numpy(dtype="float64")
        timing = price * np.sign(clipped) * np.abs(logged)

        def row_stat(idx, timing=timing):
            return float(np.nanmean(timing[idx]))

        return Scored(metrics, len(frame), row_stat, clipped, logged)


# COMMAND ----------

# DBTITLE 1,Categorical action policy evaluator


class ActionEvaluator(Evaluator):
    """Predicted action against the logged action, with the logged reward where the
    policy agrees and disagrees. Covers the redispatch and session policies."""

    def __init__(self, ctx, cfg):
        super().__init__(ctx, cfg)
        self.session = "session_col" in self.spec

    def _frame(self, df):
        spec = self.spec
        feats = resolve_features(
            self.ctx,
            df.columns,
            id_features=spec.get("id_features", ()),
            drop=spec.get("drop", ()),
        )
        ac, rc = spec["action_col"], spec["reward_col"]
        wanted = [*feats, ac, rc, "partition", *spec["key_cols"]]
        wanted += self.extra_columns(df)
        keys = spec["key_cols"]
        if self.session:
            sc, tc = spec["session_col"], spec["step_col"]
            wanted += [sc, tc, spec["prev_col"], "seq_event_type"]
            keys = [sc]
        cols = [c for c in dict.fromkeys(wanted) if c in set(df.columns)]
        pdf, _ = to_training_frame(df, key_cols=keys, columns=cols)
        pdf = pdf.dropna(subset=[ac, rc])
        if self.session:
            pdf = pdf.sort_values([sc, tc])
        pdf[ac] = pdf[ac].astype(str)
        return pdf.reset_index(drop=True)

    def load(self, df_eval, df_validation, validation_fraction: float = 1.0):
        self.frames["eval"] = self._frame(df_eval)
        note_read(self.ctx, self.frames["eval"], None, self.cfg["blocks"].get("col"))
        if df_validation is not None:
            self.frames["validation"] = self._frame(df_validation)

    def score(self, bundle, part, bundles):
        frame = self.frame(part)
        pred = np.asarray(bundle.action(frame)).astype(str)
        metrics = action_metrics(frame, pred, self.spec)
        logged = frame[self.spec["action_col"]].astype(str).to_numpy()
        agree = (logged == pred).astype("float64")

        def row_stat(idx, agree=agree):
            return float(np.mean(agree[idx]))

        return Scored(metrics, len(frame), row_stat, None, None)
