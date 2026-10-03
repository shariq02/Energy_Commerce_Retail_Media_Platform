# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # RANKING EVALUATION LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** scoring of the stored next-item rankers and the popularity baseline
# MAGIC on the evaluated partition, pulled in with `%run ../lib/_eval_ranking` after
# MAGIC `_fit_ranking` and `_eval_common`.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np

# COMMAND ----------

# DBTITLE 1,Ranking evaluator


class RankingEvaluator(Evaluator):
    """Recall, NDCG and MRR of the true next item; sessions are sampled above the
    same row cap the candidates were scored with."""

    def _frame(self, df):
        spec = self.spec
        sc, tc = spec["session_col"], spec["step_col"]
        ic, yc = spec["src_col"], spec["truth_col"]
        wanted = [sc, tc, ic, yc, "partition", *self.extra_columns(df)]
        cols = [c for c in dict.fromkeys(wanted) if c in set(df.columns)]
        pdf, _ = to_training_frame(df, key_cols=[sc], columns=cols)
        pdf = pdf.dropna(subset=[yc, ic]).sort_values([sc, tc]).reset_index(drop=True)
        if len(pdf) > SEQ_EVAL_ROWS:
            chosen = (
                pdf[sc]
                .drop_duplicates()
                .sample(frac=SEQ_EVAL_ROWS / len(pdf), random_state=MODEL_SEED)
            )
            pdf = pdf[pdf[sc].isin(chosen)]
        return pdf.reset_index(drop=True)

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
        ranks = np.asarray(bundle.ranks(frame), dtype="float64")
        metrics = ranking_metrics(ranks, RANK_KS)

        def row_stat(idx, ranks=ranks):
            r = ranks[idx]
            with np.errstate(divide="ignore", invalid="ignore"):
                gain = np.where(r <= 10, 1.0 / np.log2(r + 1.0), 0.0)
            return float(np.mean(gain))

        return Scored(metrics, len(frame), row_stat, None, None)
