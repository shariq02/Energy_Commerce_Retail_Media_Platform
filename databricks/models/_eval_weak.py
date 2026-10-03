# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # WEAK SUPERVISION AND MATCHING EVALUATION LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** scoring of the stored label model and the majority-vote baseline,
# MAGIC and of the stored match-tier classifiers, on the evaluated partition of their
# MAGIC own split manifests, pulled in with `%run ../_eval_weak` after `_fit_weak` and
# MAGIC `_eval_common`.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np

# COMMAND ----------

# DBTITLE 1,Label matrices with the group of each entity


def label_data(pdf):
    """{entity type: (votes entities x functions, function names, label names,
    partition per entity, group per entity)}; -1 marks an abstain."""
    out = {}
    for etype, part in pdf.groupby("entity_type"):
        wide = part.pivot(index="entity_id", columns="lf_name", values="lf_label")
        labels = sorted(part["lf_label"].dropna().unique().tolist())
        code = {lab: i for i, lab in enumerate(labels)}
        votes = (
            wide.apply(lambda col, code=code: col.map(code)).fillna(-1).astype("int64")
        )
        per_entity = part.drop_duplicates("entity_id").set_index("entity_id")
        parts = per_entity["partition"].reindex(wide.index).to_numpy()
        groups = per_entity["group_key"].reindex(wide.index).astype(str).to_numpy()
        out[etype] = (votes.to_numpy(), list(wide.columns), labels, parts, groups)
    return out


# COMMAND ----------

# DBTITLE 1,Weak supervision evaluator


class WeakEvaluator(Evaluator):
    """Coverage, conflict rate and agreement per entity type. There is no ground
    truth, so the label model and the vote are compared on what they resolve."""

    def load(self, df_eval, df_validation, validation_fraction: float = 1.0):
        cols = ["entity_type", "entity_id", "lf_name", "lf_label", "partition"]
        cols.append("group_key")
        keys = self.spec["key_cols"]
        pdf, _ = to_training_frame(df_eval, key_cols=keys, columns=cols)
        self.mats = label_data(pdf)
        part = pdf[pdf["partition"] == self.ctx.partition]
        self.frames["eval"] = part.reset_index(drop=True)
        earlier = pdf[pdf["partition"] == "validation"]
        self.frames["validation"] = earlier.reset_index(drop=True)
        note_read(self.ctx, part, None, "group_key")

    def _predict(self, bundle, etype, votes):
        if isinstance(bundle, dict) and etype in bundle:
            return bundle[etype].predict(votes)
        return majority_vote(votes)

    def _partition_name(self, part):
        return self.ctx.partition if part == "eval" else "validation"

    def score(self, bundle, part, bundles):
        wanted = self._partition_name(part)
        out, total, covered, flags, groups = {}, 0, 0, [], []
        for etype, (mat, _lfs, _labels, parts, grp) in self.mats.items():
            sel = parts == wanted
            votes = mat[sel]
            if not len(votes):
                continue
            resolved = self._predict(bundle, etype, votes)
            for k, v in label_matrix_metrics(votes).items():
                out[f"{k}__{etype}"] = v
            out[f"resolved_rate__{etype}"] = float(np.mean(resolved >= 0))
            has_vote = (votes >= 0).any(axis=1)
            total += len(votes)
            covered += int(has_vote.sum())
            flags.append(has_vote)
            groups.append(grp[sel])
        out["coverage"] = covered / total if total else float("nan")
        if isinstance(bundle, dict) and "rule" not in bundle:
            for etype, model in bundle.items():
                lfs = self.mats[etype][1]
                for lf, a in zip(lfs, model.accuracy, strict=True):
                    out[f"accuracy__{etype}__{lf}"] = float(a)
        covered_flags = np.concatenate(flags) if flags else np.zeros(0, dtype=bool)
        if part == "eval":
            self.entity_groups = np.concatenate(groups) if groups else np.zeros(0)

        def row_stat(idx, flags=covered_flags):
            return float(np.mean(flags[idx]))

        return Scored(out, total, row_stat, None, None)

    def block_sampler(self):
        if self.sampler is None:
            self.sampler = BlockSampler(
                self.entity_groups.astype(object),
                BOOTSTRAP_MAX_ROWS,
                self.ctx.thresholds["bootstrap_seed"],
            )
        return self.sampler

    def segments(self, sc):
        return {}


# COMMAND ----------

# DBTITLE 1,Matching evaluator


class MatchingEvaluator(Evaluator):
    """Match-tier classification; tiers with fewer than the minimum rows stay not
    estimable, as in the candidate record."""

    def load(self, df_eval, df_validation, validation_fraction: float = 1.0):
        spec = self.spec
        feats = resolve_features(self.ctx, df_eval.columns)
        wanted = [*feats, spec["text_col"], spec["target"], "partition", "group_key"]
        cols = [c for c in dict.fromkeys(wanted) if c in set(df_eval.columns)]
        pdf, _ = to_training_frame(df_eval, key_cols=spec["key_cols"], columns=cols)
        pdf[spec["target"]] = pdf[spec["target"]].astype(str)
        mine = pdf["partition"] == self.ctx.partition
        self.frames["eval"] = pdf[mine].reset_index(drop=True)
        earlier = pdf["partition"] == "validation"
        self.frames["validation"] = pdf[earlier].reset_index(drop=True)
        note_read(self.ctx, self.frames["eval"], None, "group_key")

    def attach(self, bundles, base_name):
        super().attach(bundles, base_name)
        classes = {c for b in bundles.values() for c in getattr(b, "classes", [])}
        self.labels = sorted(classes)
        if not self.labels:
            self.labels = sorted(self.frames["eval"][self.spec["target"]].unique())

    def score(self, bundle, part, bundles):
        frame = self.frame(part)
        y = frame[self.spec["target"]].to_numpy()
        pred = np.asarray(bundle.predict(frame))
        metrics = tier_metrics(y, pred, self.labels)
        labels = self.labels

        def row_stat(idx, y=y, pred=pred):
            yt, yp = y[idx], pred[idx]
            recalls = [
                np.mean(yp[yt == lab] == lab) for lab in labels if (yt == lab).any()
            ]
            return float(np.mean(recalls)) if recalls else float("nan")

        return Scored(metrics, len(frame), row_stat, None, None)
