# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # WEAK SUPERVISION AND MATCHING LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** the two small entity datasets, pulled in with `%run ../_fit_weak`
# MAGIC after `_model_common`, `_model_metrics` and `_fit_tabular` (its classifier
# MAGIC builders). Weak supervision combines labelling-function votes per entity type;
# MAGIC matching predicts the match tier of an asset text. Both read the evaluation
# MAGIC split manifest, so entities never straddle partitions.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np

# COMMAND ----------

# DBTITLE 1,Label matrices and the label model


def label_matrices(pdf):
    """{entity type: (votes entities x functions, function names, label names,
    partition per entity)}; -1 marks an abstain."""
    out = {}
    for etype, part in pdf.groupby("entity_type"):
        wide = part.pivot(index="entity_id", columns="lf_name", values="lf_label")
        labels = sorted(part["lf_label"].dropna().unique().tolist())
        code = {lab: i for i, lab in enumerate(labels)}
        votes = (
            wide.apply(lambda col, code=code: col.map(code)).fillna(-1).astype("int64")
        )
        parts = (
            part.drop_duplicates("entity_id")
            .set_index("entity_id")["partition"]
            .reindex(wide.index)
        )
        out[etype] = (votes.to_numpy(), list(wide.columns), labels, parts.to_numpy())
    return out


def majority_vote(matrix):
    """Most common vote per entity; a tie or no vote stays -1."""
    out = np.full(len(matrix), -1, dtype="int64")
    for i, row in enumerate(matrix):
        seen = row[row >= 0]
        if not len(seen):
            continue
        counts = np.bincount(seen)
        top = counts.max()
        if (counts == top).sum() == 1:
            out[i] = int(counts.argmax())
    return out


class LabelModel:
    """Binary latent class with one symmetric accuracy per labelling function,
    fitted by EM (the Dawid-Skene form with a single accuracy per function)."""

    def __init__(self, prior, accuracy):
        self.prior, self.accuracy = prior, np.asarray(accuracy, dtype="float64")

    def posterior(self, matrix):
        lo = np.full(len(matrix), np.log(self.prior / (1.0 - self.prior)))
        for k, acc in enumerate(self.accuracy):
            w = np.log(acc / (1.0 - acc))
            lo += np.where(matrix[:, k] == 1, w, 0.0) - np.where(
                matrix[:, k] == 0, w, 0.0
            )
        return 1.0 / (1.0 + np.exp(-lo))

    def predict(self, matrix):
        p = self.posterior(matrix)
        out = (p >= 0.5).astype("int64")
        return np.where((matrix >= 0).any(axis=1), out, -1)


def fit_label_model(matrix, iterations=50):
    votes = matrix >= 0
    model = LabelModel(0.5, np.full(matrix.shape[1], 0.7))
    for _ in range(iterations):
        post = model.posterior(matrix)
        prior = float(np.clip(post.mean(), 0.01, 0.99))
        acc = []
        for k in range(matrix.shape[1]):
            v = votes[:, k]
            agree = np.where(matrix[:, k] == 1, post, 1.0 - post)[v]
            acc.append(float(np.clip(agree.mean(), 0.51, 0.99)) if v.any() else 0.7)
        model = LabelModel(prior, acc)
    return model


# COMMAND ----------

# DBTITLE 1,Weak supervision runner


def run_weak_supervision(ctx, df, spec):
    """Majority vote baseline and the label model, per entity type, on the
    evaluation split. Coverage, conflict rate and agreement are reported per
    entity type; the label model needs at least two labelling functions."""
    pdf, fraction = to_training_frame(
        df,
        key_cols=spec["key_cols"],
        columns=["entity_type", "entity_id", "lf_name", "lf_label", "partition"],
    )
    mats = label_matrices(pdf)
    n_valid = sum(int((m[3] == "validation").sum()) for m in mats.values())
    start_task(ctx)

    def metrics_for(predictor):
        out, total, covered = {}, 0, 0
        for etype, (mat, _lfs, _labels, part) in mats.items():
            va = mat[part == "validation"]
            if not len(va):
                continue
            resolved = predictor(etype, mat, part)
            mm = label_matrix_metrics(va)
            for k, v in mm.items():
                out[f"{k}__{etype}"] = v
            out[f"resolved_rate__{etype}"] = float(np.mean(resolved >= 0))
            total += len(va)
            covered += int(np.sum((va >= 0).any(axis=1)))
        out["coverage"] = covered / total if total else float("nan")
        tr_total = sum(int((m[3] == "train").sum()) for m in mats.values())
        tr_covered = sum(
            int(np.sum((m[0][m[3] == "train"] >= 0).any(axis=1))) for m in mats.values()
        )
        out["train_coverage"] = tr_covered / tr_total if tr_total else float("nan")
        return out

    def vote_predict(etype, mat, part):
        return majority_vote(mat[part == "validation"])

    run_candidate(
        ctx,
        "majority_vote",
        "baseline",
        lambda: ({"rule": "majority"}, metrics_for(vote_predict), n_valid),
        params={"row_fraction": round(fraction, 4)},
        stage="baseline",
    )

    if "label_model" in spec["models"]:
        fitted = {}

        def fit():
            for etype, (mat, lfs, _labels, part) in mats.items():
                if mat.shape[1] >= 2 and len(_labels) == 2 and (part == "train").any():
                    fitted[etype] = fit_label_model(mat[part == "train"])

            def predict(etype, mat, part):
                va = mat[part == "validation"]
                if etype in fitted:
                    return fitted[etype].predict(va)
                return majority_vote(va)

            out = metrics_for(predict)
            for etype, model in fitted.items():
                for lf, a in zip(mats[etype][1], model.accuracy, strict=True):
                    out[f"accuracy__{etype}__{lf}"] = float(a)
            return fitted, out, n_valid

        run_candidate(
            ctx,
            "label_model",
            "weak_supervision",
            fit,
            params={
                "em_iterations": 50,
                "note": "two correlated functions; accuracies not identifiable",
                "applies_to": "entity types with two or more functions",
            },
        )
    finish_task(ctx)


# COMMAND ----------

# DBTITLE 1,Matching runner


class TierClassifier:
    """A fitted tier classifier: encoder (and optional text vectoriser) plus model."""

    def __init__(self, encoder, model, classes, fill, vectoriser=None, text_col=None):
        self.encoder, self.model, self.classes = encoder, model, classes
        self.fill, self.vectoriser, self.text_col = fill, vectoriser, text_col

    def _x(self, pdf):
        x = self.encoder.transform(pdf, fill=self.fill)
        if self.vectoriser is None:
            return x
        from scipy import sparse

        text = self.vectoriser.transform(pdf[self.text_col].astype(str))
        return sparse.hstack([sparse.csr_matrix(np.nan_to_num(x)), text]).tocsr()

    def predict(self, pdf):
        return np.asarray(self.model.predict(self._x(pdf)))


class MajorityTier:
    def __init__(self, tier):
        self.tier = tier

    def predict(self, pdf):
        return np.full(len(pdf), self.tier, dtype=object)


def tier_metrics(y_true, y_pred, labels):
    per = per_class_precision_recall(y_true, y_pred, labels, MIN_TIER_ROWS)
    yt, yp = np.asarray(y_true), np.asarray(y_pred)
    out = {"accuracy": float(np.mean(yt == yp))}
    recalls = []
    for lab, (p, r, n, ok) in per.items():
        out[f"precision__{lab}"] = p
        out[f"recall__{lab}"] = r
        out[f"n_true__{lab}"] = float(n)
        out[f"estimable__{lab}"] = 1.0 if ok else 0.0
        if n:
            recalls.append(float(np.sum((yt == lab) & (yp == lab))) / n)
    out["estimable_tiers"] = float(sum(v[3] for v in per.values()))
    out["balanced_accuracy"] = float(np.mean(recalls)) if recalls else float("nan")
    return out


def run_matching(ctx, df, spec):
    """Majority tier baseline, logistic on the numeric features, boosted trees and a
    character n-gram logistic on the asset text; precision and recall per tier."""
    feats = resolve_features(ctx, df.columns)
    text = spec["text_col"]
    cols = list(dict.fromkeys([*feats, text, spec["target"], "partition"]))
    pdf, fraction = to_training_frame(df, key_cols=spec["key_cols"], columns=cols)
    pdf[spec["target"]] = pdf[spec["target"]].astype(str)
    train = pdf[pdf["partition"] == "train"].reset_index(drop=True)
    valid = pdf[pdf["partition"] == "validation"].reset_index(drop=True)
    if train.empty or valid.empty:
        raise RuntimeError(
            f"need train and validation rows, have {len(train)} / {len(valid)}"
        )
    labels = sorted(train[spec["target"]].unique().tolist())
    y_tr, y_va = train[spec["target"]].to_numpy(), valid[spec["target"]].to_numpy()
    enc = FeatureEncoder(feats).fit(train)
    start_task(ctx)

    def scored(model):
        metrics = tier_metrics(y_va, model.predict(valid), labels)

        def fitted():
            t = tier_metrics(y_tr, model.predict(train), labels)
            return {
                "train_accuracy": t["accuracy"],
                "train_balanced_accuracy": t["balanced_accuracy"],
            }

        metrics.update(train_side(fitted))
        return metrics

    major = MajorityTier(train[spec["target"]].value_counts().idxmax())
    run_candidate(
        ctx,
        "majority_tier",
        "baseline",
        lambda: (major, scored(major), len(valid)),
        params={"row_fraction": round(fraction, 4)},
        stage="baseline",
    )

    def classifier(fill, build, vectorise=False):
        def fit():
            vec = None
            if vectorise:
                from sklearn.feature_extraction.text import TfidfVectorizer

                vec = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4), min_df=2)
                vec.fit(train[text].astype(str))
            bundle = TierClassifier(enc, None, labels, fill, vec, text)
            model = build().fit(bundle._x(train), y_tr)
            bundle.model = model
            return bundle, scored(bundle), len(valid)

        return fit

    from_sklearn = ("sklearn",)
    if "logistic_numeric" in spec["models"]:
        run_candidate(
            ctx,
            "logistic_numeric",
            "matching",
            classifier(True, _make_tier_logistic),
            requires=from_sklearn,
            params={"C": 1.0},
        )
    if "gbt_lightgbm" in spec["models"]:
        run_candidate(
            ctx,
            "gbt_lightgbm",
            "matching",
            classifier(False, lambda: _make_lgbm_classifier(GBT_GRID[1])),
            requires=("lightgbm",),
            params=dict(GBT_GRID[1]),
        )
    if "gbt_sklearn" in spec["models"]:
        run_candidate(
            ctx,
            "gbt_sklearn",
            "matching",
            classifier(False, lambda: _make_hgb_classifier(GBT_GRID[1])),
            requires=from_sklearn,
            params=dict(GBT_GRID[1]),
        )
    if "char_ngram_logistic" in spec["models"]:
        run_candidate(
            ctx,
            "char_ngram_logistic",
            "matching",
            classifier(True, _make_tier_logistic, vectorise=True),
            requires=from_sklearn,
            params={"C": 1.0, "ngrams": "char_wb 2-4"},
        )
    finish_task(ctx)


def _make_tier_logistic():
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    return make_pipeline(
        StandardScaler(with_mean=False),
        LogisticRegression(C=1.0, max_iter=1000, class_weight="balanced"),
    )
