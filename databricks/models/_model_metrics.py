# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL METRICS LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** the evaluation metrics used by the model notebooks, pulled in with
# MAGIC `%run ../_model_metrics`. Numpy only, no Spark, so every function is unit-tested.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np

# COMMAND ----------

# DBTITLE 1,Regression


def _pair(y, p):
    y = np.asarray(y, dtype="float64")
    p = np.asarray(p, dtype="float64")
    ok = np.isfinite(y) & np.isfinite(p)
    return y[ok], p[ok]


def mae(y, p) -> float:
    y, p = _pair(y, p)
    return float(np.mean(np.abs(y - p))) if len(y) else float("nan")


def rmse(y, p) -> float:
    y, p = _pair(y, p)
    return float(np.sqrt(np.mean((y - p) ** 2))) if len(y) else float("nan")


def skill(model_error: float, baseline_error: float) -> float:
    """1 - model / baseline; positive means better than the baseline."""
    if not np.isfinite(baseline_error) or baseline_error <= 0:
        return float("nan")
    return float(1.0 - model_error / baseline_error)


def pinball(y, q_pred, tau: float) -> float:
    y, q = _pair(y, q_pred)
    if not len(y):
        return float("nan")
    d = y - q
    return float(np.mean(np.maximum(tau * d, (tau - 1.0) * d)))


def regression_metrics(y, pred, baseline_pred=None) -> dict:
    out = {"mae": mae(y, pred), "rmse": rmse(y, pred)}
    if baseline_pred is not None:
        out["skill_mae"] = skill(out["mae"], mae(y, baseline_pred))
        out["skill_rmse"] = skill(out["rmse"], rmse(y, baseline_pred))
    return out


def quantile_metrics(y, preds: dict, baseline_preds: dict | None = None) -> dict:
    """preds: {tau: predictions}; pinball per quantile plus coverage of the
    central interval when tau 0.1 and 0.9 are both present."""
    out = {}
    for tau, q in preds.items():
        out[f"pinball_q{round(tau * 100):02d}"] = pinball(y, q, tau)
        if baseline_preds is not None and tau in baseline_preds:
            out[f"skill_pinball_q{round(tau * 100):02d}"] = skill(
                out[f"pinball_q{round(tau * 100):02d}"],
                pinball(y, baseline_preds[tau], tau),
            )
    if 0.1 in preds and 0.9 in preds:
        y_, lo = _pair(y, preds[0.1])
        _, hi = _pair(y, preds[0.9])
        if len(y_) and len(lo) == len(hi) == len(y_):
            out["interval_coverage_80"] = float(np.mean((y_ >= lo) & (y_ <= hi)))
    return out


# COMMAND ----------

# DBTITLE 1,Classification


def average_precision(y_true, score) -> float:
    """Area under the precision-recall curve as average precision."""
    y, s = _pair(y_true, score)
    n_pos = float(np.sum(y > 0))
    if not len(y) or n_pos == 0:
        return float("nan")
    if np.ptp(s) == 0:
        return n_pos / len(y)
    order = np.argsort(-s, kind="mergesort")
    hit = (y[order] > 0).astype("float64")
    tp = np.cumsum(hit)
    precision = tp / np.arange(1, len(hit) + 1)
    return float(np.sum(precision * hit) / n_pos)


def log_loss(y_true, p, eps: float = 1e-15) -> float:
    y, q = _pair(y_true, p)
    if not len(y):
        return float("nan")
    q = np.clip(q, eps, 1 - eps)
    return float(-np.mean(y * np.log(q) + (1 - y) * np.log(1 - q)))


def calibration_error(y_true, p, bins: int = 10) -> float:
    """Expected calibration error over equal-width probability bins."""
    y, q = _pair(y_true, p)
    if not len(y):
        return float("nan")
    idx = np.minimum((np.clip(q, 0, 1) * bins).astype(int), bins - 1)
    err = 0.0
    for b in range(bins):
        m = idx == b
        if m.any():
            err += m.sum() / len(y) * abs(float(y[m].mean()) - float(q[m].mean()))
    return float(err)


def classification_metrics(y_true, p) -> dict:
    y, q = _pair(y_true, p)
    return {
        "pr_auc": average_precision(y, q),
        "log_loss": log_loss(y, q),
        "calibration_error": calibration_error(y, q),
        "base_rate": float(np.mean(y)) if len(y) else float("nan"),
    }


# COMMAND ----------

# DBTITLE 1,Survival


def concordance_index(duration, event, risk) -> float:
    """Harrell's C: among comparable pairs, the share where the higher risk
    failed first; risk ties count one half."""
    d = np.asarray(duration, dtype="float64")
    e = np.asarray(event, dtype="float64") > 0
    r = np.asarray(risk, dtype="float64")
    ok = np.isfinite(d) & np.isfinite(r)
    d, e, r = d[ok], e[ok], r[ok]
    conc = 0.0
    comparable = 0.0
    for i in np.flatnonzero(e):
        later = d > d[i]
        n = float(later.sum())
        if n == 0:
            continue
        comparable += n
        conc += float(np.sum(r[i] > r[later])) + 0.5 * float(np.sum(r[i] == r[later]))
    return float(conc / comparable) if comparable else float("nan")


def kaplan_meier(duration, event):
    """(sorted unique event times, survival after each) for right-censored data."""
    d = np.asarray(duration, dtype="float64")
    e = np.asarray(event, dtype="float64") > 0
    ok = np.isfinite(d)
    d, e = d[ok], e[ok]
    times = np.unique(d[e])
    surv = np.ones(len(times))
    s = 1.0
    for k, t in enumerate(times):
        at_risk = float(np.sum(d >= t))
        deaths = float(np.sum((d == t) & e))
        if at_risk > 0:
            s *= 1.0 - deaths / at_risk
        surv[k] = s
    return times, surv


def km_at(times, surv, t) -> np.ndarray:
    """Step-function value of a Kaplan-Meier curve at t (1 before the first event)."""
    t = np.atleast_1d(np.asarray(t, dtype="float64"))
    if len(times) == 0:
        return np.ones(len(t))
    idx = np.searchsorted(times, t, side="right") - 1
    return np.where(idx >= 0, np.asarray(surv)[np.maximum(idx, 0)], 1.0)


def brier_at(duration, event, surv_at_t, horizon: float) -> float:
    """Inverse-probability-of-censoring weighted Brier score at one horizon."""
    d = np.asarray(duration, dtype="float64")
    e = np.asarray(event, dtype="float64") > 0
    s = np.asarray(surv_at_t, dtype="float64")
    ok = np.isfinite(d) & np.isfinite(s)
    d, e, s = d[ok], e[ok], s[ok]
    if not len(d):
        return float("nan")
    ct, cs = kaplan_meier(d, ~e)
    g_d = np.maximum(km_at(ct, cs, d), 1e-6)
    g_h = float(np.maximum(km_at(ct, cs, horizon)[0], 1e-6))
    score = np.zeros(len(d))
    failed = (d <= horizon) & e
    alive = d > horizon
    score[failed] = (s[failed] ** 2) / g_d[failed]
    score[alive] = ((1.0 - s[alive]) ** 2) / g_h
    return float(np.mean(score))


def survival_calibration_gap(duration, event, surv_at_t, horizon: float) -> float:
    """Absolute gap between the mean predicted survival and the Kaplan-Meier
    observed survival at the horizon."""
    times, surv = kaplan_meier(duration, event)
    observed = float(km_at(times, surv, horizon)[0])
    s = np.asarray(surv_at_t, dtype="float64")
    s = s[np.isfinite(s)]
    return float(abs(np.mean(s) - observed)) if len(s) else float("nan")


# COMMAND ----------

# DBTITLE 1,Ranking


def ranking_metrics(ranks, ks=(5, 10, 20)) -> dict:
    """ranks: 1-based rank of the true item per case; inf when it is not ranked."""
    r = np.asarray(ranks, dtype="float64")
    if not len(r):
        return {}
    out = {}
    for k in ks:
        hit = r <= k
        out[f"recall_at_{k}"] = float(np.mean(hit))
        out[f"ndcg_at_{k}"] = float(np.mean(np.where(hit, 1.0 / np.log2(r + 1.0), 0.0)))
    out["mrr"] = float(np.mean(np.where(np.isfinite(r), 1.0 / r, 0.0)))
    return out


# COMMAND ----------

# DBTITLE 1,Reconstruction, anomaly and agreement


def masked_mae(y_true, y_pred, mask) -> float:
    m = np.asarray(mask, dtype=bool)
    return mae(np.asarray(y_true)[m], np.asarray(y_pred)[m])


def detection_metrics(injected, flagged, flagged_uninjected_rate: float) -> dict:
    """Precision and recall of flags against injected anomalies, plus the flag
    rate on the same series before injection."""
    inj = np.asarray(injected, dtype=bool)
    fl = np.asarray(flagged, dtype=bool)
    tp = float(np.sum(inj & fl))
    precision = tp / float(fl.sum()) if fl.sum() else float("nan")
    recall = tp / float(inj.sum()) if inj.sum() else float("nan")
    return {
        "precision": float(precision),
        "recall": float(recall),
        "flag_rate": float(flagged_uninjected_rate),
    }


def agreement_metrics(logged, predicted, reward) -> dict:
    """Top-1 action agreement and the logged reward where the policy agrees
    and disagrees with the logged action."""
    lg = np.asarray(logged)
    pr = np.asarray(predicted)
    rw = np.asarray(reward, dtype="float64")
    agree = lg == pr
    out = {"action_agreement": float(np.mean(agree)) if len(agree) else float("nan")}
    out["reward_when_agree"] = (
        float(np.mean(rw[agree])) if agree.any() else float("nan")
    )
    out["reward_when_disagree"] = (
        float(np.mean(rw[~agree])) if (~agree).any() else float("nan")
    )
    total = float(np.sum(rw))
    out["reward_weighted_agreement"] = (
        float(np.sum(rw[agree]) / total) if total != 0 else float("nan")
    )
    return out


# COMMAND ----------

# DBTITLE 1,Weak supervision and matching


def label_matrix_metrics(matrix) -> dict:
    """matrix: entities x labelling functions, integer labels, -1 for abstain."""
    m = np.asarray(matrix)
    if m.ndim != 2 or not len(m):
        return {}
    votes = m >= 0
    n_votes = votes.sum(axis=1)
    out = {"coverage": float(np.mean(n_votes >= 1))}
    multi = n_votes >= 2
    if multi.any():
        conflict = 0
        for row, v in zip(m[multi], votes[multi], strict=True):
            conflict += int(len(set(row[v].tolist())) > 1)
        out["conflict_rate"] = float(conflict / multi.sum())
        agree, pairs = 0, 0
        for a in range(m.shape[1]):
            for b in range(a + 1, m.shape[1]):
                both = votes[:, a] & votes[:, b]
                pairs += int(both.sum())
                agree += int(np.sum(m[both, a] == m[both, b]))
        out["agreement_rate"] = float(agree / pairs) if pairs else float("nan")
    return out


def per_class_precision_recall(y_true, y_pred, labels, min_rows: int = 10) -> dict:
    """{label: (precision, recall, n_true, estimable)}; a class with fewer than
    min_rows true rows is marked not estimable and its scores are nan."""
    yt = np.asarray(y_true)
    yp = np.asarray(y_pred)
    out = {}
    for lab in labels:
        n_true = int(np.sum(yt == lab))
        tp = float(np.sum((yt == lab) & (yp == lab)))
        n_pred = float(np.sum(yp == lab))
        ok = n_true >= min_rows
        precision = tp / n_pred if n_pred else float("nan")
        recall = tp / n_true if n_true else float("nan")
        out[lab] = (
            precision if ok else float("nan"),
            recall if ok else float("nan"),
            n_true,
            ok,
        )
    return out
