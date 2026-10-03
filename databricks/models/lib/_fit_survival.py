# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # SURVIVAL MODEL LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** Kaplan-Meier baseline and the Cox, survival forest and boosted Cox
# MAGIC candidates for unit lifetimes, pulled in with `%run ../lib/_fit_survival` after
# MAGIC `_model_common` and `_model_metrics`. Only Cox uses the delayed entry time; the
# MAGIC forest and the boosted model ignore it and say so in their parameters.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Survival curves from risk scores


def breslow_baseline(duration, event, risk):
    """(event times, cumulative baseline hazard) from risk scores, Breslow form."""
    d = np.asarray(duration, dtype="float64")
    e = np.asarray(event) > 0
    r = np.asarray(risk, dtype="float64")
    times = np.unique(d[e])
    cum = np.zeros(len(times))
    total = 0.0
    for k, t in enumerate(times):
        at_risk = float(np.sum(r[d >= t]))
        deaths = float(np.sum((d == t) & e))
        if at_risk > 0:
            total += deaths / at_risk
        cum[k] = total
    return times, cum


def survival_at(times, cum_hazard, risk, horizons):
    """exp(-H0(h) * risk) for each row and horizon."""
    idx = (
        np.searchsorted(times, np.asarray(horizons, dtype="float64"), side="right") - 1
    )
    h0 = np.where(idx >= 0, np.asarray(cum_hazard)[np.maximum(idx, 0)], 0.0)
    r = np.asarray(risk, dtype="float64")[:, None]
    return np.exp(-h0[None, :] * r)


def _score(duration, event, risk, surv):
    out = {"concordance": concordance_index(duration, event, risk)}
    for j, h in enumerate(SURVIVAL_HORIZONS_YEARS):
        out[f"brier_{h}y"] = brier_at(duration, event, surv[:, j], h)
        out[f"calibration_gap_{h}y"] = survival_calibration_gap(
            duration, event, surv[:, j], h
        )
    return out


# COMMAND ----------

# DBTITLE 1,Baseline and fitted bundles


class KaplanMeierBaseline:
    """One Kaplan-Meier curve per group (for example onshore and offshore)."""

    def __init__(self, group_col):
        self.group_col = group_col
        self.curves: dict = {}
        self.overall = None

    def fit(self, train, duration_col, event_col):
        self.overall = kaplan_meier(train[duration_col], train[event_col])
        for g, part in train.groupby(self.group_col):
            self.curves[g] = kaplan_meier(part[duration_col], part[event_col])
        return self

    def survival(self, pdf):
        out = np.ones((len(pdf), len(SURVIVAL_HORIZONS_YEARS)))
        groups = pdf[self.group_col].to_numpy()
        for g in np.unique(groups):
            times, surv = self.curves.get(g, self.overall)
            out[groups == g] = km_at(times, surv, SURVIVAL_HORIZONS_YEARS)
        return out

    def risk(self, pdf):
        return 1.0 - self.survival(pdf)[:, len(SURVIVAL_HORIZONS_YEARS) // 2]


class SurvivalBundle:
    """A fitted survival candidate; risk() is higher for earlier failure."""

    def __init__(self, kind, encoder, model, baseline=None):
        self.kind = kind
        self.encoder = encoder
        self.model = model
        self.baseline = baseline  # (times, cumulative hazard) for hazard-ratio models

    def _x(self, pdf):
        return self.encoder.transform(pdf, fill=True)

    def risk(self, pdf):
        x = self._x(pdf)
        if self.kind == "cox":
            return np.asarray(self.model.predict_partial_hazard(self._frame(x)))
        return np.asarray(self.model.predict(x), dtype="float64")

    def _frame(self, x):
        return pd.DataFrame(x, columns=[f"x{i}" for i in range(x.shape[1])])

    def survival(self, pdf):
        x = self._x(pdf)
        if self.kind == "cox":
            s = self.model.predict_survival_function(
                self._frame(x), times=list(SURVIVAL_HORIZONS_YEARS)
            )
            return s.to_numpy().T
        if self.kind == "forest":
            curves = self.model.predict_survival_function(x, return_array=True)
            idx = np.searchsorted(
                self.model.unique_times_, SURVIVAL_HORIZONS_YEARS, side="right"
            )
            idx = np.clip(idx - 1, 0, curves.shape[1] - 1)
            return curves[:, idx]
        times, cum = self.baseline
        return survival_at(times, cum, self.risk(pdf), SURVIVAL_HORIZONS_YEARS)


# COMMAND ----------

# DBTITLE 1,Survival runner


SURVIVAL_TRAIN_CONCORDANCE_ROWS = 20000


def _train_concordance(bundle, train, dur_tr, ev_tr):
    idx = np.arange(len(train))
    if len(idx) > SURVIVAL_TRAIN_CONCORDANCE_ROWS:
        idx = idx[:: len(idx) // SURVIVAL_TRAIN_CONCORDANCE_ROWS + 1]
    return {
        "train_concordance": concordance_index(
            dur_tr[idx], ev_tr[idx], bundle.risk(train.iloc[idx])
        )
    }


def _make_cox(penalizer=0.1):
    from lifelines import CoxPHFitter

    return CoxPHFitter(penalizer=penalizer)


def _make_forest():
    from sksurv.ensemble import RandomSurvivalForest

    return RandomSurvivalForest(
        n_estimators=200, min_samples_leaf=15, n_jobs=-1, random_state=MODEL_SEED
    )


def _make_boosted_cox():
    import xgboost as xgb

    return xgb.XGBRegressor(
        objective="survival:cox",
        n_estimators=300,
        learning_rate=0.05,
        max_depth=3,
        random_state=MODEL_SEED,
    )


def run_survival(ctx, df, spec):
    """Kaplan-Meier baseline plus Cox, survival forest and boosted Cox.

    spec: duration_col, event_col, entry_col, key_cols, group_col, drop, models,
    optional diagnostic_feature: a Cox model on that one column, recorded beside the
    baseline, because with one calendar censor date the duration of a censored unit
    follows from its start year."""
    dcol, ecol, ncol = spec["duration_col"], spec["event_col"], spec["entry_col"]
    feats = resolve_features(ctx, df.columns, drop=spec.get("drop", ()))
    diag = spec.get("diagnostic_feature")
    diag = diag if diag in df.columns else None
    cols = list(
        dict.fromkeys(
            [
                *feats,
                *([diag] if diag else []),
                dcol,
                ecol,
                ncol,
                spec["group_col"],
                "partition",
                *spec["key_cols"],
            ]
        )
    )
    pdf, fraction = to_training_frame(df, key_cols=spec["key_cols"], columns=cols)
    pdf = pdf[pd.to_numeric(pdf[dcol], errors="coerce") > 0].reset_index(drop=True)
    tr_m = (pdf["partition"] == "train").to_numpy()
    va_m = (pdf["partition"] == "validation").to_numpy()
    if not tr_m.any() or not va_m.any():
        raise RuntimeError("need train and validation rows")
    train, valid = pdf[tr_m], pdf[va_m]
    dur = pd.to_numeric(pdf[dcol]).to_numpy(dtype="float64")
    ev = pd.to_numeric(pdf[ecol]).to_numpy(dtype="float64")
    entry = np.minimum(
        pd.to_numeric(pdf[ncol], errors="coerce").fillna(0.0).to_numpy(dtype="float64"),
        dur * 0.999,
    )
    enc = FeatureEncoder(feats).fit(train)
    start_task(ctx)

    def scored(bundle):
        metrics = _score(
            dur[va_m], ev[va_m], bundle.risk(valid), bundle.survival(valid)
        )
        metrics.update(
            train_side(lambda: _train_concordance(bundle, train, dur[tr_m], ev[tr_m]))
        )
        return bundle, metrics, int(va_m.sum())

    km = KaplanMeierBaseline(spec["group_col"]).fit(train, dcol, ecol)
    run_candidate(
        ctx,
        "kaplan_meier",
        "baseline",
        lambda: (
            km,
            _score(dur[va_m], ev[va_m], km.risk(valid), km.survival(valid)),
            int(va_m.sum()),
        ),
        params={"group": spec["group_col"]},
        stage="baseline",
    )

    keep = np.nanstd(enc.transform(train, fill=True), axis=0) > 0
    enc_keep = [f for f, k in zip(feats, keep, strict=True) if k]
    enc_k = FeatureEncoder(enc_keep).fit(train)

    def fit_cox():
        model = _make_cox()
        frame = pd.DataFrame(
            enc_k.transform(train, fill=True),
            columns=[f"x{i}" for i in range(len(enc_keep))],
        )
        frame["duration"], frame["event"], frame["entry"] = (
            dur[tr_m],
            ev[tr_m],
            entry[tr_m],
        )
        model.fit(frame, duration_col="duration", event_col="event", entry_col="entry")
        return scored(SurvivalBundle("cox", enc_k, model))

    def fit_forest():
        model = _make_forest()
        y = np.array(
            list(zip(ev[tr_m] > 0, dur[tr_m], strict=True)),
            dtype=[("event", "?"), ("time", "<f8")],
        )
        model.fit(enc_k.transform(train, fill=True), y)
        return scored(SurvivalBundle("forest", enc_k, model))

    def fit_boosted():
        model = _make_boosted_cox()
        label = np.where(ev[tr_m] > 0, dur[tr_m], -dur[tr_m])
        model.fit(enc_k.transform(train, fill=True), label)
        risk_tr = np.asarray(
            model.predict(enc_k.transform(train, fill=True)), dtype="float64"
        )
        baseline = breslow_baseline(dur[tr_m], ev[tr_m], risk_tr)
        return scored(SurvivalBundle("boosted_cox", enc_k, model, baseline))

    table = {
        "cox_lifelines": (fit_cox, ("lifelines",), {"entry": "delayed entry used"}),
        "survival_forest": (fit_forest, ("sksurv",), {"entry": "ignored"}),
        "boosted_cox_xgboost": (fit_boosted, ("xgboost",), {"entry": "ignored"}),
    }
    for name in spec["models"]:
        fn, requires, params = table[name]
        run_candidate(
            ctx,
            name,
            "survival",
            fn,
            requires=requires,
            params={**params, "row_fraction": round(fraction, 4)},
        )
    if diag:

        def fit_diagnostic():
            enc_d = FeatureEncoder([diag]).fit(train)
            frame = pd.DataFrame(enc_d.transform(train, fill=True), columns=["x0"])
            frame["duration"], frame["event"], frame["entry"] = (
                dur[tr_m],
                ev[tr_m],
                entry[tr_m],
            )
            model = _make_cox()
            model.fit(
                frame, duration_col="duration", event_col="event", entry_col="entry"
            )
            return scored(SurvivalBundle("cox", enc_d, model))

        run_candidate(
            ctx,
            f"{diag}_cox",
            "survival",
            fit_diagnostic,
            requires=("lifelines",),
            params={"feature": diag, "entry": "delayed entry used"},
            stage="diagnostic",
        )
    finish_task(ctx)
