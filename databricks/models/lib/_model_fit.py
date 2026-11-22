# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # MODEL FIT LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** baselines and candidates of every paradigm (tabular regression,
# MAGIC quantile regression and classification, multi-class, survival, anomaly,
# MAGIC reconstruction, offline policy, ranking) and `run_task`, which runs one task of
# MAGIC the task file. Pulled in with `%run ../lib/_model_fit` after `_model_common`.
# MAGIC Libraries are imported inside the builders so a missing one becomes a recorded
# MAGIC skip, never an import error.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Baselines


def _lookup(pdf, cols, table, fallback):
    merged = pdf[cols].merge(table, on=cols, how="left")
    return merged["value"].fillna(fallback).to_numpy(dtype="float64")


class BaselineModel:
    """A fitted baseline; state holds the train statistics it needs."""

    def __init__(self, kind, **state):
        self.kind = kind
        self.state = state

    def predict(self, pdf):
        k, s = self.kind, self.state
        if k == "constant":
            return np.full(len(pdf), s["value"], dtype="float64")
        if k == "column":
            col = (
                s["column"] if s["column"] in pdf.columns else s["column"] + "_imputed"
            )
            v = pd.to_numeric(pdf[col], errors="coerce").to_numpy(dtype="float64")
            return np.where(np.isfinite(v), v, s["fallback"])
        if k == "group_mean":
            return _lookup(pdf, s["cols"], s["table"], s["fallback"])
        if k == "group_ratio":
            d = s["denominator"]
            d = d + "_imputed" if d + "_imputed" in pdf.columns else d
            denom = pd.to_numeric(pdf[d], errors="coerce").to_numpy(dtype="float64")
            denom = np.where(np.isfinite(denom), denom, s["denominator_fallback"])
            return _lookup(pdf, s["cols"], s["table"], s["fallback"]) * denom
        raise ValueError(f"unknown baseline kind {k}")


def fit_baseline(train: pd.DataFrame, target: str, spec: dict) -> BaselineModel:
    """spec: kind plus its arguments; fitted on the training rows only."""
    kind = spec["kind"]
    y = pd.to_numeric(train[target], errors="coerce")
    if kind in ("train_mean", "base_rate"):
        return BaselineModel("constant", value=float(y.mean()))
    if kind == "zero":
        return BaselineModel("constant", value=0.0)
    if kind == "column":
        return BaselineModel(
            "column", column=spec["column"], fallback=float(y.median())
        )
    if kind == "group_mean":
        cols = spec["cols"]
        table = train.assign(_y=y).groupby(cols)["_y"].mean().reset_index(name="value")
        return BaselineModel(
            "group_mean", cols=cols, table=table, fallback=float(y.mean())
        )
    if kind == "group_ratio":
        d = spec["denominator"]
        dcol = d + "_imputed" if d + "_imputed" in train.columns else d
        denom = pd.to_numeric(train[dcol], errors="coerce")
        ratio = (y / denom).where(denom > 0)
        table = (
            train.assign(_y=ratio)
            .groupby(spec["cols"])["_y"]
            .mean()
            .reset_index(name="value")
        )
        return BaselineModel(
            "group_ratio",
            cols=spec["cols"],
            table=table,
            fallback=float(ratio.mean()),
            denominator=d,
            denominator_fallback=float(denom.median()),
        )
    raise ValueError(f"unknown baseline kind {kind}")


class QuantileBaseline:
    """Empirical train quantiles, optionally by group (for example month)."""

    def __init__(self, taus, cols, tables, fallback):
        self.taus, self.cols, self.tables, self.fallback = taus, cols, tables, fallback

    def predict(self, pdf):
        out = {}
        for tau in self.taus:
            if self.cols:
                out[tau] = _lookup(pdf, self.cols, self.tables[tau], self.fallback[tau])
            else:
                out[tau] = np.full(len(pdf), self.fallback[tau])
        return out


def fit_quantile_baseline(train, target, taus, cols) -> QuantileBaseline:
    y = pd.to_numeric(train[target], errors="coerce")
    tables, fallback = {}, {}
    for tau in taus:
        fallback[tau] = float(y.quantile(tau))
        if cols:
            tables[tau] = (
                train.assign(_y=y)
                .groupby(cols)["_y"]
                .quantile(tau)
                .reset_index(name="value")
            )
    return QuantileBaseline(list(taus), cols, tables, fallback)


# COMMAND ----------

# DBTITLE 1,Estimator builders (each imports its library when called)

GBT_GRID = [
    {"n_estimators": 300, "learning_rate": 0.05, "num_leaves": 31},
    {
        "n_estimators": 300,
        "learning_rate": 0.05,
        "num_leaves": 15,
        "min_child_samples": 50,
    },
]
RIDGE_GRID = [{"alpha": a} for a in (1.0, 10.0, 100.0)]
LOGISTIC_GRID = [{"C": c} for c in (0.1, 1.0, 10.0)]
MLP_GRID = [{"hidden": (32,)}, {"hidden": (64, 32)}]


def _make_ridge(p):
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    return make_pipeline(StandardScaler(), Ridge(alpha=p["alpha"]))


def _make_mlp(p):
    from sklearn.compose import TransformedTargetRegressor
    from sklearn.neural_network import MLPRegressor
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    net = make_pipeline(
        StandardScaler(),
        MLPRegressor(
            hidden_layer_sizes=p["hidden"],
            early_stopping=True,
            max_iter=300,
            random_state=MODEL_SEED,
        ),
    )
    return TransformedTargetRegressor(regressor=net, transformer=StandardScaler())


def _make_lgbm_regressor(p, objective="regression", alpha=None):
    import lightgbm as lgb

    kw = {
        "objective": objective,
        "n_estimators": p["n_estimators"],
        "learning_rate": p["learning_rate"],
        "num_leaves": p["num_leaves"],
        "min_child_samples": p.get("min_child_samples", 20),
        "random_state": MODEL_SEED,
        "verbose": -1,
    }
    if alpha is not None:
        kw["alpha"] = alpha
    return lgb.LGBMRegressor(**kw)


def _make_hgb_regressor(p, loss="squared_error", quantile=None):
    from sklearn.ensemble import HistGradientBoostingRegressor

    kw = {
        "loss": loss,
        "max_iter": p["n_estimators"],
        "learning_rate": p["learning_rate"],
        "max_leaf_nodes": p["num_leaves"],
        "min_samples_leaf": p.get("min_child_samples", 20),
        "random_state": MODEL_SEED,
    }
    if quantile is not None:
        kw["quantile"] = quantile
    return HistGradientBoostingRegressor(**kw)


def _make_logistic(p):
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    return make_pipeline(StandardScaler(), LogisticRegression(C=p["C"], max_iter=1000))


def _make_lgbm_classifier(p):
    import lightgbm as lgb

    return lgb.LGBMClassifier(
        n_estimators=p["n_estimators"],
        learning_rate=p["learning_rate"],
        num_leaves=p["num_leaves"],
        min_child_samples=p.get("min_child_samples", 20),
        random_state=MODEL_SEED,
        verbose=-1,
    )


def _make_hgb_classifier(p):
    from sklearn.ensemble import HistGradientBoostingClassifier

    return HistGradientBoostingClassifier(
        max_iter=p["n_estimators"],
        learning_rate=p["learning_rate"],
        max_leaf_nodes=p["num_leaves"],
        min_samples_leaf=p.get("min_child_samples", 20),
        random_state=MODEL_SEED,
    )


# name -> (required libraries, needs filled inputs, grid, builder)
REGRESSORS = {
    "ridge": (("sklearn",), True, RIDGE_GRID, _make_ridge),
    "gbt_lightgbm": (("lightgbm",), False, GBT_GRID, _make_lgbm_regressor),
    "gbt_sklearn": (("sklearn",), False, GBT_GRID, _make_hgb_regressor),
    "poisson_gbt_lightgbm": (
        ("lightgbm",),
        False,
        GBT_GRID,
        lambda p: _make_lgbm_regressor(p, "poisson"),
    ),
    "poisson_gbt_sklearn": (
        ("sklearn",),
        False,
        GBT_GRID,
        lambda p: _make_hgb_regressor(p, "poisson"),
    ),
    "mlp": (("sklearn",), True, MLP_GRID, _make_mlp),
}
CLASSIFIERS = {
    "logistic": (("sklearn",), True, LOGISTIC_GRID, _make_logistic),
    "gbt_lightgbm": (("lightgbm",), False, GBT_GRID, _make_lgbm_classifier),
    "gbt_sklearn": (("sklearn",), False, GBT_GRID, _make_hgb_classifier),
}
QUANTILE_MODELS = {
    "quantile_gbt_lightgbm": (
        ("lightgbm",),
        False,
        lambda p, tau: _make_lgbm_regressor(p, "quantile", alpha=tau),
    ),
    "quantile_gbt_sklearn": (
        ("sklearn",),
        False,
        lambda p, tau: _make_hgb_regressor(p, "quantile", quantile=tau),
    ),
}


class TabularBundle:
    """A fitted candidate plus its encoder; predict() takes the raw frame."""

    def __init__(self, encoder, model, task, fill, quantile_models=None):
        self.encoder = encoder
        self.model = model
        self.task = task
        self.fill = fill
        self.quantile_models = quantile_models

    def predict(self, pdf):
        x = self.encoder.transform(pdf, fill=self.fill)
        if self.task == "classification":
            return self.model.predict_proba(x)[:, 1]
        if self.task == "quantile":
            raw = np.column_stack(
                [
                    self.quantile_models[t].predict(x)
                    for t in sorted(self.quantile_models)
                ]
            )
            srt = np.sort(raw, axis=1)
            return dict(zip(sorted(self.quantile_models), srt.T, strict=True))
        return self.model.predict(x)


# COMMAND ----------

# DBTITLE 1,Frame preparation shared by the tabular runners


def _prepare(ctx, df, spec):
    """(pandas frame, features, row fraction): train and validation rows with
    the target, features, folds and any baseline columns. `extra_features` are
    columns added after the contract was registered (see the load notebook)."""
    feats = resolve_features(
        ctx,
        df.columns,
        id_features=[*spec.get("id_features", ()), *spec.get("extra_features", ())],
        drop=spec.get("drop", ()),
    )
    base = spec.get("baseline", {})
    extra = [base.get("column"), base.get("denominator"), *base.get("cols", [])]
    extra += [*spec.get("quantile_cols", []), spec.get("date_col")]
    extra += spec.get("diagnostic", {}).get("cols", [])
    needed = [spec["target"], "partition", "fold_id", *spec["key_cols"], *extra]
    have = set(df.columns)
    cols = []
    for c in [*feats, *needed]:
        for cand in (c, f"{c}_imputed" if c else None):
            if cand and cand in have and cand not in cols:
                cols.append(cand)
    pdf, fraction = to_training_frame(df, key_cols=spec["key_cols"], columns=cols)
    y = pd.to_numeric(pdf[spec["target"]], errors="coerce")
    pdf = pdf[np.isfinite(y.to_numpy(dtype="float64"))].reset_index(drop=True)
    parts = pdf["partition"].value_counts().to_dict()
    if not parts.get("train") or not parts.get("validation"):
        raise RuntimeError(f"need train and validation rows, have {parts}")
    return pdf, feats, fraction


def _masks(pdf):
    return (pdf["partition"] == "train").to_numpy(), (
        pdf["partition"] == "validation"
    ).to_numpy()


def _train_sample(tr_m):
    """Row positions of the training rows used for the train-side metrics, capped."""
    idx = np.flatnonzero(tr_m)
    if len(idx) > TUNE_MAX_ROWS:
        idx = idx[:: len(idx) // TUNE_MAX_ROWS + 1]
    return idx


def _tune(ctx, spec, pdf, x, y, grid, build, scorer, tr_m):
    """Best grid entry by rolling or grouped folds inside train; the first entry
    when the frame is large or has no folds."""
    if int(tr_m.sum()) > TUNE_MAX_ROWS or len(grid) == 1:
        return grid[0]
    folds = fold_splits(
        pdf,
        spec.get("fold_mode", "none"),
        date_col=spec.get("date_col"),
        gap_days=spec.get("gap_days", 0),
    )

    def fit_score(params, tr, va):
        model = build(params).fit(x[tr], y[tr])
        return scorer(model, x[va], y[va])

    return pick_params(grid, folds, fit_score)


# COMMAND ----------

# DBTITLE 1,Regression runner (point and quantile candidates)


def _point_metrics(y, pred, base_pred, price):
    m = regression_metrics(y, pred, base_pred)
    if price:
        m["pinball_q50"] = pinball(y, pred, 0.5)
    return m


def _diagnostic_metrics(y, pred, frame, diag, diag_pred, skill=True):
    """Skill over the group-mean diagnostic and the error per group."""
    out = {}
    denominator = mae(y, diag_pred)
    if skill and denominator > 0:
        out[f"skill_mae_vs_{diag['name']}"] = 1.0 - mae(y, pred) / denominator
    groups = frame[diag["group_col"]].astype(str).to_numpy()
    for g in np.unique(groups):
        sel = groups == g
        out[f"mae__{diag['group_col']}__{g}"] = mae(y[sel], pred[sel])
    return out


def run_regression(ctx, df, spec):
    """Baseline, point candidates and (spec["quantile"]) quantile candidates.

    spec: target, key_cols, models, baseline{kind,...}, optional id_features,
    extra_features, drop, fold_mode, date_col, gap_days, price, quantile,
    quantile_cols, diagnostic{name, cols, group_col}: a group-mean model recorded
    beside the baseline, with every model's skill over it and its error per group."""
    pdf, feats, fraction = _prepare(ctx, df, spec)
    tr_m, va_m = _masks(pdf)
    y = pd.to_numeric(pdf[spec["target"]], errors="coerce").to_numpy(dtype="float64")
    enc = FeatureEncoder(feats).fit(pdf[tr_m])
    price = bool(spec.get("price"))
    note_target(y, tr_m, va_m, "regression")
    start_task(ctx)

    base = fit_baseline(pdf[tr_m], spec["target"], spec["baseline"])
    base_pred = base.predict(pdf[va_m])
    diag = spec.get("diagnostic")
    diag_pred = None
    if diag:
        diag_model = fit_baseline(
            pdf[tr_m], spec["target"], {"kind": "group_mean", "cols": diag["cols"]}
        )
        diag_pred = diag_model.predict(pdf[va_m])

    def group_extras(pred, skill=True):
        if not diag:
            return {}
        return _diagnostic_metrics(
            y[va_m], pred, pdf[va_m], diag, diag_pred, skill=skill
        )

    run_candidate(
        ctx,
        spec["baseline"].get("name", spec["baseline"]["kind"]),
        "baseline",
        lambda: (
            base,
            {
                **_point_metrics(y[va_m], base_pred, None, price),
                **group_extras(base_pred),
            },
            int(va_m.sum()),
        ),
        params={"row_fraction": round(fraction, 4)},
        stage="baseline",
    )
    if diag:
        run_candidate(
            ctx,
            diag["name"],
            "baseline",
            lambda: (
                diag_model,
                {
                    **regression_metrics(y[va_m], diag_pred, base_pred),
                    **group_extras(diag_pred, skill=False),
                },
                int(va_m.sum()),
            ),
            params={"cols": diag["cols"]},
            stage="diagnostic",
        )
    qbase = None
    if spec.get("quantile"):
        qbase = fit_quantile_baseline(
            pdf[tr_m], spec["target"], QUANTILES, spec.get("quantile_cols", [])
        )
        qpred = qbase.predict(pdf[va_m])
        run_candidate(
            ctx,
            "empirical_quantiles",
            "baseline",
            lambda: (qbase, quantile_metrics(y[va_m], qpred), int(va_m.sum())),
            params={"cols": spec.get("quantile_cols", [])},
            stage="baseline",
        )

    for name in spec["models"]:
        requires, fill, grid, build = REGRESSORS[name]
        params = {"row_fraction": round(fraction, 4)}

        def fit(requires=requires, fill=fill, grid=grid, build=build, params=params):
            x = enc.transform(pdf, fill=fill)
            best = _tune(
                ctx,
                spec,
                pdf,
                x,
                y,
                grid,
                build,
                lambda m, xv, yv: mae(yv, m.predict(xv)),
                tr_m,
            )
            params.update(best)
            model = build(best).fit(x[tr_m], y[tr_m])
            pred = model.predict(x[va_m])
            metrics = _point_metrics(y[va_m], pred, base_pred, price)
            metrics.update(group_extras(pred))
            tr_s = _train_sample(tr_m)
            metrics.update(
                train_side(
                    lambda: {
                        f"train_{k}": v
                        for k, v in regression_metrics(
                            y[tr_s], model.predict(x[tr_s])
                        ).items()
                        if k in ("mae", "rmse")
                    }
                )
            )
            return (
                TabularBundle(enc, model, "regression", fill),
                metrics,
                int(va_m.sum()),
            )

        run_candidate(ctx, name, "tabular", fit, requires=requires, params=params)

    if spec.get("quantile"):
        for name in spec["quantile"]:
            requires, fill, build = QUANTILE_MODELS[name]
            params = {"quantiles": list(QUANTILES), **GBT_GRID[0]}

            def fit_q(requires=requires, fill=fill, build=build, params=params):
                x = enc.transform(pdf, fill=fill)
                models = {
                    tau: build(params, tau).fit(x[tr_m], y[tr_m]) for tau in QUANTILES
                }
                bundle = TabularBundle(enc, None, "quantile", fill, models)
                preds = bundle.predict(pdf[va_m])
                metrics = quantile_metrics(y[va_m], preds, qbase.predict(pdf[va_m]))
                metrics.update(regression_metrics(y[va_m], preds[0.5], base_pred))
                tr_s = _train_sample(tr_m)
                metrics.update(
                    train_side(
                        lambda: {
                            "train_pinball_q50": pinball(
                                y[tr_s], bundle.predict(pdf.iloc[tr_s])[0.5], 0.5
                            )
                        }
                    )
                )
                return bundle, metrics, int(va_m.sum())

            run_candidate(ctx, name, "tabular", fit_q, requires=requires, params=params)
    finish_task(ctx)


# COMMAND ----------

# DBTITLE 1,Binary classification runner


def _class_metrics(y, p):
    m = classification_metrics(y, p)
    base = m["base_rate"]
    m["pr_auc_lift"] = (
        m["pr_auc"] / base if base and np.isfinite(base) else float("nan")
    )
    return m


def run_classification(ctx, df, spec):
    """Base rate baseline and the logistic and boosted-tree candidates.

    spec: target, key_cols, models, optional id_features, drop, fold_mode."""
    pdf, feats, fraction = _prepare(ctx, df, spec)
    tr_m, va_m = _masks(pdf)
    y = pd.to_numeric(pdf[spec["target"]], errors="coerce").to_numpy(dtype="int64")
    enc = FeatureEncoder(feats).fit(pdf[tr_m])
    note_target(y, tr_m, va_m, "classification")
    start_task(ctx)

    rate = float(np.mean(y[tr_m]))
    const = BaselineModel("constant", value=rate)
    run_candidate(
        ctx,
        "base_rate",
        "baseline",
        lambda: (
            const,
            _class_metrics(y[va_m], const.predict(pdf[va_m])),
            int(va_m.sum()),
        ),
        params={"row_fraction": round(fraction, 4)},
        stage="baseline",
    )
    for name in spec["models"]:
        requires, fill, grid, build = CLASSIFIERS[name]
        params = {"row_fraction": round(fraction, 4)}

        def fit(requires=requires, fill=fill, grid=grid, build=build, params=params):
            x = enc.transform(pdf, fill=fill)
            best = _tune(
                ctx,
                spec,
                pdf,
                x,
                y,
                grid,
                build,
                lambda m, xv, yv: log_loss(yv, m.predict_proba(xv)[:, 1]),
                tr_m,
            )
            params.update(best)
            model = build(best).fit(x[tr_m], y[tr_m])
            p = model.predict_proba(x[va_m])[:, 1]
            metrics = _class_metrics(y[va_m], p)
            tr_s = _train_sample(tr_m)
            metrics.update(
                train_side(
                    lambda: {
                        f"train_{k}": v
                        for k, v in classification_metrics(
                            y[tr_s], model.predict_proba(x[tr_s])[:, 1]
                        ).items()
                        if k in ("pr_auc", "log_loss")
                    }
                )
            )
            return (
                TabularBundle(enc, model, "classification", fill),
                metrics,
                int(va_m.sum()),
            )

        run_candidate(ctx, name, "tabular", fit, requires=requires, params=params)
    finish_task(ctx)


# COMMAND ----------

# DBTITLE 1,Imports
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


# COMMAND ----------

# DBTITLE 1,Imports
import json as _json

import pandas as pd

# COMMAND ----------

# DBTITLE 1,Configuration
INJECT_RATE = 0.02
FLAG_QUANTILE = 0.99
ISOLATION_TRAIN_ROWS = 200_000
AUTOENCODER_TRAIN_ROWS = 300_000
AUTOENCODER_EPOCHS = 5
AUTOENCODER_BATCH = 512

# COMMAND ----------

# DBTITLE 1,Injection


def inject_anomalies(
    values, groups, sd, lengths, weights, kinds, magnitudes, rate, seed
):
    """(modified values, injected mask). Rows must be sorted by group then time.

    Windows of the given lengths are placed at random inside each group, never
    overlapping, as a spike (+m sd), a drop (-m sd) or a flat line at the value
    where the window starts; m is drawn from the magnitude range."""
    rng = np.random.default_rng(seed)
    vals = np.asarray(values, dtype="float64").copy()
    injected = np.zeros(len(vals), dtype=bool)
    lengths = np.asarray(lengths, dtype="int64")
    w = np.asarray(weights, dtype="float64")
    w = w / w.sum()
    mean_len = float(np.sum(lengths * w))
    for g in np.unique(groups):
        idx = np.flatnonzero(groups == g)
        n = len(idx)
        for _ in range(round(rate * n / mean_len)):
            length = int(rng.choice(lengths, p=w))
            if n <= length:
                continue
            start = int(rng.integers(0, n - length))
            span = idx[start : start + length]
            if injected[span].any():
                continue
            kind = kinds[int(rng.integers(len(kinds)))]
            m = float(rng.uniform(magnitudes[0], magnitudes[1]))
            if kind == "spike":
                vals[span] += m * sd[g]
            elif kind == "drop":
                vals[span] -= m * sd[g]
            else:
                vals[span] = vals[span[0]]
            injected[span] = True
    return vals, injected


def read_injection_spec(ecosystem, dataset_id):
    """(lengths, weights, kinds, magnitude range) from the stored specification."""
    rows = (
        read_ml("mask_specification", ecosystem=ecosystem)
        .filter(
            (F.col("dataset_id") == dataset_id)
            & (F.col("split_version") == SPLIT_VERSION)
        )
        .collect()
    )
    if not rows:
        raise RuntimeError(f"no mask specification for {dataset_id}")
    cfg = _json.loads(rows[0]["held_out_units"])
    return (
        [int(r["mask_length_hours"]) for r in rows],
        [float(r["weight"]) for r in rows],
        list(cfg["kinds"]),
        list(cfg["magnitude_sd"]),
    )


# COMMAND ----------

# DBTITLE 1,Scoring models


class ZScoreModel:
    """Distance from the train mean of the same series, hour and weekday."""

    def __init__(self, cols, table, fallback_mean, fallback_sd, target):
        self.cols, self.table, self.target = cols, table, target
        self.fallback_mean, self.fallback_sd = fallback_mean, fallback_sd

    def score(self, pdf):
        merged = pdf[self.cols].merge(self.table, on=self.cols, how="left")
        mean = merged["mean"].fillna(self.fallback_mean).to_numpy(dtype="float64")
        sd = merged["sd"].fillna(self.fallback_sd).to_numpy(dtype="float64")
        y = pd.to_numeric(pdf[self.target], errors="coerce").to_numpy(dtype="float64")
        return np.abs(y - mean) / np.maximum(sd, 1e-9)


class ResidualModel:
    """Absolute forecast error in units of the train residual spread per series."""

    def __init__(self, encoder, model, series_cols, sd_table, fallback_sd, target):
        self.encoder, self.model, self.series_cols = encoder, model, series_cols
        self.sd_table, self.fallback_sd, self.target = sd_table, fallback_sd, target

    def score(self, pdf):
        pred = self.model.predict(self.encoder.transform(pdf))
        y = pd.to_numeric(pdf[self.target], errors="coerce").to_numpy(dtype="float64")
        merged = pdf[self.series_cols].merge(
            self.sd_table, on=self.series_cols, how="left"
        )
        sd = merged["sd"].fillna(self.fallback_sd).to_numpy(dtype="float64")
        return np.abs(y - pred) / np.maximum(sd, 1e-9)


class IsolationModel:
    def __init__(self, encoder, model):
        self.encoder, self.model = encoder, model

    def score(self, pdf):
        return -self.model.score_samples(self.encoder.transform(pdf, fill=True))


class AutoencoderModel:
    def __init__(self, encoder, mean, std, net):
        self.encoder, self.mean, self.std, self.net = encoder, mean, std, net

    def score(self, pdf):
        import torch

        z = (self.encoder.transform(pdf, fill=True) - self.mean) / self.std
        self.net.eval()
        out = []
        with torch.no_grad():
            for i in range(0, len(z), 4096):
                xb = torch.as_tensor(z[i : i + 4096], dtype=torch.float32)
                out.append(((self.net(xb) - xb) ** 2).mean(dim=1).numpy())
        return np.concatenate(out) if out else np.zeros(0)


def _make_autoencoder(width):
    from torch import nn

    return nn.Sequential(
        nn.Linear(width, 32),
        nn.ReLU(),
        nn.Linear(32, 8),
        nn.ReLU(),
        nn.Linear(8, 32),
        nn.ReLU(),
        nn.Linear(32, width),
    )


def _train_autoencoder(z):
    import torch

    torch.manual_seed(MODEL_SEED)
    net = _make_autoencoder(z.shape[1])
    opt = torch.optim.Adam(net.parameters(), lr=1e-3)
    x = torch.as_tensor(z, dtype=torch.float32)
    net.train()
    for _ in range(AUTOENCODER_EPOCHS):
        order = torch.randperm(len(x))
        for i in range(0, len(x), AUTOENCODER_BATCH):
            xb = x[order[i : i + AUTOENCODER_BATCH]]
            opt.zero_grad()
            ((net(xb) - xb) ** 2).mean().backward()
            opt.step()
    return net


# COMMAND ----------

# DBTITLE 1,Anomaly runner


def _evaluate(
    ctx, name, family, make_scorer, data, params, requires=(), stage="candidate"
):
    """Build the scorer inside the isolated candidate, threshold at a train score
    quantile, then report precision, recall and flag rate on the injected series."""
    train, valid, valid_injected, injected = data

    def fit():
        scorer = make_scorer()
        s_train = scorer.score(train)
        thr = float(np.nanquantile(s_train, FLAG_QUANTILE))
        s_inj = scorer.score(valid_injected)
        s_orig = scorer.score(valid)
        metrics = detection_metrics(injected, s_inj > thr, float(np.mean(s_orig > thr)))
        metrics["pr_auc"] = average_precision(injected, s_inj)
        metrics["threshold"] = thr
        metrics["train_flag_rate"] = float(np.mean(s_train > thr))
        return scorer, metrics, len(valid)

    return run_candidate(
        ctx, name, family, fit, requires=requires, params=dict(params), stage=stage
    )


def run_anomaly(ctx, df, spec):
    """Seasonal z-score baseline, forecast-residual, isolation forest, autoencoder.

    spec: series_cols, time_col, target, key_cols, models, drop."""
    sc, tc, yc = spec["series_cols"], spec["time_col"], spec["target"]
    feats = resolve_features(ctx, df.columns, drop=spec.get("drop", ()))
    seasonal = [*sc, "hour_of_day", "day_of_week"]
    cols = list(
        dict.fromkeys([*feats, *seasonal, tc, yc, "partition", *spec["key_cols"]])
    )
    pdf, fraction = to_training_frame(df, key_cols=spec["key_cols"], columns=cols)
    pdf = pdf.dropna(subset=[yc]).sort_values([*sc, tc]).reset_index(drop=True)
    train = pdf[pdf["partition"] == "train"].reset_index(drop=True)
    valid = pdf[pdf["partition"] == "validation"].reset_index(drop=True)
    if train.empty or valid.empty:
        raise RuntimeError("need train and validation rows")

    lengths, weights, kinds, magnitudes = read_injection_spec(
        ctx.ecosystem, ctx.dataset_id
    )
    group_index = valid.groupby(sc).ngroup().to_numpy()
    keys = valid.assign(_g=group_index).drop_duplicates("_g").sort_values("_g")[sc]
    sd_train = train.groupby(sc)[yc].std().rename("sd").reset_index()
    sd_by_group = (
        keys.merge(sd_train, on=sc, how="left")["sd"]
        .fillna(float(train[yc].std()))
        .to_numpy(dtype="float64")
    )
    vals, injected = inject_anomalies(
        valid[yc].to_numpy(dtype="float64"),
        group_index,
        sd_by_group,
        lengths,
        weights,
        kinds,
        magnitudes,
        INJECT_RATE,
        MODEL_SEED,
    )
    valid_injected = valid.copy()
    valid_injected[yc] = vals
    data = (train, valid, valid_injected, injected)
    params = {
        "inject_rate": INJECT_RATE,
        "flag_quantile": FLAG_QUANTILE,
        "injected_rows": int(injected.sum()),
        "row_fraction": round(fraction, 4),
    }
    start_task(ctx)

    table = (
        train.groupby(seasonal)[yc]
        .agg(["mean", "std"])
        .rename(columns={"std": "sd"})
        .reset_index()
    )
    base = ZScoreModel(
        seasonal, table, float(train[yc].mean()), float(train[yc].std()), yc
    )
    _evaluate(
        ctx, "seasonal_zscore", "baseline", lambda: base, data, params, stage="baseline"
    )

    features = [f for f in feats if f != yc]
    enc = FeatureEncoder(features).fit(train)
    y_tr = train[yc].to_numpy(dtype="float64")

    def residual(builder):
        model = builder(GBT_GRID[0]).fit(enc.transform(train), y_tr)
        err = np.abs(y_tr - model.predict(enc.transform(train)))
        sd = train[sc].assign(_r=err).groupby(sc)["_r"].std().rename("sd").reset_index()
        return ResidualModel(enc, model, sc, sd, float(np.std(err)), yc)

    for name, lib, builder in (
        ("forecast_residual_lightgbm", "lightgbm", _make_lgbm_regressor),
        ("forecast_residual_sklearn", "sklearn", _make_hgb_regressor),
    ):
        if name in spec["models"]:
            _evaluate(
                ctx,
                name,
                "anomaly",
                lambda b=builder: residual(b),
                data,
                params,
                requires=(lib,),
            )

    enc_y = FeatureEncoder([*features, yc]).fit(train)

    def isolation():
        from sklearn.ensemble import IsolationForest

        rows = train.sample(
            n=min(len(train), ISOLATION_TRAIN_ROWS), random_state=MODEL_SEED
        )
        model = IsolationForest(n_estimators=200, random_state=MODEL_SEED, n_jobs=-1)
        model.fit(enc_y.transform(rows, fill=True))
        return IsolationModel(enc_y, model)

    def autoencoder():
        rows = train.sample(
            n=min(len(train), AUTOENCODER_TRAIN_ROWS), random_state=MODEL_SEED
        )
        x = enc_y.transform(rows, fill=True)
        mean = x.mean(axis=0)
        std = np.where(x.std(axis=0) > 0, x.std(axis=0), 1.0)
        return AutoencoderModel(enc_y, mean, std, _train_autoencoder((x - mean) / std))

    if "isolation_forest" in spec["models"]:
        _evaluate(
            ctx,
            "isolation_forest",
            "anomaly",
            isolation,
            data,
            params,
            requires=("sklearn",),
        )
    if "autoencoder" in spec["models"]:
        _evaluate(
            ctx,
            "autoencoder",
            "anomaly",
            autoencoder,
            data,
            params,
            requires=("torch",),
        )
    finish_task(ctx)


# COMMAND ----------

# DBTITLE 1,Imports

import pandas as pd

# COMMAND ----------

# DBTITLE 1,Configuration
TRAIN_EVENTS_PER_VARIABLE = 3000
EVAL_EVENTS_PER_VARIABLE = 1500
SEQUENCE_TRAIN_EVENTS = 15000
SEQUENCE_EPOCHS = 3
SEQUENCE_BATCH = 256
SEQUENCE_HIDDEN = 64
SMOKE_DIVISOR = 20
SMOKE_SERIES = 4
MIN_SERIES_STEPS = 200
CIRCULAR_DEGREES = 360.0

# COMMAND ----------

# DBTITLE 1,Series sets on a regular grid


class SeriesSet:
    """Regular-grid series of one family: values (T, V) per series, a forward
    filled copy (for the last observation before a block) and calendar arrays."""

    def __init__(self, name, ids, arrays, calendars, variables, period, circular=()):
        self.name, self.ids, self.arrays = name, ids, arrays
        self.calendars, self.variables = calendars, list(variables)
        self.period, self.circular = period, set(circular)
        self.filled = [
            pd.DataFrame(a).ffill().to_numpy(dtype="float64") for a in arrays
        ]


def build_series_set(name, pdf, id_col, ts_col, variables, freq, period, circular=()):
    ids, arrays, cals = [], [], []
    for sid, part in pdf.groupby(id_col):
        t = pd.to_datetime(part[ts_col]).dt.floor(freq)
        part = part.assign(_t=t).drop_duplicates("_t").sort_values("_t")
        if len(part) < MIN_SERIES_STEPS:
            continue
        idx = pd.date_range(part["_t"].iloc[0], part["_t"].iloc[-1], freq=freq)
        values = (
            part.set_index("_t")[list(variables)]
            .apply(pd.to_numeric, errors="coerce")
            .reindex(idx)
            .to_numpy(dtype="float64")
        )
        ids.append(sid)
        arrays.append(values)
        cals.append(
            (
                idx.hour.to_numpy(),
                idx.dayofweek.to_numpy(),
                idx.month.to_numpy(),
            )
        )
    if not arrays:
        raise RuntimeError(f"no series with {MIN_SERIES_STEPS} steps for {name}")
    return SeriesSet(name, ids, arrays, cals, variables, period, circular)


def read_mask_spec(ecosystem, dataset_id):
    """(block lengths in hours, weights, held-out series ids) from the stored spec."""
    rows = (
        read_ml("mask_specification", ecosystem=ecosystem)
        .filter(
            (F.col("dataset_id") == dataset_id)
            & (F.col("split_version") == SPLIT_VERSION)
        )
        .collect()
    )
    if not rows:
        raise RuntimeError(f"no mask specification for {dataset_id}")
    held = rows[0]["held_out_units"]
    return (
        [int(r["mask_length_hours"]) for r in rows],
        [float(r["weight"]) for r in rows],
        set(_json.loads(held)) if held else set(),
    )


# COMMAND ----------

# DBTITLE 1,Mask events and their features


def sample_events(rng, lengths_steps, weights, series_len, eligible, n):
    """[(series index, block start, block length)] placed inside eligible series,
    starting at step 1 or later so a last observation can exist before the block."""
    w = np.asarray(weights, dtype="float64")
    w = w / w.sum()
    out = []
    attempts = 0
    while len(out) < n and attempts < n * 20:
        attempts += 1
        i = int(rng.choice(eligible))
        length = int(rng.choice(lengths_steps, p=w))
        t = series_len[i]
        if t < length + 2:
            continue
        out.append((i, int(rng.integers(1, t - length + 1)), length))
    return out


def event_features(sset, series_i, var_j, start, length):
    """(features, truth) for each masked step; only data before `start` is used
    for the anchor and the seasonal carry."""
    arr, filled = sset.arrays[series_i], sset.filled[series_i]
    hour, dow, month = sset.calendars[series_i]
    p = np.arange(start, start + length)
    anchor = np.full(length, filled[start - 1, var_j])
    k = np.ceil((p - start + 1) / sset.period).astype("int64")
    q = p - k * sset.period
    seasonal = np.where(q >= 0, arr[np.maximum(q, 0), var_j], np.nan)
    cols = [
        anchor,
        seasonal,
        (p - start + 1).astype("float64"),
        hour[p],
        dow[p],
        month[p],
    ]
    if arr.shape[1] > 1:
        cross = arr[p].copy()
        cross[:, var_j] = np.nan
        cols += [cross[:, c] for c in range(cross.shape[1])]
    return np.column_stack(cols).astype("float64"), arr[p, var_j]


def build_examples(sset, var_j, events):
    xs, ys, sid = [], [], []
    for i, s, length in events:
        x, y = event_features(sset, i, var_j, s, length)
        xs.append(x)
        ys.append(y)
        sid.append(np.full(length, i))
    return np.vstack(xs), np.concatenate(ys), np.concatenate(sid)


def masked_abs_error(variable, circular, truth, pred):
    err = np.abs(np.asarray(truth, dtype="float64") - np.asarray(pred, dtype="float64"))
    if variable in circular:
        err = np.minimum(err, CIRCULAR_DEGREES - err)
    return err[np.isfinite(err)]


# COMMAND ----------

# DBTITLE 1,Masked sequence network


def _make_reconstruction_net(channels, n_vars):
    from torch import nn

    class Net(nn.Module):
        def __init__(self):
            super().__init__()
            self.gru = nn.GRU(channels, SEQUENCE_HIDDEN, batch_first=True)
            self.out = nn.Linear(SEQUENCE_HIDDEN, n_vars)

        def forward(self, x):
            o, _ = self.gru(x)
            return self.out(o)

    return Net()


def series_stats(sset):
    stacked = np.vstack(sset.arrays)
    mean = np.nanmean(stacked, axis=0)
    std = np.nanstd(stacked, axis=0)
    return np.where(np.isfinite(mean), mean, 0.0), np.where(std > 0, std, 1.0)


def make_windows(sset, events, var_js, window, stats):
    """(inputs, target, loss mask) for a batch: the block is the last `length`
    steps of the window; the masked variable is zeroed there and flagged."""
    mean, std = stats
    n_vars = len(sset.variables)
    x = np.zeros((len(events), window, 2 * n_vars + 1), dtype="float32")
    y = np.zeros((len(events), window), dtype="float32")
    m = np.zeros((len(events), window), dtype=bool)
    for b, ((i, s, length), j) in enumerate(zip(events, var_js, strict=True)):
        arr = sset.arrays[i]
        end = s + length
        lo = max(end - window, 0)
        seg = (arr[lo:end] - mean) / std
        obs = np.isfinite(seg)
        z = np.where(obs, seg, 0.0)
        off = window - len(seg)
        x[b, off:, :n_vars] = z
        x[b, off:, n_vars : 2 * n_vars] = obs
        y[b, window - length :] = z[-length:, j]
        m[b, window - length :] = obs[-length:, j]
        x[b, window - length :, j] = 0.0
        x[b, window - length :, n_vars + j] = 0.0
        x[b, window - length :, 2 * n_vars] = 1.0
    return x, y, m


def _sequence_predict(net, sset, events, var_js, window, stats):
    import torch

    mean, std = stats
    net.eval()
    preds = []
    with torch.no_grad():
        for i in range(0, len(events), SEQUENCE_BATCH):
            ev = events[i : i + SEQUENCE_BATCH]
            js = var_js[i : i + SEQUENCE_BATCH]
            x, _, _ = make_windows(sset, ev, js, window, stats)
            out = net(torch.as_tensor(x))
            idx = torch.as_tensor(js, dtype=torch.long)[:, None, None].expand(
                -1, window, 1
            )
            col = out.gather(2, idx).squeeze(2).numpy()
            for b, (_, _, length) in enumerate(ev):
                preds.append(col[b, window - length :] * std[js[b]] + mean[js[b]])
    return np.concatenate(preds) if preds else np.zeros(0)


class SequenceReconstructor:
    def __init__(self, net, stats, window):
        self.net, self.stats, self.window = net, stats, window

    def predict(self, sset, events, var_js):
        return _sequence_predict(
            self.net, sset, events, var_js, self.window, self.stats
        )


def train_sequence(sset, events, var_js, window):
    import torch

    torch.manual_seed(MODEL_SEED)
    stats = series_stats(sset)
    n_vars = len(sset.variables)
    net = _make_reconstruction_net(2 * n_vars + 1, n_vars)
    opt = torch.optim.Adam(net.parameters(), lr=1e-3)
    order_rng = np.random.default_rng(MODEL_SEED)
    net.train()
    for _ in range(SEQUENCE_EPOCHS):
        order = order_rng.permutation(len(events))
        for i in range(0, len(order), SEQUENCE_BATCH):
            sel = order[i : i + SEQUENCE_BATCH]
            ev = [events[k] for k in sel]
            js = [var_js[k] for k in sel]
            x, y, m = make_windows(sset, ev, js, window, stats)
            out = net(torch.as_tensor(x))
            idx = torch.as_tensor(js, dtype=torch.long)[:, None, None].expand(
                -1, window, 1
            )
            pred = out.gather(2, idx).squeeze(2)
            mask = torch.as_tensor(m)
            if not mask.any():
                continue
            loss = ((pred - torch.as_tensor(y)) ** 2)[mask].mean()
            opt.zero_grad()
            loss.backward()
            opt.step()
    return SequenceReconstructor(net, stats, window)


# COMMAND ----------

# DBTITLE 1,Bundles for the per-variable feature models


class FeatureReconstructor:
    """One fitted estimator per variable; predict() takes that variable's
    event feature matrix."""

    def __init__(self, models, medians, fill):
        self.models, self.medians, self.fill = models, medians, fill

    def predict(self, var_j, x):
        x = np.where(np.isnan(x), self.medians[var_j], x) if self.fill else x
        return self.models[var_j].predict(x)


class BaselineReconstructor:
    def __init__(self, kind, fallbacks):
        self.kind, self.fallbacks = kind, fallbacks

    def predict(self, var_j, x):
        anchor, seasonal = x[:, 0], x[:, 1]
        pick = seasonal if self.kind == "seasonal_carry" else anchor
        pick = np.where(np.isfinite(pick), pick, anchor)
        return np.where(np.isfinite(pick), pick, self.fallbacks[var_j])


# COMMAND ----------

# DBTITLE 1,Reconstruction runner


def _lengths_in_steps(lengths_hours, step_hours):
    return [max(1, round(h / step_hours)) for h in lengths_hours]


def _smoke_series(d, id_col, held):
    """A few whole series that are long enough in both partitions."""
    counts = d.groupBy(id_col).pivot("partition", list(ALLOWED_PARTITIONS)).count()
    long_enough = counts.filter(
        (F.col("train") >= MIN_SERIES_STEPS) & (F.col("validation") >= MIN_SERIES_STEPS)
    )
    ids = [r[id_col] for r in long_enough.orderBy(id_col).collect()]
    keep = [i for i in ids if i not in held][:SMOKE_SERIES]
    return d.filter(F.col(id_col).isin(keep))


def _prepare_set(df, cfg, n_train, n_eval, steps, weights, held, smoke=False):
    """(train set, validation set, sampled events per variable) for one family."""
    d = df
    if cfg.get("filter_col"):
        d = d.filter(F.col(cfg["filter_col"]) == cfg["filter_value"])
    if smoke:
        d = _smoke_series(d, cfg["id_col"], held)
    cols = [cfg["id_col"], cfg["ts_col"], *cfg["variables"], "partition"]
    pdf, _ = to_training_frame(d, key_cols=[cfg["id_col"], cfg["ts_col"]], columns=cols)
    sets = {}
    for part in ALLOWED_PARTITIONS:
        rows = pdf[pdf["partition"] == part]
        if part == "train" and held:
            rows = rows[~rows[cfg["id_col"]].isin(held)]
        sets[part] = build_series_set(
            cfg["name"],
            rows,
            cfg["id_col"],
            cfg["ts_col"],
            cfg["variables"],
            cfg["freq"],
            cfg["period"],
            cfg.get("circular", ()),
        )
    rng = np.random.default_rng(MODEL_SEED)
    events = {}
    for part, n in (("train", n_train), ("validation", n_eval)):
        lens = [len(a) for a in sets[part].arrays]
        for j in range(len(cfg["variables"])):
            events[(part, j)] = sample_events(
                rng, steps, weights, lens, np.arange(len(lens)), n
            )
    return sets["train"], sets["validation"], events


def run_reconstruction(ctx, df, spec):
    """Causal baseline, cross-variable ridge and boosted trees, masked GRU.

    spec: sets (one config per series family: name, id_col, ts_col, variables,
    freq, period, step_hours, baseline, optional circular, filter_col,
    filter_value) and models. The mask specification is read for ctx.dataset_id."""
    set_utc_session()
    lengths, weights, held = read_mask_spec(ctx.ecosystem, ctx.dataset_id)
    div = SMOKE_DIVISOR if ctx.smoke else 1
    prepared = []
    for cfg in spec["sets"]:
        steps = _lengths_in_steps(lengths, cfg["step_hours"])
        tr, va, ev = _prepare_set(
            df,
            cfg,
            TRAIN_EVENTS_PER_VARIABLE // div,
            EVAL_EVENTS_PER_VARIABLE // div,
            steps,
            weights,
            held,
            ctx.smoke,
        )
        prepared.append((cfg, tr, va, ev, steps))
    start_task(ctx)

    data = {}
    for cfg, tr, va, ev, _ in prepared:
        for j in range(len(cfg["variables"])):
            xt, yt, _s = build_examples(tr, j, ev[("train", j)])
            xv, yv, sv = build_examples(va, j, ev[("validation", j)])
            keep = np.isfinite(yt)
            data[(cfg["name"], j)] = (xt[keep], yt[keep], xv, yv, sv)

    fallbacks = {
        key: float(np.nanmean(v[1])) if len(v[1]) else 0.0 for key, v in data.items()
    }

    def baseline_predict(cfg, tr, va, ev, j, xv, steps):
        fb = {j: fallbacks[(cfg["name"], j)]}
        return BaselineReconstructor(cfg["baseline"], fb).predict(j, xv)

    def errors(cfg, j, pred):
        _, _, _, yv, _ = data[(cfg["name"], j)]
        return masked_abs_error(cfg["variables"][j], cfg.get("circular", ()), yv, pred)

    base_mae = {}
    for cfg, tr, va, ev, steps in prepared:
        for j in range(len(cfg["variables"])):
            xv = data[(cfg["name"], j)][2]
            e = errors(cfg, j, baseline_predict(cfg, tr, va, ev, j, xv, steps))
            base_mae[(cfg["name"], j)] = float(e.mean()) if len(e) else float("nan")

    def score(predict):
        out, skills = {}, []
        for cfg, tr, va, ev, steps in prepared:
            for j, var in enumerate(cfg["variables"]):
                _, _, xv, yv, sv = data[(cfg["name"], j)]
                pred = np.asarray(predict(cfg, tr, va, ev, j, xv, steps))
                e = errors(cfg, j, pred)
                key = f"{cfg['name']}__{var}"
                mae_v = float(e.mean()) if len(e) else float("nan")
                out[f"mae__{key}"] = mae_v
                out[f"skill_mae__{key}"] = skill(mae_v, base_mae[(cfg["name"], j)])
                skills.append(out[f"skill_mae__{key}"])
                if held:
                    hm = np.isin(np.array(va.ids, dtype=object)[sv], list(held))
                    eh = masked_abs_error(
                        var, cfg.get("circular", ()), yv[hm], pred[hm]
                    )
                    out[f"mae_heldout__{key}"] = (
                        float(eh.mean()) if len(eh) else float("nan")
                    )
        out["skill_mae_mean"] = float(np.nanmean(skills)) if skills else float("nan")
        return out

    def train_skill(examples, predict):
        """Skill over the baseline on the training events, averaged over variables.
        examples(key) -> (inputs, truth); predict(key, inputs) -> predictions."""
        skills = []
        for cfg in spec["sets"]:
            circ = cfg.get("circular", ())
            for j, var in enumerate(cfg["variables"]):
                key = (cfg["name"], j)
                xt, yt = examples(key)
                if not len(yt):
                    continue
                base = BaselineReconstructor(cfg["baseline"], {j: fallbacks[key]})
                e_m = masked_abs_error(var, circ, yt, np.asarray(predict(key, xt)))
                e_b = masked_abs_error(var, circ, yt, base.predict(j, xt))
                skills.append(skill(float(e_m.mean()), float(e_b.mean())))
        mean = float(np.nanmean(skills)) if skills else float("nan")
        return {"train_skill_mae_mean": mean}

    kinds = {cfg["name"]: cfg["baseline"] for cfg in spec["sets"]}
    run_candidate(
        ctx,
        "causal_baseline",
        "baseline",
        lambda: (
            {"kinds": kinds, "fallbacks": fallbacks},
            score(baseline_predict),
            len(data),
        ),
        params={"baselines": kinds},
        stage="baseline",
    )

    builders = {
        "ridge_cross_variable": (("sklearn",), True, _make_ridge, RIDGE_GRID[1]),
        "gbt_cross_variable_lightgbm": (
            ("lightgbm",),
            False,
            _make_lgbm_regressor,
            GBT_GRID[0],
        ),
        "gbt_cross_variable_sklearn": (
            ("sklearn",),
            False,
            _make_hgb_regressor,
            GBT_GRID[0],
        ),
    }
    for name in spec["models"]:
        if name not in builders:
            continue
        requires, fill, build, params = builders[name]

        def fit(build=build, fill=fill, params=params):
            models, medians = {}, {}
            for cfg in spec["sets"]:
                for j in range(len(cfg["variables"])):
                    key = (cfg["name"], j)
                    xt, yt = data[key][0], data[key][1]
                    med = np.nanmedian(xt, axis=0) if len(xt) else np.zeros(xt.shape[1])
                    medians[key] = np.where(np.isfinite(med), med, 0.0)
                    x = np.where(np.isnan(xt), medians[key], xt) if fill else xt
                    models[key] = build(params).fit(x, yt)

            def predict(cfg, tr, va, ev, j, xv, steps):
                key = (cfg["name"], j)
                x = np.where(np.isnan(xv), medians[key], xv) if fill else xv
                return models[key].predict(x)

            metrics = score(predict)
            metrics.update(
                train_side(
                    lambda: train_skill(
                        lambda key: data[key][:2],
                        lambda key, x: models[key].predict(
                            np.where(np.isnan(x), medians[key], x) if fill else x
                        ),
                    )
                )
            )
            return FeatureReconstructor(models, medians, fill), metrics, len(data)

        run_candidate(
            ctx, name, "reconstruction", fit, requires=requires, params=dict(params)
        )

    if "masked_sequence_model" in spec["models"]:

        def fit_sequence():
            nets = {}
            for cfg, tr, va, ev, steps in prepared:
                n_vars = len(cfg["variables"])
                per = max(1, (SEQUENCE_TRAIN_EVENTS // div) // n_vars)
                rng = np.random.default_rng(MODEL_SEED)
                lens = [len(a) for a in tr.arrays]
                events, var_js = [], []
                for j in range(n_vars):
                    events += sample_events(
                        rng, steps, weights, lens, np.arange(len(lens)), per
                    )
                    var_js += [j] * per
                nets[cfg["name"]] = train_sequence(tr, events, var_js, max(steps) + 24)

            def predict(cfg, tr, va, ev, j, xv, steps):
                events = ev[("validation", j)]
                return nets[cfg["name"]].predict(va, events, [j] * len(events))

            train_events = {
                (cfg["name"], j): (tr, ev[("train", j)])
                for cfg, tr, va, ev, steps in prepared
                for j in range(len(cfg["variables"]))
            }

            def train_examples(key):
                tr, events = train_events[key]
                xt, yt, _s = build_examples(tr, key[1], events)
                return xt, yt

            def train_predict(key, x):
                tr, events = train_events[key]
                return nets[key[0]].predict(tr, events, [key[1]] * len(events))

            metrics = score(predict)
            metrics.update(
                train_side(lambda: train_skill(train_examples, train_predict))
            )
            return nets, metrics, len(data)

        run_candidate(
            ctx,
            "masked_sequence_model",
            "reconstruction",
            fit_sequence,
            requires=("torch",),
            params={
                "hidden": SEQUENCE_HIDDEN,
                "epochs": SEQUENCE_EPOCHS,
                "causal": True,
            },
        )
    finish_task(ctx)


# COMMAND ----------

# DBTITLE 1,Imports
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Configuration
ACTION_BINS = 9
CQL_ALPHA = 1.0
CQL_EPOCHS = 10
CQL_BATCH = 1024
WEIGHT_CLIP = 3.0
SESSION_HISTORY = 10
SESSION_EPOCHS = 2
SESSION_BATCH = 2048
SESSION_TRAIN_ROWS = 1_500_000

# COMMAND ----------

# DBTITLE 1,Reward weights and action bins


def reward_weights(reward):
    """Positive weights that grow with reward: exp of the clipped z-score."""
    r = np.asarray(reward, dtype="float64")
    sd = r.std()
    z = (r - r.mean()) / sd if sd > 0 else np.zeros(len(r))
    return np.exp(np.clip(z, -WEIGHT_CLIP, WEIGHT_CLIP))


def action_bins(actions, n_bins=ACTION_BINS):
    """(bin edges, bin centres) from train quantiles."""
    edges = np.quantile(actions, np.linspace(0, 1, n_bins + 1))
    idx = bin_index(actions, edges)
    centres = np.array(
        [
            actions[idx == k].mean()
            if (idx == k).any()
            else 0.5 * (edges[k] + edges[k + 1])
            for k in range(n_bins)
        ]
    )
    return edges, centres


def bin_index(values, edges):
    return np.clip(
        np.searchsorted(edges[1:-1], values, side="right"), 0, len(edges) - 2
    )


# COMMAND ----------

# DBTITLE 1,Pumped storage policies (numeric action, recomputable reward)


class RulePolicy:
    """Discharge when the price is at or above the train median, else charge."""

    def __init__(self, column, threshold, magnitude):
        self.column, self.threshold, self.magnitude = column, threshold, magnitude

    def action(self, pdf):
        price = pd.to_numeric(pdf[self.column], errors="coerce").to_numpy(
            dtype="float64"
        )
        return np.where(price >= self.threshold, self.magnitude, -self.magnitude)


class RegressionPolicy:
    def __init__(self, encoder, model, fill):
        self.encoder, self.model, self.fill = encoder, model, fill

    def action(self, pdf):
        return self.model.predict(self.encoder.transform(pdf, fill=self.fill))


class QPolicy:
    """Conservative Q-learning policy over binned actions; greedy in Q."""

    def __init__(self, encoder, mean, std, net, centres):
        self.encoder, self.mean, self.std = encoder, mean, std
        self.net, self.centres = net, centres

    def action(self, pdf):
        import torch

        z = (self.encoder.transform(pdf, fill=True) - self.mean) / self.std
        self.net.eval()
        with torch.no_grad():
            q = self.net(torch.as_tensor(z, dtype=torch.float32)).numpy()
        return self.centres[q.argmax(axis=1)]


def _train_cql(z, action_idx, reward, n_actions):
    import torch
    from torch import nn

    torch.manual_seed(MODEL_SEED)
    net = nn.Sequential(
        nn.Linear(z.shape[1], 64),
        nn.ReLU(),
        nn.Linear(64, 64),
        nn.ReLU(),
        nn.Linear(64, n_actions),
    )
    opt = torch.optim.Adam(net.parameters(), lr=1e-3)
    x = torch.as_tensor(z, dtype=torch.float32)
    a = torch.as_tensor(action_idx, dtype=torch.long)
    r = torch.as_tensor(
        (reward - reward.mean()) / max(reward.std(), 1e-9), dtype=torch.float32
    )
    net.train()
    for _ in range(CQL_EPOCHS):
        order = torch.randperm(len(x))
        for i in range(0, len(x), CQL_BATCH):
            sel = order[i : i + CQL_BATCH]
            q = net(x[sel])
            q_a = q.gather(1, a[sel][:, None]).squeeze(1)
            loss = ((q_a - r[sel]) ** 2).mean() + CQL_ALPHA * (
                torch.logsumexp(q, dim=1) - q_a
            ).mean()
            opt.zero_grad()
            loss.backward()
            opt.step()
    return net


def _episode_net_abs(frame, action, episode_col):
    """Mean over episodes of the absolute summed net energy."""
    return float(
        pd.Series(action, index=frame.index)
        .groupby(frame[episode_col])
        .sum()
        .abs()
        .mean()
    )


def pumped_metrics(valid, action, spec):
    """Actions are clipped to the logged range. reward_policy values any size of
    action; reward_timing keeps the logged size and scores only the direction, since
    no storage state or power limit is modelled."""
    lo, hi = spec.get("action_bounds", (-np.inf, np.inf))
    action = np.clip(np.asarray(action, dtype="float64"), lo, hi)
    logged = valid[spec["action_col"]].to_numpy(dtype="float64")
    price = valid[spec["price_col"]].to_numpy(dtype="float64")
    out = {
        "action_mae": mae(logged, action),
        "reward_policy": float(np.nanmean(price * action)),
        "reward_timing": float(np.nanmean(price * np.sign(action) * np.abs(logged))),
        "reward_logged": float(
            np.nanmean(valid[spec["reward_col"]].to_numpy(dtype="float64"))
        ),
        "sign_agreement": float(np.mean(np.sign(action) == np.sign(logged))),
    }
    out["reward_gain"] = out["reward_policy"] - out["reward_logged"]
    out["reward_timing_gain"] = out["reward_timing"] - out["reward_logged"]
    episode = spec["key_cols"][0]
    out["episode_net_abs_policy"] = _episode_net_abs(valid, action, episode)
    out["episode_net_abs_logged"] = _episode_net_abs(valid, logged, episode)
    return out


RL_TRAIN_EVAL_ROWS = 200_000


def _train_eval_frame(train, session_col=None):
    """The training rows the train-side scores are computed on: all of them, or an
    evenly spaced sample (whole sessions when the policy reads session history)."""
    if len(train) <= RL_TRAIN_EVAL_ROWS:
        return train
    step = len(train) // RL_TRAIN_EVAL_ROWS + 1
    if session_col:
        keep = train[session_col].drop_duplicates().iloc[::step]
        return train[train[session_col].isin(keep)]
    return train.iloc[::step]


def _policy_scores(policy, valid, train_eval, spec, kind):
    """Validation metrics of a policy plus its action score on the training rows."""
    if kind == "pumped":
        metrics = pumped_metrics(valid, policy.action(valid), spec)

        def fitted():
            t = pumped_metrics(train_eval, policy.action(train_eval), spec)
            return {
                "train_action_mae": t["action_mae"],
                "train_sign_agreement": t["sign_agreement"],
            }

    else:
        metrics = action_metrics(valid, policy.action(valid), spec)

        def fitted():
            t = action_metrics(train_eval, policy.action(train_eval), spec)
            return {"train_action_agreement": t["action_agreement"]}

    metrics.update(train_side(fitted))
    return metrics


def run_pumped_storage(ctx, df, spec):
    """Rule baseline, behaviour cloning, reward-weighted regression, conservative Q.

    spec: state_cols, action_col, reward_col, price_col, key_cols, models."""
    cols = list(
        dict.fromkeys(
            [
                *spec["state_cols"],
                spec["action_col"],
                spec["reward_col"],
                spec["price_col"],
                "partition",
                *spec["key_cols"],
            ]
        )
    )
    pdf, fraction = to_training_frame(df, key_cols=spec["key_cols"], columns=cols)
    pdf = pdf.dropna(subset=[spec["action_col"], spec["reward_col"], spec["price_col"]])
    train = pdf[pdf["partition"] == "train"].reset_index(drop=True)
    valid = pdf[pdf["partition"] == "validation"].reset_index(drop=True)
    if train.empty or valid.empty:
        raise RuntimeError("need train and validation rows")
    train_eval = _train_eval_frame(train)
    enc = FeatureEncoder(spec["state_cols"]).fit(train)
    y = train[spec["action_col"]].to_numpy(dtype="float64")
    w = reward_weights(train[spec["reward_col"]])
    spec = {**spec, "action_bounds": (float(y.min()), float(y.max()))}
    base_params = {
        "row_fraction": round(fraction, 4),
        "reward": "price x net energy, price-taker",
    }
    start_task(ctx)

    rule = RulePolicy(
        spec["price_col"],
        float(train[spec["price_col"]].median()),
        float(np.abs(y).mean()),
    )
    run_candidate(
        ctx,
        "rule_policy",
        "baseline",
        lambda: (
            rule,
            _policy_scores(rule, valid, train_eval, spec, "pumped"),
            len(valid),
        ),
        params=base_params,
        stage="baseline",
    )

    builders = {
        "lightgbm": (("lightgbm",), lambda: _make_lgbm_regressor(GBT_GRID[0])),
        "sklearn": (("sklearn",), lambda: _make_hgb_regressor(GBT_GRID[0])),
    }
    for lib, (requires, build) in builders.items():
        for prefix, weighted in (
            ("behaviour_cloning", False),
            ("reward_weighted_regression", True),
        ):
            name = f"{prefix}_{lib}"
            if name not in spec["models"]:
                continue

            def fit(build=build, weighted=weighted):
                model = build()
                x = enc.transform(train)
                if weighted:
                    model.fit(x, y, sample_weight=w)
                else:
                    model.fit(x, y)
                pol = RegressionPolicy(enc, model, False)
                return (
                    pol,
                    _policy_scores(pol, valid, train_eval, spec, "pumped"),
                    len(valid),
                )

            run_candidate(
                ctx,
                name,
                "offline_rl",
                fit,
                requires=requires,
                params=dict(base_params),
            )

    if "conservative_q" in spec["models"]:

        def fit_q():
            x = enc.transform(train, fill=True)
            mean, std = x.mean(axis=0), np.where(x.std(axis=0) > 0, x.std(axis=0), 1.0)
            edges, centres = action_bins(y)
            net = _train_cql(
                (x - mean) / std,
                bin_index(y, edges),
                train[spec["reward_col"]].to_numpy(dtype="float64"),
                ACTION_BINS,
            )
            pol = QPolicy(enc, mean, std, net, centres)
            return (
                pol,
                _policy_scores(pol, valid, train_eval, spec, "pumped"),
                len(valid),
            )

        run_candidate(
            ctx,
            "conservative_q",
            "offline_rl",
            fit_q,
            requires=("torch",),
            params={
                **base_params,
                "alpha": CQL_ALPHA,
                "bins": ACTION_BINS,
                "discount": 0.0,
            },
        )
    finish_task(ctx)


# COMMAND ----------

# DBTITLE 1,Categorical action policies (redispatch direction, session event type)


class ClassPolicy:
    """Predicts the logged action class from the state."""

    def __init__(self, encoder, model, fill):
        self.encoder, self.model, self.fill = encoder, model, fill

    def action(self, pdf):
        return np.asarray(
            self.model.predict(self.encoder.transform(pdf, fill=self.fill))
        )


class MajorityAction:
    def __init__(self, action):
        self.value = action

    def action(self, pdf):
        return np.full(len(pdf), self.value, dtype=object)


def action_metrics(valid, predicted, spec):
    return agreement_metrics(
        valid[spec["action_col"]].astype(str).to_numpy(),
        np.asarray(predicted).astype(str),
        valid[spec["reward_col"]].to_numpy(dtype="float64"),
    )


def run_action_policy(ctx, df, spec):
    """Majority baseline, behaviour cloning and reward-weighted classifiers.

    spec: action_col, reward_col, key_cols, models, drop, id_features."""
    feats = resolve_features(
        ctx,
        df.columns,
        id_features=spec.get("id_features", ()),
        drop=spec.get("drop", ()),
    )
    cols = list(
        dict.fromkeys(
            [
                *feats,
                spec["action_col"],
                spec["reward_col"],
                "partition",
                *spec["key_cols"],
            ]
        )
    )
    pdf, fraction = to_training_frame(df, key_cols=spec["key_cols"], columns=cols)
    pdf = pdf.dropna(subset=[spec["action_col"], spec["reward_col"]])
    pdf[spec["action_col"]] = pdf[spec["action_col"]].astype(str)
    train = pdf[pdf["partition"] == "train"].reset_index(drop=True)
    valid = pdf[pdf["partition"] == "validation"].reset_index(drop=True)
    if train.empty or valid.empty:
        raise RuntimeError("need train and validation rows")
    train_eval = _train_eval_frame(train)
    enc = FeatureEncoder(feats).fit(train)
    y = train[spec["action_col"]].to_numpy()
    w = reward_weights(train[spec["reward_col"]])
    params = {"row_fraction": round(fraction, 4)}
    start_task(ctx)

    major = MajorityAction(train[spec["action_col"]].value_counts().idxmax())
    run_candidate(
        ctx,
        "majority_action",
        "baseline",
        lambda: (
            major,
            _policy_scores(major, valid, train_eval, spec, "action"),
            len(valid),
        ),
        params=params,
        stage="baseline",
    )
    classifiers = {
        "logistic": (("sklearn",), True, _make_logistic, LOGISTIC_GRID[1], False),
        "gbt_lightgbm": (
            ("lightgbm",),
            False,
            _make_lgbm_classifier,
            GBT_GRID[0],
            True,
        ),
        "gbt_sklearn": (("sklearn",), False, _make_hgb_classifier, GBT_GRID[0], True),
    }
    for kind, weighted in (("behaviour_cloning", False), ("reward_weighted", True)):
        for lib, (requires, fill, build, grid, can_weight) in classifiers.items():
            name = f"{kind}_{lib}"
            if name not in spec["models"] or (weighted and not can_weight):
                continue

            def fit(build=build, grid=grid, fill=fill, weighted=weighted):
                model = build(grid)
                x = enc.transform(train, fill=fill)
                if weighted:
                    model.fit(x, y, sample_weight=w)
                else:
                    model.fit(x, y)
                pol = ClassPolicy(enc, model, fill)
                return (
                    pol,
                    _policy_scores(pol, valid, train_eval, spec, "action"),
                    len(valid),
                )

            run_candidate(
                ctx, name, "offline_rl", fit, requires=requires, params=dict(params)
            )
    finish_task(ctx)


# COMMAND ----------

# DBTITLE 1,Session event-type sequence policies


def event_histories(pdf, type_ids, length, session_col):
    """Ids of the previous `length` event types in the session, oldest first,
    zero padded. The frame must be sorted by session and step."""
    ids = pdf["seq_event_type"].map(type_ids).fillna(0).astype("int64").to_numpy()
    grouped = pd.DataFrame({"s": pdf[session_col].to_numpy(), "i": ids}).groupby("s")[
        "i"
    ]
    cols = [
        grouped.shift(j).fillna(0).to_numpy(dtype="int64") for j in range(length, 0, -1)
    ]
    return np.column_stack(cols)


class SequencePolicy:
    def __init__(self, net, type_ids, classes, session_col, step_col):
        self.net, self.type_ids, self.classes = net, type_ids, classes
        self.cols = (session_col, step_col)

    def action(self, pdf):
        import torch

        session_col, step_col = self.cols
        ordered = pdf.sort_values([session_col, step_col])
        hist = event_histories(ordered, self.type_ids, SESSION_HISTORY, session_col)
        self.net.eval()
        out = []
        with torch.no_grad():
            for i in range(0, len(hist), SESSION_BATCH):
                logits = self.net(
                    torch.as_tensor(hist[i : i + SESSION_BATCH], dtype=torch.long)
                )
                out.append(logits.argmax(dim=1).numpy())
        pred = np.asarray(self.classes)[np.concatenate(out)] if out else np.zeros(0)
        return pd.Series(pred, index=ordered.index).reindex(pdf.index).to_numpy()


def _make_event_net(n_types, n_classes):
    from torch import nn

    class Net(nn.Module):
        def __init__(self):
            super().__init__()
            self.emb = nn.Embedding(n_types + 1, 16, padding_idx=0)
            self.gru = nn.GRU(16, 32, batch_first=True)
            self.out = nn.Linear(32, n_classes)

        def forward(self, x):
            o, _ = self.gru(self.emb(x))
            return self.out(o[:, -1])

    return Net()


def _train_event_net(hist, labels, weights, n_types, n_classes):
    import torch

    torch.manual_seed(MODEL_SEED)
    net = _make_event_net(n_types, n_classes)
    opt = torch.optim.Adam(net.parameters(), lr=2e-3)
    x = torch.as_tensor(hist, dtype=torch.long)
    y = torch.as_tensor(labels, dtype=torch.long)
    w = torch.as_tensor(weights, dtype=torch.float32)
    net.train()
    for _ in range(SESSION_EPOCHS):
        order = torch.randperm(len(x))
        for i in range(0, len(x), SESSION_BATCH):
            sel = order[i : i + SESSION_BATCH]
            loss = torch.nn.functional.cross_entropy(
                net(x[sel]), y[sel], reduction="none"
            )
            loss = (loss * w[sel]).sum() / w[sel].sum()
            opt.zero_grad()
            loss.backward()
            opt.step()
    return net


class MarkovPolicy:
    """Most frequent event type after the previous event type."""

    def __init__(self, table, fallback, prev_col):
        self.table, self.fallback, self.prev_col = table, fallback, prev_col

    def action(self, pdf):
        merged = pdf[[self.prev_col]].merge(
            self.table, left_on=self.prev_col, right_on="prev", how="left"
        )
        return merged["action"].fillna(self.fallback).to_numpy(dtype=object)


def run_session_policy(ctx, df, spec):
    """Frequent-event baseline, Markov, boosted behaviour cloning (plain and reward
    weighted) and the GRU policies over the previous event types.

    spec: action_col, reward_col, session_col, step_col, prev_col, key_cols,
    drop, id_features, models."""
    feats = resolve_features(
        ctx,
        df.columns,
        id_features=spec.get("id_features", ()),
        drop=spec.get("drop", ()),
    )
    sc, tc, ac, rc = (
        spec["session_col"],
        spec["step_col"],
        spec["action_col"],
        spec["reward_col"],
    )
    cols = list(
        dict.fromkeys(
            [*feats, ac, rc, sc, tc, spec["prev_col"], "seq_event_type", "partition"]
        )
    )
    pdf, fraction = to_training_frame(df, key_cols=[sc], columns=cols)
    pdf = pdf.dropna(subset=[ac, rc]).sort_values([sc, tc]).reset_index(drop=True)
    pdf[ac] = pdf[ac].astype(str)
    train = pdf[pdf["partition"] == "train"].reset_index(drop=True)
    valid = pdf[pdf["partition"] == "validation"].reset_index(drop=True)
    if train.empty or valid.empty:
        raise RuntimeError("need train and validation rows")
    train_eval = _train_eval_frame(train, sc)
    enc = FeatureEncoder(feats).fit(train)
    y = train[ac].to_numpy()
    w = reward_weights(train[rc])
    params = {"row_fraction": round(fraction, 4)}
    start_task(ctx)

    major = MajorityAction(train[ac].value_counts().idxmax())
    run_candidate(
        ctx,
        "most_frequent_event",
        "baseline",
        lambda: (
            major,
            _policy_scores(major, valid, train_eval, spec, "action"),
            len(valid),
        ),
        params=params,
        stage="baseline",
    )
    if "markov" in spec["models"]:
        counts = train.groupby([spec["prev_col"], ac]).size().reset_index(name="n")
        top = counts.sort_values("n", ascending=False).drop_duplicates(spec["prev_col"])
        table = top.rename(columns={spec["prev_col"]: "prev", ac: "action"})[
            ["prev", "action"]
        ]
        markov = MarkovPolicy(table, major.value, spec["prev_col"])
        run_candidate(
            ctx,
            "markov",
            "offline_rl",
            lambda: (
                markov,
                _policy_scores(markov, valid, train_eval, spec, "action"),
                len(valid),
            ),
            params=params,
        )

    classifiers = {
        "gbt_lightgbm": (("lightgbm",), _make_lgbm_classifier),
        "gbt_sklearn": (("sklearn",), _make_hgb_classifier),
    }
    for kind, weighted in (("behaviour_cloning", False), ("reward_weighted", True)):
        for lib, (requires, build) in classifiers.items():
            name = f"{kind}_{lib}"
            if name not in spec["models"]:
                continue

            def fit(build=build, weighted=weighted):
                model = build(GBT_GRID[0])
                x = enc.transform(train)
                if weighted:
                    model.fit(x, y, sample_weight=w)
                else:
                    model.fit(x, y)
                pol = ClassPolicy(enc, model, False)
                return (
                    pol,
                    _policy_scores(pol, valid, train_eval, spec, "action"),
                    len(valid),
                )

            run_candidate(
                ctx, name, "offline_rl", fit, requires=requires, params=dict(params)
            )

    types = sorted(pdf["seq_event_type"].dropna().astype(str).unique().tolist())
    type_ids = {t: i + 1 for i, t in enumerate(types)}
    classes = sorted(train[ac].unique().tolist())
    label_ids = {c: i for i, c in enumerate(classes)}
    for kind, weighted in (
        ("sequence_gru", False),
        ("sequence_gru_reward_weighted", True),
    ):
        if kind not in spec["models"]:
            continue

        def fit_gru(weighted=weighted):
            hist = event_histories(train, type_ids, SESSION_HISTORY, sc)
            labels = train[ac].map(label_ids).to_numpy()
            weights = w if weighted else np.ones(len(train))
            if len(train) > SESSION_TRAIN_ROWS:
                keep = np.sort(
                    np.random.default_rng(MODEL_SEED).choice(
                        len(train), SESSION_TRAIN_ROWS, replace=False
                    )
                )
                hist, labels, weights = hist[keep], labels[keep], weights[keep]
            net = _train_event_net(hist, labels, weights, len(types), len(classes))
            pol = SequencePolicy(net, type_ids, classes, sc, tc)
            return (
                pol,
                _policy_scores(pol, valid, train_eval, spec, "action"),
                len(valid),
            )

        run_candidate(
            ctx,
            kind,
            "offline_rl",
            fit_gru,
            requires=("torch",),
            params={**params, "history": SESSION_HISTORY, "epochs": SESSION_EPOCHS},
        )
    finish_task(ctx)


# COMMAND ----------

# DBTITLE 1,Imports
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Configuration
SEQ_LENGTH = 10
SEQ_VOCAB = 20000
SEQ_DIM = 64
SEQ_EPOCHS = 2
SEQ_BATCH = 1024
SEQ_TRAIN_ROWS = 1_000_000
SEQ_EVAL_ROWS = 300_000
COOCCURRENCE_TOP = 50

# COMMAND ----------

# DBTITLE 1,Popularity and co-occurrence rankers


class PopularityRanker:
    def __init__(self, rank: dict, truth_col: str):
        self.rank = rank
        self.truth_col = truth_col

    def ranks(self, pdf):
        return (
            pdf[self.truth_col].map(self.rank).fillna(np.inf).to_numpy(dtype="float64")
        )


class CooccurrenceRanker:
    """Next items seen after the current item, most frequent first; items never
    seen after it fall back to popularity behind the listed ones."""

    def __init__(self, table, popularity, src_col, truth_col, top):
        self.table = table
        self.popularity = popularity
        self.src_col = src_col
        self.truth_col = truth_col
        self.top = top

    def ranks(self, pdf):
        pair = pdf[[self.src_col, self.truth_col]].rename(
            columns={self.src_col: "src", self.truth_col: "tgt"}
        )
        merged = pair.merge(self.table, on=["src", "tgt"], how="left")
        listed = merged["rank"].to_numpy(dtype="float64")
        return np.where(
            np.isfinite(listed), listed, self.top + self.popularity.ranks(pdf)
        )


def fit_popularity(train, truth_col) -> PopularityRanker:
    counts = train[truth_col].value_counts()
    rank = {item: i + 1 for i, item in enumerate(counts.index)}
    return PopularityRanker(rank, truth_col)


def fit_cooccurrence(train, src_col, truth_col, popularity, top=COOCCURRENCE_TOP):
    pairs = (
        train[[src_col, truth_col]]
        .rename(columns={src_col: "src", truth_col: "tgt"})
        .groupby(["src", "tgt"])
        .size()
        .reset_index(name="n")
    )
    pairs["rank"] = pairs.groupby("src")["n"].rank(method="first", ascending=False)
    table = pairs[pairs["rank"] <= top][["src", "tgt", "rank"]]
    return CooccurrenceRanker(table, popularity, src_col, truth_col, top)


# COMMAND ----------

# DBTITLE 1,Item diagnostics: vocabulary and targets already seen in the session


def item_diagnostics(train, valid, session_col, step_col, item_col, truth_col):
    """Item vocabulary, and how often the target item already occurred earlier in
    the same session (which a ranker can exploit by repeating its history)."""
    first = (
        valid.groupby([session_col, item_col])[step_col]
        .min()
        .rename("first_step")
        .reset_index()
        .rename(columns={item_col: truth_col})
    )
    merged = valid[[session_col, step_col, truth_col]].merge(
        first, on=[session_col, truth_col], how="left"
    )
    vocabulary = set(train[item_col]) | set(train[truth_col])
    return {
        "item_vocabulary_train": len(vocabulary),
        "target_seen_earlier_in_session": float(
            merged["first_step"].le(merged[step_col]).mean()
        ),
        "target_in_train_vocabulary": float(valid[truth_col].isin(vocabulary).mean()),
    }


# COMMAND ----------

# DBTITLE 1,Session histories and the sequence network


def build_histories(pdf, item_ids, length, session_col, item_col):
    """Item ids of the last `length` events up to and including each row, oldest
    first, zero padded. The frame must already be sorted by session and step."""
    ids = pdf[item_col].map(item_ids).fillna(0).astype("int64").to_numpy()
    frame = pd.DataFrame({"s": pdf[session_col].to_numpy(), "i": ids})
    grouped = frame.groupby("s")["i"]
    cols = [
        grouped.shift(j).fillna(0).to_numpy(dtype="int64")
        for j in range(length - 1, -1, -1)
    ]
    return np.column_stack(cols)


def _make_sequence_net(kind, vocab, dim=SEQ_DIM, length=SEQ_LENGTH):
    import torch
    from torch import nn

    class Net(nn.Module):
        def __init__(self):
            super().__init__()
            self.emb = nn.Embedding(vocab + 1, dim, padding_idx=0)
            if kind == "gru":
                self.enc = nn.GRU(dim, dim, batch_first=True)
            else:
                layer = nn.TransformerEncoderLayer(
                    d_model=dim, nhead=4, dim_feedforward=2 * dim, batch_first=True
                )
                self.enc = nn.TransformerEncoder(layer, num_layers=2)
                self.pos = nn.Parameter(torch.zeros(length, dim))
            self.out = nn.Linear(dim, vocab + 1)

        def forward(self, x):
            h = self.emb(x)
            if kind == "gru":
                o, _ = self.enc(h)
            else:
                o = self.enc(h + self.pos)
            return self.out(o[:, -1])

    return Net()


def _train_sequence(kind, hist, target_ids, vocab):
    import torch

    torch.manual_seed(MODEL_SEED)
    net = _make_sequence_net(kind, vocab)
    opt = torch.optim.Adam(net.parameters(), lr=1e-3)
    loss_fn = torch.nn.CrossEntropyLoss(ignore_index=0)
    x = torch.as_tensor(hist, dtype=torch.long)
    y = torch.as_tensor(target_ids, dtype=torch.long)
    n = len(x)
    net.train()
    for _ in range(SEQ_EPOCHS):
        order = torch.randperm(n)
        for i in range(0, n, SEQ_BATCH):
            idx = order[i : i + SEQ_BATCH]
            opt.zero_grad()
            loss_fn(net(x[idx]), y[idx]).backward()
            opt.step()
    return net


def sequence_ranks(net, hist, truth_ids):
    import torch

    net.eval()
    out = []
    with torch.no_grad():
        for i in range(0, len(hist), SEQ_BATCH):
            logits = net(torch.as_tensor(hist[i : i + SEQ_BATCH], dtype=torch.long))
            logits[:, 0] = -float("inf")
            truth = torch.as_tensor(truth_ids[i : i + SEQ_BATCH], dtype=torch.long)
            own = logits.gather(1, truth[:, None])
            rank = (logits > own).sum(1) + 1
            out.append(torch.where(truth == 0, torch.full_like(rank, -1), rank).numpy())
    ranks = np.concatenate(out).astype("float64") if out else np.zeros(0)
    ranks[ranks < 0] = np.inf
    return ranks


class SequenceRanker:
    """A trained sequence network with its item vocabulary."""

    def __init__(self, kind, net, item_ids, session_col, step_col, item_col, truth_col):
        self.kind = kind
        self.net = net
        self.item_ids = item_ids
        self.cols = (session_col, step_col, item_col, truth_col)

    def ranks(self, pdf):
        session_col, step_col, item_col, truth_col = self.cols
        ordered = pdf.sort_values([session_col, step_col]).reset_index(drop=True)
        hist = build_histories(
            ordered, self.item_ids, SEQ_LENGTH, session_col, item_col
        )
        truth = (
            ordered[truth_col].map(self.item_ids).fillna(0).astype("int64").to_numpy()
        )
        return sequence_ranks(self.net, hist, truth)


# COMMAND ----------

# DBTITLE 1,Ranking runner


def run_ranking(ctx, df, spec):
    """Popularity baseline and the co-occurrence and sequence candidates.

    spec: session_col, step_col, src_col, truth_col, key_cols, models."""
    sc, tc, ic, yc = (
        spec["session_col"],
        spec["step_col"],
        spec["src_col"],
        spec["truth_col"],
    )
    cols = list(dict.fromkeys([sc, tc, ic, yc, "partition"]))
    pdf, fraction = to_training_frame(df, key_cols=[sc], columns=cols)
    pdf = pdf.dropna(subset=[yc, ic]).sort_values([sc, tc]).reset_index(drop=True)
    tr_m = (pdf["partition"] == "train").to_numpy()
    va_m = (pdf["partition"] == "validation").to_numpy()
    if not tr_m.any() or not va_m.any():
        raise RuntimeError("need train and validation rows")
    train, valid = pdf[tr_m], pdf[va_m]
    rng = np.random.default_rng(MODEL_SEED)
    if len(valid) > SEQ_EVAL_ROWS:
        chosen = (
            valid[sc]
            .drop_duplicates()
            .sample(frac=SEQ_EVAL_ROWS / len(valid), random_state=MODEL_SEED)
        )
        valid = valid[valid[sc].isin(chosen)]
    train_eval = train
    if len(train) > SEQ_EVAL_ROWS:
        chosen_tr = (
            train[sc]
            .drop_duplicates()
            .sample(frac=SEQ_EVAL_ROWS / len(train), random_state=MODEL_SEED)
        )
        train_eval = train[train[sc].isin(chosen_tr)]
    note_diagnostics(item_diagnostics(train, valid, sc, tc, ic, yc))
    start_task(ctx)

    def scored(ranker):
        metrics = ranking_metrics(ranker.ranks(valid), RANK_KS)

        def fitted():
            r = ranking_metrics(ranker.ranks(train_eval), RANK_KS)
            return {
                f"train_{k}": r[k]
                for k in ("recall_at_10", "ndcg_at_10", "mrr")
                if k in r
            }

        metrics.update(train_side(fitted))
        return metrics

    pop = fit_popularity(train, yc)
    run_candidate(
        ctx,
        "popularity",
        "baseline",
        lambda: (pop, scored(pop), len(valid)),
        params={"row_fraction": round(fraction, 4)},
        stage="baseline",
    )
    if "cooccurrence" in spec["models"]:
        cooc = fit_cooccurrence(train, ic, yc, pop)
        run_candidate(
            ctx,
            "cooccurrence",
            "ranking",
            lambda: (cooc, scored(cooc), len(valid)),
            params={"top": COOCCURRENCE_TOP},
        )

    items = pd.concat([train[ic], train[yc]]).value_counts().head(SEQ_VOCAB).index
    item_ids = {item: i + 1 for i, item in enumerate(items)}
    hist_tr = build_histories(train, item_ids, SEQ_LENGTH, sc, ic)
    target_tr = train[yc].map(item_ids).fillna(0).astype("int64").to_numpy()
    usable = np.flatnonzero(target_tr > 0)
    if len(usable) > SEQ_TRAIN_ROWS:
        usable = np.sort(rng.choice(usable, SEQ_TRAIN_ROWS, replace=False))

    for kind, name in (
        ("gru", "sequence_gru"),
        ("transformer", "sequence_transformer"),
    ):
        if name not in spec["models"]:
            continue
        params = {
            "vocab": len(item_ids),
            "dim": SEQ_DIM,
            "epochs": SEQ_EPOCHS,
            "history": SEQ_LENGTH,
            "train_rows": len(usable),
            "eval_rows": len(valid),
            "row_fraction": round(fraction, 4),
        }

        def fit(kind=kind):
            net = _train_sequence(
                kind, hist_tr[usable], target_tr[usable], len(item_ids)
            )
            ranker = SequenceRanker(kind, net, item_ids, sc, tc, ic, yc)
            return ranker, scored(ranker), len(valid)

        run_candidate(ctx, name, "sequence", fit, requires=("torch",), params=params)
    finish_task(ctx)


# COMMAND ----------

# DBTITLE 1,Multi-class runner


class ClassClassifier:
    """A fitted multi-class classifier: encoder plus model."""

    def __init__(self, encoder, model, classes, fill):
        self.encoder, self.model, self.classes, self.fill = (
            encoder,
            model,
            classes,
            fill,
        )

    def predict(self, pdf):
        return np.asarray(
            self.model.predict(self.encoder.transform(pdf, fill=self.fill))
        )


class MajorityClass:
    def __init__(self, label):
        self.label = label

    def predict(self, pdf):
        return np.full(len(pdf), self.label, dtype=object)


def _make_multiclass_logistic():
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    return make_pipeline(
        StandardScaler(with_mean=False),
        LogisticRegression(C=1.0, max_iter=1000, class_weight="balanced"),
    )


def run_multiclass(ctx, df, spec):
    """Majority-class baseline, logistic regression on the numeric features and
    boosted trees; precision and recall per class."""
    feats = resolve_features(ctx, df.columns)
    cols = list(dict.fromkeys([*feats, spec["target"], "partition"]))
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
        metrics = multiclass_metrics(y_va, model.predict(valid), labels)

        def fitted():
            t = multiclass_metrics(y_tr, model.predict(train), labels)
            return {
                "train_accuracy": t["accuracy"],
                "train_balanced_accuracy": t["balanced_accuracy"],
            }

        metrics.update(train_side(fitted))
        return metrics

    major = MajorityClass(train[spec["target"]].value_counts().idxmax())
    run_candidate(
        ctx,
        "majority_class",
        "baseline",
        lambda: (major, scored(major), len(valid)),
        params={"row_fraction": round(fraction, 4)},
        stage="baseline",
    )

    def classifier(fill, build):
        def fit():
            bundle = ClassClassifier(enc, None, labels, fill)
            bundle.model = build().fit(enc.transform(train, fill=fill), y_tr)
            return bundle, scored(bundle), len(valid)

        return fit

    builders = {
        "logistic_numeric": (True, _make_multiclass_logistic, ("sklearn",), {"C": 1.0}),
        "gbt_lightgbm": (
            False,
            lambda: _make_lgbm_classifier(GBT_GRID[1]),
            ("lightgbm",),
            dict(GBT_GRID[1]),
        ),
        "gbt_sklearn": (
            False,
            lambda: _make_hgb_classifier(GBT_GRID[1]),
            ("sklearn",),
            dict(GBT_GRID[1]),
        ),
    }
    for name, (fill, build, requires, params) in builders.items():
        if name in spec["models"]:
            run_candidate(
                ctx,
                name,
                "multiclass",
                classifier(fill, build),
                requires=requires,
                params=params,
            )
    finish_task(ctx)


# COMMAND ----------

# DBTITLE 1,Run one task of the task file

RUNNERS = {
    "regression": "run_regression",
    "classification": "run_classification",
    "multiclass": "run_multiclass",
    "survival": "run_survival",
    "anomaly": "run_anomaly",
    "reconstruction": "run_reconstruction",
    "pumped_storage": "run_pumped_storage",
    "action_policy": "run_action_policy",
    "session_policy": "run_session_policy",
    "ranking": "run_ranking",
}


def prepare_frame(task: dict, df: DataFrame, ctx) -> DataFrame:
    """The frame of a task as its candidates read it: the calendar and realised lags
    of the load, or the event days of the redispatch energy."""
    if task.get("prepare") == "load_features":
        return complete_load_features(df, ctx)
    if task.get("prepare") == "event_days":
        return df.filter(
            F.col("target_any_event") & F.col("target_log_event_energy_mwh").isNotNull()
        )
    return df


def run_task(task_id: str, rid: str, smoke: bool) -> None:
    """Read the frozen dataset of one task and run its candidates."""
    task = TASK_BY_ID[task_id]
    ctx = TaskContext(task_id, rid, smoke)
    df = read_frozen(ctx)
    if task.get("sample_users"):
        df = sample_users(df)
    df = prepare_frame(task, df, ctx)
    globals()[RUNNERS[task["runner"]]](ctx, df, task["fit"])
