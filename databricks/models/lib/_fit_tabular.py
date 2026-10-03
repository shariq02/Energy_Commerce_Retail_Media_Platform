# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # TABULAR MODEL LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** baselines and candidates for one-row-one-example datasets (regression,
# MAGIC quantile regression, binary classification), pulled in with `%run ../lib/_fit_tabular`
# MAGIC after `_model_common` and `_model_metrics`. Libraries are imported inside the
# MAGIC builders so a missing one becomes a recorded skip, never an import error.

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
