# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ANOMALY MODEL LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** anomaly scores for unlabelled sensor series, evaluated on anomalies
# MAGIC injected into the validation period. Pulled in with `%run ../lib/_fit_anomaly` after
# MAGIC `_model_common`, `_model_metrics` and `_fit_tabular` (its regressor builders).
# MAGIC Injection follows the stored mask specification; the dataset is never changed.

# COMMAND ----------

# DBTITLE 1,Imports
import json as _json

import numpy as np
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
