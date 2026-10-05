# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # RECONSTRUCTION MODEL LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** self-supervised reconstruction of masked blocks in regular series,
# MAGIC pulled in with `%run ../lib/_fit_reconstruction` after `_model_common`,
# MAGIC `_model_metrics` and `_fit_tabular` (its regressor builders). A block of one
# MAGIC variable is hidden and predicted from what was observed before it (causal), plus
# MAGIC the other variables at the same time. Block lengths and held-out stations come
# MAGIC from the stored mask specification; nothing is stored as a masked copy.

# COMMAND ----------

# DBTITLE 1,Imports
import json as _json

import numpy as np
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


def _make_sequence_net(channels, n_vars):
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
    net = _make_sequence_net(2 * n_vars + 1, n_vars)
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
