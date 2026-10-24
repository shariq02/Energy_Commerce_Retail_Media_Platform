# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION ANOMALY AND RECONSTRUCTION LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** pretrained masked-reconstruction models (MOMENT, Energy-TSPulse) as
# MAGIC extra candidates for the anomaly task and the masked-block reconstruction task,
# MAGIC pulled in with `%run ../lib/_fit_foundation_anomaly` after `_fit_foundation_ts`,
# MAGIC `_fit_anomaly` and `_fit_reconstruction`. Both run zero-shot: an anomaly
# MAGIC score is the reconstruction error of a window, a masked block is filled from
# MAGIC the window that ends at the block.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Candidates, window and task settings
RECON_FAMILY = "foundation_recon"
RECON_WINDOW = 512
ANOMALY_TRAIN_WINDOWS = 2000
RECON_CANDIDATES = (
    ("moment_zero_shot", "moment", "momentfm"),
    ("energy_tspulse_zero_shot", "energy_tspulse", "tsfm_public"),
)
ANOMALY_TASKS = {
    "honda_anomaly.anomaly": {
        "spec": {
            "series_cols": ["location_key", "channel"],
            "time_col": "interval_start_utc",
            "target": "target_increment",
            "key_cols": [
                "location_key",
                "channel",
                "interval_seconds",
                "interval_start_utc",
            ],
            "drop": [],
        }
    }
}
RECON_TASKS = {
    "weather_imputation.reconstruction": {
        "spec": {
            "sets": [
                {
                    "name": "weather",
                    "id_col": "location_key",
                    "ts_col": "observation_timestamp_utc",
                    "variables": [
                        "air_temperature",
                        "dew_point_temperature",
                        "relative_humidity",
                        "pressure_station",
                        "pressure_sea_level",
                        "wind_speed",
                        "wind_direction",
                        "cloud_cover",
                        "visibility",
                        "soil_temperature",
                    ],
                    "freq": "h",
                    "period": 24,
                    "step_hours": 1,
                    "baseline": "carry_forward",
                    "circular": ["wind_direction"],
                }
            ]
        }
    }
}

# COMMAND ----------

# DBTITLE 1,Reconstruction backends (each imports its library when loaded)


class MomentBackend:
    """MOMENT in reconstruction mode; the input is (batch, 1, window) with a mask
    that is 1 where a value is known."""

    name = "moment"
    batch = 64

    def __init__(self, weights_dir):
        self.weights_dir = weights_dir
        self.model = None
        self.device = None

    def load(self):
        if self.model is None:
            from momentfm import MOMENTPipeline

            self.device = _device()
            model = MOMENTPipeline.from_pretrained(
                self.weights_dir, model_kwargs={"task_name": "reconstruction"}
            )
            model.init()
            self.model = model.to(self.device).eval()
        return self.model

    def reconstruct(self, windows, observed):
        import torch

        model = self.load()
        out = np.empty(windows.shape, dtype="float32")
        for start in range(0, len(windows), self.batch):
            part = slice(start, start + self.batch)
            x = torch.tensor(windows[part, None, :], dtype=torch.float32)
            m = torch.tensor(observed[part], dtype=torch.float32)
            with torch.no_grad():
                result = model(
                    x_enc=x.to(self.device), input_mask=m.to(self.device)
                ).reconstruction
            out[part] = result[:, 0, :].float().cpu().numpy()
        return out


class TSPulseBackend:
    """Energy-TSPulse: past values and the observed mask as (batch, window, 1)."""

    name = "energy_tspulse"
    batch = 256

    def __init__(self, weights_dir):
        self.weights_dir = weights_dir
        self.model = None
        self.device = None

    def load(self):
        if self.model is None:
            from tsfm_public.models.tspulse import TSPulseForReconstruction

            self.device = _device()
            self.model = TSPulseForReconstruction.from_pretrained(
                self.weights_dir, num_input_channels=1
            )
            self.model = self.model.to(self.device).eval()
        return self.model

    def reconstruct(self, windows, observed):
        import torch

        model = self.load()
        out = np.empty(windows.shape, dtype="float32")
        for start in range(0, len(windows), self.batch):
            part = slice(start, start + self.batch)
            x = torch.tensor(windows[part, :, None], dtype=torch.float32)
            m = torch.tensor(observed[part, :, None], dtype=torch.float32)
            with torch.no_grad():
                result = model(
                    past_values=x.to(self.device),
                    past_observed_mask=m.to(self.device),
                )
            rec = getattr(result, "reconstruction_outputs", None)
            if rec is None:
                rec = result.prediction_outputs
            out[part] = rec[:, :, 0].float().cpu().numpy()
        return out


RECON_BACKENDS = {"moment": MomentBackend, "energy_tspulse": TSPulseBackend}

# COMMAND ----------

# DBTITLE 1,Windows of one series


def window_starts(n: int, size: int = RECON_WINDOW) -> list:
    """Start rows of the windows that cover a series of n rows: whole windows from
    the start and one window that ends at the last row."""
    if n <= size:
        return [0]
    starts = list(range(0, n - size + 1, size))
    if starts[-1] + size < n:
        starts.append(n - size)
    return starts


def padded_window(values, start: int, size: int = RECON_WINDOW):
    """(values, observed) of one window; a short window is padded on the left and a
    missing value is zero with observed 0."""
    seg = np.asarray(values[start : start + size], dtype="float64")
    ok = np.isfinite(seg)
    win, obs = np.zeros(size), np.zeros(size)
    win[size - len(seg) :] = np.where(ok, seg, 0.0)
    obs[size - len(seg) :] = ok
    return win, obs


def series_errors(backend, values, max_windows=None) -> np.ndarray:
    """Absolute reconstruction error of each row of one series in units of the
    series spread; rows outside the sampled windows stay NaN."""
    values = np.asarray(values, dtype="float64")
    n = len(values)
    starts = window_starts(n)
    if max_windows and len(starts) > max_windows:
        pick = np.linspace(0, len(starts) - 1, max_windows).round().astype(int)
        starts = [starts[i] for i in dict.fromkeys(pick.tolist())]
    pairs = [padded_window(values, s) for s in starts]
    rec = backend.reconstruct(
        np.stack([p[0] for p in pairs]), np.stack([p[1] for p in pairs])
    )
    total, count = np.zeros(n), np.zeros(n)
    for s, (win, obs), r in zip(starts, pairs, rec, strict=True):
        k = min(RECON_WINDOW, n - s)
        err = np.abs(win[RECON_WINDOW - k :] - r[RECON_WINDOW - k :])
        seen = obs[RECON_WINDOW - k :] > 0
        total[s : s + k] += np.where(seen, err, 0.0)
        count[s : s + k] += seen
    spread = float(np.nanstd(values)) if np.isfinite(values).any() else 1.0
    spread = spread if spread > 1e-9 else 1.0
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(count > 0, total / np.maximum(count, 1) / spread, np.nan)


# COMMAND ----------

# DBTITLE 1,Bundles -- anomaly scorer and block reconstructor


class ReconstructionScorer:
    """Anomaly score of every row of a frame sorted by series and time: the
    reconstruction error of the windows that cover it."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self._backend = None

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_backend"] = None
        return state

    def backend(self):
        if self._backend is None:
            with timed_stage(f"load {self.cfg['backend']} weights"):
                self._backend = RECON_BACKENDS[self.cfg["backend"]](
                    self.cfg["weights_dir"]
                )
        return self._backend

    def score(self, pdf, max_windows=None):
        c = self.cfg
        y = pd.to_numeric(pdf[c["target"]], errors="coerce").to_numpy(dtype="float64")
        out = np.full(len(pdf), np.nan)
        for idx in pdf.groupby(c["series_cols"], sort=False).indices.values():
            out[idx] = series_errors(self.backend(), y[idx], max_windows)
        return out


class BlockReconstructor:
    """Fills a masked block of one variable. The window ends at the end of the block,
    so the model sees the observed values before the block and nothing after it."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self._backend = None

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_backend"] = None
        return state

    def backend(self):
        if self._backend is None:
            with timed_stage(f"load {self.cfg['backend']} weights"):
                self._backend = RECON_BACKENDS[self.cfg["backend"]](
                    self.cfg["weights_dir"]
                )
        return self._backend

    def predict(self, sset, events, var_js):
        wins, obs, lengths = [], [], []
        for (i, start, length), j in zip(events, var_js, strict=True):
            arr = sset.arrays[i][:, j]
            end = start + length
            seg = np.asarray(arr[max(0, end - RECON_WINDOW) : end], dtype="float64")
            hidden = min(length, RECON_WINDOW, len(seg))
            ok = np.isfinite(seg)
            ok[len(seg) - hidden :] = False
            win, seen = np.zeros(RECON_WINDOW), np.zeros(RECON_WINDOW)
            win[RECON_WINDOW - len(seg) :] = np.where(ok, seg, 0.0)
            seen[RECON_WINDOW - len(seg) :] = ok
            wins.append(win)
            obs.append(seen)
            lengths.append((length, hidden))
        if not wins:
            return np.zeros(0)
        rec = self.backend().reconstruct(np.stack(wins), np.stack(obs))
        parts = []
        for r, (length, hidden) in zip(rec, lengths, strict=True):
            block = np.full(length, np.nan)
            block[length - hidden :] = r[RECON_WINDOW - hidden :]
            parts.append(block)
        return np.concatenate(parts).astype("float64")


def recon_config(entry: dict, backend: str, **extra) -> dict:
    return {
        "backend": backend,
        "mode": "zero_shot",
        "model_key": entry["model_key"],
        "repo_id": entry["repo_id"],
        "revision": entry["revision"],
        "licence": entry["licence"],
        "files": entry["files"],
        "weights_dir": entry["dir"],
        **extra,
    }


def recon_params(cfg: dict, **extra) -> dict:
    keys = ("model_key", "repo_id", "revision", "licence", "mode")
    return {**{k: cfg[k] for k in keys}, "window": RECON_WINDOW, **extra}


# COMMAND ----------

# DBTITLE 1,Anomaly runner -- the same injected series as the accepted detectors


def run_foundation_anomaly(ctx, df, models=None) -> None:
    """MOMENT and Energy-TSPulse on the validation rows of the anomaly task: the
    same rows, the same injected anomalies and the same flag rule as the accepted
    detectors. The flag threshold is a train score quantile over sampled windows."""
    spec = ANOMALY_TASKS[ctx.task_id]["spec"]
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
    values, injected = inject_anomalies(
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
    valid_injected[yc] = values
    per_series = max(1, ANOMALY_TRAIN_WINDOWS // max(1, train.groupby(sc).ngroups))
    start_task(ctx)

    chosen = [c for c in RECON_CANDIDATES if models is None or c[0] in models]
    for name, backend, library in chosen:
        params = {
            "inject_rate": INJECT_RATE,
            "flag_quantile": FLAG_QUANTILE,
            "injected_rows": int(injected.sum()),
            "row_fraction": round(fraction, 4),
            "train_windows_per_series": per_series,
        }

        def fit(backend=backend, params=params):
            entry = model_entry(backend)
            cfg = recon_config(
                entry, backend, series_cols=list(sc), target=yc, time_col=tc
            )
            scorer = ReconstructionScorer(cfg)
            s_train = scorer.score(train, per_series)
            s_train = s_train[np.isfinite(s_train)]
            thr = float(np.quantile(s_train, FLAG_QUANTILE))
            s_inj = np.nan_to_num(scorer.score(valid_injected))
            s_orig = np.nan_to_num(scorer.score(valid))
            metrics = detection_metrics(
                injected, s_inj > thr, float(np.mean(s_orig > thr))
            )
            metrics["pr_auc"] = average_precision(injected, s_inj)
            metrics["threshold"] = thr
            metrics["train_flag_rate"] = float(np.mean(s_train > thr))
            params.update(recon_params(cfg, seed=MODEL_SEED))
            return scorer, metrics, len(valid)

        run_candidate(ctx, name, RECON_FAMILY, fit, requires=(library,), params=params)
    finish_foundation_task(ctx)


# COMMAND ----------

# DBTITLE 1,Reconstruction scores of a predictor, per variable and over the baseline


def reconstruction_scores(prepared, data, base_mae, held, predict) -> dict:
    """The metrics of `_fit_reconstruction`: error and skill over the causal baseline
    per variable, the error on held-out series, and the mean skill."""
    out, skills = {}, []
    for cfg, tr, va, ev, steps in prepared:
        for j, var in enumerate(cfg["variables"]):
            _, _, xv, yv, sv = data[(cfg["name"], j)]
            pred = np.asarray(predict(cfg, tr, va, ev, j, xv, steps))
            e = masked_abs_error(var, cfg.get("circular", ()), yv, pred)
            key = f"{cfg['name']}__{var}"
            mae_v = float(e.mean()) if len(e) else float("nan")
            out[f"mae__{key}"] = mae_v
            out[f"skill_mae__{key}"] = skill(mae_v, base_mae[(cfg["name"], j)])
            skills.append(out[f"skill_mae__{key}"])
            if held:
                hm = np.isin(np.array(va.ids, dtype=object)[sv], list(held))
                eh = masked_abs_error(var, cfg.get("circular", ()), yv[hm], pred[hm])
                out[f"mae_heldout__{key}"] = (
                    float(eh.mean()) if len(eh) else float("nan")
                )
    out["skill_mae_mean"] = float(np.nanmean(skills)) if skills else float("nan")
    return out


def run_foundation_reconstruction(ctx, df, models=None) -> None:
    """MOMENT and Energy-TSPulse on the masked blocks of the reconstruction task:
    the same series, events and causal baseline as the accepted models."""
    spec = RECON_TASKS[ctx.task_id]["spec"]
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
    base_mae = {}
    for cfg, _tr, _va, _ev, _steps in prepared:
        for j, var in enumerate(cfg["variables"]):
            key = (cfg["name"], j)
            base = BaselineReconstructor(cfg["baseline"], {j: fallbacks[key]})
            truth, inputs = data[key][3], data[key][2]
            e = masked_abs_error(
                var, cfg.get("circular", ()), truth, base.predict(j, inputs)
            )
            base_mae[key] = float(e.mean()) if len(e) else float("nan")

    chosen = [c for c in RECON_CANDIDATES if models is None or c[0] in models]
    for name, backend, library in chosen:
        params = {"row_fraction": 1.0}

        def fit(backend=backend, params=params):
            entry = model_entry(backend)
            cfg0 = recon_config(entry, backend)
            bundle = {c["name"]: BlockReconstructor(cfg0) for c in spec["sets"]}

            def predict(cfg, tr, va, ev, j, xv, steps):
                events = ev[("validation", j)]
                return bundle[cfg["name"]].predict(va, events, [j] * len(events))

            metrics = reconstruction_scores(prepared, data, base_mae, held, predict)
            params.update(recon_params(cfg0, seed=MODEL_SEED))
            return bundle, metrics, len(data)

        run_candidate(ctx, name, RECON_FAMILY, fit, requires=(library,), params=params)
    finish_foundation_task(ctx)
