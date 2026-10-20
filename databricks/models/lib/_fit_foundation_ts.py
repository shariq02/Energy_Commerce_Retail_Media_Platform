# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION TIME-SERIES LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** pretrained forecasting models (Chronos-2, TimesFM 2.5, Granite TTM)
# MAGIC as extra candidates for the time-series tasks, pulled in with
# MAGIC `%run ../lib/_fit_foundation_ts` after `_model_common`, `_model_metrics` and
# MAGIC `_fit_tabular`. A model is stored as a small reference bundle; its history is
# MAGIC read from the frozen dataset (train and validation rows) when it predicts.

# COMMAND ----------

# DBTITLE 1,Imports
import hashlib
import json
import os

import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Tables, volume and result family
FOUNDATION_RESULTS = "foundation_results"
FOUNDATION_CONTEXT = "foundation_run_context"
FOUNDATION_SELECTION = "foundation_selection"
FOUNDATION_REGISTER = "foundation_model_register"
FOUNDATION_CONTAMINATION = "foundation_contamination"
FOUNDATION_REGISTRY = "foundation_registry_entry"
FOUNDATION_REQUESTS = "scoring_requests"
FOUNDATION_VOLUME = "foundation_models"
FOUNDATION_MANIFEST = "foundation_manifest.json"
FOUNDATION_FAMILY = "foundation_ts"
FOUNDATION_RANK = FORWARD_TOP_N + 1
FOUNDATION_DDL = {
    FOUNDATION_RESULTS: RESULT_COLUMNS,
    FOUNDATION_CONTEXT: RUN_CONTEXT_COLUMNS,
    FOUNDATION_SELECTION: MODEL_DDL["candidate_selection"],
    FOUNDATION_REGISTER: (
        "model_key string, repo_id string, revision string, licence string, "
        "licence_restriction string, pretraining_data string, files string, "
        "run_id string, recorded_at timestamp"
    ),
    FOUNDATION_CONTAMINATION: (
        "model_key string, task_id string, dataset_id string, "
        "evaluated_window string, overlap_note string, flag string, "
        "run_id string, recorded_at timestamp"
    ),
    FOUNDATION_REGISTRY: (
        "task_id string, model_name string, role string, registry_version int, "
        "model_key string, repo_id string, revision string, weights_files string, "
        "adapter_ref string, licence string, licence_restriction string, "
        "mode string, settings string, run_id string, recorded_at timestamp"
    ),
    FOUNDATION_REQUESTS: (
        "request_id string, task_id string, model_name string, "
        "registry_version int, key_filter string, window_start string, "
        "window_end string, row_key string, prediction double, "
        "prediction_q10 double, prediction_q90 double, "
        "frozen_delta_version bigint, run_id string, scored_at timestamp"
    ),
}

# COMMAND ----------

# DBTITLE 1,Settings of the candidates and of the LoRA start
QUANTILE_LEVELS = (0.1, 0.5, 0.9)
MIN_CONTEXT_VALUES = 8
LORA_SETTINGS = {
    "learning_rate": 1e-5,
    "num_steps": 500,
    "batch_size": 32,
    "context_length": 512,
    "max_series": 200,
}
LORA_SMOKE_STEPS = 20
CANDIDATES = (
    ("chronos2_zero_shot", "chronos2", "chronos", False),
    ("chronos2_lora", "chronos2", "chronos", True),
    ("timesfm25_zero_shot", "timesfm25", "timesfm", False),
    ("ttm_r2_zero_shot", "ttm_r2", "tsfm_public", False),
    ("toto2_zero_shot", "toto2", "toto2", False),
    ("energy_ttm_zero_shot", "energy_ttm", "tsfm_public", False),
)
# a candidate that is not listed here runs on every time-series task
CANDIDATE_TASKS = {"energy_ttm_zero_shot": ("load.load",)}

# COMMAND ----------

# DBTITLE 1,Time-series tasks: series, time axis, gap and the baseline settings
TS_TASKS = {
    "price_daily.price": {
        "series": ["market_area_code"],
        "time": "local_date",
        "seconds": 86400,
        "gap_days": 2,
        "context": 512,
        "spec": {
            "target": "target_price_eur_per_mwh",
            "key_cols": ["market_area_code", "local_date"],
            "id_features": ["market_area_code"],
            "baseline": {
                "kind": "column",
                "column": "price_eur_per_mwh_lag7",
                "name": "seasonal_naive",
            },
            "quantile_cols": ["month"],
            "price": True,
            "date_col": "local_date",
        },
    },
    "price_quarter_hour.price": {
        "series": ["market_area_code"],
        "time": "interval_start_utc",
        "seconds": 900,
        "gap_days": 2,
        "context": 672,
        "spec": {
            "target": "target_price_eur_per_mwh",
            "key_cols": ["market_area_code", "interval_start_utc"],
            "id_features": ["market_area_code"],
            "baseline": {
                "kind": "column",
                "column": "price_eur_per_mwh_lag2d",
                "name": "persistence",
            },
            "quantile_cols": ["month"],
            "price": True,
            "date_col": "local_date",
        },
    },
    "load.load": {
        "series": ["market_area_code", "load_kind"],
        "time": "local_date",
        "seconds": 86400,
        "gap_days": 2,
        "context": 512,
        "complete_load_features": True,
        "spec": {
            "target": "target_value_mwh",
            "key_cols": ["market_area_code", "local_date", "load_kind"],
            "id_features": ["market_area_code", "load_kind"],
            "baseline": {
                "kind": "group_mean",
                "cols": ["market_area_code", "load_kind", "day_of_week", "month"],
                "name": "seasonal_mean",
            },
            "date_col": "local_date",
        },
    },
    "bias.bias": {
        "series": ["market_area_code", "forecast_scope"],
        "time": "local_date",
        "seconds": 86400,
        "gap_days": 2,
        "context": 512,
        "spec": {
            "target": "target_bias_mwh",
            "key_cols": ["market_area_code", "local_date", "forecast_scope"],
            "id_features": ["market_area_code", "forecast_scope"],
            "baseline": {"kind": "zero", "name": "zero_correction"},
            "date_col": "local_date",
        },
    },
    "zone_generation.generation": {
        "series": ["market_area_code", "carrier"],
        "time": "local_date",
        "seconds": 86400,
        "gap_days": 2,
        "context": 512,
        "spec": {
            "target": "target_generation_mwh",
            "key_cols": ["market_area_code", "local_date", "carrier"],
            "id_features": ["market_area_code", "carrier"],
            "baseline": {
                "kind": "group_ratio",
                "cols": ["market_area_code", "carrier", "month"],
                "denominator": "capacity_net_mw",
                "name": "capacity_factor",
            },
            "date_col": "local_date",
        },
    },
    "honda_forecast.increment": {
        "series": ["location_key", "channel", "interval_seconds"],
        "time": "interval_start_utc",
        "seconds": None,
        "seconds_col": "interval_seconds",
        "gap_days": 1,
        "context": 672,
        "spec": {
            "target": "target_increment",
            "key_cols": [
                "location_key",
                "channel",
                "interval_seconds",
                "interval_start_utc",
            ],
            "id_features": ["location_key", "channel", "interval_seconds"],
            "baseline": {
                "kind": "column",
                "column": "increment_lag2d",
                "name": "persistence",
            },
            "date_col": "local_date",
        },
    },
}

# COMMAND ----------

# DBTITLE 1,Context and result tables of the foundation candidates


class FoundationContext(TaskContext):
    """One time-series task for one foundation notebook run; the results and the
    run context go to the foundation tables, never to the accepted ones."""

    results_table = FOUNDATION_RESULTS
    context_table = FOUNDATION_CONTEXT

    def __init__(self, task_id: str, rid: str, smoke: bool = False):
        super().__init__(task_id, rid, smoke, record=False)
        self.component = f"models/foundation/{task_id}"
        record_task_context(self, "started")


# COMMAND ----------

# DBTITLE 1,Every task that has foundation candidates


FOUNDATION_TASK_TABLES = (
    "TS_TASKS",
    "TAB_TASKS",
    "TIER_TASKS",
    "ANOMALY_TASKS",
    "RECON_TASKS",
    "SURVIVAL_TASKS",
    "RANK_TASKS",
    "POLICY_TASKS",
)


def foundation_task_ids() -> list:
    """The time-series tasks, plus the tasks of every other foundation library that
    is loaded."""
    ids: list = []
    for name in FOUNDATION_TASK_TABLES:
        ids += [t for t in globals().get(name, {}) if t not in ids]
    return ids


# COMMAND ----------

# DBTITLE 1,Weights in the volume: manifest, hash check, folders


def foundation_root() -> str:
    return f"/Volumes/{CATALOG}/{MODEL_SCHEMAS['energy']}/{FOUNDATION_VOLUME}"


def read_foundation_manifest() -> dict:
    path = f"{foundation_root()}/{FOUNDATION_MANIFEST}"
    if not os.path.isfile(path):
        raise FileNotFoundError(f"{path} not found: run the foundation model upload")
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _file_sha256(path: str) -> str:
    with open(path, "rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


_VERIFIED: dict = {}


def model_entry(model_key: str, verify: bool = True) -> dict:
    """The manifest entry of a model with its folder; the files are hashed once per
    process and must equal the recorded hashes."""
    entry = read_foundation_manifest()["models"].get(model_key)
    if entry is None:
        raise KeyError(f"{model_key} is not in the foundation manifest")
    for field in ("repo_id", "revision", "licence", "files"):
        if not entry.get(field):
            raise ValueError(f"{model_key}: manifest has no {field}")
    folder = f"{foundation_root()}/{model_key}/{entry['revision']}"
    if verify and model_key not in _VERIFIED:
        bad = [
            name
            for name, digest in entry["files"].items()
            if _file_sha256(f"{folder}/{name}") != digest
        ]
        if bad:
            raise ValueError(f"{model_key}: files differ from the manifest: {bad}")
        _VERIFIED[model_key] = True
    return {**entry, "model_key": model_key, "dir": folder}


# COMMAND ----------

# DBTITLE 1,Regular time grid and series ids

_EPOCH = pd.Timestamp("1970-01-01", tz="UTC")


def grid_index(values, seconds: int) -> np.ndarray:
    """Position of each timestamp on a grid of `seconds` since the epoch; NaN where
    the timestamp cannot be read."""
    t = pd.to_datetime(pd.Series(values), utc=True, errors="coerce")
    elapsed = (t - _EPOCH).dt.total_seconds().to_numpy(dtype="float64")
    return np.floor(elapsed / seconds)


def series_ids(frame: pd.DataFrame, series) -> pd.Series:
    sid = frame[series[0]].astype(str)
    for column in series[1:]:
        sid = sid + "|" + frame[column].astype(str)
    return sid


def fill_gaps(values):
    """Forward then backward fill; None when nothing is known."""
    v = pd.Series(values, dtype="float64")
    if not v.notna().any():
        return None
    return v.ffill().bfill().to_numpy(dtype="float64")


# COMMAND ----------

# DBTITLE 1,Forecast plan -- the context of a row ends at its time minus the gap


def plan_forecasts(known, wanted, *, gap_steps, block, context, min_values=8):
    """One forecast item per series and origin.

    known: sid, k, y (realised values); wanted: sid, k, pos (rows to forecast, k on
    the grid). A row at k gets the context that ends at its block start minus
    gap_steps, so no value after k - gap_steps reaches the model. Returns the item
    contexts (None where too little is known), the item and step of each row, the
    horizon and the number of rows that break the rule (always 0 by construction).
    """
    horizon = gap_steps + block - 1
    known = known.dropna(subset=["k"]).drop_duplicates(["sid", "k"])
    known = known.sort_values(["sid", "k"])
    arrays = {}
    for sid, grp in known.groupby("sid", sort=False):
        k = grp["k"].to_numpy(dtype="int64")
        dense = np.full(int(k[-1] - k[0]) + 1, np.nan)
        dense[k - k[0]] = grp["y"].to_numpy(dtype="float64")
        arrays[sid] = (int(k[0]), dense)
    work = wanted.dropna(subset=["k"]).copy()
    work["k"] = work["k"].astype("int64")
    work["origin"] = (work["k"] // block) * block - gap_steps
    work["step"] = work["k"] - work["origin"]
    groups = work.groupby(["sid", "origin"], sort=False)
    work["item"] = groups.ngroup()
    firsts = groups.head(1)
    contexts = []
    for sid, origin in zip(firsts["sid"], firsts["origin"], strict=True):
        found = arrays.get(sid)
        if found is None:
            contexts.append(None)
            continue
        k0, dense = found
        end = int(origin) - k0
        if end < 0:
            contexts.append(None)
            continue
        start = max(0, end - context + 1)
        window = np.full(end - start + 1, np.nan)
        hi = min(end, len(dense) - 1)
        if hi >= start:
            window[: hi - start + 1] = dense[start : hi + 1]
        usable = np.isfinite(window).sum() >= min_values
        contexts.append(window if usable else None)
    violations = int((work["origin"] > work["k"] - gap_steps).sum())
    return {
        "contexts": contexts,
        "pos": work["pos"].to_numpy(dtype="int64"),
        "item": work["item"].to_numpy(dtype="int64"),
        "step": work["step"].to_numpy(dtype="int64"),
        "horizon": horizon,
        "violations": violations,
    }


def assemble_forecasts(plan, forecasts, n_rows, levels) -> dict:
    """Per quantile level, one value per frame row (NaN where no context)."""
    out = {q: np.full(n_rows, np.nan) for q in levels}
    valid = np.array([c is not None for c in plan["contexts"]], dtype=bool)
    rows = valid[plan["item"]] if len(valid) else np.zeros(0, dtype=bool)
    for q in levels:
        table = np.full((len(plan["contexts"]), plan["horizon"]), np.nan)
        table[valid] = forecasts[q]
        out[q][plan["pos"][rows]] = table[plan["item"][rows], plan["step"][rows] - 1]
    return out


# COMMAND ----------

# DBTITLE 1,Forecast backends (each imports its library when loaded)


def _device() -> str:
    import torch

    return "cuda" if torch.cuda.is_available() else "cpu"


class Chronos2Backend:
    name = "chronos2"
    has_quantiles = True
    max_horizon = 1024
    chunk_items = 1000

    def __init__(self, weights_dir, adapter_dir=None):
        self.weights_dir, self.adapter_dir = weights_dir, adapter_dir
        self.pipeline = None
        self.device = None

    def load(self):
        if self.pipeline is None:
            from chronos import Chronos2Pipeline

            self.device = _device()
            self.pipeline = Chronos2Pipeline.from_pretrained(
                self.adapter_dir or self.weights_dir, device_map=self.device
            )
        return self.pipeline

    def forecast(self, contexts, horizon, levels, seconds):
        pipeline = self.load()
        out = {q: np.empty((len(contexts), horizon)) for q in levels}
        step = pd.Timedelta(seconds=seconds)
        for start in range(0, len(contexts), self.chunk_items):
            part = contexts[start : start + self.chunk_items]
            frames = [
                pd.DataFrame(
                    {
                        "id": i,
                        "timestamp": pd.date_range(
                            "2000-01-01", periods=len(c), freq=step
                        ),
                        "target": c,
                    }
                )
                for i, c in enumerate(part)
            ]
            result = pipeline.predict_df(
                pd.concat(frames, ignore_index=True),
                prediction_length=horizon,
                quantile_levels=list(levels),
                id_column="id",
                timestamp_column="timestamp",
                target="target",
            ).sort_values(["id", "timestamp"])
            for q in levels:
                column = next(
                    c for c in (str(q), q, f"{q:g}", "predictions") if c in result
                )
                values = result[column].to_numpy(dtype="float64")
                out[q][start : start + len(part)] = values.reshape(len(part), horizon)
        return out


class TimesFMBackend:
    name = "timesfm25"
    has_quantiles = True
    max_horizon = 1024

    def __init__(self, weights_dir, adapter_dir=None):
        self.weights_dir = weights_dir
        self.model = None
        self.compiled = (0, 0)
        self.device = None

    def load(self, horizon, context):
        import timesfm

        if self.model is None:
            self.device = _device()
            self.model = timesfm.TimesFM_2p5_200M_torch.from_pretrained(
                self.weights_dir
            )
        if horizon > self.compiled[0] or context > self.compiled[1]:
            wide = -(-max(context, 32) // 32) * 32
            self.compiled = (
                max(horizon, self.compiled[0]),
                max(wide, self.compiled[1]),
            )
            self.model.compile(
                timesfm.ForecastConfig(
                    max_context=self.compiled[1],
                    max_horizon=self.compiled[0],
                    normalize_inputs=True,
                    use_continuous_quantile_head=True,
                    force_flip_invariance=True,
                    infer_is_positive=False,
                    fix_quantile_crossing=True,
                )
            )
        return self.model

    def forecast(self, contexts, horizon, levels, seconds):
        filled = [fill_gaps(c) for c in contexts]
        model = self.load(horizon, max(len(c) for c in filled))
        _, quant = model.forecast(horizon=horizon, inputs=filled)
        return {q: np.asarray(quant)[:, :horizon, round(q * 10)] for q in levels}


class TTMBackend:
    name = "ttm_r2"
    has_quantiles = False
    max_horizon = 720
    context_length = 512
    chunk_items = 2048

    def __init__(self, weights_dir, adapter_dir=None):
        self.weights_dir = weights_dir
        self.model = None
        self.device = None

    def load(self):
        if self.model is None:
            from tsfm_public import TinyTimeMixerForPrediction

            self.device = _device()
            self.model = TinyTimeMixerForPrediction.from_pretrained(
                self.weights_dir
            ).to(self.device)
            self.model.eval()
        return self.model

    def _pad(self, values):
        v = fill_gaps(values)[-self.context_length :]
        if len(v) < self.context_length:
            v = np.concatenate([np.full(self.context_length - len(v), v[0]), v])
        return v

    def forecast(self, contexts, horizon, levels, seconds):
        import torch

        model = self.load()
        out = np.empty((len(contexts), horizon))
        for start in range(0, len(contexts), self.chunk_items):
            chunk = contexts[start : start + self.chunk_items]
            part = np.stack([self._pad(c) for c in chunk])
            tensor = torch.tensor(part, dtype=torch.float32, device=self.device)
            with torch.no_grad():
                pred = model(past_values=tensor[:, :, None]).prediction_outputs
            out[start : start + len(part)] = pred[:, :horizon, 0].cpu().numpy()
        return {0.5: out}


class EnergyTTMBackend(TTMBackend):
    """Granite TTM adapted to hourly electricity load: 168 values in, 24 out."""

    name = "energy_ttm"
    max_horizon = 24
    context_length = 168

    def load(self):
        if self.model is None:
            from tsfm_public import TinyTimeMixerForPrediction

            self.device = _device()
            self.model = TinyTimeMixerForPrediction.from_pretrained(
                self.weights_dir, num_input_channels=1
            ).to(self.device)
            self.model.eval()
        return self.model


class TotoBackend:
    """Toto 2.0: nine quantiles (0.1 to 0.9) for each series; a missing value is
    masked, and a short context is padded on the left."""

    name = "toto2"
    has_quantiles = True
    max_horizon = 4096
    chunk_items = 256
    decode_block_size = 768

    def __init__(self, weights_dir, adapter_dir=None):
        self.weights_dir = weights_dir
        self.model = None
        self.device = None

    def load(self):
        if self.model is None:
            from toto2 import Toto2Model

            self.device = _device()
            self.model = Toto2Model.from_pretrained(self.weights_dir)
            self.model = self.model.to(self.device).eval()
        return self.model

    def forecast(self, contexts, horizon, levels, seconds):
        import torch

        model = self.load()
        out = {q: np.empty((len(contexts), horizon)) for q in levels}
        for start in range(0, len(contexts), self.chunk_items):
            part = contexts[start : start + self.chunk_items]
            width = max(len(c) for c in part)
            values = np.zeros((len(part), 1, width), dtype="float32")
            seen = np.zeros((len(part), 1, width), dtype=bool)
            for i, c in enumerate(part):
                v = np.asarray(c, dtype="float64")
                ok = np.isfinite(v)
                values[i, 0, width - len(c) :] = np.where(ok, v, 0.0)
                seen[i, 0, width - len(c) :] = ok
            batch = {
                "target": torch.tensor(values, device=self.device),
                "target_mask": torch.tensor(seen, device=self.device),
                "series_ids": torch.zeros(
                    len(part), 1, dtype=torch.long, device=self.device
                ),
            }
            with torch.no_grad():
                quant = model.forecast(
                    batch,
                    horizon=horizon,
                    decode_block_size=self.decode_block_size,
                    has_missing_values=True,
                )
            quant = quant.float().cpu().numpy()
            for q in levels:
                out[q][start : start + len(part)] = quant[round(q * 10) - 1, :, 0, :]
        return out


BACKENDS = {
    "chronos2": Chronos2Backend,
    "timesfm25": TimesFMBackend,
    "ttm_r2": TTMBackend,
    "toto2": TotoBackend,
    "energy_ttm": EnergyTTMBackend,
}

# COMMAND ----------

# DBTITLE 1,History of the frozen dataset (train and validation rows only)


def read_history(table, version, columns, partitions=("train", "validation")):
    """Series rows of the frozen dataset. Only train and validation rows can be read;
    nothing is sampled."""
    if not set(partitions) <= {"train", "validation"}:
        raise RuntimeError(f"history may read train and validation, not {partitions}")
    from pyspark.sql import SparkSession

    quoted = ", ".join(f"'{p}'" for p in partitions)
    session = SparkSession.builder.getOrCreate()
    rows = session.sql(
        f"SELECT {', '.join(columns)} FROM {table} VERSION AS OF {int(version)} "
        f"WHERE partition IN ({quoted})"
    ).collect()
    return pd.DataFrame.from_records(rows, columns=columns)


# COMMAND ----------

# DBTITLE 1,Reference bundle -- what is stored for a foundation candidate


class ForecastReference:
    """Stored in place of a fitted model: the model identity (repository, revision,
    file hashes, licence, adapter) and the forecast rules. predict(frame) reads the
    history of the frozen dataset, adds the realised values already in the frame and
    forecasts every row from a context that ends at its time minus the gap."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.task = "quantile" if cfg["has_quantiles"] else "regression"
        self.last_report: dict = {}
        self._backend = None
        self._history = None

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_backend"] = None
        state["_history"] = None
        return state

    def history(self):
        if self._history is None:
            c = self.cfg
            columns = list(dict.fromkeys([*c["series"], c["time"], c["target"]]))
            self._history = read_history(
                c["history_table"], c["frozen_version"], columns
            )
        return self._history

    def backend(self):
        if self._backend is None:
            c = self.cfg
            self._backend = BACKENDS[c["backend"]](
                c["weights_dir"], c.get("adapter_dir")
            )
        return self._backend

    def _groups(self, frame):
        c = self.cfg
        if c["seconds"]:
            return [(int(c["seconds"]), np.arange(len(frame)))]
        values = pd.to_numeric(frame[c["seconds_col"]], errors="coerce")
        return [
            (int(s), np.flatnonzero((values == s).to_numpy()))
            for s in sorted(values.dropna().unique())
        ]

    def _known(self, frame, hist, seconds):
        c = self.cfg
        parts = []
        for part in (hist, frame):
            if not c["seconds"]:
                part = part[
                    pd.to_numeric(part[c["seconds_col"]], errors="coerce") == seconds
                ]
            parts.append(
                pd.DataFrame(
                    {
                        "sid": series_ids(part, c["series"]).to_numpy(),
                        "k": grid_index(part[c["time"]], seconds),
                        "y": pd.to_numeric(
                            part[c["target"]], errors="coerce"
                        ).to_numpy(),
                    }
                ).dropna(subset=["y"])
            )
        return pd.concat(parts, ignore_index=True)

    def predict(self, frame, history=None):
        c = self.cfg
        frame = frame.reset_index(drop=True)
        hist = self.history() if history is None else history
        levels = tuple(c["quantiles"])
        out = {q: np.full(len(frame), np.nan) for q in levels}
        report = {"items": 0, "rows_without_context": 0, "rule_violations": 0}
        for seconds, index in self._groups(frame):
            sub = frame.iloc[index]
            per_day = max(1, 86400 // seconds)
            plan = plan_forecasts(
                self._known(sub, hist, seconds),
                pd.DataFrame(
                    {
                        "sid": series_ids(sub, c["series"]).to_numpy(),
                        "k": grid_index(sub[c["time"]], seconds),
                        "pos": index,
                    }
                ),
                gap_steps=c["gap_days"] * per_day,
                block=per_day,
                context=c["context"],
                min_values=MIN_CONTEXT_VALUES,
            )
            backend = self.backend()
            if plan["horizon"] > backend.max_horizon:
                raise ValueError(
                    f"horizon of {plan['horizon']} steps is over the limit "
                    f"{backend.max_horizon} of {backend.name}"
                )
            valid = [x for x in plan["contexts"] if x is not None]
            if valid:
                got = backend.forecast(valid, plan["horizon"], levels, seconds)
            else:
                got = {q: np.empty((0, plan["horizon"])) for q in levels}
            part = assemble_forecasts(plan, got, len(frame), levels)
            for q in levels:
                fresh = np.isfinite(part[q])
                out[q][fresh] = part[q][fresh]
            report["items"] += len(valid)
            report["rule_violations"] += plan["violations"]
        report["rows_without_context"] = int(np.isnan(out[levels[0]]).sum())
        self.last_report = report
        if self.task == "quantile":
            stack = np.column_stack([out[q] for q in sorted(out)])
            done = np.isfinite(stack).all(axis=1)
            stack[done] = np.sort(stack[done], axis=1)
            return dict(zip(sorted(out), stack.T, strict=True))
        return out[0.5]


# COMMAND ----------

# DBTITLE 1,Score one reference bundle on the validation rows


def score_reference(bundle, frame, y, base_pred, qbase_pred, price) -> dict:
    """The metrics of the accepted candidates for a foundation candidate, plus what
    the forecast rules did."""
    pred = bundle.predict(frame)
    if isinstance(pred, dict):
        metrics = quantile_metrics(y, pred, qbase_pred)
        metrics.update(regression_metrics(y, pred[0.5], base_pred))
    else:
        metrics = regression_metrics(y, pred, base_pred)
        if price:
            metrics["pinball_q50"] = pinball(y, pred, 0.5)
    report = bundle.last_report
    metrics["forecast_items"] = float(report["items"])
    metrics["rows_without_context"] = float(report["rows_without_context"])
    metrics["context_rule_violations"] = float(report["rule_violations"])
    return metrics


# COMMAND ----------

# DBTITLE 1,Chronos-2 LoRA fine-tuning on the train partition


def training_arrays(train_rows, cfg, seconds, max_series) -> list:
    """Dense train-only target arrays, one per series, longest first."""
    sid = series_ids(train_rows, cfg["series"]).to_numpy()
    known = pd.DataFrame(
        {
            "sid": sid,
            "k": grid_index(train_rows[cfg["time"]], seconds),
            "y": pd.to_numeric(train_rows[cfg["spec"]["target"]], errors="coerce"),
        }
    ).dropna()
    arrays = []
    for _, grp in known.sort_values(["sid", "k"]).groupby("sid", sort=False):
        k = grp["k"].to_numpy(dtype="int64")
        dense = np.full(int(k[-1] - k[0]) + 1, np.nan)
        dense[k - k[0]] = grp["y"].to_numpy(dtype="float64")
        if np.isfinite(dense).sum() >= 4 * MIN_CONTEXT_VALUES:
            arrays.append(dense.astype("float32"))
    arrays.sort(key=len, reverse=True)
    return arrays[:max_series]


def tune_on_train(ctx, cfg, weights_dir, settings, adapter_dir) -> None:
    """Read the train rows of the task and fine-tune Chronos-2 on them."""
    columns = list(dict.fromkeys([*cfg["series"], cfg["time"], cfg["spec"]["target"]]))
    rows = read_history(
        ml_fqn(f"dataset_{ctx.dataset_id}", ctx.ecosystem),
        ctx.frozen_version,
        columns,
        ("train",),
    )
    seconds = cfg["seconds"]
    if not seconds:
        found = pd.to_numeric(rows[cfg["seconds_col"]], errors="coerce")
        seconds = int(found.mode().iloc[0])
        rows = rows[found == seconds]
    horizon = cfg["gap_days"] * max(1, 86400 // seconds)
    arrays = training_arrays(rows, cfg, seconds, settings["max_series"])
    tune_chronos2(weights_dir, arrays, horizon, settings, adapter_dir)


def tune_chronos2(weights_dir, arrays, horizon, settings, adapter_dir) -> None:
    """LoRA fine-tune of Chronos-2 on `arrays`; the adapter model is saved."""
    import torch
    from chronos import Chronos2Pipeline

    torch.manual_seed(MODEL_SEED)
    pipeline = Chronos2Pipeline.from_pretrained(weights_dir, device_map=_device())
    with _tempfile.TemporaryDirectory() as work:
        tuned = pipeline.fit(
            inputs=[{"target": a} for a in arrays],
            prediction_length=horizon,
            finetune_mode="lora",
            context_length=settings["context_length"],
            learning_rate=settings["learning_rate"],
            num_steps=settings["num_steps"],
            batch_size=settings["batch_size"],
            output_dir=work,
        )
        os.makedirs(adapter_dir, exist_ok=True)
        tuned.save_pretrained(adapter_dir)


# COMMAND ----------

# DBTITLE 1,Reference bundle settings of one candidate


def reference_config(ctx, cfg, entry, backend, mode, adapter_dir=None) -> dict:
    cls = BACKENDS[backend]
    return {
        "task_id": ctx.task_id,
        "backend": backend,
        "mode": mode,
        "model_key": entry["model_key"],
        "repo_id": entry["repo_id"],
        "revision": entry["revision"],
        "licence": entry["licence"],
        "files": entry["files"],
        "weights_dir": entry["dir"],
        "adapter_dir": adapter_dir,
        "series": cfg["series"],
        "time": cfg["time"],
        "target": cfg["spec"]["target"],
        "seconds": cfg["seconds"],
        "seconds_col": cfg.get("seconds_col"),
        "gap_days": cfg["gap_days"],
        "context": cfg["context"],
        "history_table": ml_fqn(f"dataset_{ctx.dataset_id}", ctx.ecosystem),
        "frozen_version": int(ctx.frozen_version),
        "has_quantiles": bool(cls.has_quantiles and cfg["spec"].get("price")),
        "quantiles": list(QUANTILE_LEVELS if cls.has_quantiles else (0.5,)),
    }


def complete_load_features(frame, ctx):
    """Calendar columns for every market area and realised load two, seven and
    fourteen days back, as the load task of the accepted models has them."""
    keys = ["market_area_code", "local_date"]
    series = ["market_area_code", "load_kind"]
    calendar_columns = [
        "day_of_week",
        "is_weekend",
        "month",
        "day_of_year",
        "iso_week",
        "year",
        "is_holiday",
        "is_bridge_day",
        "local_day_hours",
    ]
    have = [c for c in calendar_columns if c in frame.columns]
    calendar = read_ml("features_calendar_market_area", ecosystem=ctx.ecosystem)
    out = frame.drop(*have).join(calendar.select(*keys, *have), keys, "left")
    realised = frame.select(
        *series, "local_date", F.col("target_value_mwh").alias("_realised")
    )
    for n in (2, 7, 14):
        shifted = realised.select(
            *series,
            F.date_add("local_date", n).alias("local_date"),
            F.col("_realised").alias(f"realised_lag{n}"),
        )
        out = out.join(shifted, [*series, "local_date"], "left")
    return out


# COMMAND ----------

# DBTITLE 1,Runner -- foundation candidates of one time-series task


def finish_foundation_task(ctx) -> None:
    """Show what was recorded; a task whose candidates all failed is reported by the
    gate, not stopped here."""
    mine = read_model(FOUNDATION_RESULTS, ecosystem=ctx.ecosystem).filter(
        (F.col("run_id") == ctx.rid) & (F.col("task_id") == ctx.task_id)
    )
    mine.filter(
        F.col("metric").isNull() | (F.col("metric") == ctx.primary_metric)
    ).select("model_name", "status", "metric", "value").show(20, truncate=False)
    recorded = mine.limit(1).count() > 0
    record_task_context(ctx, "finished", detail=f"recorded={recorded}")
    check(
        ctx.component,
        "models",
        "foundation_candidates_recorded",
        recorded,
        detail=f"task={ctx.task_id}",
        rid=ctx.rid,
    )


def run_foundation_ts(ctx, df, models=None) -> None:
    """Foundation candidates on the validation rows of one task, scored with the
    same metrics, baseline and rows as the accepted candidates."""
    cfg = TS_TASKS[ctx.task_id]
    spec = cfg["spec"]
    if cfg.get("complete_load_features"):
        df = complete_load_features(df, ctx)
    pdf, _, fraction = _prepare(ctx, df, spec)
    tr_m, va_m = _masks(pdf)
    y = pd.to_numeric(pdf[spec["target"]], errors="coerce").to_numpy(dtype="float64")
    note_target(y, tr_m, va_m, "regression")
    start_task(ctx)
    base = fit_baseline(pdf[tr_m], spec["target"], spec["baseline"])
    base_pred = base.predict(pdf[va_m])
    price = bool(spec.get("price"))
    qbase_pred = None
    if price:
        qbase = fit_quantile_baseline(
            pdf[tr_m], spec["target"], QUANTILES, spec.get("quantile_cols", [])
        )
        qbase_pred = qbase.predict(pdf[va_m])
    frame_va = pdf[va_m].reset_index(drop=True)
    chosen = [
        c
        for c in CANDIDATES
        if (models is None or c[0] in models)
        and ctx.task_id in CANDIDATE_TASKS.get(c[0], (ctx.task_id,))
    ]
    for name, backend, library, lora in chosen:
        params = {"row_fraction": round(fraction, 4)}

        def fit(backend=backend, lora=lora, params=params):
            entry = model_entry(backend)
            adapter = None
            if lora:
                adapter = f"{foundation_root()}/adapters/{ctx.task_id}/{ctx.rid}"
                settings = dict(LORA_SETTINGS)
                if ctx.smoke:
                    settings["num_steps"] = LORA_SMOKE_STEPS
                tune_on_train(ctx, cfg, entry["dir"], settings, adapter)
                params.update({f"lora_{k}": v for k, v in settings.items()})
            bundle = ForecastReference(
                reference_config(
                    ctx, cfg, entry, backend, "lora" if lora else "zero_shot", adapter
                )
            )
            metrics = score_reference(
                bundle, frame_va, y[va_m], base_pred, qbase_pred, price
            )
            params.update(
                {
                    "model_key": entry["model_key"],
                    "repo_id": entry["repo_id"],
                    "revision": entry["revision"],
                    "licence": entry["licence"],
                    "adapter_dir": adapter or "",
                    "mode": bundle.cfg["mode"],
                    "context": cfg["context"],
                    "gap_days": cfg["gap_days"],
                    "seed": MODEL_SEED,
                }
            )
            return bundle, metrics, int(va_m.sum())

        run_candidate(
            ctx,
            name,
            FOUNDATION_FAMILY,
            fit,
            requires=(library,),
            params=params,
        )
    finish_foundation_task(ctx)


# COMMAND ----------

# DBTITLE 1,Findings markdown of the foundation candidates (pure text, no Spark)


def render_foundation_findings(
    ecosystem,
    results,
    selection,
    context,
    register,
    checks,
    *,
    smoke,
    stamp,
    task_ids=None,
) -> str:
    """One markdown document: summary, the models with their revision and licence,
    run context, flags, check results, then per task every candidate beside the
    baseline of the accepted models. `results` holds the foundation rows and the
    baseline rows of the accepted candidates."""
    wanted = set(task_ids or foundation_task_ids())
    tasks = [t for t in TASKS if t["ecosystem"] == ecosystem and t["task_id"] in wanted]
    by_context = _context_by_task(context, smoke)
    kind = "SMOKE RUN" if smoke else "RUN"
    out = [
        f"# {ecosystem.upper()} FOUNDATION {kind} FINDINGS",
        "",
        (
            "_Auto-generated by `databricks/models/gate/06_foundation_findings`. "
            f"Validation partition only. Generated: {stamp}_"
        ),
        "",
    ]
    summary = []
    for t in tasks:
        rows = [r for r in results if r["task_id"] == t["task_id"]]
        found = {}
        for r in rows:
            if r["stage"] != "baseline":
                found.setdefault(r["model_name"], r)
        ok = sum(r["status"] == "ok" for r in found.values())
        summary.append((t["task_id"], len(found), ok, len(found) - ok))
    out += [
        "## summary",
        "",
        markdown_table(
            ["task", "foundation candidates", "ok", "failed or skipped"], summary
        ),
        "",
        "## models",
        "",
        markdown_table(
            ["model", "repository", "revision", "licence", "restriction"],
            [
                (
                    m["model_key"],
                    m["repo_id"],
                    str(m["revision"])[:12],
                    m["licence"],
                    m["licence_restriction"],
                )
                for m in register
            ],
        ),
        "",
    ]
    flags = findings_flags(tasks, results, by_context)
    out += ["## flags", ""]
    if flags:
        out += [markdown_table(["task", "model", "flag", "detail"], flags), ""]
    else:
        out += ["_no automatic flags raised_", ""]
    if checks:
        latest = {}
        for c in sorted(checks, key=lambda c: str(c["recorded_at"])):
            latest[(c["component"], c["metric_name"])] = c
        out += [
            "## check results",
            "",
            markdown_table(
                ["component", "check", "status", "detail", "recorded"],
                [
                    (
                        c["component"],
                        c["metric_name"],
                        c["status"],
                        c["error_detail"],
                        str(c["recorded_at"])[:19],
                    )
                    for c in latest.values()
                ],
            ),
            "",
        ]
    for t in tasks:
        out += _foundation_task_section(t, results, selection, by_context)
    return scrub_private("\n".join(out))


def _foundation_task_section(t, results, selection, by_context) -> list:
    rows = [r for r in results if r["task_id"] == t["task_id"]]
    out = [f"## {t['task_id']}", ""]
    if not rows:
        return [*out, "_no results recorded_", ""]
    out += [
        (
            f"_dataset `{t['dataset_id']}`, frozen version "
            f"{rows[0]['frozen_delta_version']}, primary metric "
            f"`{t['primary_metric']}` "
            f"({'higher' if t['higher_is_better'] else 'lower'} is better)_"
        ),
        "",
    ]
    notes = _data_note_lines(by_context.get(t["task_id"], []))
    if notes:
        out += ["### data seen by the models", "", *notes, ""]
    models = {}
    for r in rows:
        models.setdefault(r["model_name"], []).append(r)
    order = sorted(models, key=lambda m: (models[m][0]["stage"] != "baseline", m))
    table = []
    for m in order:
        first = models[m][0]
        primary = next(
            (r["value"] for r in models[m] if r["metric"] == t["primary_metric"]),
            None,
        )
        table.append(
            (
                m,
                first["family"],
                first["stage"],
                first["status"],
                first["artifact_status"],
                first["n_rows"],
                _number(primary),
                first["detail"] if first["status"] != "ok" else "",
            )
        )
    heads = ["model", "family", "stage", "status", "artifact", "rows"]
    out += [
        "### candidates",
        "",
        markdown_table([*heads, t["primary_metric"], "detail"], table),
        "",
    ]
    ok = [m for m in order if models[m][0]["status"] == "ok"]
    names = list(
        dict.fromkeys(r["metric"] for m in ok for r in models[m] if r["metric"])
    )
    if ok and names:
        grid = []
        for name in names:
            line = [name]
            for m in ok:
                v = next((r["value"] for r in models[m] if r["metric"] == name), None)
                line.append(_number(v))
            grid.append(line)
        out += [
            "### metrics (validation partition)",
            "",
            markdown_table(["metric", *ok], grid),
            "",
        ]
    chosen = [s for s in selection if s["task_id"] == t["task_id"]]
    if chosen:
        out += [
            "### forwarded for test evaluation",
            "",
            markdown_table(
                ["rank", "model", t["primary_metric"]],
                [
                    (s["rank"], s["model_name"], _number(s["primary_value"]))
                    for s in sorted(chosen, key=lambda s: s["rank"])
                ],
            ),
            "",
        ]
    return out


# COMMAND ----------

# DBTITLE 1,Findings section of one foundation notebook (src/findings/model_findings)

_FINDINGS_FIELDS = (
    "task_id",
    "model_name",
    "family",
    "stage",
    "status",
    "metric",
    "value",
    "n_rows",
    "detail",
    "frozen_delta_version",
    "artifact_status",
    "run_id",
    "run_at",
    "smoke",
)


def _section_body(text: str) -> str:
    """The rendered document without its title lines, headings one level lower."""
    body = text.split("\n", 4)[4] if text.count("\n") >= 4 else text
    return "\n".join("#" + ln if ln.startswith("#") else ln for ln in body.splitlines())


def write_foundation_findings(
    ecosystem, notebook_key, title, task_ids, run_id, smoke
) -> None:
    """Write the findings of this notebook run as one section of
    foundation_<ecosystem>_notebooks.md (smoke runs: ..._notebooks_smoke.md)."""
    ids = list(task_ids)
    fields = list(_FINDINGS_FIELDS)
    base = F.col("task_id").isin(*ids) & (F.col("smoke") == smoke)
    results = [
        r.asDict()
        for r in read_model(FOUNDATION_RESULTS, ecosystem=ecosystem)
        .filter((F.col("run_id") == run_id) & base)
        .select(*fields)
        .collect()
    ]
    results += [
        r.asDict()
        for r in read_model("candidate_results", ecosystem=ecosystem)
        .filter((F.col("stage") == "baseline") & base)
        .select(*fields)
        .collect()
    ]
    context = [
        r.asDict()
        for r in read_model(FOUNDATION_CONTEXT, ecosystem=ecosystem)
        .filter((F.col("run_id") == run_id) & F.col("task_id").isin(*ids))
        .collect()
    ]
    register = [
        r.asDict()
        for r in read_model(FOUNDATION_REGISTER, ecosystem=ecosystem).collect()
    ]
    now = _dt.datetime.now(tz=_dt.UTC)
    text = render_foundation_findings(
        ecosystem,
        results,
        [],
        context,
        register,
        [],
        smoke=smoke,
        stamp=now.strftime("%Y-%m-%dT%H:%MZ"),
        task_ids=ids,
    )
    write_findings_section(
        f"foundation_{ecosystem}_notebooks",
        f"{ecosystem.upper()} FOUNDATION NOTEBOOK FINDINGS",
        notebook_key,
        title,
        _section_body(text),
        smoke,
        now,
    )
