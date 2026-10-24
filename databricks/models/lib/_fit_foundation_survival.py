# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION SURVIVAL LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** in-context survival models (SurvPFN, and a discrete-time hazard
# MAGIC model on the TabICL classifier) as extra candidates for the unit lifetime
# MAGIC task, pulled in with `%run ../lib/_fit_foundation_survival` after
# MAGIC `_fit_foundation_tab` and `_fit_survival`. The context rows are train rows only
# MAGIC and are stored in the bundle. Like the forest and the boosted model, neither
# MAGIC uses the delayed entry time.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Candidates, limits and the task setting
SURV_FAMILY = "foundation_surv"
SURVPFN_MAX_ROWS = 5_000
SURVPFN_FILE = "survpfn_nr.pt"
SURV_PREDICT_BATCH = 2_000
DISCRETE_MAX_ROWS = 50_000
DISCRETE_INTERVALS = max(SURVIVAL_HORIZONS_YEARS)
SURV_CANDIDATES = (
    ("survpfn_in_context", "survpfn", "sksurv"),
    ("tabicl_discrete_in_context", "tabicl_discrete", "tabicl"),
)
SURVIVAL_TASKS = {
    "survival.unit_lifetime": {
        "spec": {
            "duration_col": "target_duration_years",
            "event_col": "target_event",
            "entry_col": "entry_years",
            "key_cols": ["unit_id"],
            "group_col": "is_offshore",
            "diagnostic_feature": "commissioning_year",
            "drop": ["entry_years", "left_truncated", "entry_date"],
        }
    }
}

# COMMAND ----------

# DBTITLE 1,Survival curves read at the horizons


def step_value(fn, t: float) -> float:
    """A scikit-survival step function at t; 1 before its first time and the last
    value after its last time."""
    lo, hi = fn.domain
    return 1.0 if t < lo else float(fn(min(t, hi)))


def person_period(x, duration, event, n_intervals):
    """Person-interval rows for a discrete-time hazard model: the features, the
    interval index and a label that is 1 when the event falls in that interval. A
    unit that is censored inside an interval gives no row for it."""
    rows, index, label = [], [], []
    for k in range(n_intervals):
        lived = duration >= k + 1
        died = (duration >= k) & (duration < k + 1) & (event > 0)
        take = lived | died
        rows.append(x[take])
        index.append(np.full(int(take.sum()), k))
        label.append(died[take].astype("int64"))
    return np.vstack(rows), np.concatenate(index), np.concatenate(label)


def in_batches(n: int, size: int = SURV_PREDICT_BATCH):
    return [slice(s, min(s + size, n)) for s in range(0, n, size)]


# COMMAND ----------

# DBTITLE 1,Bundles -- the train rows used as context and the model identity


class SurvPFNBundle:
    """SurvPFN with its train-row context. The model is built when it is first used:
    the vendored code folder goes on the import path and the checkpoint is read."""

    def __init__(self, cfg: dict, encoder, x_context, duration, event):
        self.cfg = cfg
        self.encoder = encoder
        self.x = np.asarray(x_context, dtype="float32")
        self.duration = np.asarray(duration, dtype="float64")
        self.event = np.asarray(event, dtype="float64")
        self._model = None

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_model"] = None
        return state

    def model(self):
        if self._model is None:
            import sys

            from sksurv.util import Surv

            if self.cfg["code_dir"] not in sys.path:
                sys.path.insert(0, self.cfg["code_dir"])
            from src.SurvPFN import SurvPFN

            with timed_stage(f"load SurvPFN checkpoint on {device_label()}"):
                model = SurvPFN(model_path=self.cfg["model_path"])
            target = Surv.from_arrays(event=self.event > 0, time=self.duration)
            with timed_stage(f"fit {len(self.x):,} context rows"):
                model.fit(self.x, target)
            self._model = model
        return self._model

    def risk(self, frame):
        x = self.encoder.transform(frame, fill=True)
        model = self.model()
        batches = in_batches(len(x))
        parts = []
        with inference_mode():
            for number, b in enumerate(batches, start=1):
                with timed_stage(f"SurvPFN risk batch {number}/{len(batches)}"):
                    parts.append(model.predict(x[b]))
        return np.concatenate(parts).astype("float64") if parts else np.zeros(0)

    def survival(self, frame):
        x = self.encoder.transform(frame, fill=True)
        model = self.model()
        out = np.ones((len(x), len(SURVIVAL_HORIZONS_YEARS)))
        batches = in_batches(len(x))
        with inference_mode():
            for number, b in enumerate(batches, start=1):
                with timed_stage(f"SurvPFN curve batch {number}/{len(batches)}"):
                    curves = model.predict_survival_function(x[b])
                out[b] = [
                    [step_value(fn, float(h)) for h in SURVIVAL_HORIZONS_YEARS]
                    for fn in curves
                ]
        return out


class DiscreteSurvivalBundle:
    """Discrete-time survival on an in-context classifier: one hazard per year from
    a classifier fitted on person-interval rows of the train context."""

    def __init__(self, cfg: dict, encoder, x_context, label):
        self.cfg = cfg
        self.encoder = encoder
        self.x = np.asarray(x_context, dtype="float32")
        self.label = np.asarray(label, dtype="int64")
        self._estimator = None

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_estimator"] = None
        return state

    def estimator(self):
        if self._estimator is None:
            self._estimator = fit_estimator(self.cfg, self.x, self.label)
        return self._estimator

    def hazards(self, frame):
        x = self.encoder.transform(frame, fill=True)
        estimator = self.estimator()
        column = list(estimator.classes_).index(1)
        out = np.zeros((len(x), DISCRETE_INTERVALS))
        size = min(SURV_PREDICT_BATCH, predict_batch(len(self.x)))
        batches = in_batches(len(x), size)
        with inference_mode():
            for k in range(DISCRETE_INTERVALS):
                with timed_stage(
                    f"hazard interval {k + 1}/{DISCRETE_INTERVALS}, "
                    f"{len(batches)} batches of {size:,}"
                ):
                    for b in batches:
                        part = np.column_stack([x[b], np.full(len(x[b]), k)])
                        out[b, k] = np.asarray(estimator.predict_proba(part))[:, column]
        return out

    def survival(self, frame):
        curve = np.cumprod(1.0 - self.hazards(frame), axis=1)
        return curve[:, [h - 1 for h in SURVIVAL_HORIZONS_YEARS]]

    def risk(self, frame):
        mid = len(SURVIVAL_HORIZONS_YEARS) // 2
        return 1.0 - self.survival(frame)[:, mid]


# COMMAND ----------

# DBTITLE 1,Identity of a survival candidate


def survival_identity(entry: dict, **extra) -> dict:
    return {
        "mode": "in_context",
        "model_key": entry["model_key"],
        "repo_id": entry["repo_id"],
        "revision": entry["revision"],
        "licence": entry["licence"],
        "files": entry["files"],
        **extra,
    }


def survival_params(cfg: dict, **extra) -> dict:
    keys = ("model_key", "repo_id", "revision", "licence", "mode")
    return {**{k: cfg[k] for k in keys}, **extra}


def fit_survpfn(enc, x_train, dur, ev):
    """(bundle, parameters) of SurvPFN with a seeded sample of the train rows."""
    weights, code = model_entry("survpfn_weights"), model_entry("survpfn_code")
    cfg = survival_identity(
        weights,
        backend="survpfn",
        model_path=f"{weights['dir']}/{SURVPFN_FILE}",
        code_dir=code["dir"],
        code_repo=code["repo_id"],
        code_revision=code["revision"],
    )
    keep = context_positions(len(x_train), SURVPFN_MAX_ROWS, MODEL_SEED)
    bundle = SurvPFNBundle(cfg, enc, x_train[keep], dur[keep], ev[keep])
    return bundle, survival_params(
        cfg, context_rows=len(keep), train_rows=len(x_train), entry="ignored"
    )


def fit_discrete(enc, x_train, dur, ev):
    """(bundle, parameters) of the discrete-time model on a seeded sample of units."""
    entry, path = tab_weights("tabicl_v2", "classification")
    cfg = survival_identity(
        entry, backend="tabicl_v2", kind="classification", model_path=path, fill=True
    )
    units = context_positions(
        len(x_train), DISCRETE_MAX_ROWS // DISCRETE_INTERVALS, MODEL_SEED
    )
    rows, index, label = person_period(
        x_train[units], dur[units], ev[units], DISCRETE_INTERVALS
    )
    if len(np.unique(label)) < 2:
        raise ValueError("the person-interval rows hold one class only")
    context = np.column_stack([rows, index])
    bundle = DiscreteSurvivalBundle(cfg, enc, context, label)
    return bundle, survival_params(
        cfg,
        context_units=len(units),
        context_rows=len(context),
        intervals=DISCRETE_INTERVALS,
        entry="ignored",
    )


# COMMAND ----------

# DBTITLE 1,Runner -- survival candidates on the validation rows


def run_foundation_survival(ctx, df, models=None) -> None:
    """SurvPFN and the discrete-time model on the validation rows of the unit
    lifetime task, with the rows and the features of the accepted survival models."""
    spec = SURVIVAL_TASKS[ctx.task_id]["spec"]
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
    enc = FeatureEncoder(feats).fit(train)
    keep = np.nanstd(enc.transform(train, fill=True), axis=0) > 0
    kept = [f for f, k in zip(feats, keep, strict=True) if k]
    enc_k = FeatureEncoder(kept).fit(train)
    x_train = enc_k.transform(train, fill=True)
    start_task(ctx)

    builders = {"survpfn": fit_survpfn, "tabicl_discrete": fit_discrete}
    chosen = [c for c in SURV_CANDIDATES if models is None or c[0] in models]
    for name, backend, library in chosen:
        params = {"row_fraction": round(fraction, 4)}

        def fit(backend=backend, params=params):
            bundle, extra = builders[backend](enc_k, x_train, dur[tr_m], ev[tr_m])
            params.update(extra, seed=MODEL_SEED)
            metrics = _score(
                dur[va_m], ev[va_m], bundle.risk(valid), bundle.survival(valid)
            )
            return bundle, metrics, int(va_m.sum())

        run_candidate(ctx, name, SURV_FAMILY, fit, requires=(library,), params=params)
    finish_foundation_task(ctx)
