# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION TABULAR LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** in-context tabular models (TabPFN v2, TabPFN-2.5, TabICL v2) as extra
# MAGIC candidates for the one-row-one-example tasks, pulled in with
# MAGIC `%run ../lib/_fit_foundation_tab` after `_fit_foundation_ts`. The context rows
# MAGIC come from the train partition only and are stored in the bundle, so the bundle
# MAGIC predicts without reading data.

# COMMAND ----------

# DBTITLE 1,Imports
import contextlib as _contextlib

import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Candidates, weight files and context limits
TAB_FAMILY = "foundation_tab"
PREDICT_BATCH = 10_000
# Context rows x batch rows per prediction call; a larger context gets smaller batches.
PREDICT_BUDGET = 100_000_000
MIN_PREDICT_BATCH = 1_000
TAB_CANDIDATES = (
    ("tabpfn_v2_in_context", "tabpfn_v2", "tabpfn"),
    ("tabpfn_25_in_context", "tabpfn_25", "tabpfn"),
    ("tabicl_v2_in_context", "tabicl_v2", "tabicl"),
)
# backend -> kind -> (manifest model key, checkpoint file)
TAB_WEIGHTS = {
    "tabpfn_v2": {
        "classification": ("tabpfn_v2_clf", "tabpfn-v2-classifier.ckpt"),
        "regression": ("tabpfn_v2_reg", "tabpfn-v2-regressor.ckpt"),
    },
    "tabpfn_25": {
        "classification": (
            "tabpfn_25",
            "tabpfn-v2.5-classifier-v2.5_default.ckpt",
        ),
        "regression": ("tabpfn_25", "tabpfn-v2.5-regressor-v2.5_default.ckpt"),
    },
    "tabicl_v2": {
        "classification": ("tabicl_v2", "tabicl-classifier-v2-20260212.ckpt"),
        "regression": ("tabicl_v2", "tabicl-regressor-v2-20260212.ckpt"),
    },
}
TAB_LIMITS = {
    "tabpfn_v2": {"max_rows": 10_000, "max_features": 500},
    "tabpfn_25": {"max_rows": 20_000, "max_features": 2000},
    "tabicl_v2": {"max_rows": 50_000, "max_features": None},
}
# TabPFN reads missing values itself; TabICL gets the train medians.
TAB_FILL = {"tabpfn_v2": False, "tabpfn_25": False, "tabicl_v2": True}

# COMMAND ----------

# DBTITLE 1,One-row-one-example tasks: kind, specification and the rows the accepted models use
TAB_TASKS = {
    "price_quarter_hour.negative_price": {
        "kind": "classification",
        "spec": {
            "target": "target_is_negative_price",
            "key_cols": ["market_area_code", "interval_start_utc"],
            "id_features": ["market_area_code"],
            "date_col": "local_date",
        },
    },
    "redispatch.any_event": {
        "kind": "classification",
        "spec": {
            "target": "target_any_event",
            "key_cols": ["market_area_code", "local_date"],
            "id_features": ["market_area_code"],
            "date_col": "local_date",
        },
    },
    "redispatch.event_energy": {
        "kind": "regression",
        "events_only": True,
        "spec": {
            "target": "target_log_event_energy_mwh",
            "key_cols": ["market_area_code", "local_date"],
            "id_features": ["market_area_code"],
            "baseline": {
                "kind": "group_mean",
                "cols": ["market_area_code"],
                "name": "zone_mean",
            },
            "date_col": "local_date",
        },
    },
    "ccpp.output": {
        "kind": "regression",
        "spec": {
            "target": "target_net_output_mw",
            "key_cols": ["sample_key"],
            "id_features": [],
            "baseline": {"kind": "train_mean", "name": "train_mean"},
        },
    },
    "capacity_additions.additions": {
        "kind": "regression",
        "spec": {
            "target": "target_capacity_added_mw",
            "key_cols": ["carrier_key", "commissioning_month"],
            "id_features": ["carrier_key"],
            "baseline": {
                "kind": "group_mean",
                "cols": ["carrier_key", "month"],
                "name": "seasonal_mean",
            },
            "date_col": "commissioning_month",
        },
    },
    "session_purchase_ga4.purchase": {
        "kind": "classification",
        "spec": {"target": "target_purchase_after_prefix", "key_cols": ["session_key"]},
    },
    "session_purchase_rees46.purchase": {
        "kind": "classification",
        "spec": {"target": "target_purchase_after_prefix", "key_cols": ["session_key"]},
    },
    "lapse_ga4.return": {
        "kind": "classification",
        "spec": {"target": "target_returned", "key_cols": ["user_id", "cutoff_date"]},
    },
    "lapse_rees46.return": {
        "kind": "classification",
        "spec": {"target": "target_returned", "key_cols": ["user_id", "cutoff_date"]},
    },
}

# COMMAND ----------

# DBTITLE 1,Context rows and the estimators


def build_estimator(cfg: dict):
    """The estimator of a bundle, with the checkpoint file taken from the volume; the
    library is imported here so that a missing one is a recorded skip."""
    classification = cfg["kind"] == "classification"
    device = _device()
    if cfg["backend"].startswith("tabpfn"):
        from tabpfn import TabPFNClassifier, TabPFNRegressor

        cls = TabPFNClassifier if classification else TabPFNRegressor
        return cls(model_path=cfg["model_path"], device=device, random_state=MODEL_SEED)
    from tabicl import TabICLClassifier, TabICLRegressor

    cls = TabICLClassifier if classification else TabICLRegressor
    return cls(
        model_path=cfg["model_path"],
        allow_auto_download=False,
        device=device,
        random_state=MODEL_SEED,
    )


def predict_batch(context_rows: int) -> int:
    """Validation rows per prediction call: PREDICT_BATCH for a context of 10,000 rows
    or fewer, smaller for a larger one."""
    return int(
        min(
            PREDICT_BATCH,
            max(MIN_PREDICT_BATCH, PREDICT_BUDGET // max(context_rows, 1)),
        )
    )


def device_label() -> str:
    """The compute device for a progress line; never raises."""
    try:
        return _device()
    except Exception:
        return "unknown"


def inference_mode():
    """torch.inference_mode() when torch is importable, else a no-op."""
    try:
        import torch
    except ImportError:
        return _contextlib.nullcontext()
    return torch.inference_mode()


def fit_estimator(cfg: dict, x: np.ndarray, y: np.ndarray):
    """Build the estimator and fit it on the context rows, printing the time of each
    step."""
    backend = cfg.get("backend", "model")
    with timed_stage(f"load {backend} checkpoint on {device_label()}"):
        estimator = build_estimator(cfg)
    with timed_stage(f"fit {len(x):,} context rows x {x.shape[1]} features"):
        estimator.fit(x, y)
    return estimator


def predict_batches(estimator, x: np.ndarray, cfg: dict, context_rows: int):
    """Predictions for all rows of x, one batch at a time with a progress line."""
    size = predict_batch(context_rows)
    count = max(1, -(-len(x) // size))
    out = []
    with inference_mode():
        for number, start in enumerate(range(0, len(x), size), start=1):
            with timed_stage(f"predict batch {number}/{count} ({size:,} rows each)"):
                out.append(predict_part(estimator, x[start : start + size], cfg))
    return out


def predict_part(estimator, x: np.ndarray, cfg: dict) -> np.ndarray:
    if cfg["kind"] == "classification":
        return np.asarray(estimator.predict_proba(x))[:, 1]
    if cfg["backend"].startswith("tabpfn"):
        try:
            return np.asarray(estimator.predict(x, output_type="median"))
        except TypeError:
            return np.asarray(estimator.predict(x))
    return np.asarray(estimator.predict(x))


# COMMAND ----------

# DBTITLE 1,Bundle -- the model identity and the train rows it uses as context


class TabularContextBundle:
    """Stored for an in-context model: the identity of the checkpoint (repository,
    revision, hashes, licence), the feature encoder and the context rows, all from
    the train partition. The estimator is built when predict() is first called."""

    def __init__(self, cfg: dict, encoder, x_context, y_context):
        self.cfg = cfg
        self.encoder = encoder
        self.x = np.asarray(x_context, dtype="float32")
        self.y = np.asarray(y_context)
        self.task = cfg["kind"]
        self._estimator = None

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_estimator"] = None
        return state

    def estimator(self):
        if self._estimator is None:
            self._estimator = fit_estimator(self.cfg, self.x, self.y)
        return self._estimator

    def predict(self, frame):
        x = self.encoder.transform(frame, fill=self.cfg["fill"])
        estimator = self.estimator()
        parts = predict_batches(estimator, x, self.cfg, len(self.x))
        return np.concatenate(parts).astype("float64") if parts else np.zeros(0)


# COMMAND ----------

# DBTITLE 1,One candidate: context from the train rows, scored on the validation rows


def tab_weights(backend: str, kind: str) -> tuple[dict, str]:
    """The manifest entry of the checkpoint and its file path in the volume."""
    key, file = TAB_WEIGHTS[backend][kind]
    entry = model_entry(key)
    if file not in entry["files"]:
        raise ValueError(f"{key}: {file} is not in the manifest")
    return entry, f"{entry['dir']}/{file}"


def tab_config(kind, backend, entry, path, file_hash) -> dict:
    return {
        "backend": backend,
        "kind": kind,
        "model_key": entry["model_key"],
        "repo_id": entry["repo_id"],
        "revision": entry["revision"],
        "licence": entry["licence"],
        "model_path": path,
        "model_sha256": file_hash,
        "fill": TAB_FILL[backend],
    }


def fit_tab_candidate(kind, backend, enc, pdf, tr_m, y, frame_va, y_va, base):
    """(bundle, metrics, rows, params) of one in-context model."""
    limits = TAB_LIMITS[backend]
    entry, path = tab_weights(backend, kind)
    cfg = tab_config(kind, backend, entry, path, entry["files"][path.rsplit("/", 1)[1]])
    x_train = enc.transform(pdf[tr_m], fill=cfg["fill"])
    y_train = y[tr_m]
    if limits["max_features"] and x_train.shape[1] > limits["max_features"]:
        raise ValueError(
            f"{x_train.shape[1]} features, over the {limits['max_features']} of {backend}"
        )
    keep = context_positions(len(x_train), limits["max_rows"], MODEL_SEED)
    x_ctx, y_ctx = x_train[keep], y_train[keep]
    if kind == "classification" and len(np.unique(y_ctx)) < 2:
        raise ValueError("the context rows hold one class only")
    print(
        f"  context {len(keep):,} of {len(x_train):,} train rows, "
        f"{x_train.shape[1]} features, {len(frame_va):,} validation rows, "
        f"device {device_label()}, batch {predict_batch(len(keep)):,}"
    )
    bundle = TabularContextBundle(cfg, enc, x_ctx, y_ctx)
    pred = bundle.predict(frame_va)
    if kind == "classification":
        metrics = _class_metrics(y_va, pred)
    else:
        metrics = regression_metrics(y_va, pred, base)
    params = {
        "model_key": entry["model_key"],
        "repo_id": entry["repo_id"],
        "revision": entry["revision"],
        "licence": entry["licence"],
        "mode": "in_context",
        "context_rows": len(keep),
        "train_rows": len(x_train),
        "features": int(x_train.shape[1]),
        "seed": MODEL_SEED,
    }
    return bundle, metrics, params


# COMMAND ----------

# DBTITLE 1,Validation sample and the accepted models on the same rows


def accepted_selection(ctx) -> list[dict]:
    """The accepted models forwarded for the task at this frozen version, best first;
    the baseline is not included."""
    if not spark.catalog.tableExists(model_fqn("candidate_selection", ctx.ecosystem)):
        return []
    rows = (
        read_model("candidate_selection", ecosystem=ctx.ecosystem)
        .filter(
            (F.col("task_id") == ctx.task_id)
            & ~F.col("is_baseline")
            & (F.col("frozen_delta_version") == int(ctx.frozen_version))
        )
        .collect()
    )
    return sorted((r.asDict() for r in rows), key=lambda r: int(r["rank"]))


def record_references(ctx, score_fn, n_rows: int, params: dict, selection=None) -> None:
    """Score the accepted models of the task on the same validation sample as the
    context models. Rows with stage `reference`; no stored artifact, so the selection
    never forwards them."""
    for sel in selection if selection is not None else accepted_selection(ctx):

        def score(sel=sel):
            bundle = load_bundle({"mlflow_run_id": sel["mlflow_run_id"]})
            return None, score_fn(bundle), n_rows

        run_candidate(
            ctx,
            f"{sel['model_name']}@validation_sample",
            sel["family"],
            score,
            params=dict(params),
            stage="reference",
        )


# COMMAND ----------

# DBTITLE 1,Runner -- in-context candidates of one task


def run_foundation_tab(ctx, df, models=None) -> None:
    """In-context candidates on the validation rows of one task, scored with the
    same metrics, baseline and rows as the accepted candidates."""
    cfg = TAB_TASKS[ctx.task_id]
    spec, kind = cfg["spec"], cfg["kind"]
    if cfg.get("events_only"):
        df = df.filter(
            F.col("target_any_event") & F.col("target_log_event_energy_mwh").isNotNull()
        )
    pdf, feats, fraction = _prepare(ctx, df, spec)
    tr_m, va_m = _masks(pdf)
    target = pd.to_numeric(pdf[spec["target"]], errors="coerce")
    classification = kind == "classification"
    y = target.to_numpy(dtype="int64" if classification else "float64")
    enc = FeatureEncoder(feats).fit(pdf[tr_m])
    note_target(y, tr_m, va_m, kind)
    start_task(ctx)
    base = None
    if not classification:
        base = fit_baseline(pdf[tr_m], spec["target"], spec["baseline"]).predict(
            pdf[va_m]
        )
    frame_all = pdf[va_m].reset_index(drop=True)
    y_all = y[va_m]
    pos = context_positions(len(frame_all), IN_CONTEXT_SAMPLE_ROWS, MODEL_SEED)
    sampled = len(pos) < len(frame_all)
    frame_va, y_va = frame_all.iloc[pos].reset_index(drop=True), y_all[pos]
    if sampled:
        print(f"validation sample: {len(pos):,} of {len(frame_all):,} rows")
        if base is not None:
            base = np.asarray(base)[pos]
    sample = sample_params(len(frame_all), len(pos)) if sampled else {}
    chosen = [c for c in TAB_CANDIDATES if models is None or c[0] in models]
    for name, backend, library in chosen:
        params = {"row_fraction": round(fraction, 4), **sample}

        def fit(backend=backend, params=params):
            bundle, metrics, extra = fit_tab_candidate(
                kind, backend, enc, pdf, tr_m, y, frame_va, y_va, base
            )
            params.update(extra)
            return bundle, metrics, len(frame_va)

        run_candidate(ctx, name, TAB_FAMILY, fit, requires=(library,), params=params)
    if sampled:

        def score_reference(bundle):
            pred = bundle.predict(frame_va)
            if classification:
                return _class_metrics(y_va, pred)
            return regression_metrics(y_va, pred, base)

        record_references(ctx, score_reference, len(frame_va), sample)
    finish_foundation_task(ctx)


# COMMAND ----------

# DBTITLE 1,Match tier -- several classes, the numeric features only
TIER_TASKS = {
    "redispatch_matching.tier": {
        "kind": "multiclass",
        "spec": {
            "text_col": "affected_asset_text",
            "target": "target_match_tier",
            "key_cols": ["affected_asset_text"],
        },
    }
}
TABPFN_MAX_CLASSES = 10


class MulticlassContextBundle:
    """In-context classifier over the match tiers. predict() returns the tier names;
    the asset text is not an input, only the numeric features of the accepted
    classifiers."""

    def __init__(self, cfg: dict, encoder, x_context, y_context, classes):
        self.cfg = cfg
        self.encoder = encoder
        self.x = np.asarray(x_context, dtype="float32")
        self.y = np.asarray(y_context, dtype="int64")
        self.classes = [str(c) for c in classes]
        self.task = "multiclass"
        self._estimator = None

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_estimator"] = None
        return state

    def estimator(self):
        if self._estimator is None:
            self._estimator = fit_estimator(self.cfg, self.x, self.y)
        return self._estimator

    def predict(self, frame):
        x = self.encoder.transform(frame, fill=self.cfg["fill"])
        estimator = self.estimator()
        seen = np.asarray(estimator.classes_, dtype="int64")
        names = np.asarray(self.classes, dtype=object)
        out = np.empty(len(x), dtype=object)
        size = predict_batch(len(self.x))
        with inference_mode():
            for start in range(0, len(x), size):
                part = x[start : start + size]
                with timed_stage(f"predict rows {start:,} to {start + len(part):,}"):
                    best = np.asarray(estimator.predict_proba(part)).argmax(axis=1)
                out[start : start + len(part)] = names[seen[best]]
        return out


# COMMAND ----------

# DBTITLE 1,Runner -- in-context candidates of the match tier task


def run_foundation_tier(ctx, df, models=None) -> None:
    """In-context candidates on the validation rows of the match tier task, scored
    with the tier metrics of the accepted classifiers on the same rows."""
    spec = TIER_TASKS[ctx.task_id]["spec"]
    feats = resolve_features(ctx, df.columns)
    cols = list(dict.fromkeys([*feats, spec["text_col"], spec["target"], "partition"]))
    pdf, fraction = to_training_frame(df, key_cols=spec["key_cols"], columns=cols)
    pdf[spec["target"]] = pdf[spec["target"]].astype(str)
    train = pdf[pdf["partition"] == "train"].reset_index(drop=True)
    valid = pdf[pdf["partition"] == "validation"].reset_index(drop=True)
    if train.empty or valid.empty:
        raise RuntimeError(
            f"need train and validation rows, have {len(train)} / {len(valid)}"
        )
    labels = sorted(train[spec["target"]].unique().tolist())
    code = {lab: i for i, lab in enumerate(labels)}
    y_train = train[spec["target"]].map(code).to_numpy(dtype="int64")
    pos = context_positions(len(valid), IN_CONTEXT_SAMPLE_ROWS, MODEL_SEED)
    sampled = len(pos) < len(valid)
    sample = sample_params(len(valid), len(pos)) if sampled else {}
    if sampled:
        print(f"validation sample: {len(pos):,} of {len(valid):,} rows")
        valid = valid.iloc[pos].reset_index(drop=True)
    y_valid = valid[spec["target"]].to_numpy()
    enc = FeatureEncoder(feats).fit(train)
    start_task(ctx)

    chosen = [c for c in TAB_CANDIDATES if models is None or c[0] in models]
    for name, backend, library in chosen:
        params = {"row_fraction": round(fraction, 4), **sample}

        def fit(backend=backend, params=params):
            limits = TAB_LIMITS[backend]
            if backend.startswith("tabpfn") and len(labels) > TABPFN_MAX_CLASSES:
                raise ValueError(
                    f"{len(labels)} classes, over the {TABPFN_MAX_CLASSES}"
                )
            entry, path = tab_weights(backend, "classification")
            file_hash = entry["files"][path.rsplit("/", 1)[1]]
            cfg = tab_config("classification", backend, entry, path, file_hash)
            x_train = enc.transform(train, fill=cfg["fill"])
            if limits["max_features"] and x_train.shape[1] > limits["max_features"]:
                raise ValueError(f"{x_train.shape[1]} features, over the limit")
            keep = context_positions(len(x_train), limits["max_rows"], MODEL_SEED)
            if len(np.unique(y_train[keep])) < 2:
                raise ValueError("the context rows hold one tier only")
            bundle = MulticlassContextBundle(
                cfg, enc, x_train[keep], y_train[keep], labels
            )
            metrics = tier_metrics(y_valid, bundle.predict(valid), labels)
            params.update(
                {
                    "model_key": entry["model_key"],
                    "repo_id": entry["repo_id"],
                    "revision": entry["revision"],
                    "licence": entry["licence"],
                    "mode": "in_context",
                    "context_rows": len(keep),
                    "train_rows": len(x_train),
                    "features": int(x_train.shape[1]),
                    "classes": len(labels),
                    "seed": MODEL_SEED,
                }
            )
            return bundle, metrics, len(valid)

        run_candidate(ctx, name, TAB_FAMILY, fit, requires=(library,), params=params)
    if sampled:
        record_references(
            ctx,
            lambda bundle: tier_metrics(y_valid, bundle.predict(valid), labels),
            len(valid),
            sample,
        )
    finish_foundation_task(ctx)
