# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # OPERATE WINDOWS LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** batch scoring and monitoring of the registered models. Pulled in
# MAGIC with `%run ../lib/_operate_windows` after the model, evaluation, registry and
# MAGIC operate-rules libraries. Each time-ordered partition of the frozen dataset is a
# MAGIC window. A window is read with the evaluation code, scored with the stored model
# MAGIC and profiled. The reference window profiles the train partition. No target value
# MAGIC is scored and nothing is refitted.

# COMMAND ----------

# DBTITLE 1,Windows and the evaluation context
# kinds whose rows cannot be sampled without breaking the unit that is scored
NO_CAP_KINDS = ("weak", "matching", "reconstruction")
SPARK_CAP_FACTOR = 10


class MonitorContext(EvalContext):
    """One task read for one window."""

    def __init__(self, task_id: str, rid: str, window_id: str):
        super().__init__(task_id, rid, smoke=False)
        self.window_id = window_id
        self.partition = window_partition(window_id)
        self.component = f"models/operate/{task_id}"


def window_partition(window_id: str) -> str:
    """The partition read for a window id."""
    return {
        "train": "train",
        "validation": "validation",
        "held_out": TEST_PARTITION,
    }[window_id]


# COMMAND ----------

# DBTITLE 1,Row sampling


def key_columns(cfg: dict) -> list:
    spec = cfg["spec"]
    if spec.get("key_cols"):
        return list(spec["key_cols"])
    return [spec["session_col"]] if "session_col" in spec else []


def capped_frame(df, key_cols: list, cap: int):
    """The Spark frame, or a stable hash sample of its key columns above the cap."""
    if not key_cols or not set(key_cols) <= set(df.columns):
        return df
    if df.limit(cap + 1).count() <= cap:
        return df
    fraction = cap / df.count()
    bucket = F.pmod(
        F.xxhash64(*[F.col(c).cast("string") for c in key_cols]), F.lit(10000)
    )
    return df.filter(bucket < int(fraction * 10000))


def sample_positions(n: int, cap: int) -> np.ndarray:
    """All positions, or a sorted random sample of `cap` of them."""
    if n <= cap:
        return np.arange(n)
    rng = np.random.default_rng(MODEL_SEED)
    return np.sort(rng.choice(n, size=cap, replace=False))


def row_keys(cfg: dict, frame: pd.DataFrame) -> list:
    """The key columns of each row joined by a bar; the row number if a key column
    is not in the frame."""
    spec = cfg["spec"]
    cols = key_columns(cfg)
    if "step_col" in spec:
        cols = [*cols, spec["step_col"]]
    if not cols or not set(cols) <= set(frame.columns):
        return [str(i) for i in range(len(frame))]
    text = frame[cols[0]].astype(str)
    for c in cols[1:]:
        text = text + "|" + frame[c].astype(str)
    return text.tolist()


# COMMAND ----------

# DBTITLE 1,The monitored columns of each model kind


def monitored_columns(kind: str, ctx, evaluator, frame: pd.DataFrame) -> list:
    """The model input columns present in the window frame. A kind without such
    columns (weak supervision, reconstruction) has none."""
    spec = evaluator.spec
    if kind == "pumped":
        return [c for c in spec["state_cols"] if c in frame.columns]
    if kind == "ranking":
        return [c for c in (spec["src_col"],) if c in frame.columns]
    if kind not in ("tabular", "survival", "anomaly", "action"):
        return []
    ids = [*spec.get("id_features", ()), *spec.get("extra_features", ())]
    try:
        return resolve_features(
            ctx, frame.columns, id_features=ids, drop=spec.get("drop", ())
        )
    except RuntimeError:
        return []


# COMMAND ----------

# DBTITLE 1,One prediction per row


def sequence_top_items(bundle, frame: pd.DataFrame) -> np.ndarray:
    """The item the sequence network ranks first after each event."""
    import torch

    session_col, step_col, item_col, _truth = bundle.cols
    ordered = frame.sort_values([session_col, step_col]).reset_index(drop=True)
    hist = build_histories(ordered, bundle.item_ids, SEQ_LENGTH, session_col, item_col)
    names = {i: item for item, i in bundle.item_ids.items()}
    bundle.net.eval()
    ids = []
    with torch.no_grad():
        for i in range(0, len(hist), SEQ_BATCH):
            batch = torch.as_tensor(hist[i : i + SEQ_BATCH], dtype=torch.long)
            logits = bundle.net(batch)
            logits[:, 0] = -float("inf")
            ids.append(logits.argmax(1).numpy())
    top = np.concatenate(ids) if ids else np.zeros(0, dtype="int64")
    return np.array([str(names.get(int(i), "unknown")) for i in top], dtype=object)


def _weak_predictions(evaluator, bundle):
    """The resolved label of every entity of the window, one text label each."""
    wanted = evaluator._partition_name("eval")
    labels, keys = [], []
    for etype, (mat, _lfs, _labels, parts, _grp) in evaluator.mats.items():
        votes = mat[parts == wanted]
        if not len(votes):
            continue
        resolved = np.asarray(evaluator._predict(bundle, etype, votes))
        labels += [str(int(v)) for v in resolved]
        keys += [f"{etype}|{i}" for i in range(len(resolved))]
    return None, np.array(labels, dtype=object), keys


def _reconstruction_predictions(evaluator, bundle):
    """The predicted value of every masked event, from the model inputs only."""
    values, keys = [], []
    for cfg, sset, events in evaluator.sets:
        for j, var in enumerate(cfg["variables"]):
            x, _truth, _sid = evaluator.data[(cfg["name"], j)]
            pred = np.asarray(
                evaluator._predict(bundle, cfg, sset, events, j, x), dtype="float64"
            )
            values.append(pred)
            keys += [f"{cfg['name']}|{var}|{i}" for i in range(len(pred))]
    joined = np.concatenate(values) if values else np.zeros(0)
    return joined, None, keys


def predict_window(kind: str, evaluator, bundle):
    """(numeric predictions, text predictions, row keys) of the window frame. One of
    the first two is None. Row keys are None where the frame rows carry them."""
    frame = evaluator.frames["eval"]
    if kind == "reconstruction":
        return _reconstruction_predictions(evaluator, bundle)
    if kind == "weak":
        return _weak_predictions(evaluator, bundle)
    if kind == "action":
        return None, np.asarray(bundle.action(frame)).astype(str), None
    if kind == "ranking":
        return None, sequence_top_items(bundle, frame), None
    if kind == "tabular":
        if getattr(bundle, "task", None) == "quantile":
            values = bundle.predict(frame)[0.5]
        else:
            values = bundle.predict(frame)
    elif kind == "survival":
        values = bundle.risk(frame)
    elif kind == "anomaly":
        values = bundle.score(frame)
    elif kind == "pumped":
        values = bundle.action(frame)
    else:
        raise ValueError(f"no monitoring prediction for the kind {kind}")
    return np.asarray(values, dtype="float64"), None, None


# COMMAND ----------

# DBTITLE 1,Read, score and sample one window


def collect_window(entry: dict, window_id: str, bundle, rid: str, spec: dict) -> dict:
    """Features, predictions and row keys of one window, sampled to the row limit."""
    task_id = entry["task_id"]
    ctx = MonitorContext(task_id, rid, window_id)
    if int(entry["frozen_delta_version"]) != int(ctx.frozen_version):
        raise RuntimeError(
            f"{task_id}: registered at version {entry['frozen_delta_version']}, "
            f"frozen dataset is at {ctx.frozen_version}"
        )
    cfg = EVAL_TASKS[task_id]
    kind = cfg["kind"]
    evaluator = globals()[EVALUATOR_NAMES[kind]](ctx, cfg)
    try:
        df = task_frame(ctx, ctx.partition)
        if kind not in NO_CAP_KINDS:
            cap = SPARK_CAP_FACTOR * spec["max_window_rows"]
            df = capped_frame(df, key_columns(cfg), cap)
        evaluator.load(df, None, 1.0)
        values, labels, keys = predict_window(kind, evaluator, bundle)
        frame = evaluator.frames["eval"]
        n_rows = len(values) if values is not None else len(labels)
        pick = sample_positions(n_rows, spec["max_window_rows"])
        if keys is None:
            columns = monitored_columns(kind, ctx, evaluator, frame)
            features = frame[columns].iloc[pick]
            keys = row_keys(cfg, frame.iloc[pick])
        else:
            features = frame[[]].iloc[:0]
            keys = [keys[i] for i in pick]
        return {
            "window_id": window_id,
            "n_rows": int(n_rows),
            "features": features.reset_index(drop=True),
            "values": None if values is None else values[pick],
            "labels": None if labels is None else labels[pick],
            "keys": keys,
        }
    finally:
        evaluator.frames = {"eval": None, "validation": None}
        evaluator.sampler = None
        del evaluator
        _take_notes()
        _gc.collect()


def prediction_series(data: dict) -> pd.Series:
    """The predictions of a window as one column: numbers or text labels."""
    if data["values"] is not None:
        return pd.Series(data["values"], dtype="float64")
    return pd.Series(data["labels"], dtype="object")


# COMMAND ----------

# DBTITLE 1,Table writers


def write_predictions(entry: dict, data: dict, rid: str, now) -> None:
    link = entry_link(entry)
    labels = data["labels"]
    values = data["values"]
    rows = [
        {
            **link,
            "window_id": data["window_id"],
            "row_key": key,
            "prediction": None if values is None else float(values[i]),
            "prediction_label": None if labels is None else str(labels[i]),
            "frozen_delta_version": int(entry["frozen_delta_version"]),
            "run_id": rid,
            "scored_at": now,
        }
        for i, key in enumerate(data["keys"])
    ]
    eco = TASK_BY_ID[entry["task_id"]]["ecosystem"]
    window = f" AND window_id = '{data['window_id']}'"
    replace_rows(rows, "model_predictions", eco, entry_predicate(entry, window))


# COMMAND ----------

# DBTITLE 1,Reference profile and monitoring of one registered model


def profile_entry(entry: dict, rid: str, spec: dict) -> dict:
    """Profile the train window of one registered model and store its predictions
    and its reference profile."""
    eco = TASK_BY_ID[entry["task_id"]]["ecosystem"]
    bundle = load_bundle({"mlflow_run_id": entry["mlflow_run_id"]})
    data = collect_window(entry, REFERENCE_WINDOW, bundle, rid, spec)
    now = now_utc()
    profile = profile_window(data["features"], prediction_series(data), spec)
    rows = reference_rows(profile, entry, rid, now)
    replace_rows(rows, "reference_profile", eco, entry_predicate(entry))
    write_predictions(entry, data, rid, now)
    return {"columns": len(data["features"].columns), "rows": data["n_rows"]}


def reference_by_subject(ecosystem: str, entry: dict) -> dict:
    """The recorded reference rows of the registered version of one model."""
    rows = (
        read_model("reference_profile", ecosystem=ecosystem)
        .filter(
            (F.col("task_id") == entry["task_id"])
            & (F.col("model_name") == entry["model_name"])
            & (F.col("registry_version") == int(entry["version"]))
        )
        .collect()
    )
    return {r["subject"]: r.asDict() for r in rows}


def monitor_entry(entry: dict, rid: str, spec: dict) -> dict:
    """Score the validation and held-out windows of one registered model, store the
    predictions and compare each window with the recorded reference profile."""
    eco = TASK_BY_ID[entry["task_id"]]["ecosystem"]
    reference = reference_by_subject(eco, entry)
    if not reference:
        raise RuntimeError(f"{entry['task_id']}: no reference profile recorded")
    bundle = load_bundle({"mlflow_run_id": entry["mlflow_run_id"]})
    counts = {}
    for window_id in MONITOR_WINDOWS:
        data = collect_window(entry, window_id, bundle, rid, spec)
        now = now_utc()
        write_predictions(entry, data, rid, now)
        found = compare_window(
            reference, data["features"], prediction_series(data), spec
        )
        results = result_rows(found, entry, window_id, rid, now)
        window = f" AND window_id = '{window_id}' AND check_kind <> 'performance'"
        result_where = entry_predicate(entry, window)
        replace_rows(results, "monitoring_results", eco, result_where)
        flags = flag_rows(results, rid, now)
        flag_window = f" AND window_id = '{window_id}' AND check_kind <> 'performance'"
        flag_where = entry_predicate(entry, flag_window)
        replace_rows(flags, "monitoring_flags", eco, flag_where)
        counts[window_id] = {
            lv: sum(1 for r in results if r["level"] == lv) for lv in LEVELS
        }
    return counts


# COMMAND ----------

# DBTITLE 1,One ecosystem


def operate_ecosystem(ecosystem: str, rid: str, step: str) -> list:
    """Run `profile_entry` or `monitor_entry` for every registered selected model of
    the ecosystem. A model that fails is listed; the others still run."""
    rows = read_model("monitoring_spec", ecosystem=ecosystem).collect()
    spec = operate_spec_from_rows([r.asDict() for r in rows])
    run = profile_entry if step == "profile" else monitor_entry
    failed = []
    for entry in sorted(operate_entries(ecosystem), key=lambda e: e["task_id"]):
        try:
            out = run(entry, rid, spec)
            print(f"OK   {entry['task_id']} {entry['model_name']}: {out}")
        except Exception as exc:
            failed.append((entry["task_id"], f"{type(exc).__name__}: {str(exc)[:300]}"))
            print(f"FAIL {entry['task_id']} {entry['model_name']}: {failed[-1][1]}")
        _gc.collect()
    return failed
