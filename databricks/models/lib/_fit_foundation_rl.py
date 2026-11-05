# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION OFFLINE POLICY LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** a Decision Transformer trained offline on the logged trajectories, as
# MAGIC an extra candidate for the three policy tasks. Pulled in with
# MAGIC `%run ../lib/_fit_foundation_rl` after `_fit_foundation_ts` and `_fit_rl`. The
# MAGIC model reads the last steps of an episode as return, state and action tokens and
# MAGIC predicts the action of the current step from the state token, so the logged
# MAGIC action of the current step never reaches it. It is conditional and offline only:
# MAGIC at prediction time the return asked for is a high quantile of the train episode
# MAGIC returns, less the reward already seen in the episode. No public checkpoint is
# MAGIC used.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Settings and the policy tasks
POLICY_FAMILY = "offline_rl"
DT_NAME = "decision_transformer"
DT_CONTEXT = 10
DT_DIM = 64
DT_HEADS = 2
DT_LAYERS = 2
DT_DROPOUT = 0.1
DT_EPOCHS = 3
DT_BATCH = 1024
DT_EVAL_BATCH = 4096
DT_LEARNING_RATE = 1e-3
DT_TRAIN_ROWS = 500_000
DT_TARGET_QUANTILE = 0.9
POLICY_TASKS = {
    "rl_pumped_storage.policy": {
        "kind": "pumped",
        "spec": {
            "state_cols": [
                "state_residual_load_mwh",
                "state_price_eur_per_mwh",
                "hour_of_day",
                "day_of_week",
                "month",
            ],
            "action_col": "action_net_mwh",
            "reward_col": "reward_eur",
            "price_col": "state_price_eur_per_mwh",
            "key_cols": ["episode_id", "step"],
        },
    },
    "rl_redispatch.policy": {
        "kind": "action",
        "spec": {
            "action_col": "action_direction",
            "reward_col": "reward",
            "key_cols": ["episode_id", "step"],
            "drop": [
                "action_direction",
                "action_reason",
                "action_energy_mwh",
                "reward",
            ],
            "id_features": ["market_area_code"],
        },
    },
    "rl_session_sequences.policy": {
        "kind": "session",
        "sample_users": True,
        "spec": {
            "action_col": "target_action_event_type",
            "reward_col": "reward",
            "session_col": "session_key",
            "step_col": "step",
            "prev_col": "seq_previous_event_type",
            "key_cols": ["episode_id", "step"],
            "drop": [
                "seq_event_type",
                "seq_product_id",
                "seq_category",
                "seq_brand",
                "seq_price",
                "seq_price_imputed",
                "seq_price_is_missing",
                "reward",
                "target_action_event_type",
                "target_action_product_id",
            ],
            "id_features": ["step"],
        },
    },
}

# COMMAND ----------

# DBTITLE 1,Episodes as arrays: windows, returns and normalised inputs


def episode_windows(codes, context: int = DT_CONTEXT) -> np.ndarray:
    """(rows, context) positions of the last `context` rows of the same episode that
    end at each row, -1 where the episode has no such row. The rows must be sorted by
    episode and step."""
    n = len(codes)
    pos = np.arange(n)
    out = np.full((n, context), -1, dtype="int64")
    for lag in range(context):
        src = pos - lag
        has = src >= 0
        same = np.zeros(n, dtype=bool)
        same[has] = codes[src[has]] == codes[has]
        out[:, context - 1 - lag] = np.where(same, src, -1)
    return out


def policy_arrays(frame, cols, encoder, norms, hindsight: bool) -> dict:
    """The rows of `frame` sorted by episode and step as arrays. The return asked for
    is the whole episode return less the reward so far (`hindsight`, used to train)
    or the target return less the reward so far (used to predict). `back` restores
    the original row order."""
    f = frame.reset_index(drop=True)
    codes = pd.factorize(f[cols["episode"]])[0]
    order = np.lexsort((f[cols["step"]].to_numpy(), codes))
    f = f.iloc[order].reset_index(drop=True)
    codes = codes[order]
    reward = pd.to_numeric(f[cols["reward"]], errors="coerce").fillna(0.0)
    grouped = reward.groupby(codes)
    prior = (grouped.cumsum() - reward).to_numpy(dtype="float64")
    asked = grouped.transform("sum").to_numpy(dtype="float64")
    if not hindsight:
        asked = np.full(len(f), norms["target_return"])
    state = (encoder.transform(f, fill=True) - norms["mean"]) / norms["std"]
    if norms["classes"] is None:
        raw = pd.to_numeric(f[cols["action"]], errors="coerce")
        raw = raw.to_numpy(dtype="float64")
        action = np.nan_to_num((raw - norms["a_mean"]) / norms["a_std"])
    else:
        index = {c: i + 1 for i, c in enumerate(norms["classes"])}
        action = f[cols["action"]].astype(str).map(index).fillna(0).to_numpy("int64")
    back = np.empty(len(order), dtype="int64")
    back[order] = np.arange(len(order))
    return {
        "state": state.astype("float32"),
        "rtg": ((asked - prior) / norms["rtg_scale"]).astype("float32"),
        "action": action,
        "windows": episode_windows(codes),
        "totals": asked,
        "codes": codes,
        "back": back,
    }


def gather_window(prep: dict, rows) -> tuple:
    """(return, state, valid, action) of the windows that end at `rows`."""
    idx = prep["windows"][rows]
    ok = idx >= 0
    safe = np.where(ok, idx, 0)
    state = prep["state"][safe] * ok[..., None]
    rtg = prep["rtg"][safe] * ok
    action = prep["action"][safe] * ok
    return rtg.astype("float32"), state.astype("float32"), ok.astype("float32"), action


# COMMAND ----------

# DBTITLE 1,The network


def _make_decision_transformer(n_state, n_classes, length=DT_CONTEXT, dim=DT_DIM):
    import torch
    from torch import nn

    categorical = n_classes > 0

    class Net(nn.Module):
        def __init__(self):
            super().__init__()
            self.rtg = nn.Linear(1, dim)
            self.state = nn.Linear(n_state + 1, dim)
            if categorical:
                self.act = nn.Embedding(n_classes + 1, dim, padding_idx=0)
            else:
                self.act = nn.Linear(1, dim)
            self.pos = nn.Embedding(length, dim)
            layer = nn.TransformerEncoderLayer(
                d_model=dim,
                nhead=DT_HEADS,
                dim_feedforward=2 * dim,
                dropout=DT_DROPOUT,
                batch_first=True,
                norm_first=True,
            )
            self.enc = nn.TransformerEncoder(
                layer, num_layers=DT_LAYERS, enable_nested_tensor=False
            )
            self.norm = nn.LayerNorm(dim)
            self.head = nn.Linear(dim, n_classes if categorical else 1)
            size = 3 * length - 1
            causal = torch.triu(torch.ones(size, size, dtype=torch.bool), 1)
            self.register_buffer("causal", causal)

        def forward(self, rtg, state, valid, action):
            pos = self.pos.weight[None]
            r = self.rtg(rtg[..., None]) + pos
            s = self.state(torch.cat([state, valid[..., None]], dim=-1)) + pos
            a = self.act(action if categorical else action[..., None]) + pos
            tokens = torch.stack([r, s, a], dim=2).reshape(len(rtg), 3 * length, dim)
            # the last token is the action of the current step: it is dropped
            h = self.enc(tokens[:, :-1], mask=self.causal)
            return self.head(self.norm(h[:, -1]))

    return Net()


def _tensors(prep, rows, categorical, device):
    import torch

    rtg, state, ok, action = gather_window(prep, rows)
    kind = torch.long if categorical else torch.float32
    return (
        torch.as_tensor(rtg, device=device),
        torch.as_tensor(state, device=device),
        torch.as_tensor(ok, device=device),
        torch.as_tensor(action, dtype=kind, device=device),
    )


def _train_decision_transformer(prep, n_classes, rows):
    import torch

    torch.manual_seed(MODEL_SEED)
    categorical = n_classes > 0
    device = "cuda" if torch.cuda.is_available() else "cpu"
    net = _make_decision_transformer(prep["state"].shape[1], n_classes).to(device)
    opt = torch.optim.Adam(net.parameters(), lr=DT_LEARNING_RATE)
    target = torch.as_tensor(
        prep["action"], dtype=torch.long if categorical else torch.float32
    )
    order_rng = np.random.default_rng(MODEL_SEED)
    net.train()
    for epoch in range(DT_EPOCHS):
        print(f"  .. decision transformer epoch {epoch + 1}/{DT_EPOCHS} on {device}")
        order = order_rng.permutation(rows)
        for i in range(0, len(order), DT_BATCH):
            batch = order[i : i + DT_BATCH]
            out = net(*_tensors(prep, batch, categorical, device))
            want = target[batch].to(device)
            if categorical:
                loss = torch.nn.functional.cross_entropy(out, want - 1)
            else:
                loss = ((out[:, 0] - want) ** 2).mean()
            opt.zero_grad()
            loss.backward()
            opt.step()
    return net.cpu().eval()


# COMMAND ----------

# DBTITLE 1,Policy -- the object that is stored


class DecisionTransformerPolicy:
    """action(frame): the action of every row from the last steps of its episode. The
    frame needs the state columns, the episode, the step, the logged actions of the
    earlier steps and the rewards of the earlier steps."""

    def __init__(self, cfg: dict, encoder, norms: dict, net, cols: dict):
        self.cfg, self.encoder, self.norms = cfg, encoder, norms
        self.net, self.cols = net, cols

    def action(self, frame):
        import torch

        prep = policy_arrays(frame, self.cols, self.encoder, self.norms, False)
        categorical = self.norms["classes"] is not None
        device = "cuda" if torch.cuda.is_available() else "cpu"
        net = self.net.to(device).eval()
        parts = []
        with torch.no_grad():
            for start in range(0, len(prep["state"]), DT_EVAL_BATCH):
                rows = np.arange(start, min(start + DT_EVAL_BATCH, len(prep["state"])))
                out = net(*_tensors(prep, rows, categorical, device)).cpu().numpy()
                parts.append(out.argmax(axis=1) if categorical else out[:, 0])
        self.net.cpu()
        sorted_out = np.concatenate(parts) if parts else np.zeros(0)
        if categorical:
            names = np.asarray(self.norms["classes"], dtype=object)
            sorted_out = names[sorted_out]
        else:
            sorted_out = sorted_out * self.norms["a_std"] + self.norms["a_mean"]
        return sorted_out[prep["back"]]


# COMMAND ----------

# DBTITLE 1,Fit -- normalisation, target return, training rows


def fit_decision_transformer(train, kind, spec, feats, cols):
    """(policy, parameters) trained on the train rows of one policy task."""
    enc = FeatureEncoder(feats).fit(train)
    raw = enc.transform(train, fill=True)
    std = raw.std(axis=0)
    norms = {
        "mean": raw.mean(axis=0),
        "std": np.where(std > 0, std, 1.0),
        "classes": None,
        "a_mean": 0.0,
        "a_std": 1.0,
        "target_return": 0.0,
        "rtg_scale": 1.0,
    }
    if kind == "pumped":
        actions = pd.to_numeric(train[cols["action"]], errors="coerce")
        norms["a_mean"] = float(actions.mean())
        norms["a_std"] = float(actions.std()) or 1.0
    else:
        norms["classes"] = sorted(train[cols["action"]].astype(str).unique().tolist())
    first = policy_arrays(train, cols, enc, norms, True)
    per_episode = pd.Series(first["totals"]).groupby(first["codes"]).first()
    norms["target_return"] = float(per_episode.quantile(DT_TARGET_QUANTILE))
    scale = float(first["rtg"].std())
    norms["rtg_scale"] = scale if scale > 1e-9 else 1.0
    prep = policy_arrays(train, cols, enc, norms, True)
    rows = np.arange(len(prep["state"]))
    if len(rows) > DT_TRAIN_ROWS:
        rng = np.random.default_rng(MODEL_SEED)
        rows = np.sort(rng.choice(rows, DT_TRAIN_ROWS, replace=False))
    n_classes = len(norms["classes"] or [])
    net = _train_decision_transformer(prep, n_classes, rows)
    policy = DecisionTransformerPolicy(
        {"mode": "trained_on_platform_data"}, enc, norms, net, cols
    )
    params = {
        "context": DT_CONTEXT,
        "dim": DT_DIM,
        "layers": DT_LAYERS,
        "heads": DT_HEADS,
        "epochs": DT_EPOCHS,
        "train_rows": len(rows),
        "episodes": len(per_episode),
        "target_return_quantile": DT_TARGET_QUANTILE,
        "mode": "trained_on_platform_data",
        "seed": MODEL_SEED,
    }
    return policy, params


# COMMAND ----------

# DBTITLE 1,Frames of the three policy tasks, prepared as the accepted policies were


def policy_frames(ctx, df, kind, spec):
    """(train, valid, feats, columns, spec, fraction, train_eval) for one task."""
    ac, rc = spec["action_col"], spec["reward_col"]
    if kind == "pumped":
        feats = list(spec["state_cols"])
        cols = [*feats, ac, rc, spec["price_col"], "partition", *spec["key_cols"]]
        keys, sort = spec["key_cols"], None
        needed = [ac, rc, spec["price_col"]]
        episode, step = spec["key_cols"]
    elif kind == "action":
        feats = resolve_features(
            ctx,
            df.columns,
            id_features=spec.get("id_features", ()),
            drop=spec.get("drop", ()),
        )
        cols = [*feats, ac, rc, "partition", *spec["key_cols"]]
        keys, sort, needed = spec["key_cols"], None, [ac, rc]
        episode, step = spec["key_cols"]
    else:
        feats = resolve_features(
            ctx,
            df.columns,
            id_features=spec.get("id_features", ()),
            drop=spec.get("drop", ()),
        )
        sc, tc = spec["session_col"], spec["step_col"]
        cols = [*feats, ac, rc, sc, tc, spec["prev_col"], "seq_event_type"]
        cols.append("partition")
        keys, sort, needed = [sc], [sc, tc], [ac, rc]
        episode, step = sc, tc
    pdf, fraction = to_training_frame(
        df, key_cols=keys, columns=list(dict.fromkeys(cols))
    )
    pdf = pdf.dropna(subset=needed)
    if sort:
        pdf = pdf.sort_values(sort)
    pdf = pdf.reset_index(drop=True)
    if kind != "pumped":
        pdf[ac] = pdf[ac].astype(str)
    train = pdf[pdf["partition"] == "train"].reset_index(drop=True)
    valid = pdf[pdf["partition"] == "validation"].reset_index(drop=True)
    if train.empty or valid.empty:
        raise RuntimeError("need train and validation rows")
    if kind == "pumped":
        y = train[ac].to_numpy(dtype="float64")
        spec = {**spec, "action_bounds": (float(y.min()), float(y.max()))}
    columns = {"action": ac, "reward": rc, "episode": episode, "step": step}
    train_eval = _train_eval_frame(train, spec.get("session_col"))
    return train, valid, feats, columns, spec, fraction, train_eval


# COMMAND ----------

# DBTITLE 1,Runner -- the Decision Transformer on the validation rows


def run_foundation_policy(ctx, df, models=None) -> None:
    """The Decision Transformer on the validation rows of a policy task, scored with
    the reward or action metrics of the accepted policies on the same rows."""
    task = POLICY_TASKS[ctx.task_id]
    kind = task["kind"]
    train, valid, feats, cols, spec, fraction, train_eval = policy_frames(
        ctx, df, kind, task["spec"]
    )
    start_task(ctx)
    if models is not None and DT_NAME not in models:
        return
    params = {"row_fraction": round(fraction, 4)}

    def fit():
        policy, extra = fit_decision_transformer(train, kind, spec, feats, cols)
        params.update(extra)
        scores = _policy_scores(
            policy, valid, train_eval, spec, "pumped" if kind == "pumped" else "action"
        )
        return policy, scores, len(valid)

    run_candidate(ctx, DT_NAME, POLICY_FAMILY, fit, requires=("torch",), params=params)
    finish_foundation_task(ctx)
