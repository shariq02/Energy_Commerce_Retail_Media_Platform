# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # OFFLINE POLICY LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** offline policy candidates on logged trajectories, pulled in with
# MAGIC `%run ../_fit_rl` after `_model_common`, `_model_metrics` and `_fit_tabular`
# MAGIC (its estimator builders). The logged action is what operators or users did, so
# MAGIC policies are imitation and reward-weighted fits; only the pumped-storage reward
# MAGIC is recomputed under a new action (price times net energy, price-taker assumption).

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
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
