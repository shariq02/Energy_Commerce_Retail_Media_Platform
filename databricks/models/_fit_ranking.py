# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # RANKING MODEL LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** next-item ranking: popularity and co-occurrence baselines and the GRU
# MAGIC and transformer sequence candidates, pulled in with `%run ../_fit_ranking` after
# MAGIC `_model_common` and `_model_metrics`. Every ranker exposes `ranks(frame)`: the
# MAGIC 1-based rank of the true next item per row, infinity when it is not ranked.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Configuration
SEQ_LENGTH = 10
SEQ_VOCAB = 20000
SEQ_DIM = 64
SEQ_EPOCHS = 2
SEQ_BATCH = 1024
SEQ_TRAIN_ROWS = 1_000_000
SEQ_EVAL_ROWS = 300_000
COOCCURRENCE_TOP = 50

# COMMAND ----------

# DBTITLE 1,Popularity and co-occurrence rankers


class PopularityRanker:
    def __init__(self, rank: dict, truth_col: str):
        self.rank = rank
        self.truth_col = truth_col

    def ranks(self, pdf):
        return (
            pdf[self.truth_col].map(self.rank).fillna(np.inf).to_numpy(dtype="float64")
        )


class CooccurrenceRanker:
    """Next items seen after the current item, most frequent first; items never
    seen after it fall back to popularity behind the listed ones."""

    def __init__(self, table, popularity, src_col, truth_col, top):
        self.table = table
        self.popularity = popularity
        self.src_col = src_col
        self.truth_col = truth_col
        self.top = top

    def ranks(self, pdf):
        pair = pdf[[self.src_col, self.truth_col]].rename(
            columns={self.src_col: "src", self.truth_col: "tgt"}
        )
        merged = pair.merge(self.table, on=["src", "tgt"], how="left")
        listed = merged["rank"].to_numpy(dtype="float64")
        return np.where(
            np.isfinite(listed), listed, self.top + self.popularity.ranks(pdf)
        )


def fit_popularity(train, truth_col) -> PopularityRanker:
    counts = train[truth_col].value_counts()
    rank = {item: i + 1 for i, item in enumerate(counts.index)}
    return PopularityRanker(rank, truth_col)


def fit_cooccurrence(train, src_col, truth_col, popularity, top=COOCCURRENCE_TOP):
    pairs = (
        train[[src_col, truth_col]]
        .rename(columns={src_col: "src", truth_col: "tgt"})
        .groupby(["src", "tgt"])
        .size()
        .reset_index(name="n")
    )
    pairs["rank"] = pairs.groupby("src")["n"].rank(method="first", ascending=False)
    table = pairs[pairs["rank"] <= top][["src", "tgt", "rank"]]
    return CooccurrenceRanker(table, popularity, src_col, truth_col, top)


# COMMAND ----------

# DBTITLE 1,Session histories and the sequence network


def build_histories(pdf, item_ids, length, session_col, item_col):
    """Item ids of the last `length` events up to and including each row, oldest
    first, zero padded. The frame must already be sorted by session and step."""
    ids = pdf[item_col].map(item_ids).fillna(0).astype("int64").to_numpy()
    frame = pd.DataFrame({"s": pdf[session_col].to_numpy(), "i": ids})
    grouped = frame.groupby("s")["i"]
    cols = [
        grouped.shift(j).fillna(0).to_numpy(dtype="int64")
        for j in range(length - 1, -1, -1)
    ]
    return np.column_stack(cols)


def _make_sequence_net(kind, vocab, dim=SEQ_DIM, length=SEQ_LENGTH):
    import torch
    from torch import nn

    class Net(nn.Module):
        def __init__(self):
            super().__init__()
            self.emb = nn.Embedding(vocab + 1, dim, padding_idx=0)
            if kind == "gru":
                self.enc = nn.GRU(dim, dim, batch_first=True)
            else:
                layer = nn.TransformerEncoderLayer(
                    d_model=dim, nhead=4, dim_feedforward=2 * dim, batch_first=True
                )
                self.enc = nn.TransformerEncoder(layer, num_layers=2)
                self.pos = nn.Parameter(torch.zeros(length, dim))
            self.out = nn.Linear(dim, vocab + 1)

        def forward(self, x):
            h = self.emb(x)
            if kind == "gru":
                o, _ = self.enc(h)
            else:
                o = self.enc(h + self.pos)
            return self.out(o[:, -1])

    return Net()


def _train_sequence(kind, hist, target_ids, vocab):
    import torch

    torch.manual_seed(MODEL_SEED)
    net = _make_sequence_net(kind, vocab)
    opt = torch.optim.Adam(net.parameters(), lr=1e-3)
    loss_fn = torch.nn.CrossEntropyLoss(ignore_index=0)
    x = torch.as_tensor(hist, dtype=torch.long)
    y = torch.as_tensor(target_ids, dtype=torch.long)
    n = len(x)
    net.train()
    for _ in range(SEQ_EPOCHS):
        order = torch.randperm(n)
        for i in range(0, n, SEQ_BATCH):
            idx = order[i : i + SEQ_BATCH]
            opt.zero_grad()
            loss_fn(net(x[idx]), y[idx]).backward()
            opt.step()
    return net


def sequence_ranks(net, hist, truth_ids):
    import torch

    net.eval()
    out = []
    with torch.no_grad():
        for i in range(0, len(hist), SEQ_BATCH):
            logits = net(torch.as_tensor(hist[i : i + SEQ_BATCH], dtype=torch.long))
            logits[:, 0] = -float("inf")
            truth = torch.as_tensor(truth_ids[i : i + SEQ_BATCH], dtype=torch.long)
            own = logits.gather(1, truth[:, None])
            rank = (logits > own).sum(1) + 1
            out.append(torch.where(truth == 0, torch.full_like(rank, -1), rank).numpy())
    ranks = np.concatenate(out).astype("float64") if out else np.zeros(0)
    ranks[ranks < 0] = np.inf
    return ranks


class SequenceRanker:
    """A trained sequence network with its item vocabulary."""

    def __init__(self, kind, net, item_ids, session_col, step_col, item_col, truth_col):
        self.kind = kind
        self.net = net
        self.item_ids = item_ids
        self.cols = (session_col, step_col, item_col, truth_col)

    def ranks(self, pdf):
        session_col, step_col, item_col, truth_col = self.cols
        ordered = pdf.sort_values([session_col, step_col]).reset_index(drop=True)
        hist = build_histories(
            ordered, self.item_ids, SEQ_LENGTH, session_col, item_col
        )
        truth = (
            ordered[truth_col].map(self.item_ids).fillna(0).astype("int64").to_numpy()
        )
        return sequence_ranks(self.net, hist, truth)


# COMMAND ----------

# DBTITLE 1,Ranking runner


def run_ranking(ctx, df, spec):
    """Popularity baseline and the co-occurrence and sequence candidates.

    spec: session_col, step_col, src_col, truth_col, key_cols, models."""
    sc, tc, ic, yc = (
        spec["session_col"],
        spec["step_col"],
        spec["src_col"],
        spec["truth_col"],
    )
    cols = list(dict.fromkeys([sc, tc, ic, yc, "partition"]))
    pdf, fraction = to_training_frame(df, key_cols=[sc], columns=cols)
    pdf = pdf.dropna(subset=[yc, ic]).sort_values([sc, tc]).reset_index(drop=True)
    tr_m = (pdf["partition"] == "train").to_numpy()
    va_m = (pdf["partition"] == "validation").to_numpy()
    if not tr_m.any() or not va_m.any():
        raise RuntimeError("need train and validation rows")
    train, valid = pdf[tr_m], pdf[va_m]
    rng = np.random.default_rng(MODEL_SEED)
    if len(valid) > SEQ_EVAL_ROWS:
        chosen = (
            valid[sc]
            .drop_duplicates()
            .sample(frac=SEQ_EVAL_ROWS / len(valid), random_state=MODEL_SEED)
        )
        valid = valid[valid[sc].isin(chosen)]
    start_task(ctx)

    pop = fit_popularity(train, yc)
    run_candidate(
        ctx,
        "popularity",
        "baseline",
        lambda: (pop, ranking_metrics(pop.ranks(valid), RANK_KS), len(valid)),
        params={"row_fraction": round(fraction, 4)},
        stage="baseline",
    )
    if "cooccurrence" in spec["models"]:
        cooc = fit_cooccurrence(train, ic, yc, pop)
        run_candidate(
            ctx,
            "cooccurrence",
            "ranking",
            lambda: (cooc, ranking_metrics(cooc.ranks(valid), RANK_KS), len(valid)),
            params={"top": COOCCURRENCE_TOP},
        )

    items = pd.concat([train[ic], train[yc]]).value_counts().head(SEQ_VOCAB).index
    item_ids = {item: i + 1 for i, item in enumerate(items)}
    hist_tr = build_histories(train, item_ids, SEQ_LENGTH, sc, ic)
    target_tr = train[yc].map(item_ids).fillna(0).astype("int64").to_numpy()
    usable = np.flatnonzero(target_tr > 0)
    if len(usable) > SEQ_TRAIN_ROWS:
        usable = np.sort(rng.choice(usable, SEQ_TRAIN_ROWS, replace=False))

    for kind, name in (
        ("gru", "sequence_gru"),
        ("transformer", "sequence_transformer"),
    ):
        if name not in spec["models"]:
            continue
        params = {
            "vocab": len(item_ids),
            "dim": SEQ_DIM,
            "epochs": SEQ_EPOCHS,
            "history": SEQ_LENGTH,
            "train_rows": len(usable),
            "eval_rows": len(valid),
            "row_fraction": round(fraction, 4),
        }

        def fit(kind=kind):
            net = _train_sequence(
                kind, hist_tr[usable], target_tr[usable], len(item_ids)
            )
            ranker = SequenceRanker(kind, net, item_ids, sc, tc, ic, yc)
            return ranker, ranking_metrics(ranker.ranks(valid), RANK_KS), len(valid)

        run_candidate(ctx, name, "sequence", fit, requires=("torch",), params=params)
    finish_task(ctx)
