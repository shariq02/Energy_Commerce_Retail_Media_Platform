# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION RANKING LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** a SASRec-type next-item model (causal self-attention, item
# MAGIC embeddings shared with the output layer) trained on the train sessions, as an
# MAGIC extra candidate for the next-item tasks. Pulled in with
# MAGIC `%run ../lib/_fit_foundation_rank` after `_fit_foundation_ts` and `_fit_ranking`.
# MAGIC No public weights are used: item embeddings are specific to the platform data.

# COMMAND ----------

# DBTITLE 1,Imports
import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Settings and the next-item tasks
RANK_FAMILY = "sequence"
SASREC_NAME = "sasrec_sequence"
SASREC_DIM = 64
SASREC_HEADS = 2
SASREC_LAYERS = 2
SASREC_DROPOUT = 0.2
SASREC_EPOCHS = 3
SASREC_LEARNING_RATE = 1e-3
_RANK_SPEC = {
    "session_col": "session_key",
    "step_col": "step",
    "src_col": "seq_product_id",
    "truth_col": "target_next_product_id",
    "key_cols": ["session_key", "step"],
}
RANK_TASKS = {
    "next_item_ga4.next_item": {"spec": _RANK_SPEC},
    "next_item_rees46.next_item": {"spec": _RANK_SPEC},
}

# COMMAND ----------

# DBTITLE 1,The network and its training


def _make_sasrec(vocab, dim=SASREC_DIM, length=None):
    import torch
    from torch import nn

    length = length or SEQ_LENGTH

    class Net(nn.Module):
        def __init__(self):
            super().__init__()
            self.emb = nn.Embedding(vocab + 1, dim, padding_idx=0)
            self.pos = nn.Parameter(torch.zeros(length, dim))
            layer = nn.TransformerEncoderLayer(
                d_model=dim,
                nhead=SASREC_HEADS,
                dim_feedforward=2 * dim,
                dropout=SASREC_DROPOUT,
                batch_first=True,
                norm_first=True,
            )
            self.enc = nn.TransformerEncoder(
                layer, num_layers=SASREC_LAYERS, enable_nested_tensor=False
            )
            self.norm = nn.LayerNorm(dim)
            causal = torch.triu(torch.ones(length, length, dtype=torch.bool), 1)
            self.register_buffer("causal", causal)

        def forward(self, x):
            h = self.emb(x) * dim**0.5 + self.pos
            h = self.enc(h, mask=self.causal)
            return self.norm(h[:, -1]) @ self.emb.weight.T

    return Net()


def _train_sasrec(hist, target_ids, vocab):
    import torch

    torch.manual_seed(MODEL_SEED)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    net = _make_sasrec(vocab).to(device)
    opt = torch.optim.Adam(net.parameters(), lr=SASREC_LEARNING_RATE)
    loss_fn = torch.nn.CrossEntropyLoss(ignore_index=0)
    x = torch.as_tensor(hist, dtype=torch.long)
    y = torch.as_tensor(target_ids, dtype=torch.long)
    net.train()
    for epoch in range(SASREC_EPOCHS):
        print(f"  .. SASRec epoch {epoch + 1}/{SASREC_EPOCHS} on {device}")
        order = torch.randperm(len(x))
        for i in range(0, len(x), SEQ_BATCH):
            idx = order[i : i + SEQ_BATCH]
            opt.zero_grad()
            loss_fn(net(x[idx].to(device)), y[idx].to(device)).backward()
            opt.step()
    return net.cpu().eval()


# COMMAND ----------

# DBTITLE 1,Runner -- the SASRec candidate on the validation sessions


def run_foundation_ranking(ctx, df, models=None) -> None:
    """The SASRec-type model on the validation rows of a next-item task, with the
    sessions, the item vocabulary and the row sample of the accepted rankers."""
    spec = RANK_TASKS[ctx.task_id]["spec"]
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
    if len(valid) > SEQ_EVAL_ROWS:
        chosen = (
            valid[sc]
            .drop_duplicates()
            .sample(frac=SEQ_EVAL_ROWS / len(valid), random_state=MODEL_SEED)
        )
        valid = valid[valid[sc].isin(chosen)]
    train_eval = train
    if len(train) > SEQ_EVAL_ROWS:
        chosen_tr = (
            train[sc]
            .drop_duplicates()
            .sample(frac=SEQ_EVAL_ROWS / len(train), random_state=MODEL_SEED)
        )
        train_eval = train[train[sc].isin(chosen_tr)]
    start_task(ctx)

    items = pd.concat([train[ic], train[yc]]).value_counts().head(SEQ_VOCAB).index
    item_ids = {item: i + 1 for i, item in enumerate(items)}
    hist_tr = build_histories(train, item_ids, SEQ_LENGTH, sc, ic)
    target_tr = train[yc].map(item_ids).fillna(0).astype("int64").to_numpy()
    usable = np.flatnonzero(target_tr > 0)
    if len(usable) > SEQ_TRAIN_ROWS:
        rng = np.random.default_rng(MODEL_SEED)
        usable = np.sort(rng.choice(usable, SEQ_TRAIN_ROWS, replace=False))

    if models is not None and SASREC_NAME not in models:
        return
    params = {
        "vocab": len(item_ids),
        "dim": SASREC_DIM,
        "layers": SASREC_LAYERS,
        "heads": SASREC_HEADS,
        "epochs": SASREC_EPOCHS,
        "history": SEQ_LENGTH,
        "train_rows": len(usable),
        "eval_rows": len(valid),
        "row_fraction": round(fraction, 4),
        "mode": "trained_on_platform_data",
        "seed": MODEL_SEED,
    }

    def fit():
        net = _train_sasrec(hist_tr[usable], target_tr[usable], len(item_ids))
        ranker = SequenceRanker("sasrec", net, item_ids, sc, tc, ic, yc)
        metrics = ranking_metrics(ranker.ranks(valid), RANK_KS)

        def fitted():
            r = ranking_metrics(ranker.ranks(train_eval), RANK_KS)
            return {
                f"train_{k}": r[k]
                for k in ("recall_at_10", "ndcg_at_10", "mrr")
                if k in r
            }

        metrics.update(train_side(fitted))
        return ranker, metrics, len(valid)

    run_candidate(
        ctx, SASREC_NAME, RANK_FAMILY, fit, requires=("torch",), params=params
    )
    finish_foundation_task(ctx)
