# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # KNOWLEDGE EXTENSION -- RERANKER SCORES
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** score the (question, chunk) pairs of `pairs.jsonl` in the `rerank`
# MAGIC volume with the cross-encoder `ms-marco-MiniLM-L6-v2` (22.7 million parameters)
# MAGIC from the `foundation_models` volume and write them to `knowledge.rerank_scores`.
# MAGIC A pair whose key is already in the table is not scored again. Run on standard
# MAGIC serverless compute (CPU). The scores are written every 10 batches. The notebook
# MAGIC stops at the time limit and prints the time of every batch; run it again to
# MAGIC continue with the pairs that have no score.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../../models/lib/_model_common

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
PAIRS = f"/Volumes/{CATALOG}/knowledge/rerank/pairs.jsonl"
MODEL_ROOT = (
    f"/Volumes/{CATALOG}/energy_ml_models/foundation_models/ms_marco_minilm_l6_v2"
)
TABLE = f"{CATALOG}.knowledge.rerank_scores"
MODEL_NAME = "cross-encoder/ms-marco-MiniLM-L6-v2"
BATCH = 16
MAX_LENGTH = 512
WRITE_EVERY = 10
LIMIT_SECONDS = 1800

# COMMAND ----------

# DBTITLE 1,Imports
import time

import pandas as pd
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

# COMMAND ----------

# DBTITLE 1,The compute
print(f"torch {torch.__version__}, {torch.get_num_threads()} thread(s)")

# COMMAND ----------

# DBTITLE 1,The pairs without a stored score
spark.sql(
    f"CREATE TABLE IF NOT EXISTS {TABLE} "
    "(key string, score double, model string, revision string, scored_at timestamp) USING delta"
)
pairs = pd.read_json(PAIRS, lines=True)
done = {row.key for row in spark.table(TABLE).select("key").collect()}
pairs = pairs[~pairs["key"].isin(done)].reset_index(drop=True)
print(f"{len(pairs)} pair(s) to score")

# COMMAND ----------

# DBTITLE 1,The model folder of the pinned revision
revisions = [entry.name.rstrip("/") for entry in dbutils.fs.ls(MODEL_ROOT)]
if len(revisions) != 1:
    raise RuntimeError(
        f"expected one revision folder in {MODEL_ROOT}, found {revisions}"
    )
REVISION = revisions[0]
model_dir = f"{MODEL_ROOT}/{REVISION}"
print(f"revision {REVISION}")

# COMMAND ----------

# DBTITLE 1,Load the cross-encoder
tokenizer = AutoTokenizer.from_pretrained(model_dir)
model = AutoModelForSequenceClassification.from_pretrained(
    model_dir, dtype=torch.float32
)
model.eval()

# COMMAND ----------

# DBTITLE 1,Write a block of scores


def write_scores(keys, scores):
    if not scores:
        return
    out = pd.DataFrame(
        {
            "key": keys,
            "score": scores,
            "model": MODEL_NAME,
            "revision": REVISION,
            "scored_at": pd.Timestamp.now(tz="UTC").tz_localize(None),
        }
    )
    spark.createDataFrame(out).write.format("delta").mode("append").saveAsTable(TABLE)


# COMMAND ----------

# DBTITLE 1,Score the pairs in batches
started = time.monotonic()
block_keys, block_scores, written = [], [], 0
for number, start in enumerate(range(0, len(pairs), BATCH), 1):
    if time.monotonic() - started > LIMIT_SECONDS:
        write_scores(block_keys, block_scores)
        raise RuntimeError(
            f"time limit of {LIMIT_SECONDS} s reached after "
            f"{written + len(block_scores)} pairs; run the notebook again to continue"
        )
    part = pairs.iloc[start : start + BATCH]
    batch = tokenizer(
        part["query"].tolist(),
        part["text"].tolist(),
        padding=True,
        truncation=True,
        max_length=MAX_LENGTH,
        return_tensors="pt",
    )
    with torch.no_grad():
        logits = model(**batch).logits.view(-1).float().tolist()
    block_keys += part["key"].tolist()
    block_scores += logits
    print(
        f"{written + len(block_scores)}/{len(pairs)} pairs, "
        f"{time.monotonic() - started:.0f} s"
    )
    if number % WRITE_EVERY == 0:
        write_scores(block_keys, block_scores)
        written += len(block_scores)
        block_keys, block_scores = [], []

# COMMAND ----------

# DBTITLE 1,Write the last block
write_scores(block_keys, block_scores)
written += len(block_scores)
print(f"{written} score(s) written to {TABLE}")
