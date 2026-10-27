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
# MAGIC volume with the cross-encoder `bge-reranker-v2-m3` from the `foundation_models`
# MAGIC volume and write them to `knowledge.rerank_scores`. A pair whose key is already
# MAGIC in the table is not scored again. Run on serverless GPU compute. It stops at the
# MAGIC time limit and prints the time of every batch.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "energy_commerce_retail_media"
PAIRS = f"/Volumes/{CATALOG}/knowledge/rerank/pairs.jsonl"
MODEL_ROOT = f"/Volumes/{CATALOG}/energy_ml_models/foundation_models/bge_reranker_v2_m3"
TABLE = f"{CATALOG}.knowledge.rerank_scores"
MODEL_NAME = "BAAI/bge-reranker-v2-m3"
BATCH = 16
MAX_LENGTH = 512
LIMIT_SECONDS = 1800

# COMMAND ----------

# DBTITLE 1,Imports
import time

import pandas as pd
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

# COMMAND ----------

# DBTITLE 1,The device
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"device {device}")
if device == "cpu":
    raise RuntimeError("no GPU: attach serverless GPU compute")

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
    model_dir, torch_dtype=torch.float16
)
model.to(device).eval()

# COMMAND ----------

# DBTITLE 1,Score the pairs in batches
started = time.monotonic()
scores = []
for start in range(0, len(pairs), BATCH):
    if time.monotonic() - started > LIMIT_SECONDS:
        raise RuntimeError(
            f"time limit of {LIMIT_SECONDS} s reached after {len(scores)} pairs"
        )
    part = pairs.iloc[start : start + BATCH]
    batch = tokenizer(
        part["query"].tolist(),
        part["text"].tolist(),
        padding=True,
        truncation=True,
        max_length=MAX_LENGTH,
        return_tensors="pt",
    ).to(device)
    with torch.no_grad():
        logits = model(**batch).logits.view(-1).float().cpu().tolist()
    scores += logits
    print(f"{len(scores)}/{len(pairs)} pairs, {time.monotonic() - started:.0f} s")

# COMMAND ----------

# DBTITLE 1,Write the scores
if scores:
    out = pd.DataFrame(
        {
            "key": pairs["key"],
            "score": scores,
            "model": MODEL_NAME,
            "revision": REVISION,
            "scored_at": pd.Timestamp.now(tz="UTC").tz_localize(None),
        }
    )
    spark.createDataFrame(out).write.format("delta").mode("append").saveAsTable(TABLE)
print(f"{len(scores)} score(s) written to {TABLE}")
