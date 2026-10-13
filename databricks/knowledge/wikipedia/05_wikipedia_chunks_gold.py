# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # KNOWLEDGE GOLD -- WIKIPEDIA CHUNKS AND VECTORS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** cut the sections of `knowledge.wikipedia_selected` into chunks of
# MAGIC at most 256 tokens and embed each chunk with the all-MiniLM-L6-v2 ONNX model
# MAGIC from the `models` volume. The model files must match `model_hashes.json`,
# MAGIC written there by the upload command from the local model folder. One row per
# MAGIC chunk. The rows go to a staging table first; `knowledge.wikipedia_chunks` is
# MAGIC replaced only after the gates pass. Writes its findings. Run on serverless GPU
# MAGIC compute. Re-run after notebook 02.

# COMMAND ----------

# DBTITLE 1,Remove the CPU runtime (only one ONNX Runtime package may be installed)
# MAGIC %pip uninstall -y onnxruntime

# COMMAND ----------

# DBTITLE 1,GPU runtime with its CUDA and cuDNN libraries, and the tokenizer
# MAGIC %pip install onnxruntime-gpu[cuda,cudnn]==1.28.0 tokenizers==0.23.1

# COMMAND ----------

# DBTITLE 1,Text library
# MAGIC %run ./_wikipedia_text

# COMMAND ----------

# DBTITLE 1,Shared library
# MAGIC %run ./_wikipedia_common

# COMMAND ----------

# DBTITLE 1,Inspection library
# MAGIC %run ./_wikipedia_inspect

# COMMAND ----------

# DBTITLE 1,Chunk and embedding library
# MAGIC %run ./_wikipedia_chunks

# COMMAND ----------

# DBTITLE 1,Imports
import json
import time

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = FINDINGS_SOURCE
COMPONENT = "knowledge/wikipedia/05_wikipedia_chunks_gold"
RID = run_id()
TABLE = "wikipedia_chunks"
BATCH_SIZE = 256
# Rows per write; keeps each upload of vectors small.
WRITE_SLICE = 20000
REQUIRE_GPU = True
SCHEMA_DDL = (
    "chunk_id string, article_id bigint, piece int, section string, "
    "section_index int, text string, token_count int, text_hash string, "
    "article_content_hash string, vector array<float>"
)

# COMMAND ----------

# DBTITLE 1,Gate -- model files match the hashes of the local model
with open(f"{MODEL_DIR}/{MODEL_HASHES_FILE}", encoding="utf-8") as handle:
    pinned = json.load(handle)
found = {
    "model_onnx_sha256": file_sha256(f"{MODEL_DIR}/{MODEL_FILE}"),
    "tokenizer_sha256": file_sha256(f"{MODEL_DIR}/{TOKENIZER_FILE}"),
}
check(
    COMPONENT,
    SOURCE,
    "model_files_match_hashes",
    found == pinned,
    detail=f"found={found} pinned={pinned}",
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Load the tokenizer and the model
counter = token_counter(load_tokenizer(MODEL_DIR))
padded = load_tokenizer(MODEL_DIR, padded=True)
session, provider = load_session(MODEL_DIR)
check(
    COMPONENT,
    SOURCE,
    "gpu_provider",
    provider == "CUDAExecutionProvider" or not REQUIRE_GPU,
    detail=f"provider={provider}",
    rid=RID,
)
print(f"OK  execution provider: {provider}")

# COMMAND ----------

# DBTITLE 1,Read the selected articles
selected = spark.table(GOLD_TABLE).select(
    "article_id", "title", "sections", "terms_hash"
)
selection_hashes = [r[0] for r in selected.select("terms_hash").distinct().collect()]
assert len(selection_hashes) == 1, (
    f"selected articles hold {len(selection_hashes)} rule hashes"
)
# About 33,000 articles; the GPU node holds them in memory.
articles = selected.drop("terms_hash").toPandas()
print(f"OK  articles read: {len(articles)}")

# COMMAND ----------

# DBTITLE 1,Cut the sections into chunks
started = time.monotonic()
rows, sections_read, left_out, dropped = [], 0, 0, 0
for article in articles.itertuples(index=False):
    sections = normalize_sections(article.sections)
    sections_read += len(sections)
    pieces, long_count, drop_count = chunk_article(
        article.article_id, article.title, sections, counter
    )
    rows.extend(pieces)
    left_out += long_count
    dropped += drop_count
chunk_seconds = time.monotonic() - started
print(
    f"OK  {len(rows)} chunks from {sections_read} sections in {chunk_seconds:.0f} s; "
    f"link-list sections dropped {dropped}, sentences left out {left_out}"
)

# COMMAND ----------

# DBTITLE 1,Embed the chunks
started = time.monotonic()
vectors = embed_texts(session, padded, [row["text"] for row in rows], BATCH_SIZE)
embed_seconds = time.monotonic() - started
norm_error = float(np.abs(np.linalg.norm(vectors, axis=1) - 1.0).max())
print(
    f"OK  {len(rows)} vectors in {embed_seconds:.0f} s "
    f"({len(rows) / max(embed_seconds, 1e-9):.0f} chunks/s)"
)

# COMMAND ----------

# DBTITLE 1,Gate -- every vector has length 1
check(
    COMPONENT,
    SOURCE,
    "vectors_normalised",
    norm_error < 1e-3,
    detail=f"max_norm_error={norm_error:.6f}",
    metric_value=norm_error,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write the staging table
for start in range(0, len(rows), WRITE_SLICE):
    part = [
        {**row, "vector": vectors[start + offset].tolist()}
        for offset, row in enumerate(rows[start : start + WRITE_SLICE])
    ]
    frame = (
        spark.createDataFrame(part, SCHEMA_DDL)
        .withColumn("selection_terms_hash", F.lit(selection_hashes[0]))
        .withColumn("model_onnx_sha256", F.lit(found["model_onnx_sha256"]))
        .withColumn("tokenizer_sha256", F.lit(found["tokenizer_sha256"]))
    )
    frame = add_knowledge_provenance(frame, GOLD_TABLE.split(".")[-1], RID)
    writer = frame.write.mode("overwrite" if start == 0 else "append")
    if start == 0:
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(CHUNKS_STAGING_TABLE)
print(f"OK  staging table written: {CHUNKS_STAGING_TABLE}")

# COMMAND ----------

# DBTITLE 1,Read back the staging table
written = spark.table(CHUNKS_STAGING_TABLE)
total = written.count()
print(f"OK  chunks staged: {total}")

# COMMAND ----------

# DBTITLE 1,Gate -- one row per chunk id and all rows written
duplicates = written.groupBy("chunk_id").count().filter(F.col("count") > 1).count()
check(
    COMPONENT,
    SOURCE,
    "chunk_id_duplicates",
    duplicates == 0 and total == len(rows),
    detail=f"duplicate_groups={duplicates} written={total} expected={len(rows)}",
    metric_value=duplicates,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Gate -- vector size and token limit
bad_rows = written.filter(
    (F.size("vector") != DIMENSION) | (F.col("token_count") > MAX_TOKENS)
).count()
check(
    COMPONENT,
    SOURCE,
    "vector_size_and_token_limit",
    bad_rows == 0,
    detail=f"bad_rows={bad_rows}",
    metric_value=bad_rows,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Count the articles without a chunk (all their text was left out)
without_chunk = (
    selected.select("article_id")
    .join(written.select("article_id").distinct(), "article_id", "left_anti")
    .count()
)
print(f"INFO articles without a chunk: {without_chunk}")

# COMMAND ----------

# DBTITLE 1,Gate -- at least one chunk
check(
    COMPONENT,
    SOURCE,
    "chunks_written",
    total > 0,
    detail=f"chunks={total}",
    metric_value=total,
    rid=RID,
)

# COMMAND ----------

# DBTITLE 1,Write Gold -- replace wikipedia_chunks in one step after the gates
spark.sql(
    f"CREATE OR REPLACE TABLE {CHUNKS_TABLE} AS SELECT * FROM {CHUNKS_STAGING_TABLE}"
)
spark.sql(f"DROP TABLE {CHUNKS_STAGING_TABLE}")
written = spark.table(CHUNKS_TABLE)
print(f"OK  table written: {CHUNKS_TABLE}")

# COMMAND ----------

# DBTITLE 1,Inspect -- wikipedia_chunks
findings_blocks = inspect_knowledge_table(
    written,
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=RID,
    key_cols=["chunk_id"],
    extra_checks={
        "articles_read": len(articles),
        "sections_read": sections_read,
        "link_list_sections_dropped": dropped,
        "sentences_left_out": left_out,
        "chunks_written": total,
        "articles_without_chunk": without_chunk,
        "chunk_seconds": round(chunk_seconds),
        "embed_seconds": round(embed_seconds),
        "chunks_per_second": round(len(rows) / max(embed_seconds, 1e-9)),
        "execution_provider": provider,
        "max_norm_error": norm_error,
        "model_onnx_sha256": found["model_onnx_sha256"],
        "tokenizer_sha256": found["tokenizer_sha256"],
        "selection_terms_hash": selection_hashes[0],
    },
)

# COMMAND ----------

# DBTITLE 1,Export findings -- wikipedia_chunks
write_knowledge_findings(
    SOURCE, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, findings_blocks
)
