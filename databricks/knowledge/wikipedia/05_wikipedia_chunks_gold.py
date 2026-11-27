# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # KNOWLEDGE GOLD -- WIKIPEDIA CHUNKS AND VECTORS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** embed Wikipedia text with the all-MiniLM-L6-v2 ONNX model from the
# MAGIC `models` volume. The model files must match `model_hashes.json`, written there by
# MAGIC the upload command from the local model folder. `PARTS` names what to build.
# MAGIC `chunks`: cut the sections of `knowledge.wikipedia_selected` into chunks of at
# MAGIC most 256 tokens and embed each one; one row per chunk; the rows go to a staging
# MAGIC table first and `knowledge.wikipedia_chunks` is replaced only after the gates pass
# MAGIC (run on serverless GPU compute, after notebook 02). `entity_leads`: embed the first
# MAGIC chunk of the lead of each article that `genomics_gold.entity_article_link` links to
# MAGIC a gene, drug or disease (type check passed), and keep the first 16 principal
# MAGIC components of these vectors as numeric columns for ML in
# MAGIC `knowledge.wikipedia_entity_lead` (the fit uses the vectors only, never a label;
# MAGIC runs on CPU compute, after the Gold genomics entities). Writes its findings.

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
import time

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = FINDINGS_SOURCE
COMPONENT = "knowledge/wikipedia/05_wikipedia_chunks_gold"
RID = run_id()
# what to build: "chunks" needs the GPU, "entity_leads" runs on CPU
PARTS = ("chunks", "entity_leads")
TABLE = "wikipedia_chunks"
BATCH_SIZE = 256
# Rows per write; keeps each upload of vectors small.
WRITE_SLICE = 20000
REQUIRE_GPU = "chunks" in PARTS
SCHEMA_DDL = (
    "chunk_id string, article_id bigint, piece int, section string, "
    "section_index int, text string, token_count int, text_hash string, "
    "article_content_hash string, vector array<float>"
)
LEAD_TABLE = "wikipedia_entity_lead"
COMPONENT_COUNT = 16
COMPONENT_COLUMNS = [f"component_{i:02d}" for i in range(1, COMPONENT_COUNT + 1)]
LEAD_DDL = (
    "article_id bigint, token_count int, text_hash string, vector array<float>, "
    + ", ".join(f"{name} double" for name in COMPONENT_COLUMNS)
)

# COMMAND ----------

# DBTITLE 1,Gate -- model files match the hashes of the local model
found, pinned = model_hash_check(MODEL_DIR, MODEL_HASHES_FILE)
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
if "chunks" in PARTS:
    selected = spark.table(GOLD_TABLE).select(
        "article_id", "title", "sections", "terms_hash"
    )
    selection_hashes = [
        r[0] for r in selected.select("terms_hash").distinct().collect()
    ]
    assert len(selection_hashes) == 1, (
        f"selected articles hold {len(selection_hashes)} rule hashes"
    )
    # About 33,000 articles; the GPU node holds them in memory.
    articles = selected.drop("terms_hash").toPandas()
    print(f"OK  articles read: {len(articles)}")

# COMMAND ----------

# DBTITLE 1,Cut the sections into chunks
if "chunks" in PARTS:
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
        f"OK  {len(rows)} chunks from {sections_read} sections in "
        f"{chunk_seconds:.0f} s; link-list sections dropped {dropped}, "
        f"sentences left out {left_out}"
    )

# COMMAND ----------

# DBTITLE 1,Embed the chunks
if "chunks" in PARTS:
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
if "chunks" in PARTS:
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
if "chunks" in PARTS:
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
if "chunks" in PARTS:
    written = spark.table(CHUNKS_STAGING_TABLE)
    total = written.count()
    print(f"OK  chunks staged: {total}")

# COMMAND ----------

# DBTITLE 1,Gate -- one row per chunk id and all rows written
if "chunks" in PARTS:
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
if "chunks" in PARTS:
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
if "chunks" in PARTS:
    without_chunk = (
        selected.select("article_id")
        .join(written.select("article_id").distinct(), "article_id", "left_anti")
        .count()
    )
    print(f"INFO articles without a chunk: {without_chunk}")

# COMMAND ----------

# DBTITLE 1,Gate -- at least one chunk
if "chunks" in PARTS:
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
if "chunks" in PARTS:
    spark.sql(
        f"CREATE OR REPLACE TABLE {CHUNKS_TABLE} AS SELECT * FROM {CHUNKS_STAGING_TABLE}"
    )
    spark.sql(f"DROP TABLE {CHUNKS_STAGING_TABLE}")
    written = spark.table(CHUNKS_TABLE)
    print(f"OK  table written: {CHUNKS_TABLE}")

# COMMAND ----------

# DBTITLE 1,Inspect -- wikipedia_chunks
if "chunks" in PARTS:
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
if "chunks" in PARTS:
    write_knowledge_findings(
        SOURCE, f"{COMPONENT.split('/')[-1]}__{TABLE}", TABLE, findings_blocks
    )

# COMMAND ----------

# DBTITLE 1,Entity leads -- cut and embed the first chunk of each linked lead
if "entity_leads" in PARTS:
    leads = (
        spark.table(ENTITY_LINK_TABLE)
        .filter(F.col("type_check_passed"))
        .select("article_id", "lead_text")
        .dropDuplicates(["article_id"])
        .toPandas()
    )
    lead_left_out, lead_rows = [], []
    for lead in leads.itertuples(index=False):
        pieces = split_section(lead.lead_text, counter, lead_left_out.append)
        if pieces:
            lead_rows.append((int(lead.article_id), pieces[0]))
    started = time.monotonic()
    lead_vectors = embed_texts(
        session, padded, [text for _, text in lead_rows], BATCH_SIZE
    )
    lead_seconds = time.monotonic() - started
    lead_norm_error = float(np.abs(np.linalg.norm(lead_vectors, axis=1) - 1.0).max())
    print(
        f"OK  {len(lead_rows)} of {len(leads)} linked leads embedded in "
        f"{lead_seconds:.0f} s; sentences left out {len(lead_left_out)}"
    )
    check(
        COMPONENT,
        SOURCE,
        "lead_vectors_normalised",
        lead_norm_error < 1e-3,
        detail=f"max_norm_error={lead_norm_error:.6f}",
        metric_value=lead_norm_error,
        rid=RID,
    )

# COMMAND ----------

# DBTITLE 1,Entity leads -- reduce the vectors to 16 components and write the table
if "entity_leads" in PARTS:
    scores, variance_share = reduce_components(lead_vectors, COMPONENT_COUNT)
    print(f"OK  {COMPONENT_COUNT} components hold {variance_share:.1%} of the variance")
    lead_frame = spark.createDataFrame(
        [
            (
                article_id,
                counter(text),
                sha256_text(text),
                lead_vectors[index].tolist(),
                *[float(value) for value in scores[index]],
            )
            for index, (article_id, text) in enumerate(lead_rows)
        ],
        LEAD_DDL,
    )
    lead_frame = add_knowledge_provenance(
        lead_frame, ENTITY_LINK_TABLE.split(".")[-1], RID
    )
    (
        lead_frame.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(ENTITY_LEAD_TABLE)
    )
    lead_written = spark.table(ENTITY_LEAD_TABLE)
    lead_total = lead_written.count()
    print(f"OK  table written: {ENTITY_LEAD_TABLE} ({lead_total} rows)")

# COMMAND ----------

# DBTITLE 1,Entity leads -- gates
if "entity_leads" in PARTS:
    lead_duplicates = (
        lead_written.groupBy("article_id").count().filter(F.col("count") > 1).count()
    )
    check(
        COMPONENT,
        SOURCE,
        "lead_article_id_duplicates",
        lead_duplicates == 0 and lead_total == len(lead_rows) and lead_total > 0,
        detail=(
            f"duplicate_groups={lead_duplicates} written={lead_total} "
            f"expected={len(lead_rows)}"
        ),
        metric_value=lead_duplicates,
        rid=RID,
    )
    missing_component = F.greatest(
        *[F.col(name).isNull().cast("int") for name in COMPONENT_COLUMNS]
    )
    lead_bad_rows = lead_written.filter(
        (F.size("vector") != DIMENSION)
        | (F.col("token_count") > MAX_TOKENS)
        | (missing_component > 0)
    ).count()
    check(
        COMPONENT,
        SOURCE,
        "lead_vector_size_token_limit_and_components",
        lead_bad_rows == 0,
        detail=f"bad_rows={lead_bad_rows}",
        metric_value=lead_bad_rows,
        rid=RID,
    )

# COMMAND ----------

# DBTITLE 1,Entity leads -- inspect and export findings
if "entity_leads" in PARTS:
    lead_blocks = inspect_knowledge_table(
        lead_written,
        LEAD_TABLE,
        source=SOURCE,
        component=COMPONENT,
        rid=RID,
        key_cols=["article_id"],
        extra_checks={
            "leads_read": len(leads),
            "lead_chunks_embedded": len(lead_rows),
            "sentences_left_out": len(lead_left_out),
            "embed_seconds": round(lead_seconds),
            "execution_provider": provider,
            "max_norm_error": lead_norm_error,
            "component_variance_share": round(variance_share, 4),
            "model_onnx_sha256": found["model_onnx_sha256"],
            "tokenizer_sha256": found["tokenizer_sha256"],
        },
    )
    write_knowledge_findings(
        SOURCE, f"{COMPONENT.split('/')[-1]}__{LEAD_TABLE}", LEAD_TABLE, lead_blocks
    )
