# Pipeline: idempotency

No source is replayed as streaming events. Batch acquisition is safe to re-run: a download, a BigQuery export or a Bronze load must not duplicate records.

Silver, Gold and Analytics notebooks write a deterministic full `overwrite` with `overwriteSchema`, so processing the same input again changes nothing. `source_record_id` is a deterministic natural key or a `sha2` composite. It is never `monotonically_increasing_id()`. The Delta command `RESTORE TABLE ... TO VERSION AS OF n` is printed before each overwrite.
