---
unit_id: pipeline.idempotency
kind: pipeline
ecosystem: shared
source:
- name: pipeline_design
  sha256: af21799da1175fa1cf39358407c5d1068c455632d84ef5c7a61826c76b202fd1
version: 1
last_updated: '2026-10-07'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 7b176cfdc7132462aa669ff2b9a82192954099ca4bcfcacea0149dc89e7e346f
---

# Pipeline: idempotency

No source is replayed as streaming events. Batch acquisition is safe to re-run: a download, a BigQuery export or a Bronze load must not duplicate records.

Silver, Gold and Analytics notebooks write a deterministic full `overwrite` with `overwriteSchema`, so processing the same input again changes nothing. `source_record_id` is a deterministic natural key or a `sha2` composite. It is never `monotonically_increasing_id()`. The Delta command `RESTORE TABLE ... TO VERSION AS OF n` is printed before each overwrite.
