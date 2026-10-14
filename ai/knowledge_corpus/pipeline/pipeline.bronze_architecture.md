---
unit_id: pipeline.bronze_architecture
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
  approved_content_hash: 8339f6c16a842d0a1603fb86c7d0611d57d18c440fd9f44b39f1eef29c1207da
---

# Pipeline: Bronze architecture and logical datasets

A logical dataset is not a physical file. One logical dataset can span many staging files. A large file never becomes many datasets automatically. Many small files with one schema never become many tables.

Three counts are never the same number: the staging dataset count, the physical chunk count and the Bronze table count. Chunking exists only for upload. It never changes dataset boundaries.

Bronze rules:
1. A staging dataset is not automatically a Bronze table.
2. Chunk boundaries carry no schema meaning.
3. One notebook per source reads every chunk in one scan and writes the Bronze tables.
4. Bronze stays 1:1 with the staged shape. Schema merges wait for Silver.
5. Table names are flat: `<catalog>.bronze.<source_prefix>_<dataset>`.

The notebook checks the schema and a non-zero row count. It drops the source's Volume only after a full pass. The four Samples datasets have no Bronze tables.
