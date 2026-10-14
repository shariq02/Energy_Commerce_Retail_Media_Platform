---
unit_id: pipeline.execution_model
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
  approved_content_hash: 5e7c3d54dfe57e4ec5133bf9e5e189025cd81fd73203e1c4ddbe6a3dbd84fb9e
---

# Pipeline: execution model

The project uses no orchestration or scheduling platform. The operator runs the Databricks notebooks and the local scripts directly, in the required order.

Rerun safety and the layer-transition gates belong to the notebooks and to the `quality` schema (`pipeline_watermarks` and `quality_audit_log`). They do not belong to an external scheduler.
