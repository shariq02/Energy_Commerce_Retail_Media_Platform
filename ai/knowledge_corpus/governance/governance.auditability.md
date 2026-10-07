---
unit_id: governance.auditability
kind: governance
ecosystem: shared
source:
- name: governance_design
  sha256: eff32be5623c6cd21edad505ff965a7f5d24cc1568bbb88f55d0023d31fbb9c1
version: 1
last_updated: '2026-10-07'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: adc5bf9b8d0ec2cf10cc7b5a23352bde88d418442c98a4afbe00a4d33783cca5
---

# Governance: auditability

The tables `pipeline_watermarks` and `quality_audit_log` give run-level audit history for every layer transition.

Every AI tool call is written to the append-only `ai_tool_call_log`. A record holds `trace_id`, `timestamp`, `agent`, `mcp_server`, `tool_name`, `argument_hash`, `result_status`, `duration_ms` and `retry_attempt`. The log is stored locally and exported in batches to the PostgreSQL serving layer. It is not a live write per request.

There is no orchestration run history, because the project uses no orchestration platform.
