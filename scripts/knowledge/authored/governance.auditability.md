# Governance: auditability

The tables `pipeline_watermarks` and `quality_audit_log` give run-level audit history for every layer transition.

Every AI tool call is written to the append-only `ai_tool_call_log`. A record holds `trace_id`, `timestamp`, `agent`, `mcp_server`, `tool_name`, `argument_hash`, `result_status`, `duration_ms` and `retry_attempt`. The log is stored locally and exported in batches to the PostgreSQL serving layer. It is not a live write per request.

There is no orchestration run history, because the project uses no orchestration platform.
