---
unit_id: rule.mastr.forward_dated_commissioning_ok
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/mastr.yml
  sha256: 60dff1d9abb3b0ae79ad2e2a324f473be5f04f9e659133fcf28dcadccdfc55fc
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 112eac3d59de009b59fdb7d7290077708a73a6ecf0493f6f26f98a95e6446942
---

# Rule: forward_dated_commissioning_ok (mastr)

**Expression:** GeplantesInbetriebnahmedatum and future Inbetriebnahmedatum values are legitimate planned dates -- a column is 'known' only up to its own value (point-in-time)

**Severity:** info
