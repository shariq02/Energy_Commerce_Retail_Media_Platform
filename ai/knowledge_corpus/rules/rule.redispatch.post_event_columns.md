---
unit_id: rule.redispatch.post_event_columns
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/redispatch.yml
  sha256: 99e2b9a9b512cfcc410bde881c833587a281acfd4d04fe495530fc8a8571e9cc
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: Sharique
  approved_at: '2026-10-06'
  approved_version: 1
---

# Rule: post_event_columns (redispatch)

**Expression:** ENDE_DATUM / ENDE_UHRZEIT / GESAMTE_ARBEIT_MWH are known only after the measure -- never features for predicting whether/when a measure starts

**Severity:** warn
