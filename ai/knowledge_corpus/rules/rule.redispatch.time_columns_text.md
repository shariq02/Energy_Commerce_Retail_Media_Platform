---
unit_id: rule.redispatch.time_columns_text
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

# Rule: time_columns_text (redispatch)

**Expression:** BEGINN_UHRZEIT / ENDE_UHRZEIT are text time-of-day strings -- combine with the matching *_DATUM and convert Europe/Berlin -> UTC at Silver

**Severity:** block
