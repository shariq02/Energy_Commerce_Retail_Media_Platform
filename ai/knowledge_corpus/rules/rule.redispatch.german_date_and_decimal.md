---
unit_id: rule.redispatch.german_date_and_decimal
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/redispatch.yml
  sha256: 99e2b9a9b512cfcc410bde881c833587a281acfd4d04fe495530fc8a8571e9cc
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 765a628b232ae03deaaa167fd5b22afba9f30e5ed2dca7ad2abdc721f6f1d931
---

# Rule: german_date_and_decimal (redispatch)

**Expression:** BEGINN_DATUM / ENDE_DATUM are dd.MM.yyyy; MW / MWh columns use the German comma decimal -- parse explicitly, quarantine failures

**Severity:** block
