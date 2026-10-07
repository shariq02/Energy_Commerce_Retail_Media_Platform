---
unit_id: rule.redispatch.requesting_tso_combinations
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

# Rule: requesting_tso_combinations (redispatch)

**Expression:** ANFORDERNDER_UENB values may be '&'-joined TSO combinations -- split to a list at Silver before any per-TSO aggregation

**Severity:** warn
