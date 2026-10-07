---
unit_id: rule.redispatch.collinear_measures
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

# Rule: collinear_measures (redispatch)

**Expression:** MITTLERE_LEISTUNG_MW, MAXIMALE_LEISTUNG_MW and GESAMTE_ARBEIT_MWH are near-collinear -- do not use all three as independent features

**Severity:** info
