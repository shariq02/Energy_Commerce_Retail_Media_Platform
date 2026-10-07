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
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: af7dea3300831981c019d9d8ac48f312cbd69e56e19e1b3a394d946338450944
---

# Rule: collinear_measures (redispatch)

**Expression:** MITTLERE_LEISTUNG_MW, MAXIMALE_LEISTUNG_MW and GESAMTE_ARBEIT_MWH are near-collinear -- do not use all three as independent features

**Severity:** info
