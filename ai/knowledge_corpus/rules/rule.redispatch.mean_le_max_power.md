---
unit_id: rule.redispatch.mean_le_max_power
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
  approved_content_hash: 5e24c23e04e444aecdcfd9726ed307e4367a54c822e0f35dcd5c7cfcdd5655e3
---

# Rule: mean_le_max_power (redispatch)

**Expression:** MITTLERE_LEISTUNG_MW <= MAXIMALE_LEISTUNG_MW

**Severity:** warn
