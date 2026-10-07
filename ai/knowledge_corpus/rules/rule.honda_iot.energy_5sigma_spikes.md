---
unit_id: rule.honda_iot.energy_5sigma_spikes
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/honda_iot.yml
  sha256: ecce2378681f571721ff13c94ef4488889694aecc7508f455237a1737e27b568
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: Sharique
  approved_at: '2026-10-06'
  approved_version: 1
---

# Rule: energy_5sigma_spikes (honda_iot)

**Expression:** 5-sigma value spikes present on several _p.total columns -- flag, keep raw

**Severity:** warn
