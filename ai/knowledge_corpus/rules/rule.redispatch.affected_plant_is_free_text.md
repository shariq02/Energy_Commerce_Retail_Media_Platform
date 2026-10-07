---
unit_id: rule.redispatch.affected_plant_is_free_text
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

# Rule: affected_plant_is_free_text (redispatch)

**Expression:** BETROFFENE_ANLAGE is a free-text name -- linking to power_plant_list / MaStR is a Silver fuzzy-match with its own match rate, never a keyed join

**Severity:** block
