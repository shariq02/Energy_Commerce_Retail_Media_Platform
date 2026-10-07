---
unit_id: rule.redispatch.drop_constants
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
  approved_content_hash: 3f64c320350df3b628f7da2d252b27ad9048a1d6b38b8f79ff67ed5846dd10b3
---

# Rule: drop_constants (redispatch)

**Expression:** coverage_regime / ZEITZONE_VON / ZEITZONE_BIS are constant -- drop at Silver (a mixed-regime future extract would make coverage_regime a process feature)

**Severity:** warn
