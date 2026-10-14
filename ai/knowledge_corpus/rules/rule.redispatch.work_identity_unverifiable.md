---
unit_id: rule.redispatch.work_identity_unverifiable
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
  approved_content_hash: daa5490359c059fa02be42bf2a3435afcfd1481ceea3b69049542e5b6a844e2e
---

# Rule: work_identity_unverifiable (redispatch)

**Expression:** GESAMTE_ARBEIT_MWH ~ mean power x duration could not be checked (0/0 rows) -- documented limitation, no rule enforced

**Severity:** info
