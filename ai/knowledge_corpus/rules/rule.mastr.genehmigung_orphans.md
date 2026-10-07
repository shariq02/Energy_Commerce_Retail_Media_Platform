---
unit_id: rule.mastr.genehmigung_orphans
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/mastr.yml
  sha256: 60dff1d9abb3b0ae79ad2e2a324f473be5f04f9e659133fcf28dcadccdfc55fc
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: Sharique
  approved_at: '2026-10-06'
  approved_version: 1
---

# Rule: genehmigung_orphans (mastr)

**Expression:** 1871/37722 GenMastrNummer keys (5.0%) resolve to no live unit -- LEFT join + unmatched flag; treat the rate as a data-quality signal

**Severity:** warn
