---
unit_id: rule.mastr.milestone_vs_registration_ordering
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

# Rule: milestone_vs_registration_ordering (mastr)

**Expression:** commissioning / effective dates may legitimately precede Registrierungsdatum (migrated + backdated records); a small fraction post-date it -- flag, do not delete

**Severity:** warn
