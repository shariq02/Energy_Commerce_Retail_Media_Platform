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
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 96730b55d91035c60391e6d5afc596e1e2edd52147f752ae3857f91c3cbd6119
---

# Rule: milestone_vs_registration_ordering (mastr)

**Expression:** commissioning / effective dates may legitimately precede Registrierungsdatum (migrated + backdated records); a small fraction post-date it -- flag, do not delete

**Severity:** warn
