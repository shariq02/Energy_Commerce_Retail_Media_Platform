---
unit_id: rule.smard.additive_identity_not_clean
kind: rule
ecosystem: energy
source:
- path: src/schemas/contracts/smard.yml
  sha256: 3a017f241e155f2beda0fdc12e732adf9711b612c68bebb88e2fda19030e4b70
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: Sharique
  approved_at: '2026-10-06'
  approved_version: 1
---

# Rule: additive_identity_not_clean (smard)

**Expression:** the published generation / load series are NOT a clean additive set -- residual_load cannot be reconstructed from the other series without an extra term / vintage reconciliation

**Severity:** warn
