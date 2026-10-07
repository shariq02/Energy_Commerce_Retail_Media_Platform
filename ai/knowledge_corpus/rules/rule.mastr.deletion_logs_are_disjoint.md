---
unit_id: rule.mastr.deletion_logs_are_disjoint
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

# Rule: deletion_logs_are_disjoint (mastr)

**Expression:** geloeschte_deaktivierte_einheiten / _marktakteure are DISJOINT from the live tables by design (match rate 0.0) -- join only to confirm an entity has departed; they are the survivorship record the live tables omit

**Severity:** block
