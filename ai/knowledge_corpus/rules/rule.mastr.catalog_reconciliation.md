---
unit_id: rule.mastr.catalog_reconciliation
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

# Rule: catalog_reconciliation (mastr)

**Expression:** every coded column must reconcile against mastr_katalogwerte -- scoped to a category where the column name matches a mastr_katalogkategorien.Name, global value-id membership otherwise. Unknown code = quarantine class.

**Severity:** block
