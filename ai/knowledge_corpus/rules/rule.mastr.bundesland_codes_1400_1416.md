---
unit_id: rule.mastr.bundesland_codes_1400_1416
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

# Rule: bundesland_codes_1400_1416 (mastr)

**Expression:** Bundesland codes 1400-1416 appear on einheiten_* rows but are not in katalogwerte category 'Land' -- likely Regierungsbezirk-level codes; document, do not drop

**Severity:** warn
