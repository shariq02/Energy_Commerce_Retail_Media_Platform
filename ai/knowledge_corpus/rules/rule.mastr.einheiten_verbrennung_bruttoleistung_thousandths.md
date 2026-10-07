---
unit_id: rule.mastr.einheiten_verbrennung_bruttoleistung_thousandths
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

# Rule: einheiten_verbrennung_bruttoleistung_thousandths (mastr)

**Expression:** einheiten_verbrennung.Bruttoleistung observed min 0.001 kW -- plausible micro-generators; keep, do not treat sub-1-kW as a decimal error

**Severity:** info
