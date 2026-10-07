---
unit_id: rule.mastr.dates_europe_berlin
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
  approved_content_hash: b878b204b2783823a520a8318eada7e37e206364f98c3df61ae866e9517d7f19
---

# Rule: dates_europe_berlin (mastr)

**Expression:** all date columns are Europe/Berlin wall-clock -- convert to UTC on a documented rule; parse with an explicit multi-format list, quarantine failures and sentinel dates (Datum = 1900-01-02, BiogasDatumInanspruchnahmeFlexiPraemie pre-2007)

**Severity:** block
