---
unit_id: rule.mastr.unit_key_unique
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

# Rule: unit_key_unique (mastr)

**Expression:** EinheitMastrNummer is unique in every einheiten_* table; EegMaStRNummer / KwkMastrNummer / GenMastrNummer unique in their own table (ertuechtigungen.Id unique, EegMastrNummer 87%)

**Severity:** block
