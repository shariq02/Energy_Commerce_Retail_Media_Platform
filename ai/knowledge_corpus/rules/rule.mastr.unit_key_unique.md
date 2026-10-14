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
  approver: mr.nobody
  approved_at: '2026-10-07'
  approved_version: 1
  approved_content_hash: 3bceb1e42649f85d95f9e7f92bb6815075172776bd547c52465611ffe61836a1
---

# Rule: unit_key_unique (mastr)

**Expression:** EinheitMastrNummer is unique in every einheiten_* table; EegMaStRNummer / KwkMastrNummer / GenMastrNummer unique in their own table (ertuechtigungen.Id unique, EegMastrNummer 87%)

**Severity:** block
