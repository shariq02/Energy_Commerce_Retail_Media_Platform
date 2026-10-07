---
unit_id: rule.mastr.partial_scope_orphans_expected
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

# Rule: partial_scope_orphans_expected (mastr)

**Expression:** high orphan rates on ertuechtigungen.EegMastrNummer (55%) and einheiten_aenderung_netzbetreiberzuordnungen.EinheitMastrNummer (96%) are EXPECTED -- solar / storage object types are deferred from staging in this snapshot. Do NOT treat these as key mismatches.

**Severity:** info
