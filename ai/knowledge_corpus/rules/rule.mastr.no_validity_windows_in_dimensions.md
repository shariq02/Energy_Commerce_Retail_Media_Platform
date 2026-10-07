---
unit_id: rule.mastr.no_validity_windows_in_dimensions
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
  approved_content_hash: 09dda5e7c4e0dcf0b4d1e1e3b71979b8f9d5b6ca7bae6025c99e2abb5f5f8948
---

# Rule: no_validity_windows_in_dimensions (mastr)

**Expression:** marktakteure / netze / lokationen / netzanschlusspunkte are current-state snapshots with no validity windows -- a point-in-time operator assignment must come from einheiten_aenderung_netzbetreiberzuordnungen, not a static join

**Severity:** warn
