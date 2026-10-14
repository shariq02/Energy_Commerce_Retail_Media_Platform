---
unit_id: rule.mastr.change_logs_append_only
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
  approved_content_hash: ab6e8b1c64c1137d375413aae9275d13570d5cae19c4902aaa30af04d7f15d00
---

# Rule: change_logs_append_only (mastr)

**Expression:** geloeschte_deaktivierte_* and einheiten_aenderung_netzbetreiberzuordnungen are append-only event logs -> model as Silver change/event facts, never overwrite the current-state unit / actor tables with them

**Severity:** block
