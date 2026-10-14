---
unit_id: rule.mastr.netz_fanout_risk
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
  approved_content_hash: 79eaae65d51fa4c09d4463ca29718e63883c6a3e4184cfe51569cd9fa4715414
---

# Rule: netz_fanout_risk (mastr)

**Expression:** netzanschlusspunkte.NetzMaStRNummer -> netze has max fan-out 630081 -- a naive join explodes; aggregate or scope first

**Severity:** block
