---
unit_id: rule.mastr.marktakteure_natural_persons
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
  approved_content_hash: b67735a1968749fff03c82677deb51dba4ad98433fe7beb92916421407b6f71d
---

# Rule: marktakteure_natural_persons (mastr)

**Expression:** 95% of mastr_marktakteure are natural persons (Personenart 518) with name/address columns null by design -- absence is privacy suppression, not missing data

**Severity:** info
