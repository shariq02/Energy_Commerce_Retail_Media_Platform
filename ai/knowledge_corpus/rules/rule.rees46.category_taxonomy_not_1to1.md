---
unit_id: rule.rees46.category_taxonomy_not_1to1
kind: rule
ecosystem: commerce
source:
- path: src/schemas/contracts/rees46.yml
  sha256: 22149137515ddc6f5a8dfa8c14480b56419bee5ef47a02812b3b49b91d80fad8
version: 1
last_updated: '2026-10-06'
approval:
  status: approved
  approver: Sharique
  approved_at: '2026-10-06'
  approved_version: 1
---

# Rule: category_taxonomy_not_1to1 (rees46)

**Expression:** category_id <-> category_code is not 1:1 (58 category_code values map to >1 category_id)

**Severity:** warn
