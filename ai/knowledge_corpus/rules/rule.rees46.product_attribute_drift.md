---
unit_id: rule.rees46.product_attribute_drift
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

# Rule: product_attribute_drift (rees46)

**Expression:** 22 products have >1 category_id; 277 have >1 brand -- product attributes are not stable

**Severity:** warn
