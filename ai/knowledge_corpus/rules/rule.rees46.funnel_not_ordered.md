---
unit_id: rule.rees46.funnel_not_ordered
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

# Rule: funnel_not_ordered (rees46)

**Expression:** in-session sequence is not strictly view->cart->purchase (539567 purchases with no prior cart; 29848 carts with no prior view) -- do not assume order

**Severity:** warn
