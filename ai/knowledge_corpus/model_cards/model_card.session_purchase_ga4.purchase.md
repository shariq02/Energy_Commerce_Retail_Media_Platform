---
unit_id: model_card.session_purchase_ga4.purchase
kind: model_card
ecosystem: commerce
source:
- path: src/model_cards/commerce/session_purchase_ga4.purchase.md
  sha256: 46ffbe337b9395c11a537dd30baf99d76d6d7a8b15ced0482e64e80b08d4520a
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: b1af3d596eefd356fd3f08151ab8abb29864d6210d7d9767acfca10f3fa35b4c
---

# Model card: session_purchase_ga4.purchase

_model `gbt_sklearn`, primary metric `pr_auc` (higher is better)_

## Decision
- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task
- paradigm: supervised; task type: classification
- target: `target_purchase_after_prefix`
- baseline fallback: `base_rate`

## Intended use
- estimates the probability that a GA4 session ends in a purchase, from the first events of the session
- target `target_purchase_after_prefix`, dataset `session_purchase_ga4`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- pr_auc: gbt_sklearn held-out 0.4584; gbt_sklearn validation 0.3957; base_rate held-out 0.2551
- skill_primary: gbt_sklearn held-out 0.2033
- skill_primary_ci_high: gbt_sklearn held-out 0.2341
- skill_primary_ci_low: gbt_sklearn held-out 0.1731

## Flags
- none

## Conditions and known limits
- none

## Disclosures
- the held-out partition was read in at least 3 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `base_rate`: validation 0.2376, held-out 0.2551 on `pr_auc`
