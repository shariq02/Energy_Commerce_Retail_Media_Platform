---
unit_id: model_card.session_purchase_rees46.purchase
kind: model_card
ecosystem: commerce
source:
- path: src/model_cards/commerce/session_purchase_rees46.purchase.md
  sha256: 0a8e17628c31c01a6a0f466b9456020783a6126c81aa5b86a2ad2faf7429b7cd
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: 2320b8395b6d20b98c99803536b01fc12938d707f5715eca531881604907156d
---

# Model card: session_purchase_rees46.purchase

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
- estimates the probability that a REES46 session ends in a purchase, from the first events of the session
- target `target_purchase_after_prefix`, dataset `session_purchase_rees46`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- pr_auc: gbt_sklearn held-out 0.4398; gbt_sklearn validation 0.4378; base_rate held-out 0.08171
- skill_primary: gbt_sklearn held-out 0.358
- skill_primary_ci_high: gbt_sklearn held-out 0.3776
- skill_primary_ci_low: gbt_sklearn held-out 0.3446

## Flags
- none

## Conditions and known limits
- none

## Disclosures
- the held-out partition was read in at least 3 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `base_rate`: validation 0.08131, held-out 0.08171 on `pr_auc`
