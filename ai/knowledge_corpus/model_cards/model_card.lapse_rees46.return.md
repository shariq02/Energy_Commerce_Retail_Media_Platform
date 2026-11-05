---
unit_id: model_card.lapse_rees46.return
kind: model_card
ecosystem: commerce
source:
- path: src/model_cards/commerce/lapse_rees46.return.md
  sha256: ef215bdc988761c599652afd901ecb6ac0436b55d6de68a7bca2da18532962c3
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: 050e3fb183801f1cb89050d98f33d21206d2dd84999dad085d76dbfbb31b4d8f
---

# Model card: lapse_rees46.return

_model `gbt_lightgbm`, primary metric `pr_auc` (higher is better)_

## Decision
- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task
- paradigm: supervised; task type: classification
- target: `target_returned`
- baseline fallback: `base_rate`

## Intended use
- estimates the probability that a REES46 user returns
- target `target_returned`, dataset `lapse_rees46`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- pr_auc: gbt_lightgbm held-out 0.7321; gbt_lightgbm validation 0.7318; base_rate held-out 0.463
- skill_primary: gbt_lightgbm held-out 0.2692
- skill_primary_ci_high: gbt_lightgbm held-out 0.2717
- skill_primary_ci_low: gbt_lightgbm held-out 0.2642

## Flags
- none

## Conditions and known limits
- none

## Disclosures
- the held-out partition was read in at least 3 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `base_rate`: validation 0.4634, held-out 0.463 on `pr_auc`
