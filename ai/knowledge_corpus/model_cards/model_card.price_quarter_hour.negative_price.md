---
unit_id: model_card.price_quarter_hour.negative_price
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/price_quarter_hour.negative_price.md
  sha256: 6b8ef16b8bad172ab7a5ba6825dced3714a85e432e442505d319603216065740
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: 7c658ad16a225613bf88fecee8ce3e0212051ce4cdb8b444f9f0b76a4b28caef
---

# Model card: price_quarter_hour.negative_price

_model `logistic`, primary metric `pr_auc` (higher is better)_

## Decision
- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: R10

## Purpose and task
- paradigm: supervised; task type: classification
- target: `target_is_negative_price`
- baseline fallback: `base_rate`

## Intended use
- estimates the probability that a quarter-hour has a negative electricity price
- target `target_is_negative_price`, dataset `price_quarter_hour`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- pr_auc: logistic held-out 0.7205; logistic validation 0.6168; base_rate held-out 0.07381
- skill_primary: logistic held-out 0.6467
- skill_primary_ci_high: logistic held-out 0.735
- skill_primary_ci_low: logistic held-out 0.4814

## Flags
- none

## Conditions and known limits
- none

## Disclosures
- the held-out partition was read in at least 6 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `base_rate`: validation 0.04252, held-out 0.07381 on `pr_auc`
