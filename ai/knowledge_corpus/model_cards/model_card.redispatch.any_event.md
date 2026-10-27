---
unit_id: model_card.redispatch.any_event
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/redispatch.any_event.md
  sha256: f2cdcfc643aaafcbd8281165b792f2b7cf30cab209d2e1a058312de603799600
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: 58c2c396e2af0ea828c67cb9be6e01dc0b8da88f8823491e86ba07fc2710ce06
---

# Model card: redispatch.any_event

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
- target: `target_any_event`
- baseline fallback: `base_rate`

## Intended use
- estimates the probability that a redispatch event occurs
- target `target_any_event`, dataset `redispatch`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- pr_auc: logistic held-out 0.8976; logistic validation 0.8828; base_rate held-out 0.7008
- skill_primary: logistic held-out 0.1968
- skill_primary_ci_high: logistic held-out 0.2322
- skill_primary_ci_low: logistic held-out 0.1526

## Flags
- none

## Conditions and known limits
- none

## Disclosures
- the held-out partition was read in at least 5 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `base_rate`: validation 0.6749, held-out 0.7008 on `pr_auc`
