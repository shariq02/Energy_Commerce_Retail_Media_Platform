---
unit_id: model_card.zone_generation.generation
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/zone_generation.generation.md
  sha256: 4276b972516ae373b53293dc1b161f6a8ff1767576371d12a91160992db331e0
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: a1be059595c6f4f40856e0cb689a36b55cddf8e17583b05e2fbef4c9721fa1c2
---

# Model card: zone_generation.generation

_model `gbt_lightgbm`, primary metric `mae` (lower is better)_

## Decision
- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R04, R10

## Purpose and task
- paradigm: supervised; task type: regression
- target: `target_generation_mwh`
- baseline fallback: `capacity_factor`

## Intended use
- estimates the generation in MWh of a control zone
- target `target_generation_mwh`, dataset `zone_generation`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- mae: gbt_lightgbm held-out 1.617e+04; gbt_lightgbm validation 1.093e+04; capacity_factor held-out 3.144e+04
- skill_primary: gbt_lightgbm held-out 0.4857
- skill_primary_ci_high: gbt_lightgbm held-out 0.5632
- skill_primary_ci_low: gbt_lightgbm held-out 0.3815

## Flags
- validation-to-test degradation above the threshold: primary metric +48.0% worse than on validation

## Conditions and known limits
- primary metric +48.0% worse than on validation; revalidate on newer data before relying on it

## Disclosures
- the held-out partition was read in at least 4 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `capacity_factor`: validation 2.571e+04, held-out 3.144e+04 on `mae`
