---
unit_id: model_card.price_quarter_hour.price
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/price_quarter_hour.price.md
  sha256: 566fd6a4932bdeab9e9e3488ca722e40225c8952f9718cb9a4c9dc10ebb524bd
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: 587a9894b346ed0e41d8197c6219b083932f7dcf1fb70ecf9adf3c3a3356776f
---

# Model card: price_quarter_hour.price

_model `quantile_gbt_lightgbm`, primary metric `mae` (lower is better)_

## Decision
- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R02, R04, R08, R10

## Purpose and task
- paradigm: supervised; task type: regression_price
- target: `target_price_eur_per_mwh`
- baseline fallback: `persistence`

## Intended use
- estimates the quarter-hour electricity price in EUR per MWh
- target `target_price_eur_per_mwh`, dataset `price_quarter_hour`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- mae: quantile_gbt_lightgbm held-out 29.45; quantile_gbt_lightgbm validation 17.74; persistence held-out 37.3
- skill_primary: quantile_gbt_lightgbm held-out 0.2107
- skill_primary_ci_high: quantile_gbt_lightgbm held-out 0.36
- skill_primary_ci_low: quantile_gbt_lightgbm held-out -0.02785

## Flags
- validation-to-test degradation above the threshold: primary metric +66.0% worse than on validation
- skill interval includes zero: interval -0.02785 to 0.36

## Conditions and known limits
- skill interval -0.02785 to 0.36 includes zero
- primary metric +66.0% worse than on validation; revalidate on newer data before relying on it
- 80% interval covers 49.4%, target 80% plus or minus 10%

## Disclosures
- the held-out partition was read in at least 5 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `persistence`: validation 33.15, held-out 37.3 on `mae`
