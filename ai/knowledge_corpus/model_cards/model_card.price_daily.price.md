---
unit_id: model_card.price_daily.price
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/price_daily.price.md
  sha256: ff50189ddddff3467e4c978157ecb641fab9d15e48883f0503238a27e9e203ee
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: eeb58678a5a18de0c0242058209eee2c3c717568e98cc57832a864fce151d6bf
---

# Model card: price_daily.price

_model `quantile_gbt_lightgbm`, primary metric `mae` (lower is better)_

## Decision
- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R04, R08, R10

## Purpose and task
- paradigm: supervised; task type: regression_price
- target: `target_price_eur_per_mwh`
- baseline fallback: `seasonal_naive`

## Intended use
- estimates the daily electricity price in EUR per MWh
- target `target_price_eur_per_mwh`, dataset `price_daily`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- mae: quantile_gbt_lightgbm held-out 15.95; quantile_gbt_lightgbm validation 11.81; seasonal_naive held-out 28.62
- skill_primary: quantile_gbt_lightgbm held-out 0.4429
- skill_primary_ci_high: quantile_gbt_lightgbm held-out 0.5128
- skill_primary_ci_low: quantile_gbt_lightgbm held-out 0.3526

## Flags
- validation-to-test degradation above the threshold: primary metric +35.0% worse than on validation

## Conditions and known limits
- primary metric +35.0% worse than on validation; revalidate on newer data before relying on it
- 80% interval covers 56.8%, target 80% plus or minus 10%

## Disclosures
- the held-out partition was read in at least 5 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `seasonal_naive`: validation 28.04, held-out 28.62 on `mae`
