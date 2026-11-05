---
unit_id: model_card.weather_imputation.reconstruction
kind: model_card
ecosystem: energy
source:
- path: src/model_cards/energy/weather_imputation.reconstruction.md
  sha256: 1879567bae7585cd752a1824d3c8e31c3fa1cf0f301798a05ae75adecde0a756
version: 1
last_updated: '2026-10-27'
approval:
  status: approved
  approver: nobody
  approved_at: '2026-10-27'
  approved_version: 1
  approved_content_hash: f71a0678edbcbdc72bb0029f5ca5fed418000fe6818a1e014ff181df3a0e64ac
---

# Model card: weather_imputation.reconstruction

_model `gbt_cross_variable_lightgbm`, primary metric `skill_mae_mean` (higher is better)_

## Decision
- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R07, R10, R11

## Purpose and task
- paradigm: self_supervised; task type: reconstruction
- target: `masked_value`
- baseline fallback: `causal_baseline`

## Intended use
- fills missing weather values, per weather variable
- target `masked_value`, dataset `weather_imputation`

## Out of scope
- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Primary metric
- skill_mae_mean: gbt_cross_variable_lightgbm held-out 0.4429; gbt_cross_variable_lightgbm validation 0.4678; causal_baseline held-out 0
- skill_primary: gbt_cross_variable_lightgbm held-out 0.4429
- skill_primary_ci_high: gbt_cross_variable_lightgbm held-out 0.463
- skill_primary_ci_low: gbt_cross_variable_lightgbm held-out 0.4215

## Flags
- none

## Conditions and known limits
- segments not approved: wind_direction
- segments with positive skill but no uncertainty interval, conditional until an interval is recorded: air_temperature (0.94), cloud_cover (0.0772), dew_point_temperature (0.932), pressure_sea_level (0.313), pressure_station (0.000192), relative_humidity (0.917), soil_temperature (0.718), visibility (0.41), wind_speed (0.199)

## Disclosures
- the held-out partition was read in at least 6 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger
- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback
- `causal_baseline`: validation 0, held-out 0 on `skill_mae_mean`
