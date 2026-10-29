# Model card: weather_imputation.reconstruction

_model `gbt_cross_variable_lightgbm`, dataset `weather_imputation`, frozen version 2, primary metric `skill_mae_mean` (higher is better). Generated: 2026-10-26T17:52Z_

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

## Data

- frame: 410007 rows, columns 13, rows by partition {'test': 410007}
- held-out read: partition test, mask_events 5000, of_mask_events 15000, masked_steps 98749

## Training

- model family: gbt_cross_variable_lightgbm; MLflow run `b9e21562a21c4f5084ee08716b5789a9`
- parameters: {'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.6.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_cross_variable_lightgbm held-out | gbt_cross_variable_lightgbm validation | causal_baseline held-out |
|---|---|---|---|
| primary_change_relative | 0.05322 |  |  |
| skill_mae_mean | 0.4429 | 0.4678 | 0 |
| skill_primary | 0.4429 |  |  |
| skill_primary_ci_high | 0.463 |  |  |
| skill_primary_ci_low | 0.4215 |  |  |

## Segments

| segment | skill | decision | detail |
|---|---|---|---|
| air_temperature | 0.9395 | approved_with_conditions | no segment interval |
| cloud_cover | 0.07717 | approved_with_conditions | no segment interval |
| dew_point_temperature | 0.9318 | approved_with_conditions | no segment interval |
| pressure_sea_level | 0.3127 | approved_with_conditions | no segment interval |
| pressure_station | 0.0001922 | approved_with_conditions | no segment interval |
| relative_humidity | 0.9171 | approved_with_conditions | no segment interval |
| soil_temperature | 0.718 | approved_with_conditions | no segment interval |
| visibility | 0.4102 | approved_with_conditions | no segment interval |
| wind_direction | -0.07664 | not_approved | negative skill |
| wind_speed | 0.1986 | approved_with_conditions | no segment interval |

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
