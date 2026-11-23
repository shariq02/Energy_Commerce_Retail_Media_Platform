# Model card: weather_imputation.reconstruction

_model `gbt_cross_variable_lightgbm`, dataset `weather_imputation`, frozen version 12, primary metric `skill_mae_mean` (higher is better). Generated: 2026-11-23T13:07Z_

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
- held-out read: partition test, mask_events 15000, of_mask_events 15000, masked_steps 297986

## Training

- model family: gbt_cross_variable_lightgbm; MLflow run `9006572b91be404a8ff490b20f919b68`
- parameters: {'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_cross_variable_lightgbm held-out | gbt_cross_variable_lightgbm validation | causal_baseline held-out |
|---|---|---|---|
| primary_change_relative | 0.03184 |  |  |
| skill_mae_mean | 0.4241 | 0.438 | 0 |
| skill_primary | 0.4241 |  |  |
| skill_primary_ci_high | 0.4381 |  |  |
| skill_primary_ci_low | 0.4122 |  |  |

## Segments

| segment | skill | decision | detail |
|---|---|---|---|
| air_temperature | 0.9885 | approved_with_conditions | no segment interval |
| cloud_cover | 0.1121 | approved_with_conditions | no segment interval |
| dew_point_temperature | 0.989 | approved_with_conditions | no segment interval |
| pressure_sea_level | 0.2437 | approved_with_conditions | no segment interval |
| pressure_station | 0.09043 | approved_with_conditions | no segment interval |
| relative_humidity | 0.5623 | approved_with_conditions | no segment interval |
| soil_temperature | 0.6985 | approved_with_conditions | no segment interval |
| visibility | 0.3996 | approved_with_conditions | no segment interval |
| wind_direction | -0.06888 | not_approved | negative skill |
| wind_speed | 0.2254 | approved_with_conditions | no segment interval |

## Flags

- none

## Conditions and known limits

- segments not approved: wind_direction
- segments with positive skill but no uncertainty interval, conditional until an interval is recorded: air_temperature (0.988), cloud_cover (0.112), dew_point_temperature (0.989), pressure_sea_level (0.244), pressure_station (0.0904), relative_humidity (0.562), soil_temperature (0.699), visibility (0.4), wind_speed (0.225)

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `causal_baseline`: validation 0, held-out 0 on `skill_mae_mean`
