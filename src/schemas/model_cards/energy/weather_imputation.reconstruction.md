# Model card: weather_imputation.reconstruction

_model `gbt_cross_variable_lightgbm`, dataset `weather_imputation`, frozen version 2, primary metric `skill_mae_mean` (higher is better). Generated: 2026-10-03T23:11Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R07, R10

## Purpose and task

- paradigm: self_supervised; task type: reconstruction
- target: `masked_value`
- baseline fallback: `causal_baseline`

## Intended use

- estimates `masked_value` for the entities and period of dataset `weather_imputation`
- comparison and analysis with the stated conditions below

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 410007 rows, columns 13, rows by partition {'test': 410007}
- held-out read: partition test, events 297986

## Training

- model family: gbt_cross_variable_lightgbm; MLflow run `ca0d775acc044272aa3e0e03fc1d9c9a`
- parameters: {'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: cloudpickle 3.0.0, lifelines 0.30.3, lightgbm 4.7.0, psutil 5.9.0, sklearn 1.6.1, sksurv 0.25.0, torch 2.14.1+cpu, xgboost 3.4.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_cross_variable_lightgbm held-out | gbt_cross_variable_lightgbm validation | causal_baseline held-out |
|---|---|---|---|
| bootstrap_blocks | 28 |  |  |
| bootstrap_resamples_valid | 200 |  |  |
| bootstrap_rows_used | 2.98e+05 |  |  |
| primary_change_relative | 0.04501 |  |  |
| skill_mae_mean | 0.4467 | 0.4678 | 0 |
| skill_primary | 0.4467 |  |  |
| skill_primary_ci_high | 0.4609 |  |  |
| skill_primary_ci_low | 0.4358 |  |  |

## Segments

| segment | skill | decision | detail |
|---|---|---|---|
| air_temperature | 0.9316 | approved_with_conditions | no segment interval |
| cloud_cover | 0.1107 | approved_with_conditions | no segment interval |
| dew_point_temperature | 0.934 | approved_with_conditions | no segment interval |
| pressure_sea_level | 0.2395 | approved_with_conditions | no segment interval |
| pressure_station | 0.09155 | approved_with_conditions | no segment interval |
| relative_humidity | 0.9167 | approved_with_conditions | no segment interval |
| soil_temperature | 0.701 | approved_with_conditions | no segment interval |
| visibility | 0.3967 | approved_with_conditions | no segment interval |
| wind_direction | -0.07781 | not_approved | negative skill |
| wind_speed | 0.223 | approved_with_conditions | no segment interval |

## Flags

- none

## Conditions and known limits

- segments not approved: wind_direction

## Disclosures

- held-out partition read 2 times (re-runs); scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `causal_baseline`: validation 0, held-out 0 on `skill_mae_mean`
