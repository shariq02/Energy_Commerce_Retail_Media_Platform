# Model card: honda_forecast.increment

_model `gbt_lightgbm`, dataset `honda_forecast`, frozen version 0, primary metric `mae` (lower is better). Generated: 2026-10-04T18:03Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R07, R11

## Purpose and task

- paradigm: supervised; task type: regression
- target: `target_increment`
- baseline fallback: `persistence`

## Intended use

- estimates the next energy increment of a Honda site channel
- target `target_increment`, dataset `honda_forecast`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 131394 rows, columns 22, rows by partition {'test': 131394}
- held-out read: partition test, frozen_delta_version 0, rows 131394, time_min 2023-01-01 00:00:00, time_max 2023-12-31 00:00:00, groups 365
- frame: 131040 rows, columns 22, rows by partition {'validation': 131040}

## Training

- model family: gbt_lightgbm; MLflow run `b64ee8daf1244c1bb74f2aab87532d51`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 15, 'min_child_samples': 50}
- library versions: lightgbm 4.7.0
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | persistence held-out |
|---|---|---|---|
| mae | 20.47 | 17.97 | 23.28 |
| mean_error | 2.656 | 1.544 | 0.02136 |
| mean_error_relative | 0.06998 | 0.03938 | 0.0005626 |
| primary_change_relative | 0.1394 |  | 0.1468 |
| rmse | 37.82 | 35.3 | 49.99 |
| skill_mae | 0.1208 | 0.1152 |  |
| skill_primary | 0.1208 |  |  |
| skill_primary_ci_high | 0.2058 |  |  |
| skill_primary_ci_low | 0.0336 |  |  |
| skill_rmse | 0.2435 | 0.2329 |  |

## Segments

| segment | skill | decision | detail |
|---|---|---|---|
| combined_heat_and_power | -0.02877 | not_approved | negative skill |
| solar_photovoltaic | 0.04022 | approved_with_conditions | no segment interval |
| total | 0.2168 | approved_with_conditions | no segment interval |

## Flags

- none

## Conditions and known limits

- segments not approved: combined_heat_and_power
- segments with positive skill but no uncertainty interval, conditional until an interval is recorded: solar_photovoltaic (0.0402), total (0.217)

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `persistence`: validation 20.3, held-out 23.28 on `mae`
