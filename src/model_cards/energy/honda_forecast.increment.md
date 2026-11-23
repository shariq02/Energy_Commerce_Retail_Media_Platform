# Model card: honda_forecast.increment

_model `gbt_sklearn`, dataset `honda_forecast`, frozen version 15, primary metric `mae` (lower is better). Generated: 2026-11-23T13:07Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R02, R07, R10, R11

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

- frame: 131394 rows, columns 23, rows by partition {'test': 131394}
- held-out read: partition test, frozen_delta_version 15, rows 131394, time_min 2023-01-01 00:00:00, time_max 2023-12-31 00:00:00, groups 365
- frame: 131040 rows, columns 23, rows by partition {'validation': 131040}

## Training

- model family: gbt_sklearn; MLflow run `d0dfc3c26e3441f58589d4e41c26c1e0`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 15, 'min_child_samples': 50}
- library versions: sklearn 1.7.2
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_sklearn held-out | gbt_sklearn validation | persistence held-out |
|---|---|---|---|
| mae | 20.41 | 17.93 | 23.28 |
| mean_error | 2.606 | 1.544 | 0.02136 |
| mean_error_relative | 0.06866 | 0.03939 | 0.0005626 |
| primary_change_relative | 0.1387 |  | 0.1468 |
| rmse | 37.8 | 35.34 | 49.99 |
| skill_mae | 0.1233 | 0.1171 |  |
| skill_primary | 0.1233 |  |  |
| skill_primary_ci_high | 0.1419 |  |  |
| skill_primary_ci_low | -0.01733 |  |  |
| skill_rmse | 0.2438 | 0.232 |  |

## Segments

| segment | skill | decision | detail |
|---|---|---|---|
| electricity_combined_heat_and_power | -0.0203 | not_approved | negative skill |
| electricity_solar_photovoltaic | 0.04193 | approved_with_conditions | no segment interval |
| electricity_total | 0.2167 | approved_with_conditions | no segment interval |

## Flags

- skill interval includes zero: interval -0.01733 to 0.1419

## Conditions and known limits

- skill interval -0.01733 to 0.1419 includes zero
- segments not approved: electricity_combined_heat_and_power
- segments with positive skill but no uncertainty interval, conditional until an interval is recorded: electricity_solar_photovoltaic (0.0419), electricity_total (0.217)

## Disclosures

- the held-out partition was read in at least 2 evaluation runs; the exact number of held-out reads is not recoverable; scores unchanged

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `persistence`: validation 20.3, held-out 23.28 on `mae`
