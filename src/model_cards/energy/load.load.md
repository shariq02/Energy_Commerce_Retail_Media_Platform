# Model card: load.load

_model `gbt_sklearn`, dataset `load`, frozen version 0, primary metric `mae` (lower is better). Generated: 2026-10-04T18:03Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: none

## Purpose and task

- paradigm: supervised; task type: regression
- target: `target_value_mwh`
- baseline fallback: `seasonal_mean`

## Intended use

- estimates the electricity load in MWh
- target `target_value_mwh`, dataset `load`

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 3666 rows, columns 104, rows by partition {'test': 3666}
- held-out read: partition test, frozen_delta_version 0, rows 3666, time_min 2025-01-01 00:00:00, time_max 2026-09-03 00:00:00, groups 611
- frame: 2184 rows, columns 104, rows by partition {'validation': 2184}

## Training

- model family: gbt_sklearn; MLflow run `e45bd9d3d2c244ed80b585739d3283e5`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: sklearn 1.6.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_sklearn held-out | gbt_sklearn validation | seasonal_mean held-out |
|---|---|---|---|
| mae | 2.684e+04 | 2.486e+04 | 6.419e+04 |
| mean_error | 3291 | -88.19 | 4.46e+04 |
| mean_error_relative | 0.006061 | -0.0001608 | 0.08214 |
| primary_change_relative | 0.07957 |  | 0.126 |
| rmse | 4.455e+04 | 4.317e+04 | 1.252e+05 |
| skill_mae | 0.5819 | 0.5639 |  |
| skill_primary | 0.5819 |  |  |
| skill_primary_ci_high | 0.6357 |  |  |
| skill_primary_ci_low | 0.5374 |  |  |
| skill_rmse | 0.6441 | 0.6082 |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `seasonal_mean`: validation 5.7e+04, held-out 6.419e+04 on `mae`
