# Model card: load.load

_model `gbt_sklearn`, dataset `load`, frozen version 0, primary metric `mae` (lower is better). Generated: 2026-10-03T23:11Z_

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

- estimates `target_value_mwh` for the entities and period of dataset `load`
- comparison and analysis with the stated conditions below

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
- library versions: cloudpickle 3.0.0, lifelines 0.30.3, lightgbm 4.7.0, psutil 5.9.0, sklearn 1.6.1, sksurv 0.25.0, torch 2.14.1+cpu, xgboost 3.4.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_sklearn held-out | gbt_sklearn validation | seasonal_mean held-out |
|---|---|---|---|
| bootstrap_blocks | 21 |  |  |
| bootstrap_resamples_valid | 200 |  |  |
| bootstrap_rows_used | 3666 |  |  |
| mae | 2.684e+04 | 2.486e+04 | 6.419e+04 |
| mean_error | 3291 | -88.19 | 4.46e+04 |
| mean_error_relative | 0.006061 | -0.0001608 | 0.08214 |
| pred_finite_share | 1 |  | 1 |
| pred_outside_range_share | 0 |  | 0 |
| pred_std | 3.919e+05 |  | 4.089e+05 |
| pred_std_ratio | 0.9901 | 0.9923 | 1.033 |
| primary_change_relative | 0.07957 |  | 0.126 |
| reproduced_validation_value | 2.486e+04 |  | 5.7e+04 |
| reproduction_gap_relative | 0 |  | 0 |
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
