# Model card: ccpp.output

_model `gbt_sklearn`, dataset `ccpp`, frozen version 0, primary metric `mae` (lower is better). Generated: 2026-10-03T23:11Z_

## Decision

- decision: `approved`
- restriction: `none`
- recommended by the rules: `approved`
- overridden by the owner: no
- decided by: owner
- rules applied: none

## Purpose and task

- paradigm: supervised; task type: regression
- target: `target_net_output_mw`
- baseline fallback: `train_mean`

## Intended use

- estimates `target_net_output_mw` for the entities and period of dataset `ccpp`
- comparison and analysis with the stated conditions below

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 1447 rows, columns 8, rows by partition {'test': 1447}
- held-out read: partition test, frozen_delta_version 0, rows 1447, groups 1447
- frame: 1369 rows, columns 8, rows by partition {'validation': 1369}

## Training

- model family: gbt_sklearn; MLflow run `1f647868a89c4313ae8b7a4b7cffb41a`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: cloudpickle 3.0.0, lifelines 0.30.3, lightgbm 4.7.0, psutil 5.9.0, sklearn 1.6.1, sksurv 0.25.0, torch 2.14.1+cpu, xgboost 3.4.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_sklearn held-out | gbt_sklearn validation | train_mean held-out |
|---|---|---|---|
| bootstrap_blocks | 1447 |  |  |
| bootstrap_resamples_valid | 200 |  |  |
| bootstrap_rows_used | 1447 |  |  |
| mae | 2.287 | 2.496 | 15.03 |
| mean_error | -0.1176 | -0.07375 | 0.01432 |
| mean_error_relative | -0.0002588 | -0.0001621 | 3.152e-05 |
| pred_finite_share | 1 |  | 1 |
| pred_outside_range_share | 0 |  | 0 |
| pred_std | 16.9 |  | 0 |
| pred_std_ratio | 0.9782 | 0.9714 | 0 |
| primary_change_relative | -0.08374 |  | 0.01593 |
| reproduced_validation_value | 2.496 |  | 14.79 |
| reproduction_gap_relative | 0 |  | 0 |
| rmse | 3.125 | 3.463 | 17.28 |
| skill_mae | 0.8478 | 0.8313 |  |
| skill_primary | 0.8478 |  |  |
| skill_primary_ci_high | 0.8558 |  |  |
| skill_primary_ci_low | 0.8384 |  |  |
| skill_rmse | 0.8192 | 0.7965 |  |

## Flags

- none

## Conditions and known limits

- none

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `train_mean`: validation 14.79, held-out 15.03 on `mae`
