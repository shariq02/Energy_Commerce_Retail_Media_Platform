# Model card: bias.bias

_model `gbt_lightgbm`, dataset `bias`, frozen version 0, primary metric `mae` (lower is better). Generated: 2026-10-03T23:11Z_

## Decision

- decision: `approved_with_conditions`
- restriction: `none`
- recommended by the rules: `approved_with_conditions`
- overridden by the owner: no
- decided by: owner
- rules applied: R07

## Purpose and task

- paradigm: supervised; task type: regression
- target: `target_bias_mwh`
- baseline fallback: `zero_correction`

## Intended use

- estimates `target_bias_mwh` for the entities and period of dataset `bias`
- comparison and analysis with the stated conditions below

## Out of scope

- use on entities, periods or ecosystems outside the frozen dataset
- operational use of a model restricted to offline or diagnostic use
- registration, versioning and serving are separate steps

## Data

- frame: 3049 rows, columns 91, rows by partition {'test': 3049}
- held-out read: partition test, frozen_delta_version 0, rows 3049, time_min 2025-01-01 00:00:00, time_max 2026-09-03 00:00:00, groups 611
- frame: 1819 rows, columns 91, rows by partition {'validation': 1819}

## Training

- model family: gbt_lightgbm; MLflow run `c41cb25a0b3343969578f27cc0b55883`
- parameters: {'row_fraction': 1.0, 'n_estimators': 300, 'learning_rate': 0.05, 'num_leaves': 31}
- library versions: cloudpickle 3.0.0, lifelines 0.30.3, lightgbm 4.7.0, psutil 5.9.0, sklearn 1.6.1, sksurv 0.25.0, torch 2.14.1+cpu, xgboost 3.4.1
- processor type: not recorded for the training and evaluation runs

## Metrics

| metric | gbt_lightgbm held-out | gbt_lightgbm validation | zero_correction held-out |
|---|---|---|---|
| bootstrap_blocks | 21 |  |  |
| bootstrap_resamples_valid | 200 |  |  |
| bootstrap_rows_used | 3049 |  |  |
| mae | 1.625e+04 | 1.533e+04 | 1.025e+05 |
| mean_error | -1827 | -725.7 | 8.691e+04 |
| mean_error_relative | -0.01782 | -0.007749 | 0.848 |
| pred_finite_share | 1 |  | 1 |
| pred_outside_range_share | 0.000328 |  | 0 |
| pred_std | 1.873e+05 |  | 0 |
| pred_std_ratio | 0.9772 | 0.9986 | 0 |
| primary_change_relative | 0.05955 |  | 0.09444 |
| reproduced_validation_value | 1.533e+04 |  | 9.365e+04 |
| reproduction_gap_relative | 0 |  | 0 |
| rmse | 2.379e+04 | 2.288e+04 | 2.105e+05 |
| skill_mae | 0.8415 | 0.8363 |  |
| skill_primary | 0.8415 |  |  |
| skill_primary_ci_high | 0.8523 |  |  |
| skill_primary_ci_low | 0.8292 |  |  |
| skill_rmse | 0.8869 | 0.8835 |  |

## Segments

| segment | skill | decision | detail |
|---|---|---|---|
| offshore_wind | -0.1053 | not_approved | negative skill |
| onshore_wind | 0.03132 | approved_with_conditions | no segment interval |
| other | 0.9718 | approved_with_conditions | no segment interval |
| photovoltaic | 0.02954 | approved_with_conditions | no segment interval |
| wind_and_photovoltaic | 0.03343 | approved_with_conditions | no segment interval |

## Flags

- none

## Conditions and known limits

- segments not approved: offshore_wind

## Disclosures

- none

## Revalidation trigger

- a new frozen dataset version, or error on newer data more than 20% above the validation error

## Baseline fallback

- `zero_correction`: validation 9.365e+04, held-out 1.025e+05 on `mae`
